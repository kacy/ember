//! Append-only file for recording mutations.
//!
//! Each shard writes its own AOF file (`shard-{id}.aof`). Records are
//! written after successful mutations. The binary format uses a simple
//! tag + payload + CRC32 structure for each record.
//!
//! File layout:
//! ```text
//! [EAOF magic: 4B][version: 1B]
//! [record]*
//! ```
//!
//! Record layout:
//! ```text
//! [tag: 1B][payload...][crc32: 4B]
//! ```
//! The CRC32 covers the tag + payload bytes.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};

use crate::format::{self, FormatError};

mod record;

pub use record::AofRecord;

/// Configurable fsync policy for the AOF writer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every write. safest, slowest.
    Always,
    /// fsync once per second. the shard tick drives this.
    #[default]
    EverySec,
    /// let the OS decide when to flush. fastest, least durable.
    No,
}

/// Buffered writer for appending AOF records to a file.
pub struct AofWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    /// The existing file is in a different format than this writer writes.
    /// See [`AofWriter::needs_rewrite`].
    needs_rewrite: bool,
    #[cfg(feature = "encryption")]
    encryption: Option<Encryption>,
}

/// The master key, kept to start new files, and the cipher of the current one.
#[cfg(feature = "encryption")]
struct Encryption {
    key: crate::encryption::EncryptionKey,
    cipher: crate::encryption::FileCipher,
}

impl AofWriter {
    /// Opens (or creates) a plaintext AOF file. A new file gets a header;
    /// an existing one is appended to.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, FormatError> {
        Self::open_versioned(
            path.into(),
            format::FORMAT_VERSION,
            #[cfg(feature = "encryption")]
            None,
        )
    }

    /// Opens (or creates) an encrypted AOF file using AES-256-GCM. A new
    /// file gets a v4 header.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(
        path: impl Into<PathBuf>,
        key: crate::encryption::EncryptionKey,
    ) -> Result<Self, FormatError> {
        Self::open_versioned(path.into(), format::FORMAT_VERSION_ENCRYPTED, Some(key))
    }

    fn open_versioned(
        path: PathBuf,
        version: u8,
        #[cfg(feature = "encryption")] key: Option<crate::encryption::EncryptionKey>,
    ) -> Result<Self, FormatError> {
        let existing = fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(false);
        let mut writer = BufWriter::new(open_persistence_file(&path)?);

        // the file's current version, and for an encrypted file the
        // cipher to keep appending with. a header that cannot be read
        // counts as a different format.
        #[cfg(feature = "encryption")]
        let (current_version, cipher) = if existing {
            match read_existing_header(&path, key.as_ref()) {
                Ok((v, cipher)) => (Some(v), cipher),
                Err(_) => (None, None),
            }
        } else {
            format::write_header_versioned(&mut writer, format::AOF_MAGIC, version)?;
            let cipher = key
                .as_ref()
                .map(|k| k.write_new_file_salt(&mut writer))
                .transpose()?;
            writer.flush()?;
            (Some(version), cipher)
        };
        #[cfg(not(feature = "encryption"))]
        let current_version = if existing {
            File::open(&path)
                .map_err(FormatError::from)
                .and_then(|f| format::read_header(&mut BufReader::new(f), format::AOF_MAGIC))
                .ok()
        } else {
            format::write_header_versioned(&mut writer, format::AOF_MAGIC, version)?;
            writer.flush()?;
            Some(version)
        };

        // a file that needs a rewrite is truncated before anything is
        // written, which picks a new salt; until then any cipher will do
        #[cfg(feature = "encryption")]
        let encryption = key.map(|key| {
            let cipher = cipher.unwrap_or_else(|| key.legacy_cipher());
            Encryption { key, cipher }
        });

        Ok(Self {
            writer,
            path,
            needs_rewrite: current_version != Some(version),
            #[cfg(feature = "encryption")]
            encryption,
        })
    }

    /// Whether the existing file is in a different format than this writer
    /// writes, such as after encryption was turned on or off. Appending
    /// would mix formats in one file and make it unreadable, so the caller
    /// must snapshot the data and [`truncate`](Self::truncate) first.
    pub fn needs_rewrite(&self) -> bool {
        self.needs_rewrite
    }

    /// Appends a record to the AOF.
    pub fn write_record(&mut self, record: &AofRecord) -> Result<(), FormatError> {
        write_encoded(
            &mut self.writer,
            record,
            #[cfg(feature = "encryption")]
            self.encryption.as_ref().map(|enc| &enc.cipher),
        )
    }

    /// Flushes the internal buffer to the OS.
    pub fn flush(&mut self) -> Result<(), FormatError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flushes and fsyncs the file to disk.
    pub fn sync(&mut self) -> Result<(), FormatError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Returns the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Starts the AOF over after a snapshot, with just a header and a
    /// checkpoint naming the snapshot by its footer CRC.
    ///
    /// Uses write-to-temp-then-rename for crash safety: the old AOF
    /// remains intact until the new file is fully synced and atomically
    /// renamed into place. If this fails, the old file stays in use.
    pub fn truncate(&mut self, snapshot_crc: u32) -> Result<(), FormatError> {
        // flush the old writer so no data is in the BufWriter
        self.writer.flush()?;

        // write a fresh header to a temp file next to the real AOF
        let tmp_path = self.path.with_extension("aof.tmp");
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let tmp_file = opts.open(&tmp_path)?;
        let mut tmp_writer = BufWriter::new(tmp_file);

        // the new file's cipher. the old one stays in use until the rename
        #[cfg(feature = "encryption")]
        let cipher = match self.encryption {
            Some(ref enc) => {
                format::write_header_versioned(
                    &mut tmp_writer,
                    format::AOF_MAGIC,
                    format::FORMAT_VERSION_ENCRYPTED,
                )?;
                Some(enc.key.write_new_file_salt(&mut tmp_writer)?)
            }
            None => {
                format::write_header(&mut tmp_writer, format::AOF_MAGIC)?;
                None
            }
        };
        #[cfg(not(feature = "encryption"))]
        format::write_header(&mut tmp_writer, format::AOF_MAGIC)?;

        write_encoded(
            &mut tmp_writer,
            &AofRecord::Checkpoint { snapshot_crc },
            #[cfg(feature = "encryption")]
            cipher.as_ref(),
        )?;
        tmp_writer.flush()?;
        tmp_writer.get_ref().sync_all()?;

        // atomic rename: old AOF is replaced only after new file is durable
        std::fs::rename(&tmp_path, &self.path)?;

        // reopen for appending
        let file = OpenOptions::new().append(true).open(&self.path)?;
        self.writer = BufWriter::new(file);
        #[cfg(feature = "encryption")]
        if let (Some(enc), Some(cipher)) = (self.encryption.as_mut(), cipher) {
            enc.cipher = cipher;
        }
        self.needs_rewrite = false;
        Ok(())
    }
}

/// Writes one record. With a cipher: `[nonce: 12B][len: 4B][ciphertext]`.
/// Otherwise the v2 format: `[tag+payload][crc32: 4B]`.
fn write_encoded(
    w: &mut impl Write,
    record: &AofRecord,
    #[cfg(feature = "encryption")] cipher: Option<&crate::encryption::FileCipher>,
) -> Result<(), FormatError> {
    let payload = record.to_bytes()?;

    #[cfg(feature = "encryption")]
    if let Some(cipher) = cipher {
        let (nonce, ciphertext) = cipher.encrypt(&payload)?;
        w.write_all(&nonce)?;
        format::write_len(w, ciphertext.len())?;
        w.write_all(&ciphertext)?;
        return Ok(());
    }

    let checksum = format::crc32(&payload);
    w.write_all(&payload)?;
    format::write_u32(w, checksum)?;
    Ok(())
}

/// Reads the header of an existing AOF: its version and, when it is
/// encrypted and `key` is given, the cipher for its records.
#[cfg(feature = "encryption")]
fn read_existing_header(
    path: &Path,
    key: Option<&crate::encryption::EncryptionKey>,
) -> Result<(u8, Option<crate::encryption::FileCipher>), FormatError> {
    let mut reader = BufReader::new(File::open(path)?);
    let version = format::read_header(&mut reader, format::AOF_MAGIC)?;
    let cipher = match key {
        Some(key) if format::is_encrypted(version) => {
            Some(key.read_file_cipher(version, &mut reader)?)
        }
        _ => None,
    };
    Ok((version, cipher))
}

/// Reader for iterating over AOF records.
pub struct AofReader {
    reader: BufReader<File>,
    /// Format version from the file header. v2 = plaintext, v3/v4 = encrypted.
    version: u8,
    /// End offset of the last complete record read. See [`AofReader::valid_len`].
    valid_len: u64,
    /// Set for an encrypted file.
    #[cfg(feature = "encryption")]
    cipher: Option<crate::encryption::FileCipher>,
}

impl fmt::Debug for AofReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AofReader")
            .field("version", &self.version)
            .finish()
    }
}

impl AofReader {
    /// Opens an AOF file and validates the header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        let reader = Self::open_with_key(
            path,
            #[cfg(feature = "encryption")]
            None,
        )?;
        if format::is_encrypted(reader.version) {
            return Err(FormatError::EncryptionRequired);
        }
        Ok(reader)
    }

    /// Opens an AOF file with an encryption key for decrypting v3/v4 records.
    ///
    /// Also handles v2 (plaintext) files — the key is simply unused,
    /// allowing transparent migration.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(
        path: impl AsRef<Path>,
        key: crate::encryption::EncryptionKey,
    ) -> Result<Self, FormatError> {
        Self::open_with_key(path, Some(key))
    }

    fn open_with_key(
        path: impl AsRef<Path>,
        #[cfg(feature = "encryption")] encryption_key: Option<crate::encryption::EncryptionKey>,
    ) -> Result<Self, FormatError> {
        let mut reader = BufReader::new(File::open(path.as_ref())?);
        let version = format::read_header(&mut reader, format::AOF_MAGIC)?;
        #[cfg(feature = "encryption")]
        let cipher = match encryption_key {
            Some(key) if format::is_encrypted(version) => {
                Some(key.read_file_cipher(version, &mut reader)?)
            }
            _ => None,
        };
        let valid_len = reader.stream_position()?;
        Ok(Self {
            reader,
            version,
            valid_len,
            #[cfg(feature = "encryption")]
            cipher,
        })
    }

    /// Reads the next record from the AOF.
    ///
    /// Returns `Ok(None)` at end of file. A record cut short by a crash
    /// mid-write also returns `Ok(None)`; [`valid_len`](Self::valid_len)
    /// then tells the caller where the complete records end.
    pub fn read_record(&mut self) -> Result<Option<AofRecord>, FormatError> {
        #[cfg(feature = "encryption")]
        let result = if format::is_encrypted(self.version) {
            self.read_encrypted_record()
        } else {
            self.read_v2_record()
        };
        #[cfg(not(feature = "encryption"))]
        let result = self.read_v2_record();

        match result {
            Ok(record) => {
                self.valid_len = self.reader.stream_position()?;
                Ok(Some(record))
            }
            Err(FormatError::UnexpectedEof) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Byte length of the file prefix that holds the header and every
    /// record read so far. Anything past it after `read_record` returns
    /// `None` is a partial record left by a crash.
    pub fn valid_len(&self) -> u64 {
        self.valid_len
    }

    /// Reads a v2 (plaintext) record: tag + payload + crc32.
    fn read_v2_record(&mut self) -> Result<AofRecord, FormatError> {
        let mut crc_reader = format::CrcReader::new(&mut self.reader);
        let record = AofRecord::decode(&mut crc_reader)?;
        let computed = crc_reader.finalize();
        let stored = format::read_u32(&mut self.reader)?;
        format::verify_crc32_values(computed, stored)?;
        Ok(record)
    }

    /// Reads an encrypted record: nonce + len + ciphertext.
    #[cfg(feature = "encryption")]
    fn read_encrypted_record(&mut self) -> Result<AofRecord, FormatError> {
        let cipher = self
            .cipher
            .as_ref()
            .ok_or(FormatError::EncryptionRequired)?;

        let mut nonce = [0u8; crate::encryption::NONCE_SIZE];
        format::read_exact(&mut self.reader, &mut nonce)?;

        let ct_len = format::read_u32(&mut self.reader)? as usize;
        if ct_len > format::MAX_FIELD_LEN {
            return Err(FormatError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("encrypted record length {ct_len} exceeds maximum"),
            )));
        }
        let mut ciphertext = vec![0u8; ct_len];
        format::read_exact(&mut self.reader, &mut ciphertext)?;

        let plaintext = cipher.decrypt(&nonce, &ciphertext)?;
        AofRecord::from_bytes(&plaintext)
    }
}

/// Opens a persistence file with create+append and restrictive permissions.
fn open_persistence_file(path: &Path) -> Result<File, FormatError> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    Ok(opts.open(path)?)
}

/// Returns the current unix time in milliseconds.
pub fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u64::MAX as u128) as u64)
}

/// Returns the milliseconds left until an absolute unix-ms deadline, or
/// `None` once it has passed.
pub fn ms_until(timestamp_ms: u64) -> Option<u64> {
    timestamp_ms.checked_sub(unix_now_ms()).filter(|&ms| ms > 0)
}

/// Returns the AOF file path for a given shard in a data directory.
pub fn aof_path(data_dir: &Path, shard_id: u16) -> PathBuf {
    data_dir.join(format!("shard-{shard_id}.aof"))
}

#[cfg(test)]
mod tests;
