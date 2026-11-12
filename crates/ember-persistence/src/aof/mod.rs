//! Append-only file for recording mutations.
//!
//! Each shard writes its own AOF file (`shard-{id}.aof`): a header, then one
//! frame per record, in the layout described in [`crate::frame`]. Records
//! are written after successful mutations.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Seek, Write};
use std::path::{Path, PathBuf};

use crate::format::{self, FormatError};
use crate::frame::{self, Codec, Frame, Key};

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

/// Appends AOF records to a file.
///
/// [`write_record`](Self::write_record) only encodes a record into memory.
/// [`flush`](Self::flush) writes everything queued with one `write` call, so
/// the file holds whole records only: if the write fails part way, the file
/// is cut back to the last whole record and the queued records stay queued
/// for the next flush. Dropping the writer flushes and fsyncs.
pub struct AofWriter {
    file: File,
    path: PathBuf,
    /// Encodes records for this file.
    codec: Codec,
    /// The key new files are encrypted with.
    key: Option<Key>,
    /// Encoded records not yet written to the file.
    pending: Vec<u8>,
    /// Length of the file up to the end of the last whole record.
    written_len: u64,
    /// A failed write left bytes past `written_len` that could not be cut
    /// off. The next flush cuts them off before writing.
    torn: bool,
    /// The existing file is in a different format than this writer writes.
    /// See [`AofWriter::needs_rewrite`].
    needs_rewrite: bool,
}

impl AofWriter {
    /// Opens (or creates) a plaintext AOF file. A new file gets a header;
    /// an existing one is appended to.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, FormatError> {
        Self::open_with(path.into(), None)
    }

    /// Opens (or creates) an AOF file encrypted with `key`.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(path: impl Into<PathBuf>, key: Key) -> Result<Self, FormatError> {
        Self::open_with(path.into(), Some(key))
    }

    fn open_with(path: PathBuf, key: Option<Key>) -> Result<Self, FormatError> {
        let existing = fs::metadata(&path).is_ok_and(|m| m.len() > 0);
        let mut file = open_persistence_file(&path)?;

        // an existing file is appended to with its own codec. one this
        // writer can't append to, encrypted the other way or unreadable,
        // must be rewritten first, which starts a new file with a new
        // header; until then any codec will do
        let mut header = Vec::new();
        let (codec, needs_rewrite) = if existing {
            let found = File::open(&path).map_err(FormatError::from).and_then(|f| {
                frame::read_header(&mut BufReader::new(f), format::AOF_MAGIC, key.as_ref())
            });
            match found {
                Ok(codec) if codec.is_encrypted() == key.is_some() => (codec, false),
                _ => (
                    frame::write_header(&mut header, format::AOF_MAGIC, key.as_ref())?,
                    true,
                ),
            }
        } else {
            let codec = frame::write_header(&mut header, format::AOF_MAGIC, key.as_ref())?;
            file.write_all(&header)?;
            (codec, false)
        };
        let written_len = file.metadata()?.len();

        Ok(Self {
            file,
            path,
            codec,
            key,
            pending: Vec::new(),
            written_len,
            torn: false,
            needs_rewrite,
        })
    }

    /// Whether the existing file is in a different format than this writer
    /// writes, such as after encryption was turned on or off. Appending
    /// would mix formats in one file and make it unreadable, so the caller
    /// must snapshot the data and [`truncate`](Self::truncate) first.
    pub fn needs_rewrite(&self) -> bool {
        self.needs_rewrite
    }

    /// Queues a record. It reaches the file on the next
    /// [`flush`](Self::flush) or [`sync`](Self::sync).
    pub fn write_record(&mut self, record: &AofRecord) -> Result<(), FormatError> {
        encode_record(&self.codec, record, &mut self.pending)
    }

    /// Writes the queued records to the OS. On failure the file is cut
    /// back to its last whole record and the records stay queued.
    pub fn flush(&mut self) -> Result<(), FormatError> {
        append_whole(
            &mut self.file,
            &mut self.pending,
            &mut self.written_len,
            &mut self.torn,
        )?;
        Ok(())
    }

    /// Writes the queued records and fsyncs the file.
    pub fn sync(&mut self) -> Result<(), FormatError> {
        self.flush()?;
        self.file.sync_data()?;
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
        // the snapshot holds the queued records' writes too, but they must
        // still reach the old file in case the rename below fails
        self.flush()?;

        // write a fresh header to a temp file next to the real AOF. the
        // file is opened for appending like the real one, which needs it
        // to start empty
        let tmp_path = self.path.with_extension("aof.tmp");
        match fs::remove_file(&tmp_path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        let mut tmp_file = open_persistence_file(&tmp_path)?;
        let mut contents = Vec::new();
        let codec = frame::write_header(&mut contents, format::AOF_MAGIC, self.key.as_ref())?;
        encode_record(
            &codec,
            &AofRecord::Checkpoint { snapshot_crc },
            &mut contents,
        )?;
        tmp_file.write_all(&contents)?;
        tmp_file.sync_all()?;

        // atomic rename: old AOF is replaced only after new file is durable.
        // the open handle follows the file to its new name, so there is
        // nothing to reopen
        fs::rename(&tmp_path, &self.path)?;
        sync_parent_dir(&self.path);
        self.file = tmp_file;
        self.codec = codec;
        self.written_len = contents.len() as u64;
        self.torn = false;
        self.needs_rewrite = false;
        Ok(())
    }
}

/// Appends `record` to `out` as one frame.
fn encode_record(codec: &Codec, record: &AofRecord, out: &mut Vec<u8>) -> Result<(), FormatError> {
    let plain = record.to_bytes()?;
    frame::write_frame(out, &codec.seal(&plain)?)?;
    Ok(())
}

/// A file that records are appended to. A trait so tests can make writes
/// fail part way.
trait AppendFile: Write {
    fn set_len(&self, len: u64) -> std::io::Result<()>;
}

impl AppendFile for File {
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        File::set_len(self, len)
    }
}

/// Appends `pending` to `file`, which is opened for appending and holds
/// `written_len` bytes of whole records, and empties `pending`.
///
/// A failed write may leave part of a record behind, which would make the
/// records appended after it unreadable. The file is cut back to
/// `written_len` then, and `pending` is kept for the next call. If even
/// that fails, `torn` is set and the next call cuts it back first.
fn append_whole(
    file: &mut impl AppendFile,
    pending: &mut Vec<u8>,
    written_len: &mut u64,
    torn: &mut bool,
) -> std::io::Result<()> {
    if *torn {
        file.set_len(*written_len)?;
        *torn = false;
    }
    if pending.is_empty() {
        return Ok(());
    }
    if let Err(e) = file.write_all(pending) {
        *torn = file.set_len(*written_len).is_err();
        return Err(e);
    }
    *written_len += pending.len() as u64;
    pending.clear();
    Ok(())
}

impl Drop for AofWriter {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            tracing::error!(path = %self.path.display(), "final aof sync failed: {e}");
        }
    }
}

/// Fsyncs the directory holding `path`, so a rename into it survives a
/// crash. Best effort: some platforms can't open a directory for this.
pub(crate) fn sync_parent_dir(path: &Path) {
    if let Some(dir) = path.parent().and_then(|p| File::open(p).ok()) {
        let _ = dir.sync_all();
    }
}

/// Reader for iterating over AOF records.
pub struct AofReader {
    reader: BufReader<File>,
    codec: Codec,
    /// End offset of the last whole record read. See [`AofReader::valid_len`].
    valid_len: u64,
}

impl fmt::Debug for AofReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AofReader")
            .field("encrypted", &self.codec.is_encrypted())
            .finish()
    }
}

impl AofReader {
    /// Opens a plaintext AOF file and reads the header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        Self::open_with(path.as_ref(), None)
    }

    /// Opens an AOF file that may be encrypted with `key`. Plaintext files
    /// are read too.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(path: impl AsRef<Path>, key: Key) -> Result<Self, FormatError> {
        Self::open_with(path.as_ref(), Some(&key))
    }

    pub(crate) fn open_with(path: &Path, key: Option<&Key>) -> Result<Self, FormatError> {
        let mut reader = BufReader::new(File::open(path)?);
        let codec = frame::read_header(&mut reader, format::AOF_MAGIC, key)?;
        let valid_len = reader.stream_position()?;
        Ok(Self {
            reader,
            codec,
            valid_len,
        })
    }

    /// Reads the next record from the AOF.
    ///
    /// Returns `Ok(None)` at end of file. A record cut short by a crash
    /// mid-write also returns `Ok(None)`; [`valid_len`](Self::valid_len)
    /// then tells the caller where the whole records end.
    pub fn read_record(&mut self) -> Result<Option<AofRecord>, FormatError> {
        match frame::read_frame(&mut self.reader)? {
            Frame::End | Frame::Torn => Ok(None),
            Frame::Body(body) => {
                let record = AofRecord::from_bytes(&self.codec.open(body)?)?;
                self.valid_len = self.reader.stream_position()?;
                Ok(Some(record))
            }
        }
    }

    /// Byte length of the file prefix that holds the header and every
    /// record read so far. Anything past it after `read_record` returns
    /// `None` is a partial record left by a crash.
    pub fn valid_len(&self) -> u64 {
        self.valid_len
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
