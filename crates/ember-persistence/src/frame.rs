//! The layout shared by AOF and snapshot files.
//!
//! Each file starts with a header:
//! ```text
//! [magic: 4B][version: 1B][flags: 1B][salt: 16B, only when encrypted]
//! ```
//! Records and snapshot entries are then stored as frames:
//! ```text
//! [len: 4B][!len: 4B][body: len B][crc32 of body: 4B]
//! ```
//! Without encryption the body is the record's bytes. With it, the body is
//! `[nonce: 12B][ciphertext]`, encrypted with a key derived from the master
//! key and the file's salt and authenticated with the frame's position in
//! the file, counting from 0. All integers are little-endian.
//!
//! The inverted copy of the length tells a damaged length apart from a
//! frame cut short. A crash while appending leaves a prefix of one frame:
//! either fewer than 8 bytes, or a length that checks out and more bytes
//! than are left. Some filesystems instead leave the end of the file
//! zeroed. Readers report both as [`Frame::Torn`]. Any other damage, a
//! length that fails its check or a body that fails its CRC, is an error.

use std::borrow::Cow;
use std::io::{self, Read};

use crate::format::{self, FormatError};

/// The file layout version. Files in any other layout are refused.
pub const VERSION: u8 = 6;

/// Header flag: frame bodies are encrypted.
const FLAG_ENCRYPTED: u8 = 1;

/// The key for encrypted files. A type with no values in builds without
/// the `encryption` feature, so code can take an `Option<&Key>` either way.
#[cfg(feature = "encryption")]
pub use crate::encryption::EncryptionKey as Key;
#[cfg(not(feature = "encryption"))]
#[derive(Debug, Clone)]
pub enum Key {}

/// How a file's frame bodies are encoded.
pub enum Codec {
    Plain,
    #[cfg(feature = "encryption")]
    Encrypted {
        /// Boxed: the cipher's key schedule is large.
        cipher: Box<crate::encryption::FileCipher>,
        /// Position of the next frame in the file.
        next: u64,
    },
}

impl Codec {
    pub fn is_encrypted(&self) -> bool {
        !matches!(self, Codec::Plain)
    }

    #[cfg(feature = "encryption")]
    fn encrypted(cipher: crate::encryption::FileCipher) -> Self {
        Codec::Encrypted {
            cipher: Box::new(cipher),
            next: 0,
        }
    }

    /// Encodes the next record or entry as a frame body. Fails, without
    /// using up the position, when the body would be too long for a frame,
    /// so the frames after it keep their positions.
    pub fn seal<'a>(&mut self, plain: &'a [u8]) -> Result<Cow<'a, [u8]>, FormatError> {
        const MAX_SEAL_OVERHEAD: usize = 64;
        if plain.len() > u32::MAX as usize - MAX_SEAL_OVERHEAD {
            return Err(FormatError::InvalidData(format!(
                "record of {} bytes is too long for a frame",
                plain.len()
            )));
        }
        match self {
            Codec::Plain => Ok(Cow::Borrowed(plain)),
            #[cfg(feature = "encryption")]
            Codec::Encrypted { cipher, next } => {
                let (nonce, ciphertext) = cipher.encrypt(*next, plain)?;
                *next += 1;
                let mut body = Vec::with_capacity(nonce.len() + ciphertext.len());
                body.extend_from_slice(&nonce);
                body.extend_from_slice(&ciphertext);
                Ok(Cow::Owned(body))
            }
        }
    }

    /// Decodes the next frame body back into the record or entry bytes.
    pub fn open(&mut self, body: Vec<u8>) -> Result<Vec<u8>, FormatError> {
        match self {
            Codec::Plain => Ok(body),
            #[cfg(feature = "encryption")]
            Codec::Encrypted { cipher, next } => {
                use crate::encryption::NONCE_SIZE;
                if body.len() < NONCE_SIZE {
                    return Err(FormatError::DecryptionFailed);
                }
                let (nonce, ciphertext) = body.split_at(NONCE_SIZE);
                let nonce: &[u8; NONCE_SIZE] = nonce.try_into().expect("split at NONCE_SIZE");
                let plain = cipher.decrypt(*next, nonce, ciphertext)?;
                *next += 1;
                Ok(plain)
            }
        }
    }

    /// Reads past the frames left in `r`, so the next frame sealed follows
    /// them. Used to append to an existing file.
    pub fn skip_frames(&mut self, r: &mut impl Read) -> Result<(), FormatError> {
        #[cfg(feature = "encryption")]
        if let Codec::Encrypted { next, .. } = self {
            while let Frame::Body(_) = read_frame(r)? {
                *next += 1;
            }
        }
        #[cfg(not(feature = "encryption"))]
        let _ = r;
        Ok(())
    }
}

/// The header fields before the salt, which an encrypted file's key is
/// bound to.
#[cfg(feature = "encryption")]
fn bound_header(magic: &[u8; 4], version: u8, flags: u8) -> [u8; 6] {
    let [a, b, c, d] = *magic;
    [a, b, c, d, version, flags]
}

/// Writes a header for a new file and returns the codec for its frames.
/// With a key, the file is encrypted under a fresh salt.
pub fn write_header(
    out: &mut Vec<u8>,
    magic: &[u8; 4],
    key: Option<&Key>,
) -> Result<Codec, FormatError> {
    out.extend_from_slice(magic);
    out.push(VERSION);
    match key {
        None => {
            out.push(0);
            Ok(Codec::Plain)
        }
        #[cfg(feature = "encryption")]
        Some(key) => {
            out.push(FLAG_ENCRYPTED);
            let header = bound_header(magic, VERSION, FLAG_ENCRYPTED);
            Ok(Codec::encrypted(key.write_new_file_salt(out, &header)?))
        }
        #[cfg(not(feature = "encryption"))]
        Some(never) => match *never {},
    }
}

/// Reads a file header and returns the codec for its frames. An encrypted
/// file needs `key`, and with a key only encrypted files are read: anyone
/// who can write to the data directory could otherwise swap in a plaintext
/// file with data of their own.
pub fn read_header(
    r: &mut impl Read,
    magic: &[u8; 4],
    #[allow(unused_variables)] key: Option<&Key>,
) -> Result<Codec, FormatError> {
    let mut found = [0u8; 4];
    format::read_exact(r, &mut found)?;
    if &found != magic {
        return Err(FormatError::InvalidMagic);
    }
    let version = format::read_u8(r)?;
    if version != VERSION {
        return Err(FormatError::UnsupportedVersion(version));
    }
    let flags = format::read_u8(r)?;
    if flags & !FLAG_ENCRYPTED != 0 {
        return Err(FormatError::InvalidData(format!(
            "unknown header flags {flags:#04x}"
        )));
    }
    match (flags & FLAG_ENCRYPTED != 0, key) {
        (false, None) => Ok(Codec::Plain),
        (false, Some(_)) => Err(FormatError::NotEncrypted),
        (true, None) => Err(FormatError::EncryptionRequired),
        #[cfg(feature = "encryption")]
        (true, Some(key)) => {
            let header = bound_header(&found, version, flags);
            Ok(Codec::encrypted(key.read_file_cipher(r, &header)?))
        }
        #[cfg(not(feature = "encryption"))]
        (true, Some(never)) => match *never {},
    }
}

/// Appends `body` to `out` as one frame.
pub fn write_frame(out: &mut Vec<u8>, body: &[u8]) -> io::Result<()> {
    let len = u32::try_from(body.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("record of {} bytes exceeds u32::MAX", body.len()),
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&(!len).to_le_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(&format::crc32(body).to_le_bytes());
    Ok(())
}

/// What [`read_frame`] found.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    /// A whole frame's body.
    Body(Vec<u8>),
    /// The end of the file, at a frame boundary.
    End,
    /// The rest of the file is part of one frame or zeros, as a crash
    /// while appending leaves it.
    Torn,
}

/// Reads the next frame.
pub fn read_frame(r: &mut impl Read) -> Result<Frame, FormatError> {
    let mut head = [0u8; 8];
    let got = read_up_to(r, &mut head)?;
    if got == 0 {
        return Ok(Frame::End);
    }
    if got < head.len() {
        return Ok(Frame::Torn);
    }
    let len = u32::from_le_bytes(head[..4].try_into().expect("4 bytes"));
    let check = u32::from_le_bytes(head[4..].try_into().expect("4 bytes"));
    if check != !len {
        if head == [0; 8] && rest_is_zero(r)? {
            return Ok(Frame::Torn);
        }
        return Err(FormatError::InvalidData(
            "frame length failed its check".into(),
        ));
    }

    // write_frame accepts any u32 length, so reading must too, or a large
    // record would make the file unreadable. the buffer grows as bytes
    // arrive, so a torn frame with a huge length doesn't allocate it all
    let mut body = Vec::with_capacity((len as usize).min(64 * 1024));
    r.by_ref().take(u64::from(len)).read_to_end(&mut body)?;
    if body.len() < len as usize {
        return Ok(Frame::Torn);
    }
    let mut crc = [0u8; 4];
    if read_up_to(r, &mut crc)? < crc.len() {
        return Ok(Frame::Torn);
    }
    format::verify_crc32(&body, u32::from_le_bytes(crc))?;
    Ok(Frame::Body(body))
}

/// Fills `buf` as far as the input allows and returns how many bytes it
/// read.
fn read_up_to(r: &mut impl Read, buf: &mut [u8]) -> Result<usize, FormatError> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(filled)
}

/// Whether everything left in `r` is zero bytes.
fn rest_is_zero(r: &mut impl Read) -> Result<bool, FormatError> {
    let mut buf = [0u8; 4096];
    loop {
        let n = read_up_to(r, &mut buf)?;
        if buf[..n].iter().any(|&b| b != 0) {
            return Ok(false);
        }
        if n < buf.len() {
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(bodies: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for body in bodies {
            write_frame(&mut out, body).unwrap();
        }
        out
    }

    fn read_all(mut data: &[u8]) -> Result<Vec<Frame>, FormatError> {
        let mut found = Vec::new();
        loop {
            let frame = read_frame(&mut data)?;
            let last = !matches!(frame, Frame::Body(_));
            found.push(frame);
            if last {
                return Ok(found);
            }
        }
    }

    fn body(b: &[u8]) -> Frame {
        Frame::Body(b.to_vec())
    }

    #[test]
    fn frames_round_trip() {
        let data = frames(&[b"one", b"", b"three"]);
        assert_eq!(
            read_all(&data).unwrap(),
            [body(b"one"), body(b""), body(b"three"), Frame::End]
        );
    }

    #[test]
    fn every_cut_of_the_last_frame_reads_as_torn() {
        let data = frames(&[b"first", b"second"]);
        let first_len = frames(&[b"first"]).len();
        for cut in first_len + 1..data.len() {
            assert_eq!(
                read_all(&data[..cut]).unwrap(),
                [body(b"first"), Frame::Torn],
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn a_zeroed_tail_reads_as_torn() {
        let mut data = frames(&[b"first"]);
        data.extend_from_slice(&[0; 100]);
        assert_eq!(read_all(&data).unwrap(), [body(b"first"), Frame::Torn]);
    }

    #[test]
    fn a_damaged_length_is_an_error_not_a_torn_tail() {
        // a length that claims more than the file holds must not cut off
        // the frames after it
        let mut data = frames(&[b"first", b"second", b"third"]);
        let second = frames(&[b"first"]).len();
        data[second + 3] = 0x7f;
        assert!(read_all(&data).is_err());
    }

    #[test]
    fn a_frame_longer_than_a_field_is_read_not_refused() {
        // a header for a frame past MAX_FIELD_LEN, cut short: it reads as
        // a torn tail, without allocating the whole length
        let len = format::MAX_FIELD_LEN as u32 + 1;
        let mut data = frames(&[b"first"]);
        data.extend_from_slice(&len.to_le_bytes());
        data.extend_from_slice(&(!len).to_le_bytes());
        data.extend_from_slice(b"partial body");
        assert_eq!(read_all(&data).unwrap(), [body(b"first"), Frame::Torn]);
    }

    #[test]
    fn zeros_followed_by_data_are_an_error() {
        let mut data = frames(&[b"first"]);
        data.extend_from_slice(&[0; 8]);
        data.extend_from_slice(&frames(&[b"second"]));
        assert!(read_all(&data).is_err());
    }

    #[test]
    fn a_damaged_body_fails_its_crc() {
        let mut data = frames(&[b"first", b"second"]);
        let last = data.len() - 5;
        data[last] ^= 1;
        assert!(matches!(
            read_all(&data),
            Err(FormatError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn header_round_trips() {
        let mut out = Vec::new();
        assert!(!write_header(&mut out, b"TEST", None)
            .unwrap()
            .is_encrypted());
        assert!(!read_header(&mut out.as_slice(), b"TEST", None)
            .unwrap()
            .is_encrypted());
        assert!(matches!(
            read_header(&mut out.as_slice(), b"ELSE", None),
            Err(FormatError::InvalidMagic)
        ));
        out[4] = VERSION - 1;
        assert!(matches!(
            read_header(&mut out.as_slice(), b"TEST", None),
            Err(FormatError::UnsupportedVersion(_))
        ));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_bodies_round_trip_and_need_the_key() {
        let key = Key::from_bytes([7; 32]);
        let mut out = Vec::new();
        let mut codec = write_header(&mut out, b"TEST", Some(&key)).unwrap();
        let sealed = codec.seal(b"secret").unwrap().into_owned();
        assert_ne!(sealed, b"secret");

        let mut reopened = read_header(&mut out.as_slice(), b"TEST", Some(&key)).unwrap();
        assert_eq!(reopened.open(sealed.clone()).unwrap(), b"secret");
        assert!(matches!(
            read_header(&mut out.as_slice(), b"TEST", None),
            Err(FormatError::EncryptionRequired)
        ));
        let wrong = Key::from_bytes([8; 32]);
        let mut other = read_header(&mut out.as_slice(), b"TEST", Some(&wrong)).unwrap();
        assert!(matches!(
            other.open(sealed),
            Err(FormatError::DecryptionFailed)
        ));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn a_changed_header_makes_the_frames_unreadable() {
        let key = Key::from_bytes([7; 32]);
        let mut out = Vec::new();
        let mut codec = write_header(&mut out, b"EAOF", Some(&key)).unwrap();
        let sealed = codec.seal(b"record").unwrap().into_owned();

        // the same file relabeled as another type
        out[..4].copy_from_slice(b"ESNP");
        let mut relabeled = read_header(&mut out.as_slice(), b"ESNP", Some(&key)).unwrap();
        assert!(matches!(
            relabeled.open(sealed),
            Err(FormatError::DecryptionFailed)
        ));
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn a_key_refuses_plaintext_files() {
        let mut out = Vec::new();
        write_header(&mut out, b"TEST", None).unwrap();
        let key = Key::from_bytes([7; 32]);
        assert!(matches!(
            read_header(&mut out.as_slice(), b"TEST", Some(&key)),
            Err(FormatError::NotEncrypted)
        ));
    }

    /// Writes an encrypted file with one frame per body and returns it with
    /// the header's length.
    #[cfg(feature = "encryption")]
    fn encrypted_file(key: &Key, bodies: &[&[u8]]) -> (Vec<u8>, usize) {
        let mut out = Vec::new();
        let mut codec = write_header(&mut out, b"TEST", Some(key)).unwrap();
        let header_len = out.len();
        for body in bodies {
            let sealed = codec.seal(body).unwrap().into_owned();
            write_frame(&mut out, &sealed).unwrap();
        }
        (out, header_len)
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_frames_must_stay_in_place() {
        let key = Key::from_bytes([7; 32]);
        let open_all = |data: &[u8]| -> Result<Vec<Vec<u8>>, FormatError> {
            let mut r = data;
            let mut codec = read_header(&mut r, b"TEST", Some(&key))?;
            let mut found = Vec::new();
            while let Frame::Body(body) = read_frame(&mut r)? {
                found.push(codec.open(body)?);
            }
            Ok(found)
        };
        // equal lengths, so both frames are the same size
        let (file, header_len) = encrypted_file(&key, &[b"aaaa", b"bbbb"]);
        assert_eq!(
            open_all(&file).unwrap(),
            [b"aaaa".to_vec(), b"bbbb".to_vec()]
        );

        let (header, frames) = file.split_at(header_len);
        let (first, second) = frames.split_at(frames.len() / 2);
        for frames in [[second, first], [first, first]] {
            let changed = [header, frames[0], frames[1]].concat();
            assert!(matches!(
                open_all(&changed),
                Err(FormatError::DecryptionFailed)
            ));
        }
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn skip_frames_continues_after_the_existing_frames() {
        let key = Key::from_bytes([7; 32]);
        let (mut file, _) = encrypted_file(&key, &[b"first"]);
        let mut r = file.as_slice();
        let mut codec = read_header(&mut r, b"TEST", Some(&key)).unwrap();
        codec.skip_frames(&mut r).unwrap();
        let sealed = codec.seal(b"second").unwrap().into_owned();
        write_frame(&mut file, &sealed).unwrap();

        let mut r = file.as_slice();
        let mut codec = read_header(&mut r, b"TEST", Some(&key)).unwrap();
        let mut found = Vec::new();
        while let Frame::Body(body) = read_frame(&mut r).unwrap() {
            found.push(codec.open(body).unwrap());
        }
        assert_eq!(found, [b"first".to_vec(), b"second".to_vec()]);
    }
}
