//! ember-protocol: RESP3 wire protocol implementation.
//!
//! Provides zero-copy parsing and direct-to-buffer serialization
//! of the RESP3 protocol used for client-server communication.
//!
//! # quick start
//!
//! ```
//! use bytes::{Bytes, BytesMut};
//! use ember_protocol::{Frame, parse_frame};
//!
//! // parse a simple string
//! let input = b"+OK\r\n";
//! let (frame, consumed) = parse_frame(input).unwrap().unwrap();
//! assert_eq!(frame, Frame::Simple("OK".into()));
//!
//! // serialize a frame
//! let mut buf = BytesMut::new();
//! frame.serialize(&mut buf);
//! assert_eq!(&buf[..], b"+OK\r\n");
//! ```

pub mod command;
pub mod error;
pub mod parse;
mod serialize;
pub mod types;

pub use command::{Command, SetExpire};
pub use error::ProtocolError;
pub use parse::parse_frame;
pub use types::Frame;
