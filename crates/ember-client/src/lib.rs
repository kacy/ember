//! ember-client: async Rust client for ember.
//!
//! Connects to an ember server over TCP (or TLS) and exposes a typed async
//! API covering all common commands. Raw RESP3 frames are available via
//! [`Client::send`] when you need something not covered by the typed methods.
//!
//! # Quick start
//!
//! ```no_run
//! use ember_client::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), ember_client::ClientError> {
//!     let mut client = Client::connect("127.0.0.1", 6379).await?;
//!
//!     client.set("greeting", "hello").await?;
//!     let value = client.get("greeting").await?;
//!     println!("{value:?}"); // Some(b"hello")
//!
//!     Ok(())
//! }
//! ```
//!
//! # Pipelining
//!
//! Send multiple commands in one round-trip using [`Pipeline`]:
//!
//! ```no_run
//! use ember_client::{Client, Pipeline};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), ember_client::ClientError> {
//!     let mut client = Client::connect("127.0.0.1", 6379).await?;
//!
//!     let frames = client.execute_pipeline(
//!         Pipeline::new()
//!             .set("a", "1")
//!             .set("b", "2")
//!             .get("a")
//!             .get("b"),
//!     ).await?;
//!
//!     println!("{} responses", frames.len());
//!     Ok(())
//! }
//! ```

mod commands;
mod connection;
mod pipeline;
#[cfg(feature = "tls")]
pub mod tls;

pub use connection::{Client, ClientError};
pub use ember_protocol::types::Frame;
pub use pipeline::Pipeline;
#[cfg(feature = "tls")]
pub use tls::TlsClientConfig;
