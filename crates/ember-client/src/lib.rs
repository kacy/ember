//! ember-client: async RESP3 client for ember.
//!
//! Provides a simple async client for connecting to an ember server over
//! TCP (or TLS), sending commands as RESP3 frames, and reading responses.
//!
//! # Example
//!
//! ```no_run
//! use ember_client::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), ember_client::ClientError> {
//!     let mut client = Client::connect("127.0.0.1", 6379).await?;
//!     let response = client.send(&["PING"]).await?;
//!     println!("{response:?}");
//!     Ok(())
//! }
//! ```

mod connection;
#[cfg(feature = "tls")]
pub mod tls;

pub use connection::{Client, ClientError};
pub use ember_protocol::types::Frame;
#[cfg(feature = "tls")]
pub use tls::TlsClientConfig;
