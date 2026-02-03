//! Per-connection handler.
//!
//! Reads RESP3 frames from a TCP stream, routes them to commands,
//! and writes responses back. Supports pipelining by processing
//! multiple frames from a single read.

use bytes::BytesMut;
use ember_protocol::{parse_frame, Command, Frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Initial read buffer capacity. 4KB covers most commands comfortably
/// without over-allocating for simple PING/SET/GET workloads.
const BUF_CAPACITY: usize = 4096;

/// Drives a single client connection to completion.
///
/// Reads data into a buffer, parses complete frames, dispatches commands,
/// and writes serialized responses back. The loop exits when the client
/// disconnects or a protocol error occurs.
pub async fn handle(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = BytesMut::with_capacity(BUF_CAPACITY);

    loop {
        // read some data — returns 0 on clean disconnect
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        // process as many complete frames as the buffer holds (pipelining)
        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);

                    let response = process(frame);

                    let mut out = BytesMut::new();
                    response.serialize(&mut out);
                    stream.write_all(&out).await?;
                }
                Ok(None) => break, // need more data
                Err(e) => {
                    let msg = format!("ERR protocol error: {e}");
                    let err_frame = Frame::Error(msg);

                    let mut out = BytesMut::new();
                    err_frame.serialize(&mut out);
                    stream.write_all(&out).await?;

                    return Ok(());
                }
            }
        }
    }
}

/// Converts a raw frame into a command and executes it.
fn process(frame: Frame) -> Frame {
    match Command::from_frame(frame) {
        Ok(cmd) => execute(cmd),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Executes a parsed command and returns the response frame.
fn execute(cmd: Command) -> Frame {
    match cmd {
        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),
        Command::Unknown(name) => {
            Frame::Error(format!("ERR unknown command '{name}'"))
        }
    }
}
