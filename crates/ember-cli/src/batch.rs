//! Batch mode: reads commands from stdin line by line and executes them.
//!
//! Useful for scripting and CI pipelines:
//!
//!   echo "SET mykey hello" | ember-cli
//!   ember-cli < commands.txt
//!
//! Blank lines and lines beginning with `#` are treated as comments and
//! skipped. Each non-empty line is split on whitespace and sent as a RESP3
//! command. Results are printed to stdout; errors go to stderr.

use std::io::{self, BufRead};
use std::process::ExitCode;

use colored::Colorize;

use crate::connection::Connection;
use crate::format::format_response;
use crate::tls::TlsClientConfig;

/// Reads commands from stdin line by line and executes them against the server.
///
/// Exits with failure if any command cannot be sent (i.e. the connection
/// breaks). Server-level errors (e.g. wrong type, wrong arity) are printed
/// to stderr but do not stop processing.
pub fn run_batch(
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{}", format!("failed to create runtime: {e}").red());
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(run_batch_async(host, port, password, tls))
}

async fn run_batch_async(
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> ExitCode {
    let mut conn = match Connection::connect(host, port, tls).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "{}",
                format!("could not connect to {host}:{port}: {e}").red()
            );
            return ExitCode::FAILURE;
        }
    };

    if let Some(pw) = password {
        if let Err(e) = conn.authenticate(pw).await {
            eprintln!("{}", format!("authentication failed: {e}").red());
            conn.shutdown().await;
            return ExitCode::FAILURE;
        }
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("{}", format!("read error: {e}").red());
                conn.shutdown().await;
                return ExitCode::FAILURE;
            }
        };

        let trimmed = line.trim();

        // skip blank lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let tokens: Vec<String> = trimmed.split_whitespace().map(|s| s.to_string()).collect();

        match conn.send_command(&tokens).await {
            Ok(frame) => println!("{}", format_response(&frame)),
            Err(e) => {
                eprintln!("{}", format!("error: {e}").red());
                conn.shutdown().await;
                return ExitCode::FAILURE;
            }
        }
    }

    conn.shutdown().await;
    ExitCode::SUCCESS
}
