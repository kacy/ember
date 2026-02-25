//! Watch mode: polls a key at a configurable interval and prints its value
//! whenever it changes.
//!
//! Run until ctrl-c is pressed:
//!   ember-cli watch mykey
//!   ember-cli watch mykey 500

use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use colored::Colorize;

use crate::connection::Connection;
use crate::format::format_response;
use crate::tls::TlsClientConfig;

/// Polls `key` every `interval_ms` milliseconds and prints its value when
/// it changes. Runs until ctrl-c is pressed or the connection fails.
pub fn run_watch(
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
    key: &str,
    interval_ms: u64,
) -> ExitCode {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{}", format!("failed to create runtime: {e}").red());
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(run_watch_async(host, port, password, tls, key, interval_ms))
}

async fn run_watch_async(
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
    key: &str,
    interval_ms: u64,
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

    println!("watching key: {}", key.cyan());
    println!("interval: {}ms  (ctrl-c to stop)", interval_ms);
    println!();

    let interval = Duration::from_millis(interval_ms);
    let mut last_value: Option<String> = None;
    let mut exit_code = ExitCode::SUCCESS;

    // Pin the shutdown future once so select! can poll it on every iteration
    // without re-registering the signal handler.
    let mut shutdown = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown => {
                println!("\nstopped.");
                break;
            }

            _ = tokio::time::sleep(interval) => {
                match conn.send_command_strs(&["GET", key]).await {
                    Ok(frame) => {
                        let formatted = format_response(&frame);
                        if Some(&formatted) != last_value.as_ref() {
                            println!("{} {}", timestamp().dimmed(), formatted);
                            last_value = Some(formatted);
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", format!("error: {e}").red());
                        exit_code = ExitCode::FAILURE;
                        break;
                    }
                }
            }
        }
    }

    conn.shutdown().await;
    exit_code
}

/// Returns the current Unix timestamp formatted for display.
fn timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("[{secs}]")
}
