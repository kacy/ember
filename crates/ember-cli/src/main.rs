//! ember-cli: interactive command-line client for ember.
//!
//! Connects to an ember server over TCP (or TLS), sends commands as RESP3
//! frames, and pretty-prints responses. Supports one-shot mode, interactive
//! REPL, batch/pipe mode, watch mode, and named subcommands for cluster
//! management and benchmarking.

mod batch;
mod bench_conn;
mod benchmark;
mod cluster;
mod commands;
mod connection;
mod format;
mod repl;
mod tls;
mod watch;

use std::ffi::OsString;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use colored::Colorize;

use crate::tls::TlsClientConfig;

/// Interactive CLI client for ember.
#[derive(Parser)]
#[command(name = "ember-cli", version, about)]
struct Args {
    /// Server hostname.
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Server port.
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Password for AUTH.
    #[arg(short = 'a', long)]
    password: Option<String>,

    /// Enable TLS for the connection.
    #[arg(long)]
    tls: bool,

    /// Path to a CA certificate (PEM) for verifying the server.
    /// Defaults to the system trust store when not set.
    #[arg(long)]
    tls_ca_cert: Option<String>,

    /// Skip TLS certificate verification (insecure, for development only).
    #[arg(long)]
    tls_insecure: bool,

    #[command(subcommand)]
    mode: Option<Mode>,
}

/// How to run the CLI.
#[derive(Subcommand)]
enum Mode {
    /// Cluster management commands.
    Cluster {
        #[command(subcommand)]
        cmd: cluster::ClusterCommand,
    },

    /// Run a built-in benchmark against the server.
    Benchmark(benchmark::BenchmarkArgs),

    /// Watch a key and print its value whenever it changes.
    ///
    /// Polls the server at the given interval. Press ctrl-c to stop.
    Watch {
        /// Key to monitor.
        key: String,
        /// Poll interval in milliseconds.
        #[arg(default_value_t = 1000)]
        interval_ms: u64,
    },

    /// One-shot mode: pass a raw command (e.g. `ember-cli SET key value`).
    #[command(external_subcommand)]
    Raw(Vec<OsString>),
}

impl Args {
    /// Builds a `TlsClientConfig` from the CLI flags.
    ///
    /// Returns `None` when `--tls` is not set.
    fn tls_config(&self) -> Option<TlsClientConfig> {
        if !self.tls {
            return None;
        }
        Some(TlsClientConfig {
            ca_cert: self.tls_ca_cert.clone(),
            insecure: self.tls_insecure,
        })
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    let tls = args.tls_config();

    match args.mode {
        None => {
            if is_stdin_tty() {
                // interactive REPL mode
                repl::run_repl(
                    &args.host,
                    args.port,
                    args.password.as_deref(),
                    tls.as_ref(),
                );
                ExitCode::SUCCESS
            } else {
                // batch mode: stdin is a pipe or redirected file
                batch::run_batch(
                    &args.host,
                    args.port,
                    args.password.as_deref(),
                    tls.as_ref(),
                )
            }
        }
        Some(Mode::Watch { key, interval_ms }) => watch::run_watch(
            &args.host,
            args.port,
            args.password.as_deref(),
            tls.as_ref(),
            &key,
            interval_ms,
        ),
        Some(Mode::Cluster { cmd }) => cluster::run_cluster(
            &cmd,
            &args.host,
            args.port,
            args.password.as_deref(),
            tls.as_ref(),
        ),
        Some(Mode::Benchmark(bench_args)) => benchmark::run_benchmark(
            &bench_args,
            &args.host,
            args.port,
            args.password.as_deref(),
            tls.as_ref(),
        ),
        Some(Mode::Raw(raw)) => {
            let tokens: Vec<String> = raw
                .into_iter()
                .map(|s| s.to_string_lossy().into_owned())
                .collect();
            run_oneshot(
                &args.host,
                args.port,
                args.password.as_deref(),
                tls.as_ref(),
                &tokens,
            )
        }
    }
}

/// Returns `true` when stdin is connected to an interactive terminal.
///
/// When stdin is a pipe or redirected file this returns `false`, which
/// triggers batch mode.
fn is_stdin_tty() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal()
}

/// Sends a single command and prints the response.
fn run_oneshot(
    host: &str,
    port: u16,
    password: Option<&str>,
    tls: Option<&TlsClientConfig>,
    command: &[String],
) -> ExitCode {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("{}", format!("failed to create runtime: {e}").red());
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async {
        let mut conn = match connection::Connection::connect(host, port, tls).await {
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

        let exit_code = match conn.send_command(command).await {
            Ok(frame) => {
                println!("{}", format::format_response(&frame));
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{}", format!("error: {e}").red());
                ExitCode::FAILURE
            }
        };

        conn.shutdown().await;
        exit_code
    })
}
