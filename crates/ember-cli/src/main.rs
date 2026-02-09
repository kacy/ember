//! ember-cli: interactive command-line client for ember.
//!
//! Connects to an ember server over TCP, sends commands as RESP3 frames,
//! and pretty-prints responses. Supports both one-shot and interactive
//! (REPL) modes.

mod commands;
mod connection;
mod format;
mod repl;

use clap::Parser;
use colored::Colorize;

/// Interactive CLI client for ember.
#[derive(Parser)]
#[command(name = "ember-cli", version, about)]
struct Args {
    /// Server hostname.
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    host: String,

    /// Server port.
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Password for AUTH.
    #[arg(short = 'a', long)]
    password: Option<String>,

    /// Enable TLS (not yet supported).
    #[arg(long)]
    tls: bool,

    /// Command to execute (one-shot mode). If omitted, starts the REPL.
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

fn main() {
    let args = Args::parse();

    if args.tls {
        eprintln!("{}", "tls is not yet supported".yellow());
        std::process::exit(1);
    }

    if args.command.is_empty() {
        // interactive REPL mode
        repl::run_repl(&args.host, args.port, args.password.as_deref(), args.tls);
    } else {
        // one-shot mode: send a single command and exit
        run_oneshot(&args.host, args.port, args.password.as_deref(), &args.command);
    }
}

/// Sends a single command and prints the response.
fn run_oneshot(host: &str, port: u16, password: Option<&str>, command: &[String]) {
    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");

    rt.block_on(async {
        let mut conn = match connection::Connection::connect(host, port).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "{}",
                    format!("could not connect to {host}:{port}: {e}").red()
                );
                std::process::exit(1);
            }
        };

        if let Some(pw) = password {
            if let Err(e) = conn.authenticate(pw).await {
                eprintln!("{}", format!("authentication failed: {e}").red());
                std::process::exit(1);
            }
        }

        match conn.send_command(command).await {
            Ok(frame) => {
                println!("{}", format::format_response(&frame));
            }
            Err(e) => {
                eprintln!("{}", format!("error: {e}").red());
                std::process::exit(1);
            }
        }
    });
}
