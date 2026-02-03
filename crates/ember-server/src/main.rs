mod connection;
mod server;

use std::net::SocketAddr;

use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(name = "ember-server", about = "ember cache server")]
struct Args {
    /// address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// port to listen on
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ember=info".into()),
        )
        .init();

    let args = Args::parse();

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("invalid bind address");

    info!("ember server starting...");

    if let Err(e) = server::run(addr).await {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
