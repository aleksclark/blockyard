//! Blockyard CLI (byard) — lightweight remote client.
//!
//! Provides volume, disk, node, and cluster management commands.

use clap::Parser;
use tracing_subscriber::EnvFilter;

use blockyard_cli::cli::Cli;
use blockyard_cli::commands::execute;
use blockyard_cli::http_client::HttpClient;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();

    tracing::debug!(?cli, "parsed CLI arguments");

    let client = HttpClient::new(&cli.endpoint);
    match execute(&cli, &client).await {
        Ok(output) => {
            println!("{output}");
            Ok(())
        }
        Err(e) => {
            eprintln!("byard: {e:#}");
            std::process::exit(1);
        }
    }
}
