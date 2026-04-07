//! Blockyard CLI (byard) — lightweight remote client.
//!
//! Provides volume, disk, node, and cluster management commands.

use clap::Parser;
use tracing_subscriber::EnvFilter;

use blockyard_cli::cli::Cli;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();

    tracing::debug!(?cli, "parsed CLI arguments");

    eprintln!(
        "byard: not yet connected to a cluster (endpoint: {})",
        cli.endpoint
    );
    eprintln!("byard: this build only supports --help and argument validation");

    std::process::exit(1);
}
