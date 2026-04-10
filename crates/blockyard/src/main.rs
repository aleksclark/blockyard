//! Blockyard node — main entry point.
//!
//! Initializes tracing, loads configuration, and starts the node process.

mod node;

use std::path::PathBuf;

use blockyard_common::NodeConfig;
use blockyard_protocol::DataPlaneServer;
use clap::Parser;
use node::BlockyardNode;
use tracing_subscriber::EnvFilter;

/// Blockyard distributed block storage node.
#[derive(Parser, Debug)]
#[command(name = "blockyard", about = "Blockyard distributed block storage node")]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Print an example configuration and exit.
    #[arg(long)]
    generate_config: bool,
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.generate_config {
        print!("{}", NodeConfig::example_toml());
        return Ok(());
    }

    let json_output = std::env::var("BLOCKYARD_LOG_JSON")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    init_tracing(json_output);

    let config_path = cli
        .config
        .ok_or_else(|| anyhow::anyhow!("--config <path> is required"))?;

    tracing::info!(path = %config_path.display(), "loading configuration");
    let config = NodeConfig::from_file(&config_path)?;
    config.validate()?;

    let listen_addr = config.listen_addr;
    let blockyard_node = BlockyardNode::start(config).await?;
    let node_id = blockyard_node.node_id();
    let shutdown_token = blockyard_node.shutdown_token();

    // Start TCP data plane server
    let data_service = blockyard_node.data_service().clone();
    let server = DataPlaneServer::bind(listen_addr, data_service, node_id).await?;

    let server_shutdown = shutdown_token.clone();
    let server_handle = tokio::spawn(async move {
        server.run(server_shutdown).await;
    });

    tracing::info!(%node_id, %listen_addr, "blockyard node is ready");

    // Wait for ctrl-c
    tokio::signal::ctrl_c().await?;
    tracing::info!("received ctrl-c, initiating shutdown");

    blockyard_node.shutdown().await?;
    server_handle.await?;

    tracing::info!("blockyard node stopped");
    Ok(())
}
