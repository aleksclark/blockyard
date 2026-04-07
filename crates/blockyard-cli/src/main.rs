//! Blockyard CLI (byard) — lightweight remote client.
//!
//! Provides volume, disk, node, and cluster management commands.

use tracing_subscriber::EnvFilter;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    tracing::info!("byard cli starting");
    Ok(())
}
