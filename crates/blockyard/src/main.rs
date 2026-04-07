//! Blockyard node — main entry point.
//!
//! Initializes tracing, loads configuration, and starts the node process.

use tracing_subscriber::EnvFilter;

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
    let json_output = std::env::var("BLOCKYARD_LOG_JSON")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    init_tracing(json_output);

    tracing::info!("blockyard node starting");
    Ok(())
}
