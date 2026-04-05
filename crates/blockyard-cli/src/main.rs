use clap::Parser;

#[derive(Parser)]
#[command(name = "byard", about = "Blockyard CLI client")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:7400")]
    endpoint: String,
}

fn main() -> anyhow::Result<()> {
    let _cli = Cli::parse();
    println!("byard: blockyard remote CLI (not yet implemented)");
    Ok(())
}
