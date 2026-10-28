mod collector;

use std::time::Duration;

use collector::engine::{self, Config};
use collector::exchange::Exchange;
use collector::polymarket::Polymarket;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr; stdout is reserved for SBE output.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let output = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/dev/stdout".into());
    let cfg = Config {
        replicas: env_u64("REPLICAS", 2).max(1) as usize,
        lag: Duration::from_millis(env_u64("LAG_MS", 3000)),
        snapshot_every: Duration::from_secs(env_u64("SNAPSHOT_SECS", 60)),
        discover_every: Duration::from_secs(env_u64("DISCOVER_SECS", 60)),
    };
    let exchange = Polymarket::new();
    let name = exchange.name();
    tracing::info!(output, replicas = cfg.replicas, exchange = name, "starting");
    engine::run(exchange, output.as_ref(), cfg).await
}
