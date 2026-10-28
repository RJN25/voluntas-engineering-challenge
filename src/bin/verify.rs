//! Reads a collector output file, rebuilds every book from snapshots plus
//! deltas, and prints sanity statistics.
//!
//! Usage: cargo run --release --bin verify -- out.sbe

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Default)]
struct SymbolState {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    last_snapshot: Option<DateTime<Utc>>,
    max_snapshot_gap_s: i64,
    last_exchange_ts: Option<DateTime<Utc>>,
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: verify <file>"))?;
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut books: HashMap<String, SymbolState> = HashMap::new();
    let (mut snapshots, mut updates, mut orphan_updates) = (0u64, 0u64, 0u64);
    let (mut crossed, mut non_monotonic) = (0u64, 0u64);
    let mut latencies_ms = Vec::new();

    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d)?;
                snapshots += 1;
                let st = books.entry(s.symbol).or_default();
                if let Some(prev) = st.last_snapshot {
                    let gap = (s.received_at - prev).num_seconds();
                    st.max_snapshot_gap_s = st.max_snapshot_gap_s.max(gap);
                }
                st.last_snapshot = Some(s.received_at);
                st.bids = s.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
                st.asks = s.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                updates += 1;
                latencies_ms.push((u.backtest_timestamp - u.exchange_timestamp).num_milliseconds());
                let st = books.entry(u.symbol).or_default();
                if st.last_snapshot.is_none() {
                    orphan_updates += 1;
                    continue;
                }
                if st
                    .last_exchange_ts
                    .is_some_and(|t| u.exchange_timestamp < t)
                {
                    non_monotonic += 1;
                }
                st.last_exchange_ts = Some(u.exchange_timestamp);
                let side = match u.side {
                    Side::Buy => &mut st.bids,
                    Side::Sell => &mut st.asks,
                };
                if u.quantity == 0 {
                    side.remove(&u.price_level);
                } else {
                    side.insert(u.price_level, u.quantity);
                }
                if let (Some(bid), Some(ask)) = (st.bids.keys().last(), st.asks.keys().next())
                    && bid >= ask
                {
                    crossed += 1;
                }
            }
            _ => {}
        }
    }

    latencies_ms.sort_unstable();
    let pct = |p: f64| {
        latencies_ms
            .get(((latencies_ms.len() as f64 - 1.0) * p) as usize)
            .copied()
            .unwrap_or(0)
    };
    let max_gap = books
        .values()
        .map(|s| s.max_snapshot_gap_s)
        .max()
        .unwrap_or(0);
    println!("symbols:                     {}", books.len());
    println!("snapshots:                   {snapshots}");
    println!("price level updates:         {updates}");
    println!("updates before any snapshot: {orphan_updates}");
    println!("crossed-book updates:        {crossed}");
    println!("non-monotonic exchange ts:   {non_monotonic}");
    println!("max snapshot gap (s):        {max_gap}");
    println!(
        "recv - exchange latency ms:  p50 {} p90 {} p99 {}",
        pct(0.5),
        pct(0.9),
        pct(0.99)
    );
    Ok(())
}
