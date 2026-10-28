//! The only exchange-specific surface. Everything else is exchange-agnostic.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;

#[derive(Debug, Clone, PartialEq)]
pub enum MarketEvent {
    /// Full book; replaces whatever we had.
    Snapshot {
        symbol: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        exchange_ts: DateTime<Utc>,
    },
    /// Absolute quantity at one level; 0 = removed.
    Level {
        symbol: String,
        side: Side,
        price: i64,
        qty: i64,
        exchange_ts: DateTime<Utc>,
    },
    /// Proves the subscription is alive without changing the book.
    Heartbeat { symbol: String },
}

pub trait Exchange: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn ws_url(&self) -> &str;
    fn max_symbols_per_socket(&self) -> usize;
    /// First message after connect.
    fn initial_subscribe(&self, symbols: &[String]) -> String;
    fn subscribe(&self, symbols: &[String]) -> String;
    fn unsubscribe(&self, symbols: &[String]) -> String;
    /// Application-level keepalive, if the exchange needs one.
    fn ping(&self) -> Option<String>;
    /// Decodes one text frame. Malformed parts are logged and skipped.
    fn decode(&self, text: &str, out: &mut Vec<MarketEvent>);
    /// Full set of live symbols. `on_page` is called as each page arrives so
    /// the engine can start subscribing before a slow full pass finishes.
    fn discover(
        &self,
        on_page: &mut (dyn FnMut(Vec<String>) + Send),
    ) -> impl Future<Output = anyhow::Result<HashSet<String>>> + Send;
}
