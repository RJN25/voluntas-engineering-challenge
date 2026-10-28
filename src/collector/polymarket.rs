//! Polymarket CLOB market channel. Symbols are CLOB token ids (one per outcome).

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use serde::Deserialize;
use serde_json::{Value, json};

use super::exchange::{Exchange, MarketEvent};
use super::fixed;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
// Gamma caps offset paging; deep pagination must use the keyset endpoint.
const MARKETS_URL: &str =
    "https://gamma-api.polymarket.com/markets/keyset?active=true&closed=false&limit=100";

pub struct Polymarket {
    http: reqwest::Client,
}

impl Polymarket {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }
}

#[derive(Deserialize)]
struct Level {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct Change {
    asset_id: Option<String>,
    price: String,
    size: String,
    side: String,
}

#[derive(Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum Wire {
    Book {
        asset_id: String,
        #[serde(default)]
        bids: Vec<Level>,
        #[serde(default)]
        asks: Vec<Level>,
        timestamp: Value,
    },
    PriceChange {
        /// Legacy shape: asset on the envelope plus `changes`.
        asset_id: Option<String>,
        #[serde(default)]
        price_changes: Vec<Change>,
        #[serde(default)]
        changes: Vec<Change>,
        timestamp: Value,
    },
    TickSizeChange {
        asset_id: String,
    },
    LastTradePrice {
        asset_id: String,
    },
    BestBidAsk {
        asset_id: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct Market {
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    #[serde(rename = "enableOrderBook", default)]
    enable_order_book: bool,
    #[serde(rename = "acceptingOrders", default)]
    accepting_orders: bool,
    #[serde(default)]
    closed: bool,
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    next_cursor: Option<String>,
}

/// Millisecond timestamps arrive as strings ("1782753357257"), sometimes numbers.
fn millis(v: &Value) -> Option<DateTime<Utc>> {
    let ms = match v {
        Value::String(s) => s.parse().ok()?,
        Value::Number(n) => n.as_i64()?,
        _ => return None,
    };
    DateTime::from_timestamp_millis(ms)
}

fn levels(raw: &[Level]) -> Option<Vec<(i64, i64)>> {
    raw.iter()
        .map(|l| Some((fixed::parse(&l.price)?, fixed::parse(&l.size)?)))
        .collect()
}

fn decode_one(wire: Wire, out: &mut Vec<MarketEvent>) -> Option<()> {
    match wire {
        Wire::Book {
            asset_id,
            bids,
            asks,
            timestamp,
        } => out.push(MarketEvent::Snapshot {
            symbol: asset_id,
            bids: levels(&bids)?,
            asks: levels(&asks)?,
            exchange_ts: millis(&timestamp)?,
        }),
        Wire::PriceChange {
            asset_id,
            price_changes,
            changes,
            timestamp,
        } => {
            let exchange_ts = millis(&timestamp)?;
            for c in price_changes.into_iter().chain(changes) {
                let side = match c.side.as_str() {
                    "BUY" => Side::Buy,
                    "SELL" => Side::Sell,
                    _ => return None,
                };
                out.push(MarketEvent::Level {
                    symbol: c.asset_id.or_else(|| asset_id.clone())?,
                    side,
                    price: fixed::parse(&c.price)?,
                    qty: fixed::parse(&c.size)?,
                    exchange_ts,
                });
            }
        }
        Wire::TickSizeChange { asset_id }
        | Wire::LastTradePrice { asset_id }
        | Wire::BestBidAsk { asset_id } => out.push(MarketEvent::Heartbeat { symbol: asset_id }),
        Wire::Other => {}
    }
    Some(())
}

impl Exchange for Polymarket {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn ws_url(&self) -> &str {
        WS_URL
    }

    fn max_symbols_per_socket(&self) -> usize {
        400
    }

    fn initial_subscribe(&self, symbols: &[String]) -> String {
        json!({ "assets_ids": symbols, "type": "market" }).to_string()
    }

    fn subscribe(&self, symbols: &[String]) -> String {
        json!({ "assets_ids": symbols, "operation": "subscribe" }).to_string()
    }

    fn unsubscribe(&self, symbols: &[String]) -> String {
        json!({ "assets_ids": symbols, "operation": "unsubscribe" }).to_string()
    }

    fn ping(&self) -> Option<String> {
        Some("PING".into())
    }

    fn decode(&self, text: &str, out: &mut Vec<MarketEvent>) {
        if text == "PONG" {
            return;
        }
        let items = match serde_json::from_str::<Value>(text) {
            Ok(Value::Array(items)) => items,
            Ok(item) => vec![item],
            Err(e) => {
                tracing::warn!("bad frame ({e}): {:.200}", text);
                return;
            }
        };
        for item in items {
            // Decode each event into a scratch buffer so a malformed one never
            // leaves half its changes applied.
            let mut events = Vec::new();
            let ok = serde_json::from_value::<Wire>(item)
                .ok()
                .and_then(|w| decode_one(w, &mut events));
            match ok {
                Some(()) => out.append(&mut events),
                None => tracing::warn!("skipping malformed event in: {:.200}", text),
            }
        }
    }

    async fn discover(
        &self,
        on_page: &mut (dyn FnMut(Vec<String>) + Send),
    ) -> anyhow::Result<HashSet<String>> {
        let mut all = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            // Cursors are URL-safe base64, so no escaping is needed.
            let url = match &cursor {
                Some(c) => format!("{MARKETS_URL}&after_cursor={c}"),
                None => MARKETS_URL.to_owned(),
            };
            let page: MarketsPage = self
                .http
                .get(url)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let mut tokens = Vec::new();
            for m in &page.markets {
                if !m.enable_order_book || !m.accepting_orders || m.closed {
                    continue;
                }
                let ids: Vec<String> = m
                    .clob_token_ids
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                tokens.extend(ids.into_iter().filter(|id| all.insert(id.clone())));
            }
            if !tokens.is_empty() {
                on_page(tokens);
            }
            match page.next_cursor {
                Some(c) if !c.is_empty() && !page.markets.is_empty() => cursor = Some(c),
                _ => return Ok(all),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(text: &str) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        Polymarket::new().decode(text, &mut out);
        out
    }

    const BOOK: &str = r#"{"event_type":"book","market":"0xabc","asset_id":"A",
        "bids":[{"price":"0.29","size":"100"}],"asks":[{"price":".31","size":"2.5"}],
        "timestamp":"1700000000123","hash":"h"}"#;

    #[test]
    fn decodes_book_object_and_array() {
        let expected = MarketEvent::Snapshot {
            symbol: "A".into(),
            bids: vec![(290_000, 100_000_000)],
            asks: vec![(310_000, 2_500_000)],
            exchange_ts: DateTime::from_timestamp_millis(1_700_000_000_123).unwrap(),
        };
        assert_eq!(decode(BOOK), vec![expected.clone()]);
        assert_eq!(
            decode(&format!("[{BOOK},{BOOK}]")),
            vec![expected.clone(), expected]
        );
    }

    #[test]
    fn decodes_new_and_legacy_price_change() {
        let new = r#"{"event_type":"price_change","market":"m","timestamp":"1700000000000",
            "price_changes":[{"asset_id":"A","price":"0.5","size":"0","side":"BUY","hash":"h",
            "best_bid":"0.4","best_ask":"0.6"},{"asset_id":"B","price":"0.6","size":"3","side":"SELL"}]}"#;
        let legacy = r#"{"event_type":"price_change","asset_id":"A","timestamp":"1700000000000",
            "changes":[{"price":"0.5","size":"0","side":"BUY"}]}"#;
        let new = decode(new);
        assert_eq!(new.len(), 2);
        assert!(
            matches!(&new[0], MarketEvent::Level { symbol, side: Side::Buy, price: 500_000, qty: 0, .. } if symbol == "A")
        );
        assert!(
            matches!(&new[1], MarketEvent::Level { symbol, side: Side::Sell, qty: 3_000_000, .. } if symbol == "B")
        );
        assert_eq!(decode(legacy)[0], new[0]);
    }

    #[test]
    fn ignores_pong_and_garbage_and_counts_heartbeats() {
        assert!(decode("PONG").is_empty());
        assert!(decode("not json").is_empty());
        let bad = r#"{"event_type":"price_change","timestamp":"1","price_changes":[{"asset_id":"A","price":"1e5","size":"1","side":"BUY"}]}"#;
        assert!(decode(bad).is_empty());
        let hb = r#"{"event_type":"last_trade_price","asset_id":"A","price":"0.5"}"#;
        assert_eq!(
            decode(hb),
            vec![MarketEvent::Heartbeat { symbol: "A".into() }]
        );
    }

    #[test]
    fn encodes_subscriptions() {
        let p = Polymarket::new();
        let ids = ["1".to_string(), "2".to_string()];
        assert_eq!(
            p.initial_subscribe(&ids),
            r#"{"assets_ids":["1","2"],"type":"market"}"#
        );
        assert_eq!(
            p.subscribe(&ids),
            r#"{"assets_ids":["1","2"],"operation":"subscribe"}"#
        );
        assert_eq!(
            p.unsubscribe(&ids),
            r#"{"assets_ids":["1","2"],"operation":"unsubscribe"}"#
        );
    }
}
