//! A single order book: absolute quantity per price level.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;

/// (price, quantity) pairs, best first.
pub type Levels = Vec<(i64, i64)>;

#[derive(Debug, Default, Clone)]
pub struct Book {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    /// Exchange time of the last applied message.
    pub exchange_ts: Option<DateTime<Utc>>,
}

impl Book {
    pub fn from_levels(bids: &[(i64, i64)], asks: &[(i64, i64)], ts: DateTime<Utc>) -> Self {
        let side =
            |levels: &[(i64, i64)]| levels.iter().filter(|(_, qty)| *qty > 0).copied().collect();
        Self {
            bids: side(bids),
            asks: side(asks),
            exchange_ts: Some(ts),
        }
    }

    /// Sets the absolute quantity at a level; 0 removes it.
    pub fn apply(&mut self, side: Side, price: i64, qty: i64, ts: DateTime<Utc>) {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if qty == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
        self.exchange_ts = Some(ts);
    }

    /// Bids best-first (descending), asks best-first (ascending).
    pub fn levels(&self) -> (Levels, Levels) {
        let bids = self.bids.iter().rev().map(|(p, q)| (*p, *q)).collect();
        let asks = self.asks.iter().map(|(p, q)| (*p, *q)).collect();
        (bids, asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_delete_and_order() {
        let ts = Utc::now();
        let mut book = Book::from_levels(&[(400_000, 1), (300_000, 0)], &[(600_000, 2)], ts);
        assert!(!book.bids.contains_key(&300_000), "zero qty never stored");
        book.apply(Side::Buy, 450_000, 5, ts);
        book.apply(Side::Sell, 550_000, 7, ts);
        book.apply(Side::Sell, 600_000, 0, ts);
        let (bids, asks) = book.levels();
        assert_eq!(bids, vec![(450_000, 5), (400_000, 1)]);
        assert_eq!(asks, vec![(550_000, 7)]);
    }
}
