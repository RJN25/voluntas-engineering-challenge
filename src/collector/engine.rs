//! Single-writer engine: owns every book and the output writer, so the hot
//! path needs no locks. Sockets feed it over an mpsc channel.
//!
//! Symbols are sharded; each shard runs `replicas` identical sockets. Every
//! symbol has one *source* replica whose updates are written. When the source
//! drops or lags, we switch to the freshest sibling and write a snapshot,
//! because the replicas' books may differ.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval};

use super::book::Book;
use super::connection::{self, Command, SocketMsg};
use super::exchange::{Exchange, MarketEvent};

const CHUNK_SIZE: usize = 64 << 20;

pub struct Config {
    pub replicas: usize,
    pub lag: Duration,
    pub snapshot_every: Duration,
    pub discover_every: Duration,
}

enum Discovery {
    Page(Vec<String>),
    /// A full pass finished; anything we track that is not in here is gone.
    Complete(HashSet<String>),
}

struct Shard {
    symbols: HashSet<String>,
    conns: Vec<usize>,
}

struct Engine<E: Exchange> {
    exchange: Arc<E>,
    cfg: Config,
    writer: AlignedSbeWriter,
    shards: Vec<Shard>,
    socket_tx: UnboundedSender<SocketMsg>,
    conn_cmds: Vec<UnboundedSender<Command>>,
    conn_tasks: Vec<JoinHandle<()>>,
    shard_of: HashMap<String, usize>,
    books: HashMap<(usize, String), Book>,
    last_seen: HashMap<(usize, String), Instant>,
    source: HashMap<String, usize>,
    written: u64,
    failovers: u64,
}

pub async fn run<E: Exchange>(exchange: E, output: &Path, cfg: Config) -> anyhow::Result<()> {
    let exchange = Arc::new(exchange);
    let (socket_tx, mut socket_rx) = unbounded_channel();
    let (disc_tx, mut disc_rx) = unbounded_channel();

    // Discovery runs on its own task so slow HTTP never blocks the data path.
    let discovery = {
        let exchange = exchange.clone();
        let every = cfg.discover_every;
        tokio::spawn(async move {
            let mut tick = interval(every);
            loop {
                tick.tick().await;
                let tx = disc_tx.clone();
                let mut on_page = move |page| {
                    let _ = tx.send(Discovery::Page(page));
                };
                match exchange.discover(&mut on_page).await {
                    Ok(all) => {
                        tracing::info!(symbols = all.len(), "discovery pass complete");
                        let _ = disc_tx.send(Discovery::Complete(all));
                    }
                    Err(e) => tracing::warn!("discovery failed: {e:#}"),
                }
            }
        })
    };

    let mut engine = Engine {
        exchange,
        writer: AlignedSbeWriter::create(output, CHUNK_SIZE)?,
        cfg,
        shards: Vec::new(),
        socket_tx,
        conn_cmds: Vec::new(),
        conn_tasks: Vec::new(),
        shard_of: HashMap::new(),
        books: HashMap::new(),
        last_seen: HashMap::new(),
        source: HashMap::new(),
        written: 0,
        failovers: 0,
    };

    let mut snapshot_tick = interval(engine.cfg.snapshot_every);
    snapshot_tick.tick().await;
    let mut lag_tick = interval(Duration::from_secs(1));
    let mut stats_tick = interval(Duration::from_secs(10));
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            Some(msg) = socket_rx.recv() => engine.on_socket(msg)?,
            Some(d) = disc_rx.recv() => match d {
                Discovery::Page(page) => engine.add_symbols(page),
                Discovery::Complete(all) => engine.remove_missing(&all),
            },
            _ = snapshot_tick.tick() => engine.snapshot_all()?,
            _ = lag_tick.tick() => engine.check_lag()?,
            _ = stats_tick.tick() => tracing::info!(
                symbols = engine.shard_of.len(),
                sockets = engine.conn_cmds.len(),
                written = engine.written,
                failovers = engine.failovers,
                backlog = socket_rx.len(),
                "stats"
            ),
            _ = &mut ctrl_c => break,
        }
    }

    tracing::info!("SIGINT: closing sockets and flushing output");
    discovery.abort();
    for cmd in &engine.conn_cmds {
        let _ = cmd.send(Command::Shutdown);
    }
    let all = futures_util::future::join_all(engine.conn_tasks.drain(..));
    if tokio::time::timeout(Duration::from_secs(5), all)
        .await
        .is_err()
    {
        tracing::warn!("some sockets did not close within 5s");
    }
    engine.writer.flush()?;
    tracing::info!(
        written = engine.written,
        failovers = engine.failovers,
        "done"
    );
    Ok(())
}

impl<E: Exchange> Engine<E> {
    fn add_symbols(&mut self, symbols: Vec<String>) {
        let max = self.exchange.max_symbols_per_socket();
        let mut batches: HashMap<usize, Vec<String>> = HashMap::new();
        for symbol in symbols {
            if self.shard_of.contains_key(&symbol) {
                continue;
            }
            // Least-loaded shard with room, else a new one.
            let shard = match (0..self.shards.len())
                .filter(|&i| self.shards[i].symbols.len() < max)
                .min_by_key(|&i| self.shards[i].symbols.len())
            {
                Some(i) => i,
                None => self.new_shard(),
            };
            self.shards[shard].symbols.insert(symbol.clone());
            self.shard_of.insert(symbol.clone(), shard);
            batches.entry(shard).or_default().push(symbol);
        }
        for (shard, batch) in batches {
            self.send_to_shard(shard, || Command::Subscribe(batch.clone()));
        }
    }

    fn remove_missing(&mut self, live: &HashSet<String>) {
        let gone: Vec<String> = self
            .shard_of
            .keys()
            .filter(|s| !live.contains(*s))
            .cloned()
            .collect();
        if gone.is_empty() {
            return;
        }
        tracing::info!(
            count = gone.len(),
            "unsubscribing symbols no longer trading"
        );
        let mut batches: HashMap<usize, Vec<String>> = HashMap::new();
        for symbol in gone {
            let Some(shard) = self.shard_of.remove(&symbol) else {
                continue;
            };
            self.shards[shard].symbols.remove(&symbol);
            for &conn in &self.shards[shard].conns {
                self.books.remove(&(conn, symbol.clone()));
                self.last_seen.remove(&(conn, symbol.clone()));
            }
            self.source.remove(&symbol);
            batches.entry(shard).or_default().push(symbol);
        }
        for (shard, batch) in batches {
            self.send_to_shard(shard, || Command::Unsubscribe(batch.clone()));
        }
    }

    fn new_shard(&mut self) -> usize {
        let mut conns = Vec::new();
        for _ in 0..self.cfg.replicas {
            let id = self.conn_cmds.len();
            let (tx, rx) = unbounded_channel();
            let task = tokio::spawn(connection::run(
                id,
                self.exchange.clone(),
                rx,
                self.socket_tx.clone(),
            ));
            self.conn_cmds.push(tx);
            self.conn_tasks.push(task);
            conns.push(id);
        }
        self.shards.push(Shard {
            symbols: HashSet::new(),
            conns,
        });
        self.shards.len() - 1
    }

    fn send_to_shard(&self, shard: usize, cmd: impl Fn() -> Command) {
        for &conn in &self.shards[shard].conns {
            let _ = self.conn_cmds[conn].send(cmd());
        }
    }

    fn shard_of_conn(&self, conn: usize) -> usize {
        conn / self.cfg.replicas
    }

    fn on_socket(&mut self, msg: SocketMsg) -> anyhow::Result<()> {
        match msg {
            SocketMsg::Up(conn) => {
                tracing::debug!(conn, "socket up");
                Ok(())
            }
            SocketMsg::Down(conn) => self.on_down(conn),
            SocketMsg::Events {
                conn,
                received_at,
                events,
            } => {
                for event in events {
                    self.on_event(conn, received_at, event)?;
                }
                Ok(())
            }
        }
    }

    fn on_event(
        &mut self,
        conn: usize,
        received_at: DateTime<Utc>,
        event: MarketEvent,
    ) -> anyhow::Result<()> {
        let symbol = match &event {
            MarketEvent::Snapshot { symbol, .. }
            | MarketEvent::Level { symbol, .. }
            | MarketEvent::Heartbeat { symbol } => symbol.clone(),
        };
        // Ignore late messages for symbols we already dropped.
        if !self.shard_of.contains_key(&symbol) {
            return Ok(());
        }
        let key = (conn, symbol.clone());
        self.last_seen.insert(key.clone(), Instant::now());
        match event {
            MarketEvent::Snapshot {
                bids,
                asks,
                exchange_ts,
                ..
            } => {
                self.books
                    .insert(key, Book::from_levels(&bids, &asks, exchange_ts));
                let source = *self.source.entry(symbol.clone()).or_insert(conn);
                if source == conn {
                    self.write_snapshot(conn, &symbol, received_at)?;
                }
            }
            MarketEvent::Level {
                side,
                price,
                qty,
                exchange_ts,
                ..
            } => {
                // Never apply a delta to a book we have no snapshot for.
                let Some(book) = self.books.get_mut(&key) else {
                    return Ok(());
                };
                book.apply(side, price, qty, exchange_ts);
                if self.source.get(&symbol) == Some(&conn) {
                    self.writer.write_msg(&PriceLevelUpdateDomain {
                        backtest_timestamp: received_at,
                        exchange_timestamp: exchange_ts,
                        price_level: price,
                        quantity: qty,
                        side,
                        symbol,
                    })?;
                    self.written += 1;
                }
            }
            MarketEvent::Heartbeat { .. } => {}
        }
        Ok(())
    }

    fn on_down(&mut self, conn: usize) -> anyhow::Result<()> {
        let shard = self.shard_of_conn(conn);
        let symbols: Vec<String> = self.shards[shard].symbols.iter().cloned().collect();
        for symbol in symbols {
            self.books.remove(&(conn, symbol.clone()));
            self.last_seen.remove(&(conn, symbol.clone()));
            if self.source.get(&symbol) == Some(&conn) {
                self.source.remove(&symbol);
                if let Some(next) = self.freshest_replica(shard, &symbol) {
                    self.switch_source(&symbol, next)?;
                }
                // With no live replica, the next snapshot to arrive is adopted.
            }
        }
        Ok(())
    }

    /// The sibling that holds a book and heard about the symbol most recently.
    fn freshest_replica(&self, shard: usize, symbol: &str) -> Option<usize> {
        self.shards[shard]
            .conns
            .iter()
            .copied()
            .filter(|&c| self.books.contains_key(&(c, symbol.to_owned())))
            .max_by_key(|&c| self.last_seen.get(&(c, symbol.to_owned())).copied())
    }

    fn switch_source(&mut self, symbol: &str, conn: usize) -> anyhow::Result<()> {
        self.source.insert(symbol.to_owned(), conn);
        self.failovers += 1;
        self.write_snapshot(conn, symbol, Utc::now())
    }

    /// If a sibling heard about a symbol much more recently than its source,
    /// the source is probably delayed or silently dead: fail over.
    fn check_lag(&mut self) -> anyhow::Result<()> {
        let mut switches = Vec::new();
        for (symbol, &source) in &self.source {
            let Some(&shard) = self.shard_of.get(symbol) else {
                continue;
            };
            let Some(best) = self.freshest_replica(shard, symbol) else {
                continue;
            };
            let seen = |c: usize| self.last_seen.get(&(c, symbol.clone())).copied();
            if let (Some(best_seen), Some(source_seen)) = (seen(best), seen(source))
                && best != source
                && best_seen.duration_since(source_seen) > self.cfg.lag
            {
                switches.push((symbol.clone(), best));
            }
        }
        for (symbol, conn) in switches {
            tracing::info!(%symbol, conn, "source lagging, failing over");
            self.switch_source(&symbol, conn)?;
        }
        Ok(())
    }

    fn snapshot_all(&mut self) -> anyhow::Result<()> {
        let sources: Vec<(String, usize)> =
            self.source.iter().map(|(s, &c)| (s.clone(), c)).collect();
        let now = Utc::now();
        for (symbol, conn) in sources {
            self.write_snapshot(conn, &symbol, now)?;
        }
        Ok(())
    }

    fn write_snapshot(
        &mut self,
        conn: usize,
        symbol: &str,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let Some(book) = self.books.get(&(conn, symbol.to_owned())) else {
            return Ok(());
        };
        let (bids, asks) = book.levels();
        self.writer.write_msg(&SnapshotDomain {
            received_at,
            exchange_timestamp: book.exchange_ts.unwrap_or(received_at),
            bids: bids
                .into_iter()
                .map(|(price_level, quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(price_level, quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        })?;
        self.written += 1;
        Ok(())
    }
}
