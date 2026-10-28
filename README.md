# Voluntas engineering challenge: Polymarket order-book collector

Connects to Polymarket, subscribes to every live CLOB token (one symbol per
outcome), keeps an order book per symbol, and writes order-book deltas plus a
full snapshot of every book each minute in the template's FIX SBE format
(`AlignedSbeWriter`). Discovery re-runs every minute: new symbols are
subscribed, and symbols that stop trading are unsubscribed. Every symbol set
streams on **two redundant websockets**. When the source socket drops or lags,
the collector fails over to the sibling and writes a snapshot.

Polymarket was chosen because it needs no API key (the spec allows it).

## Run

```sh
cargo run --release -- out.sbe        # or no argument for /dev/stdout
# Ctrl-C: closes every websocket, flushes, closes the file, exits 0
cargo run --release --bin verify -- out.sbe
```

Logs go to stderr (`RUST_LOG` is not needed; INFO by default). Environment:

| var | default | meaning |
|---|---|---|
| `REPLICAS` | 2 | redundant websockets per symbol shard |
| `LAG_MS` | 3000 | fail over when a sibling's book has exchange data this much newer than the source's |
| `SNAPSHOT_SECS` | 60 | full snapshot cadence |
| `DISCOVER_SECS` | 60 | discovery cadence |

## Architecture

```
 discovery task (HTTP, Gamma keyset paging) --Page/Complete--> +-----------------+
                                                               |     Engine      |
 shard 0: socket 0 --+                                         | (single writer) |
          socket 1 --+--- SocketMsg::{Up,Down,Events} -------> | books[(sock,sym)]
 shard 1: socket 2 --+     (unbounded mpsc,                    | source[sym]     |--> AlignedSbeWriter
          socket 3 --+      received_at stamped                | lag check 1s    |    (file / stdout)
   ...                      before parsing)                    | snapshots 60s   |
          <--- Command::{Subscribe,Unsubscribe,Shutdown} ----- +-----------------+
```

- `collector/exchange.rs`: the `Exchange` trait and the exchange-neutral `MarketEvent`. It is the only exchange-specific surface (Ext 3).
- `collector/polymarket.rs`: discovery, subscribe encoding and frame decoding, with unit tests on sample frames.
- `collector/fixed.rs`: exact decimal-string to fixed-point conversion, using no floats.
- `collector/book.rs`: `BTreeMap` books. Snapshot bids are written best-first descending, asks ascending.
- `collector/connection.rs`: one websocket task. It reconnects with exponential backoff (250ms to 10s) and re-subscribes its whole set. It sends PING every 10s, reconnects after 30s with no frames, and sends a Close frame on shutdown.
- `collector/engine.rs`: sharding (least-loaded shard; a new shard of `REPLICAS` sockets once every shard holds 400 symbols), per-replica books, source selection, failover, timers and SIGINT.
- `bin/verify.rs`: rebuilds the books from an output file and reports integrity stats.

Only the engine task touches the books and the writer, so the hot path takes no locks.

## Correctness: how each failure is handled

- **Socket drops.** The connection reports `Down`, and the engine discards that replica's books. A book is never patched with deltas it may have missed. Every symbol that socket sourced moves to the sibling with the freshest book, and the engine **writes a snapshot on the switch**. The dropped socket reconnects, re-subscribes, and its book is rebuilt from the fresh `book` event Polymarket sends on subscribe.
- **Silent, delayed or rate-limited socket.** Every second, the engine compares the exchange timestamp of each replica's book. If a sibling holds data more than `LAG_MS` newer than the source, it switches source and writes a snapshot. Exchange timestamps are identical across replicas for the same update, so quiet markets never trigger a false failover. An earlier version compared local receive times, and the live run exposed false failovers on quiet symbols. A socket with no frames for 30s is reconnected.
- **Deltas without a book.** These are dropped: a `Level` is applied only to a replica that already holds a snapshot.
- **Crash or restart.** On startup, every subscription produces a `book` event, so the output starts with a snapshot per symbol.
- **Malformed data.** Nothing on the network path panics. Bad frames and events are logged and skipped. Each event decodes all-or-nothing, so a bad level never half-applies a `price_change`.
- **Time accuracy.** `received_at`/`backtest_timestamp` is stamped the moment the frame is read, before parsing. `exchange_timestamp` is Polymarket's millisecond timestamp.

### Doc differences found while building

- Gamma `/markets?offset=` now rejects deep offsets ("offset too large, use /markets/keyset"). Discovery therefore uses `/markets/keyset` with `after_cursor`, at 100 per page. A full pass takes minutes for over 50k tokens, so pages are streamed to the engine as they arrive, and subscribing starts within a second. Removals are applied only after a complete pass, so a partial pass never unsubscribes live symbols.
- The docs give no per-connection asset limit, so shards are capped at 400.

## Output buffering trade-off

`AlignedSbeReader::open` reads only 64 MiB chunks, and `flush()` pads to a full chunk. The writer therefore uses 64 MiB chunks and flushes only when a chunk fills and at shutdown. Flushing more often would waste up to 64 MiB on disk per flush. The cost is that a hard crash (SIGKILL, power loss) loses the unflushed tail. In production I would rotate files on a timer, or use a smaller-chunk reader.

## Extensions

| # | Extension | Status |
|---|---|---|
| 1 | Redundant websockets, snapshot on source switch | **Done.** 2 replicas per shard; failover on disconnect and on exchange-time lag |
| 2 | Subscription monitoring | **Partial.** Stale-socket reconnect (30s silence). Not done: per-symbol rate tracking with resubscribe, and scheduled recycle |
| 3 | Generic encode/decode | **Done in structure.** Engine, connection and book are exchange-agnostic behind `Exchange`. No second adapter (no Kalshi key) |
| 4 | Latency | **Not done.** Out of time. `verify` reports receive-minus-exchange percentiles as the baseline measurement |

Not done, because of the deadline: a fake-exchange integration test (scripted drops, silence and delay, with output asserted through `AlignedSbeReader`), per-symbol resubscribe, scheduled recycle, the `best_bid`/`best_ask` cross-check, and LATENCY.md.

## What I would do next in production

1. An integration test against a local fake exchange, which is the strongest evidence of failover correctness.
2. Resubscribe a symbol when it goes quiet on one replica while its sibling is live, plus EWMA rate baselines and staggered scheduled recycles.
3. Cross-check Polymarket's `best_bid`/`best_ask` and `hash` against our book, and resubscribe on mismatch. Use sequence-gap detection on exchanges that provide it (Kalshi `seq`).
4. Output rotation and persistence, metrics and alerts (failovers, backlog, reconnects), and redundancy across processes and hosts.

## Live run

A 150-second live run (Oct 27, 2026) was stopped with SIGINT. It exited 0 in under 1s, and the output was fully readable by `AlignedSbeReader`:

```
symbols:                     78000
snapshots:                   199484
price level updates:         1030190
updates before any snapshot: 0
crossed-book updates:        6
non-monotonic exchange ts:   538
max snapshot gap (s):        60
recv - exchange latency ms:  p50 24 p90 323 p99 2274
```

The run peaked at 390 websockets with channel backlog in the low thousands, and it drained.

How to read these numbers:
- This run used the build *before* the lag-failover fix (commit `9ab6be0`). Its 9,530 failovers are mostly the false positives described above. Every one of them still wrote a correct snapshot, so the data stays consistent, just with extra snapshots.
- **Non-monotonic timestamps** (0.05%) are expected around source switches. A replica's book can be a few ms behind or ahead, and the switch snapshot re-bases the book.
- **Crossed updates** (6) are transient states inside a multi-level `price_change` that is applied one level at a time. The `best_bid`/`best_ask` cross-check in next steps would confirm this.
- The **latency tail** is inflated by the startup burst: 78k subscriptions in about 90s. A steady-state measurement belongs in LATENCY.md.
