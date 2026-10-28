# Polymarket order-book collector

My submission for the Voluntas engineering challenge, by Arjan Waraich.

## 1. What it does

The collector connects to Polymarket and subscribes to every live CLOB token. Each outcome of a market is its own symbol. It keeps an order book per symbol and writes two kinds of messages in the template's FIX SBE format, using `AlignedSbeWriter`:

- a `PriceLevelUpdate` for every change to a level
- a full `Snapshot` of every book once a minute

Discovery runs again every minute. New symbols are subscribed, and symbols that have stopped trading are unsubscribed and dropped.

Each set of symbols streams on two redundant websockets. If the socket we are writing from drops, or falls behind its twin, the collector switches to the twin and writes a snapshot so the output stays consistent.

I built on Polymarket because it needs no API key, which the challenge explicitly allows. Everything outside the exchange adapter is exchange-agnostic, so a Kalshi adapter would plug in behind the same trait.

## 2. How to run

```sh
cargo run --release -- out.sbe                # writes to out.sbe
cargo run --release                           # writes to /dev/stdout
cargo run --release --bin verify -- out.sbe   # rebuilds the books and prints integrity stats
```

Ctrl-C closes every websocket with a Close frame, flushes the writer, closes the file and exits 0. All logs go to stderr, so stdout carries only SBE data.

| Variable | Default | Meaning |
|---|---|---|
| `REPLICAS` | 2 | Redundant websockets per shard of symbols |
| `LAG_MS` | 3000 | Fail over when a twin's book holds exchange data this much newer than the source's |
| `SNAPSHOT_SECS` | 60 | How often a full snapshot is written for every symbol |
| `DISCOVER_SECS` | 60 | How often discovery runs |

## 3. Architecture

```
 discovery task (HTTP, Gamma keyset paging) --Page/Complete--> +-------------------+
                                                               |      Engine       |
 shard 0: socket 0 --+                                         |  (single writer)  |
          socket 1 --+--- SocketMsg::{Up,Down,Events} -------> |  books[(sock,sym)]|
 shard 1: socket 2 --+     unbounded mpsc; received_at         |  source[sym]      |--> AlignedSbeWriter
          socket 3 --+     stamped before parsing              |  lag check 1s     |    (file or stdout)
   ...                                                         |  snapshots 60s    |
          <------ Command::{Subscribe,Unsubscribe,Shutdown} -- +-------------------+
```

| File | Role |
|---|---|
| `collector/exchange.rs` | The `Exchange` trait and the exchange-neutral `MarketEvent`. This is the only exchange-specific surface |
| `collector/polymarket.rs` | Discovery, subscription messages and frame decoding, with unit tests on sample frames |
| `collector/fixed.rs` | Exact decimal-string to fixed-point conversion with no floats, since `f64` turns "0.29" into 289,999 |
| `collector/book.rs` | `BTreeMap` books. Snapshots list bids best-first (descending) and asks best-first (ascending) |
| `collector/connection.rs` | One websocket task, described below |
| `collector/engine.rs` | Sharding, the per-socket books, choosing the source socket, failover, timers and shutdown |
| `bin/verify.rs` | Reads an output file back and reports integrity statistics |

Each connection task owns its symbol set. It reconnects with exponential backoff (250ms up to 10s) and re-subscribes everything when it does. It sends a PING every 10 seconds and reconnects after 30 seconds with no frames.

Sharding fills the least-loaded shard first and opens a new shard of `REPLICAS` sockets once every shard holds 400 symbols. Polymarket's docs give no per-connection limit, so 400 is a conservative cap.

Only the engine task touches the books and the writer, so the hot path needs no locks. Parsing, the expensive part, runs in parallel inside the connection tasks.

## 4. Correctness: how each failure is handled

The output has to be complete, because a missed delete leaves a phantom level in a backtest. It also has to be time-accurate. These rules protect both:

- **A socket drops.** Its books are thrown away rather than patched, because we can't know which deltas it missed. Every symbol it was sourcing switches to the twin with the freshest book, and the engine writes a snapshot at the switch. The dropped socket reconnects and re-subscribes, and Polymarket sends it a fresh `book` for every symbol.
- **A socket goes silent, or is delayed or rate-limited.** Every second, the engine compares the exchange timestamps held in each replica's book. If the twin holds data more than `LAG_MS` newer than the source, the engine switches to it and writes a snapshot. The same update carries the same exchange timestamp on both sockets, so a quiet market cannot trigger a false switch. A socket that receives nothing for 30 seconds is reconnected.
- **A delta arrives before any snapshot.** It is ignored. Deltas are only ever applied on top of a snapshot.
- **The process crashes or restarts.** Every subscription answers with a full `book`, so a new run starts with a snapshot of every symbol.
- **The exchange sends malformed data.** Nothing on the network path can panic. Bad frames are logged and skipped. Each event is decoded all-or-nothing, so one bad level never half-applies a `price_change`.
- **Timestamps.** `received_at` and `backtest_timestamp` are stamped the instant a frame is read, before any parsing. `exchange_timestamp` is Polymarket's own millisecond timestamp.

**One lesson from the live run.** My first lag check compared local receive times. On a quiet market, the second socket can connect a few seconds after the first and receive its snapshot later. That looked like lag and caused false failovers. Comparing exchange time fixed it, since only genuinely newer data can trigger a switch.

**Where Polymarket's docs differ from reality**
- Gamma's `/markets?offset=` now rejects deep offsets with "offset too large, use /markets/keyset". Discovery therefore pages through `/markets/keyset` with `after_cursor`, 100 markets at a time.
- A full pass over about 78,000 tokens takes a few minutes. So each page is handed to the engine as it arrives, and subscribing starts within a second.
- Removals happen only after a complete pass, so a partial pass can never unsubscribe live symbols.

## 5. Extensions

| # | Extension | Status |
|---|---|---|
| 1 | Redundant websockets, with a snapshot on every source switch | **Done.** Two sockets per shard; failover on disconnect and on exchange-time lag |
| 2 | Subscription monitoring and scheduled reconnects | **Partial.** Silent sockets are reconnected after 30s. Per-symbol rate tracking with resubscribe and scheduled recycling are not built yet |
| 3 | Exchange-agnostic core | **Done.** Engine, connections and books depend only on the `Exchange` trait. I had no Kalshi key, so there is no second adapter |
| 4 | Latency experiments | **Not started.** `verify` reports receive-minus-exchange percentiles as a baseline |

I prioritized a correct, working core and the most important extension over breadth. I would rather hand in less that I can fully defend than more that I can't.

## 6. Trade-offs and what I would do next

**Output buffering.** `AlignedSbeReader::open` reads only 64 MiB chunks, and `flush()` pads to a full chunk, so the writer uses 64 MiB chunks. It flushes when a chunk fills and at shutdown. Flushing more often would write up to 64 MiB of padding each time. The cost is that a hard kill (SIGKILL or power loss) loses the unflushed tail.

**What I would do next, in order:**
1. **Fake-exchange integration test.** A local websocket server that scripts drops, silence and delays. It would assert that the output is complete, that a snapshot is written on every failover, and that the file is readable after shutdown. This is the strongest evidence that failover is correct.
2. **Per-symbol monitoring.** Resubscribe a symbol that has gone quiet on one socket while its twin is live. Track per-symbol message-rate baselines, and recycle sockets on a staggered schedule.
3. **Book cross-checks.** Compare Polymarket's `best_bid`/`best_ask` and `hash` against our book, and resubscribe on a mismatch. On exchanges with sequence numbers, such as Kalshi's `seq`, treat a gap as data loss.
4. **Production hardening.** Output-file rotation on a timer, metrics and alerts (failovers, reconnects, backlog), and redundant processes on separate hosts.
5. **LATENCY.md.** Race many connections and keep the fastest, try different source ports, and check DNS and anycast resolution.

## 7. Live run

The collector ran live for 150 seconds on Oct 27, 2026 and was stopped with Ctrl-C. It exited 0 in under a second, and `AlignedSbeReader` read the entire 268 MB output back:

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

The run peaked at 380 websockets. The engine's channel backlog peaked around a thousand messages and drained.

How to read these numbers:
- **This run used the build from before the lag fix** in section 4, so its 9,530 failovers are mostly false positives. Each one still wrote a correct snapshot, so the data stayed consistent; it just contains extra snapshots.
- **Updates before any snapshot: 0** and **max snapshot gap: 60s** are the two key completeness checks, and both pass.
- **Non-monotonic timestamps (0.05%)** cluster around source switches, where the two sockets' books can be a few milliseconds apart.
- **Crossed books (6)** look like transient states inside a multi-level `price_change` that is applied one level at a time. The `best_bid`/`best_ask` cross-check in section 6 would confirm this.
- **The latency tail** is inflated by the startup burst of 78,000 subscriptions in about 90 seconds. The median, 24ms, better reflects steady state.
