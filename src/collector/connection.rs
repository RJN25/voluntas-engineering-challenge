//! One websocket. Owns its symbol set, reconnects with backoff and
//! re-subscribes everything, pings, and reconnects if the socket goes quiet.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, interval, sleep};
use tokio_tungstenite::tungstenite::Message;

use super::exchange::{Exchange, MarketEvent};

const PING_EVERY: Duration = Duration::from_secs(10);
const STALE_AFTER: Duration = Duration::from_secs(30);
const BACKOFF_MIN: Duration = Duration::from_millis(250);
const BACKOFF_MAX: Duration = Duration::from_secs(10);

pub enum Command {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Shutdown,
}

pub enum SocketMsg {
    Up(usize),
    /// Books held for this socket are no longer trustworthy.
    Down(usize),
    Events {
        conn: usize,
        received_at: DateTime<Utc>,
        events: Vec<MarketEvent>,
    },
}

enum Exit {
    Shutdown,
    Reconnect(String),
}

pub async fn run<E: Exchange>(
    id: usize,
    exchange: Arc<E>,
    mut cmds: UnboundedReceiver<Command>,
    out: UnboundedSender<SocketMsg>,
) {
    let mut symbols = BTreeSet::new();
    let mut backoff = BACKOFF_MIN;
    loop {
        // Nothing to subscribe to yet: wait instead of holding an idle socket.
        while symbols.is_empty() {
            match cmds.recv().await {
                Some(Command::Subscribe(s)) => symbols.extend(s),
                Some(Command::Unsubscribe(_)) => {}
                Some(Command::Shutdown) | None => return,
            }
        }
        let started = Instant::now();
        let exit = session(id, &*exchange, &mut symbols, &mut cmds, &out).await;
        let _ = out.send(SocketMsg::Down(id));
        match exit {
            Exit::Shutdown => return,
            Exit::Reconnect(why) => tracing::warn!(conn = id, "reconnecting: {why}"),
        }
        if started.elapsed() > Duration::from_secs(60) {
            backoff = BACKOFF_MIN;
        }
        // Commands keep flowing while we back off so the symbol set stays current.
        let wake = sleep(backoff);
        tokio::pin!(wake);
        loop {
            tokio::select! {
                _ = &mut wake => break,
                cmd = cmds.recv() => match cmd {
                    Some(Command::Subscribe(s)) => symbols.extend(s),
                    Some(Command::Unsubscribe(s)) => s.iter().for_each(|x| { symbols.remove(x); }),
                    Some(Command::Shutdown) | None => return,
                },
            }
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn session<E: Exchange>(
    id: usize,
    exchange: &E,
    symbols: &mut BTreeSet<String>,
    cmds: &mut UnboundedReceiver<Command>,
    out: &UnboundedSender<SocketMsg>,
) -> Exit {
    let (mut ws, _) = match tokio_tungstenite::connect_async(exchange.ws_url()).await {
        Ok(ok) => ok,
        Err(e) => return Exit::Reconnect(format!("connect: {e}")),
    };
    let all: Vec<String> = symbols.iter().cloned().collect();
    if let Err(e) = ws
        .send(Message::text(exchange.initial_subscribe(&all)))
        .await
    {
        return Exit::Reconnect(format!("subscribe: {e}"));
    }
    tracing::info!(conn = id, symbols = all.len(), "connected");
    let _ = out.send(SocketMsg::Up(id));

    let mut ping = interval(PING_EVERY);
    let mut last_frame = Instant::now();
    loop {
        tokio::select! {
            frame = ws.next() => {
                // Stamp before any parsing: this is our receive time.
                let received_at = Utc::now();
                last_frame = Instant::now();
                let text = match frame {
                    Some(Ok(Message::Text(t))) => t,
                    Some(Ok(Message::Close(c))) => return Exit::Reconnect(format!("closed by server: {c:?}")),
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => return Exit::Reconnect(format!("read: {e}")),
                    None => return Exit::Reconnect("stream ended".into()),
                };
                let mut events = Vec::new();
                exchange.decode(&text, &mut events);
                if !events.is_empty() {
                    let _ = out.send(SocketMsg::Events { conn: id, received_at, events });
                }
            }
            _ = ping.tick() => {
                if last_frame.elapsed() > STALE_AFTER {
                    return Exit::Reconnect("stale: no frames".into());
                }
                if let Some(p) = exchange.ping() && let Err(e) = ws.send(Message::text(p)).await {
                    return Exit::Reconnect(format!("ping: {e}"));
                }
            }
            cmd = cmds.recv() => {
                let msg = match cmd {
                    Some(Command::Subscribe(s)) => {
                        let new: Vec<String> = s.into_iter().filter(|x| symbols.insert(x.clone())).collect();
                        if new.is_empty() { continue; }
                        exchange.subscribe(&new)
                    }
                    Some(Command::Unsubscribe(s)) => {
                        s.iter().for_each(|x| { symbols.remove(x); });
                        exchange.unsubscribe(&s)
                    }
                    Some(Command::Shutdown) | None => {
                        let _ = ws.close(None).await;
                        return Exit::Shutdown;
                    }
                };
                if let Err(e) = ws.send(Message::text(msg)).await {
                    return Exit::Reconnect(format!("send: {e}"));
                }
            }
        }
    }
}
