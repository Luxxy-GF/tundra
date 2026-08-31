use super::{GetNodeAuth, NodeAuth, State};
use crate::routes::GetState;
use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
    routing::any,
};
use futures::{SinkExt, StreamExt};
use std::time::{SystemTime, UNIX_EPOCH};
use tundra_common::{
    state::Snapshot,
    sync::{NodeMsg, RemoteMsg},
};

pub async fn handle_ws(
    axum::extract::State(state): GetState,
    axum::extract::Extension(NodeAuth(node)): GetNodeAuth,
    ws: WebSocketUpgrade,
) -> Response {
    // the 64 MiB default would let a buggy node pin that much before the parse even fails
    ws.max_message_size(1024 * 1024)
        .on_upgrade(move |socket| node_ws_task(socket, state, node))
}

async fn node_ws_task(socket: WebSocket, state: State, node: uuid::Uuid) {
    let mut registration = state.hub.register(node);
    let conn_id = registration.id;
    if let Err(err) = state.database.touch_node(&node, now_secs()) {
        tracing::debug!(
            node = %node,
            "failed to record the node heartbeat: {:?}",
            err
        );
    }
    tracing::info!(
        node = %node,
        conn_id,
        "node websocket connected"
    );

    let (mut sink, mut stream) = socket.split();

    let first = state
        .snapshot()
        .map(|s| RemoteMsg::Snapshot { snapshot: s })
        .ok();
    if let Some(msg) = first
        && send(&mut sink, &msg).await.is_err()
    {
        state.hub.unregister(&node, conn_id);
        return;
    }

    registration.snapshots.mark_unchanged();

    loop {
        tokio::select! {
            outgoing = registration.commands.recv() => match outgoing {
                Some(msg) => {
                    if send(&mut sink, &msg).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            changed = registration.snapshots.changed() => {
                if changed.is_err() {
                    break;
                }
                let latest = registration.snapshots.borrow_and_update().clone();
                if let Some(snapshot) = latest {
                    let msg = RemoteMsg::Snapshot { snapshot: Snapshot::clone(&snapshot) };
                    if send(&mut sink, &msg).await.is_err() {
                        break;
                    }
                }
            },
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<NodeMsg>(&text) {
                    Ok(msg) => state.hub.deliver(msg),
                    Err(err) => tracing::warn!(
                        node = %node,
                        "failed to parse a node message: {:?}",
                        err
                    ),
                },
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(err)) => {
                    tracing::warn!(
                        node = %node,
                        "node websocket failed: {:?}",
                        err
                    );
                    break;
                }
            },
        }
    }

    state.hub.unregister(&node, conn_id);
    if let Err(err) = state.database.touch_node(&node, now_secs()) {
        tracing::debug!(
            node = %node,
            "failed to record the node heartbeat: {:?}",
            err
        );
    }
    tracing::info!(
        node = %node,
        conn_id,
        "node websocket closed"
    );
}

#[inline]
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

async fn send(
    sink: &mut futures::stream::SplitSink<WebSocket, Message>,
    msg: &RemoteMsg,
) -> Result<(), ()> {
    let text = serde_json::to_string(msg).map_err(|_| ())?;
    sink.send(Message::Text(text.into())).await.map_err(|_| ())
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", any(handle_ws))
        .with_state(state.clone())
}
