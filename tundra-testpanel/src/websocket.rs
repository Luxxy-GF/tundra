use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use tundra_common::{
    state::Snapshot,
    sync::{NodeMsg, RemoteMsg},
};

const OUTBOX_DEPTH: usize = 8;
const METRICS_TIMEOUT: Duration = Duration::from_secs(5);

struct Conn {
    id: u64,
    commands: mpsc::Sender<RemoteMsg>,
    snapshots: watch::Sender<Option<Arc<Snapshot>>>,
}

#[derive(Default)]
pub struct Hub {
    conns: parking_lot::Mutex<HashMap<uuid::Uuid, Conn>>,
    pending: parking_lot::Mutex<HashMap<(uuid::Uuid, u64), oneshot::Sender<serde_json::Value>>>,
    counter: AtomicU64,
}

impl Hub {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, node: uuid::Uuid) -> Registration {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        let (commands, command_rx) = mpsc::channel(OUTBOX_DEPTH);
        let (snapshots, snapshot_rx) = watch::channel(None);

        if let Some(old) = self.conns.lock().insert(
            node,
            Conn {
                id,
                commands,
                snapshots,
            },
        ) {
            tracing::info!(
                node = %node,
                old_conn = old.id,
                "replacing an existing node websocket"
            );
        }

        Registration {
            id,
            commands: command_rx,
            snapshots: snapshot_rx,
        }
    }

    // a reconnect that raced ahead of the old socket's teardown must not be evicted by it
    #[inline]
    pub fn unregister(&self, node: &uuid::Uuid, id: u64) {
        let mut conns = self.conns.lock();
        if conns.get(node).is_some_and(|c| c.id == id) {
            conns.remove(node);
        }
    }

    #[inline]
    pub fn disconnect(&self, node: &uuid::Uuid) {
        self.conns.lock().remove(node);
    }

    #[inline]
    pub fn online(&self) -> Vec<uuid::Uuid> {
        self.conns.lock().keys().copied().collect()
    }

    // each snapshot carries the full state, so only the newest matters
    pub fn broadcast(&self, snapshot: &Arc<Snapshot>) {
        let conns = self.conns.lock();
        for conn in conns.values() {
            let _ = conn.snapshots.send(Some(Arc::clone(snapshot)));
        }
    }

    pub async fn request_metrics(
        &self,
        node: &uuid::Uuid,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let req_id = self.counter.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        let sender = {
            let conns = self.conns.lock();
            conns
                .get(node)
                .map(|c| c.commands.clone())
                .ok_or_else(|| anyhow::anyhow!("node {node} is not connected"))?
        };

        self.pending.lock().insert((*node, req_id), tx);

        let sent = sender.try_send(RemoteMsg::MetricsRequest { req_id });
        if let Err(err) = sent {
            self.pending.lock().remove(&(*node, req_id));

            return Err(anyhow::anyhow!("node {node} outbox is full: {err}"));
        }

        match tokio::time::timeout(METRICS_TIMEOUT, rx).await {
            Ok(Ok(value)) => Ok(value),
            other => {
                self.pending.lock().remove(&(*node, req_id));

                Err(anyhow::anyhow!("no metrics reply from {node}: {other:?}"))
            }
        }
    }

    pub fn deliver(&self, node: &uuid::Uuid, msg: NodeMsg) {
        let NodeMsg::Metrics { req_id, body } = msg;
        if let Some(tx) = self.pending.lock().remove(&(*node, req_id)) {
            let _ = tx.send(body);
        }
    }
}

pub struct Registration {
    pub id: u64,
    pub commands: mpsc::Receiver<RemoteMsg>,
    pub snapshots: watch::Receiver<Option<Arc<Snapshot>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tundra_common::hash::Hash32;

    fn snapshot(epoch: u64) -> Arc<Snapshot> {
        Arc::new(Snapshot {
            epoch,
            jwt_pubkey: Hash32([0; 32]),
            nodes: vec![],
            servers: vec![],
            acls: vec![],
        })
    }

    // Hub

    #[test]
    fn snapshots_reach_every_connected_node() {
        tokio_test::block_on(async {
            let hub = Hub::new();
            let a = uuid::Uuid::from_u128(1);
            let b = uuid::Uuid::from_u128(2);
            let mut ra = hub.register(a);
            let mut rb = hub.register(b);

            hub.broadcast(&snapshot(5));

            for rx in [&mut ra, &mut rb] {
                let latest = rx.snapshots.borrow_and_update().clone().unwrap();
                assert_eq!(latest.epoch, 5);
            }
        });
    }

    #[test]
    fn slow_nodes_fall_behind_by_state_and_are_never_evicted() {
        tokio_test::block_on(async {
            let hub = Hub::new();
            let node = uuid::Uuid::from_u128(1);
            let mut reg = hub.register(node);

            for epoch in 1..=(OUTBOX_DEPTH as u64 * 10) {
                hub.broadcast(&snapshot(epoch));
            }
            assert_eq!(hub.online(), [node]);

            let latest = reg.snapshots.borrow_and_update().clone().unwrap();
            assert_eq!(latest.epoch, OUTBOX_DEPTH as u64 * 10);
        });
    }

    #[test]
    fn pending_metrics_requests_cannot_cost_a_node_its_socket() {
        // otherwise the METRICS_TIMEOUT of every unanswered request is real wall clock
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();

        rt.block_on(async {
            let hub = Hub::new();
            let node = uuid::Uuid::from_u128(1);
            let _reg = hub.register(node);

            for _ in 0..OUTBOX_DEPTH * 2 {
                let _ = hub.request_metrics(&node).await;
            }
            assert_eq!(hub.online(), [node]);

            hub.broadcast(&snapshot(3));
            assert_eq!(hub.online(), [node]);
        });
    }

    #[test]
    fn registering_twice_replaces_the_previous_socket() {
        tokio_test::block_on(async {
            let hub = Hub::new();
            let node = uuid::Uuid::from_u128(1);
            let mut old = hub.register(node);
            let new = hub.register(node);

            assert_ne!(old.id, new.id);
            assert_eq!(hub.online(), [node]);
            assert!(old.commands.recv().await.is_none());

            hub.unregister(&node, old.id);
            assert_eq!(hub.online(), [node]);

            hub.unregister(&node, new.id);
            assert!(hub.online().is_empty());
        });
    }

    #[test]
    fn metrics_requests_correlate_replies_and_time_out_when_unanswered() {
        tokio_test::block_on(async {
            let hub = Arc::new(Hub::new());
            let node = uuid::Uuid::from_u128(1);
            let mut reg = hub.register(node);

            let task = tokio::spawn({
                let hub = Arc::clone(&hub);

                async move { hub.request_metrics(&node).await }
            });

            let req_id = match reg.commands.recv().await.unwrap() {
                RemoteMsg::MetricsRequest { req_id } => Some(req_id),
                RemoteMsg::Snapshot { .. } => None,
            }
            .unwrap();

            hub.deliver(
                &node,
                NodeMsg::Metrics {
                    req_id,
                    body: serde_json::json!({ "ok": true }),
                },
            );

            assert_eq!(
                task.await.unwrap().unwrap(),
                serde_json::json!({ "ok": true })
            );

            hub.deliver(
                &node,
                NodeMsg::Metrics {
                    req_id: 999,
                    body: serde_json::Value::Null,
                },
            );
        });
    }

    #[test]
    fn a_reply_from_another_node_cannot_answer_a_metrics_request() {
        tokio_test::block_on(async {
            let hub = Arc::new(Hub::new());
            let node = uuid::Uuid::from_u128(1);
            let other = uuid::Uuid::from_u128(2);
            let mut reg = hub.register(node);

            let task = tokio::spawn({
                let hub = Arc::clone(&hub);

                async move { hub.request_metrics(&node).await }
            });

            let req_id = match reg.commands.recv().await.unwrap() {
                RemoteMsg::MetricsRequest { req_id } => Some(req_id),
                RemoteMsg::Snapshot { .. } => None,
            }
            .unwrap();

            hub.deliver(
                &other,
                NodeMsg::Metrics {
                    req_id,
                    body: serde_json::json!({ "forged": true }),
                },
            );
            hub.deliver(
                &node,
                NodeMsg::Metrics {
                    req_id,
                    body: serde_json::json!({ "ok": true }),
                },
            );

            assert_eq!(
                task.await.unwrap().unwrap(),
                serde_json::json!({ "ok": true })
            );
        });
    }

    #[test]
    fn metrics_for_an_offline_node_fail_immediately() {
        tokio_test::block_on(async {
            let hub = Hub::new();
            assert!(
                hub.request_metrics(&uuid::Uuid::from_u128(9))
                    .await
                    .is_err()
            );
        });
    }
}
