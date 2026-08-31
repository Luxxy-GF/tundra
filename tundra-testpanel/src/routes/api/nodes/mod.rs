use super::State;
use axum::{Router, routing::get};

mod _node_;

mod get {
    use crate::routes::{ApiError, GetState};
    use axum::Json;
    use serde::Serialize;
    use tundra_common::hash::Hash32;

    #[derive(Serialize)]
    pub struct Response {
        uuid: uuid::Uuid,
        name: String,
        host: String,
        tunnel_port: u16,

        cert_sha256: Option<Hash32>,
        online: bool,
        last_seen: Option<i64>,
    }

    pub async fn route(state: GetState) -> Result<Json<Vec<Response>>, ApiError> {
        let online = state.hub.online();
        let rows = state.database.list_nodes()?;
        Ok(Json(
            rows.into_iter()
                .map(|n| Response {
                    online: online.contains(&n.uuid),
                    uuid: n.uuid,
                    name: n.name,
                    host: n.host,
                    tunnel_port: n.tunnel_port,
                    cert_sha256: n.cert_sha256,
                    last_seen: n.last_seen,
                })
                .collect(),
        ))
    }
}

mod post {
    use crate::routes::{ApiError, GetState};
    use axum::Json;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize)]
    pub struct Payload {
        name: String,
        host: String,
        tunnel_port: u16,
    }

    pub async fn route(
        state: GetState,
        Json(data): Json<Payload>,
    ) -> Result<Json<serde_json::Value>, ApiError> {
        if data.name.trim().is_empty() || data.host.trim().is_empty() {
            return Err(ApiError::BadRequest("name and host are required".into()));
        }

        let (uuid, token) = state
            .database
            .create_node(&data.name, &data.host, data.tunnel_port)?;
        state.publish()?;
        tracing::info!(
            node = %uuid,
            name = %data.name,
            "node created"
        );

        Ok(Json(json!({ "uuid": uuid, "node_token": token })))
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", get(get::route).post(post::route))
        .nest("/{node}", _node_::router(state))
        .with_state(state.clone())
}
