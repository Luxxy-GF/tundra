use super::State;
use axum::{Router, routing::get};

mod _server_;

mod get {
    use crate::routes::{ApiError, GetState};
    use axum::Json;
    use serde_json::json;

    pub async fn route(state: GetState) -> Result<Json<serde_json::Value>, ApiError> {
        Ok(Json(json!(state.snapshot()?.servers)))
    }
}

mod post {
    use crate::routes::{ApiError, GetState};
    use axum::Json;
    use serde::Deserialize;
    use tundra_common::state::PortSpec;

    #[derive(Deserialize)]
    pub struct Payload {
        node_uuid: uuid::Uuid,
        name: String,
        #[serde(default)]
        aliases: Vec<String>,
        container_ref: String,

        ports: Vec<PortSpec>,
        #[serde(default)]
        dial_addr: Option<String>,
    }

    pub async fn route(
        state: GetState,
        Json(data): Json<Payload>,
    ) -> Result<Json<serde_json::Value>, ApiError> {
        let server = state
            .database
            .create_server(
                &data.node_uuid,
                &data.name,
                &data.aliases,
                &data.container_ref,
                data.dial_addr.as_deref(),
                &data.ports,
            )
            .map_err(|err| ApiError::BadRequest(err.to_string()))?;
        state.publish()?;
        tracing::info!(
            server = %server.uuid,
            idx = server.idx,
            name = %server.name,
            "server created"
        );

        Ok(Json(serde_json::to_value(server).unwrap_or_else(|err| {
            tracing::error!("failed to serialize the server view: {:?}", err);
            serde_json::Value::Null
        })))
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", get(get::route).post(post::route))
        .nest("/{server}", _server_::router(state))
        .with_state(state.clone())
}
