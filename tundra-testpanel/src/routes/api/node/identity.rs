use super::State;
use axum::{Router, routing::get};

mod get {
    use crate::routes::{
        ApiError, GetState,
        api::node::{GetNodeAuth, NodeAuth},
    };
    use axum::Json;
    use serde_json::json;

    pub async fn route(
        state: GetState,
        axum::extract::Extension(NodeAuth(node)): GetNodeAuth,
    ) -> Result<Json<serde_json::Value>, ApiError> {
        let cert = state
            .database
            .list_nodes()?
            .into_iter()
            .find(|n| n.uuid == node)
            .ok_or(ApiError::Unauthorized)?
            .cert_sha256;

        Ok(Json(json!({ "uuid": node, "cert_sha256": cert })))
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", get(get::route))
        .with_state(state.clone())
}
