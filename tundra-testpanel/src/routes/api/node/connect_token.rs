use super::State;
use axum::{Router, routing::get};

mod get {
    use crate::routes::{
        ApiError, GetState,
        api::node::{GetNodeAuth, NodeAuth},
    };
    use anyhow::Context;
    use axum::{Json, extract::Query};
    use serde::Deserialize;
    use serde_json::json;
    use tundra_common::jwt;

    #[derive(Deserialize)]
    pub struct Params {
        target: uuid::Uuid,
    }

    pub async fn route(
        state: GetState,
        axum::extract::Extension(NodeAuth(node)): GetNodeAuth,
        Query(params): Query<Params>,
    ) -> Result<Json<serde_json::Value>, ApiError> {
        let nodes = state.database.list_nodes()?;
        let me = nodes
            .iter()
            .find(|n| n.uuid == node)
            .ok_or(ApiError::Unauthorized)?;
        let cert = me
            .cert_sha256
            .ok_or_else(|| ApiError::BadRequest("this node has no certificate yet".into()))?;

        if !nodes.iter().any(|n| n.uuid == params.target) {
            return Err(ApiError::NotFound(format!("no node {}", params.target)));
        }

        let claims = jwt::ConnectClaims::new(node, params.target, &cert, jwt::unix_now());
        let jwt = state
            .keys
            .jwt
            .create(&claims)
            .context("failed to issue the connect token")?;

        Ok(Json(json!({ "jwt": jwt })))
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", get(get::route))
        .with_state(state.clone())
}
