use super::State;
use axum::{Router, routing::post};

mod post {
    use crate::routes::{
        ApiError, GetState,
        api::node::{GetNodeAuth, NodeAuth},
    };
    use axum::Json;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize)]
    pub struct Payload {
        csr_pem: String,
    }

    pub async fn route(
        state: GetState,
        axum::extract::Extension(NodeAuth(node)): GetNodeAuth,
        Json(data): Json<Payload>,
    ) -> Result<Json<serde_json::Value>, ApiError> {
        let signed = state
            .keys
            .sign_node_csr(&data.csr_pem, &node)
            .map_err(|err| ApiError::BadRequest(err.to_string()))?;

        state.database.store_node_cert(&node, signed.sha256)?;
        state.publish()?;
        tracing::info!(
            node = %node,
            fingerprint = %signed.sha256,
            "issued node certificate"
        );

        Ok(Json(json!({
            "cert_pem": signed.pem,
            "cert_sha256": signed.sha256,
            "ca_pem": state.keys.ca_cert_pem,
        })))
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", post(post::route))
        .with_state(state.clone())
}
