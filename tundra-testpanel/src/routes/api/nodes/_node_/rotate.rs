use super::State;
use axum::{Router, routing::post};

mod post {
    use crate::routes::{ApiError, GetState};
    use axum::{Json, extract::Path};
    use serde_json::json;

    pub async fn route(
        state: GetState,
        Path(node): Path<uuid::Uuid>,
    ) -> Result<Json<serde_json::Value>, ApiError> {
        let token = state.database.rotate_node(&node)?;
        state.hub.disconnect(&node);
        state.publish()?;
        tracing::info!(
            node = %node,
            "node token rotated, certificate cleared"
        );

        Ok(Json(json!({ "node_token": token })))
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", post(post::route))
        .with_state(state.clone())
}
