use super::State;
use axum::{Router, routing::delete};

mod metrics;
mod rotate;

mod delete {
    use crate::routes::{ApiError, GetState};
    use axum::{extract::Path, http::StatusCode};

    pub async fn route(
        state: GetState,
        Path(node): Path<uuid::Uuid>,
    ) -> Result<StatusCode, ApiError> {
        if !state.database.delete_node(&node)? {
            return Err(ApiError::NotFound(format!("no node {node}")));
        }

        state.hub.disconnect(&node);
        state.publish()?;
        tracing::info!(
            node = %node,
            "node deleted"
        );

        Ok(StatusCode::NO_CONTENT)
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", delete(delete::route))
        .nest("/metrics", metrics::router(state))
        .nest("/rotate", rotate::router(state))
        .with_state(state.clone())
}
