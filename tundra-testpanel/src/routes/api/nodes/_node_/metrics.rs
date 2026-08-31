use super::State;
use axum::{Router, routing::get};

mod get {
    use crate::routes::{ApiError, GetState};
    use axum::{Json, extract::Path};

    pub async fn route(
        state: GetState,
        Path(node): Path<uuid::Uuid>,
    ) -> Result<Json<serde_json::Value>, ApiError> {
        state
            .hub
            .request_metrics(&node)
            .await
            .map(Json)
            .map_err(|err| ApiError::NotFound(err.to_string()))
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", get(get::route))
        .with_state(state.clone())
}
