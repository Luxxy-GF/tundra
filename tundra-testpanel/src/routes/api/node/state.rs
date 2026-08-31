use super::State;
use axum::{Router, routing::get};

mod get {
    use crate::routes::{ApiError, GetState};
    use axum::Json;
    use tundra_common::state::Snapshot;

    pub async fn route(state: GetState) -> Result<Json<Snapshot>, ApiError> {
        state.snapshot().map(Json)
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", get(get::route))
        .with_state(state.clone())
}
