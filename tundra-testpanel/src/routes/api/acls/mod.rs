use super::State;
use axum::{Router, routing::get};

mod _src_;

mod get {
    use crate::routes::{ApiError, GetState};
    use axum::Json;
    use serde_json::json;

    pub async fn route(state: GetState) -> Result<Json<serde_json::Value>, ApiError> {
        Ok(Json(json!(state.snapshot()?.acls)))
    }
}

mod post {
    use crate::routes::{ApiError, GetState};
    use axum::{Json, http::StatusCode};
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Payload {
        src_server: uuid::Uuid,
        dst_server: uuid::Uuid,
    }

    pub async fn route(state: GetState, Json(data): Json<Payload>) -> Result<StatusCode, ApiError> {
        state
            .database
            .create_acl(&data.src_server, &data.dst_server)
            .map_err(|err| ApiError::BadRequest(err.to_string()))?;
        state.publish()?;
        tracing::info!(
            src = %data.src_server,
            dst = %data.dst_server,
            "acl added"
        );

        Ok(StatusCode::NO_CONTENT)
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", get(get::route).post(post::route))
        .nest("/{src}", _src_::router(state))
        .with_state(state.clone())
}
