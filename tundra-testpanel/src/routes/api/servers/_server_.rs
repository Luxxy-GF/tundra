use super::State;
use axum::{Router, routing::delete};

mod delete {
    use crate::routes::{ApiError, GetState};
    use axum::{extract::Path, http::StatusCode};

    pub async fn route(
        state: GetState,
        Path(server): Path<uuid::Uuid>,
    ) -> Result<StatusCode, ApiError> {
        if !state.database.delete_server(&server)? {
            return Err(ApiError::NotFound(format!("no server {server}")));
        }

        state.publish()?;
        tracing::info!(
            server = %server,
            "server deleted"
        );

        Ok(StatusCode::NO_CONTENT)
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", delete(delete::route))
        .with_state(state.clone())
}
