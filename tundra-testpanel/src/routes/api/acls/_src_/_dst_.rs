use super::State;
use axum::{Router, routing::delete};

mod delete {
    use crate::routes::{ApiError, GetState};
    use axum::{extract::Path, http::StatusCode};

    pub async fn route(
        state: GetState,
        Path((src, dst)): Path<(uuid::Uuid, uuid::Uuid)>,
    ) -> Result<StatusCode, ApiError> {
        if !state.database.delete_acl(&src, &dst)? {
            return Err(ApiError::NotFound("no such acl".into()));
        }

        state.publish()?;
        tracing::info!(
            src = %src,
            dst = %dst,
            "acl removed"
        );

        Ok(StatusCode::NO_CONTENT)
    }
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .route("/", delete(delete::route))
        .with_state(state.clone())
}
