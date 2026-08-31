use crate::{database::Database, keys::PanelKeys, websocket::Hub};
use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::sync::Arc;
use tundra_common::state::Snapshot;

mod api;

pub type State = Arc<AppState>;
pub type GetState = axum::extract::State<State>;

pub struct AppState {
    pub database: Arc<Database>,
    pub keys: Arc<PanelKeys>,
    pub hub: Arc<Hub>,
    pub admin_token: Arc<str>,
}

impl AppState {
    #[inline]
    pub fn snapshot(&self) -> Result<Snapshot, ApiError> {
        Ok(self.database.snapshot(self.keys.jwt_public())?)
    }

    pub fn publish(&self) -> Result<(), ApiError> {
        let snapshot = self.snapshot()?;
        tracing::info!(
            epoch = snapshot.epoch,
            nodes = snapshot.nodes.len(),
            servers = snapshot.servers.len(),
            acls = snapshot.acls.len(),
            "publishing snapshot"
        );

        self.hub.broadcast(&Arc::new(snapshot));

        Ok(())
    }
}

pub enum ApiError {
    Unauthorized,
    NotFound(String),
    BadRequest(String),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".into()),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(err) => {
                tracing::error!("a request error occurred: {:?}", err);
                // the cause would leak sqlite and rcgen internals
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".into(),
                )
            }
        };

        (status, Json(json!({ "error": msg }))).into_response()
    }
}

#[inline]
pub fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }

    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }

    diff == 0
}

pub fn router(state: &State) -> Router {
    Router::new()
        .nest("/api", api::router(state))
        .with_state(state.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    // constant_time_eq

    #[test]
    fn constant_time_eq_matches_plain_string_equality() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "ab"));
        assert!(!constant_time_eq("", "a"));
        assert!(constant_time_eq("", ""));
    }
}
