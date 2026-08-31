use super::State;
use crate::routes::{ApiError, GetState, bearer};
use axum::{Router, extract::Request, middleware::Next, response::Response};

mod connect_token;
mod csr;
mod identity;
mod state;
mod ws;

#[derive(Clone, Copy)]
pub struct NodeAuth(pub uuid::Uuid);

pub type GetNodeAuth = axum::extract::Extension<NodeAuth>;

pub async fn auth(state: GetState, mut req: Request, next: Next) -> Result<Response, ApiError> {
    let token = bearer(req.headers()).ok_or(ApiError::Unauthorized)?;
    let node = state
        .database
        .node_by_token(token)?
        .ok_or(ApiError::Unauthorized)?;

    req.extensions_mut().insert(NodeAuth(node));

    Ok(next.run(req).await)
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .nest("/connect-token", connect_token::router(state))
        .nest("/csr", csr::router(state))
        .nest("/identity", identity::router(state))
        .nest("/state", state::router(state))
        .nest("/ws", ws::router(state))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state.clone())
}
