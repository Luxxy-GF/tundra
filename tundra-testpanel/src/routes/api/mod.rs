use super::{ApiError, GetState, State, bearer, constant_time_eq};
use axum::{Router, extract::Request, middleware::Next, response::Response};

mod acls;
mod node;
mod nodes;
mod servers;
mod state;

pub async fn auth(state: GetState, req: Request, next: Next) -> Result<Response, ApiError> {
    let ok = bearer(req.headers()).is_some_and(|token| constant_time_eq(token, &state.admin_token));
    if !ok {
        return Err(ApiError::Unauthorized);
    }

    Ok(next.run(req).await)
}

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .nest("/acls", acls::router(state))
        .nest("/nodes", nodes::router(state))
        .nest("/servers", servers::router(state))
        .nest("/state", state::router(state))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth))
        .nest("/node", node::router(state))
        .with_state(state.clone())
}
