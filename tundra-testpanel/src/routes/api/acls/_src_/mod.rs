use super::State;
use axum::Router;

mod _dst_;

pub fn router(state: &State) -> Router<State> {
    Router::new()
        .nest("/{dst}", _dst_::router(state))
        .with_state(state.clone())
}
