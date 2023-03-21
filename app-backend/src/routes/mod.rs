use axum::Router;

use crate::server::ApiContext;

mod auth;
mod gql;
mod pg;

pub fn router(context: ApiContext) -> Router {
    let auth_url = context.config.gotrue_url.clone();
    Router::new()
        .route("/healthcheck", axum::routing::get(healthcheck))
        .nest("/pg/v1", pg::router(context.clone()))
        .nest("/gql/v1", gql::router(context))
        .nest("/auth/v1", auth::router(auth_url))
}

async fn healthcheck() -> &'static str {
    "OK"
}
