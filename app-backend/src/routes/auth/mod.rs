use std::sync::Arc;

use axum::{
    extract::State,
    http::{uri::Uri, Request, Response},
    routing::get,
    Router,
};
use hyper::{client::HttpConnector, Body};

type Client = hyper::client::Client<HttpConnector, Body>;

#[derive(Clone)]
struct AuthContext {
    client: Client,
    auth_url: Arc<String>,
}

pub fn router(auth_url: String) -> Router {
    let client = Client::new();

    let auth_context = AuthContext {
        client,
        auth_url: Arc::new(auth_url),
    };

    Router::new()
        .route("/", get(handler).post(handler))
        .route("/*route", get(handler).post(handler))
        .with_state(auth_context)
}

async fn handler(State(context): State<AuthContext>, mut req: Request<Body>) -> Response<Body> {
    let path = req.uri().path();
    let path_query = req
        .uri()
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or(path);

    let uri = format!("{}{}", context.auth_url, path_query);

    *req.uri_mut() = Uri::try_from(uri).unwrap();

    context.client.request(req).await.unwrap()
}
