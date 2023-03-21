use anyhow::Result;
use axum::{
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
    Json,
};

use super::auth::{get_service_key, get_user_id};

pub async fn is_user<B>(
    mut req: Request<B>,
    next: Next<B>,
) -> Result<Response, (StatusCode, Json<String>)> {
    let auth_header = req.headers().get(http::header::AUTHORIZATION);

    match get_user_id(auth_header) {
        Ok(user_id) => {
            req.extensions_mut().insert(user_id);
            Ok(next.run(req).await)
        }
        Err(err) => Err((
            StatusCode::UNAUTHORIZED,
            Json(format!("Something went wrong: {}", err)),
        )),
    }
}

pub async fn is_service<B>(
    mut req: Request<B>,
    next: Next<B>,
) -> Result<Response, (StatusCode, Json<String>)> {
    let auth_header = req.headers().get(http::header::AUTHORIZATION);

    match get_service_key(auth_header) {
        Ok(user_id) => {
            req.extensions_mut().insert(user_id);
            Ok(next.run(req).await)
        }
        Err(err) => Err((
            StatusCode::UNAUTHORIZED,
            Json(format!("Something went wrong: {}", err)),
        )),
    }
}
