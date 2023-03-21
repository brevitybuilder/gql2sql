use axum::{extract::State, middleware, response::IntoResponse, routing::get, Extension, Router};
use sqlx::{Acquire, Executor, Row};

use crate::{
    server::ApiContext,
    utils::{app_error::AppResponse, auth::Claims, middleware::is_user},
};

pub fn router(context: ApiContext) -> Router {
    Router::new()
        .route("/", get(handler))
        .with_state(context)
        .route_layer(middleware::from_fn(is_user))
}

async fn handler(State(context): State<ApiContext>, user_claims: Extension<Claims>) -> AppResponse {
    let db = context.user_db;
    let mut conn = db.acquire().await?;
    let mut transaction = conn.begin().await?;

    transaction
        .execute(sqlx::query(&format!(
            r#"SET LOCAL jwt.claims.sub = '{}'"#,
            user_claims.sub
        )))
        .await?;

    transaction
        .execute(sqlx::query(r#"SET LOCAL ROLE authenticated"#))
        .await?;

    let result = transaction
        .fetch_all(sqlx::query(
            r#"SELECT current_setting('jwt.claims.sub', true) as sub FROM "users""#,
        ))
        .await?;

    // let result: Vec<Value> = sqlx::query_as(r#"SELECT *, current_setting('jwt.claims.sub', true) FROM "users""#).fetch_all(&mut transaction).await?;

    for row in result {
        let id: String = row.try_get(0)?;
        println!("id: {:?}", id);
    }

    transaction.commit().await?;

    Ok(().into_response())
}
