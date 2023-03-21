use axum::{extract::State, response::IntoResponse, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Executor};
use validator::Validate;

use crate::server::ApiContext;
use crate::utils::nanoid::{is_valid_nanoid, nanoid};
use crate::utils::{app_error::AppResponse, is_valid_snake_case::is_valid_snake_case};

#[derive(Deserialize, Validate)]
pub struct AddTableRequest {
    #[validate(custom = "is_valid_snake_case")]
    pub table_name: String,

    #[validate(custom = "is_valid_nanoid")]
    pub table_id: Option<String>,
}

#[derive(Serialize)]
pub struct AddTableResponse {
    pub table_id: String,
    pub message: String,
}

pub async fn add_table(
    State(context): State<ApiContext>,
    Json(body): Json<AddTableRequest>,
) -> AppResponse {
    body.validate()?;

    let table_id = match body.table_id {
        Some(table_id) => table_id,
        None => nanoid(),
    };

    let db = context.admin_db;
    let mut conn = db.acquire().await?;
    let mut transaction = conn.begin().await?;

    transaction.execute(sqlx::query(&format!(
        // "CREATE TABLE \"{}\" (\"id\" nanoid primary key default nanoid(), \"created_at\" timestamp with time zone default now(), \"updated_at\" timestamp with time zone default now())",
        "CREATE TABLE \"{}\" (\"id\" uuid primary key default gen_random_uuid(), \"created_at\" timestamp with time zone default now(), \"updated_at\" timestamp with time zone default now())",
        table_id
    ))).await?;

    transaction
        .execute(sqlx::query(&format!(
            "COMMENT ON TABLE \"{}\" IS '{}'",
            table_id, body.table_name
        )))
        .await?;

    transaction.commit().await?;

    Ok((
        StatusCode::OK,
        Json(AddTableResponse {
            table_id,
            message: "Successfully created table".to_string(),
        }),
    )
        .into_response())
}
