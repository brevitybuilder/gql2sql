use axum::{
    extract::State,
    response::{IntoResponse, Response},
    Json,
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Executor};
use validator::Validate;

use crate::server::ApiContext;
use crate::utils::app_error::AppError;
use crate::utils::is_valid_snake_case::is_valid_snake_case;
use crate::utils::nanoid::{is_valid_nanoid, nanoid};

#[derive(Deserialize, Validate)]
pub struct AddColumnIndexRequest {
    #[validate(custom = "is_valid_nanoid")]
    pub table_id: String,

    #[validate(custom = "is_valid_nanoid")]
    pub column_id: String,

    #[validate(custom = "is_valid_nanoid")]
    pub index_id: Option<String>,

    #[validate(custom = "is_valid_snake_case")]
    pub index_name: String,
}

#[derive(Serialize)]
pub struct AddColumnIndexResponse {
    pub index_id: String,
    pub message: String,
}

pub async fn add_column_index(
    State(context): State<ApiContext>,
    Json(body): Json<AddColumnIndexRequest>,
) -> Result<Response, AppError> {
    body.validate()?;

    let db = &context.admin_db;
    let mut conn = db.acquire().await?;
    let mut transaction = conn.begin().await?;

    let index_id = match body.index_id {
        Some(index_id) => index_id,
        None => nanoid(),
    };

    transaction
        .execute(sqlx::query(&format!(
            "CREATE INDEX \"{}\" ON \"{}\" (\"{}\")",
            index_id, body.table_id, body.column_id
        )))
        .await?;

    transaction
        .execute(sqlx::query(&format!(
            "COMMENT ON INDEX \"{}\" IS '{}'",
            index_id, body.index_name
        )))
        .await?;

    transaction.commit().await?;

    Ok((
        StatusCode::OK,
        Json(AddColumnIndexResponse {
            index_id,
            message: "Successfully added column index".to_string(),
        }),
    )
        .into_response())
}
