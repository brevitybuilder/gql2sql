use axum::{extract::State, response::IntoResponse, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, Executor};
use strum::Display;
use validator::Validate;

use crate::server::ApiContext;
use crate::utils::nanoid::{is_valid_nanoid, nanoid};
use crate::utils::{app_error::AppResponse, is_valid_snake_case::is_valid_snake_case};

#[derive(Deserialize, Validate)]
pub struct AddColumnRequest {
    #[validate(custom = "is_valid_nanoid")]
    pub table_id: String,

    #[validate(custom = "is_valid_snake_case")]
    pub column_name: String,

    #[validate(custom = "is_valid_nanoid")]
    pub column_id: Option<String>,

    pub column_type: ColumnDataType,

    pub is_list: Option<bool>,

    pub constraints: Option<Vec<Constraints>>,
}

#[derive(Serialize)]
pub struct AddColumnResponse {
    pub column_id: String,
    pub message: String,
}

pub async fn add_column(
    State(context): State<ApiContext>,
    Json(body): Json<AddColumnRequest>,
) -> AppResponse {
    body.validate()?;

    let column_id = match body.column_id {
        Some(column_id) => column_id,
        None => nanoid(),
    };

    let db = context.admin_db;
    let mut conn = db.acquire().await?;
    let mut transaction = conn.begin().await?;

    let is_array = match body.is_list {
        Some(true) => "[]",
        _ => "",
    };

    transaction
        .execute(sqlx::query(&format!(
            "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}{} NULL",
            body.table_id, column_id, body.column_type, is_array
        )))
        .await?;

    transaction
        .execute(sqlx::query(&format!(
            "COMMENT ON COLUMN \"{}\".\"{}\" IS '{}'",
            body.table_id, column_id, body.column_name
        )))
        .await?;

    transaction.commit().await?;

    Ok((
        StatusCode::OK,
        Json(AddColumnResponse {
            column_id,
            message: "Successfully created column".to_string(),
        }),
    )
        .into_response())
}

#[derive(Deserialize, Display)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ColumnDataType {
    Text,
    Integer,
    Numeric,
    Boolean,
    Time,
    TimestampZ,
    NanoId,
    Json,
    JsonB,
}

#[derive(Deserialize, Display, Debug)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Constraints {
    Unique,
}
