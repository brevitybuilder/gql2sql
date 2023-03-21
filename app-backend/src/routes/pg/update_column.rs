use axum::{extract::State, response::IntoResponse, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::utils::app_error::AppResponse;
use crate::utils::nanoid::is_valid_nanoid;
use crate::{server::ApiContext, utils::is_valid_snake_case::is_valid_snake_case};

#[derive(Deserialize, Validate)]
pub struct UpdateColumnRequest {
    #[validate(custom = "is_valid_nanoid")]
    pub table_id: String,

    #[validate(custom = "is_valid_nanoid")]
    pub column_id: String,

    #[validate(custom = "is_valid_snake_case")]
    pub new_column_name: String,
}

#[derive(Serialize)]
pub struct UpdateColumnResponse {
    pub message: String,
}

pub async fn update_column(
    State(context): State<ApiContext>,
    Json(body): Json<UpdateColumnRequest>,
) -> AppResponse {
    body.validate()?;

    let db = &context.admin_db;

    let query_string = format!(
        "COMMENT ON COLUMN \"{}\".\"{}\" IS '{}'",
        body.table_id, body.column_id, body.new_column_name
    );

    sqlx::query(&query_string).execute(db).await?;

    Ok((
        StatusCode::OK,
        Json(UpdateColumnResponse {
            message: "Successfully renamed the column".to_string(),
        }),
    )
        .into_response())
}
