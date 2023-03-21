use axum::{extract::State, response::IntoResponse, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::utils::app_error::AppResponse;
use crate::utils::nanoid::is_valid_nanoid;
use crate::{server::ApiContext, utils::is_valid_snake_case::is_valid_snake_case};

#[derive(Deserialize, Validate)]
pub struct UpdateTableRequest {
    #[validate(custom = "is_valid_nanoid")]
    pub table_id: String,

    #[validate(custom = "is_valid_snake_case")]
    pub new_table_name: String,
}

#[derive(Serialize)]
pub struct UpdateTableResponse {
    pub message: String,
}

pub async fn update_table(
    State(context): State<ApiContext>,
    Json(body): Json<UpdateTableRequest>,
) -> AppResponse {
    body.validate()?;

    let db = &context.admin_db;

    let query_string = format!(
        "COMMENT ON TABLE \"{}\" IS '{}'",
        body.table_id, body.new_table_name
    );

    sqlx::query(&query_string).execute(db).await?;

    Ok((
        StatusCode::OK,
        Json(UpdateTableResponse {
            message: "Successfully renamed the table".to_string(),
        }),
    )
        .into_response())
}
