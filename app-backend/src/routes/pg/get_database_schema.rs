use axum::{extract::State, response::IntoResponse, Json};
use http::StatusCode;
use serde::Serialize;
use sqlx::Row;

use crate::server::ApiContext;
use crate::utils::app_error::AppResponse;

pub async fn get_database_schema(State(context): State<ApiContext>) -> AppResponse {
    let db = context.admin_db;

    let result = sqlx::query(
        r#"
    SELECT
        c.relname AS table_name,
        pg_catalog.obj_description(c.oid) AS table_comment
    FROM
        pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE
        c.relkind = 'r' -- Only tables (not views, indexes, etc.)
        AND n.nspname = 'public' -- Only public schema
    ORDER BY
        table_name;
    "#,
    )
    .fetch_all(&db)
    .await?;

    #[derive(Debug, Serialize)]
    pub struct TableInfo {
        pub table_name: String,
        pub table_comment: Option<String>,
    }

    let data = result
        .iter()
        .map(|row| TableInfo {
            table_name: row.get(0),
            table_comment: row.get(1),
        })
        .collect::<Vec<TableInfo>>();

    Ok((StatusCode::OK, Json(data)).into_response())
}
