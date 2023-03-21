use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use http::StatusCode;
use serde::Serialize;
use sqlx::{postgres::types::Oid, Row};

use crate::utils::app_error::AppResponse;
use crate::utils::nanoid::is_valid_nanoid;
use crate::{server::ApiContext, utils::app_error::AppError};

pub async fn get_table_schema(
    State(context): State<ApiContext>,
    Path(table_id): Path<String>,
) -> AppResponse {
    if is_valid_nanoid(&table_id).is_err() {
        return AppError::new(StatusCode::BAD_REQUEST, "Invalid table id".to_string());
    }

    let db = context.admin_db;

    let result = sqlx::query(r#"
    WITH table_comments AS (
        SELECT
            c.oid,
            obj_description(c.oid, 'pg_class') AS table_comment
        FROM
            pg_class c
        JOIN
            pg_namespace n ON (c.relnamespace = n.oid)
        WHERE
            c.relkind = 'r'
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ),
    column_comments AS (
        SELECT
            a.attrelid,
            a.attname,
            col_description(a.attrelid, a.attnum) AS column_comment
        FROM
            pg_attribute a
        WHERE
            a.attnum > 0
            AND NOT a.attisdropped
    ),
    constraints AS (
        SELECT
            conname,
            conrelid,
            conkey,
            confrelid,
            confkey,
            contype
        FROM
            pg_constraint
    ),
    primary_keys AS (
        SELECT
            conrelid,
            conkey
        FROM
            pg_constraint
        WHERE
            contype = 'p'
    )
    SELECT
        c.oid AS table_oid,
        n.nspname AS schema_name,
        c.relname AS table_name,
        tc.table_comment,
        a.attname AS column_name,
        cc.column_comment,
        pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type,
        CASE
            WHEN con.contype = 'p' THEN 'PRIMARY KEY'
            WHEN con.contype = 'f' THEN 'FOREIGN KEY'
        END AS constraint_type,
        con.conname AS constraint_name,
        ARRAY(SELECT column_name FROM primary_keys, unnest(conkey) WITH ORDINALITY AS u(column_name, column_position) WHERE con.contype = 'p' AND conrelid = a.attrelid) AS primary_key_columns,
        (SELECT relname FROM pg_class WHERE oid = con.confrelid) AS foreign_table,
        ARRAY(SELECT column_name FROM primary_keys, unnest(confkey) WITH ORDINALITY AS u(column_name, column_position) WHERE con.contype = 'f' AND conrelid = a.attrelid) AS foreign_key_columns
    FROM
        pg_attribute a
    JOIN
        pg_class c ON a.attrelid = c.oid
    JOIN
        pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN
        table_comments tc ON c.oid = tc.oid
    LEFT JOIN
        column_comments cc ON a.attrelid = cc.attrelid AND a.attname = cc.attname
    LEFT JOIN
        constraints con ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
    WHERE
        a.attnum > 0
        AND NOT a.attisdropped
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND c.relname = $1
    ORDER BY
        n.nspname,
        c.relname,
        a.attnum;
    "#).bind(table_id).fetch_all(&db).await?;

    #[derive(Debug, Serialize)]
    pub struct TableInfo {
        pub table_oid: Oid,
        pub schema_name: String,
        pub table_name: String,
        pub table_comment: Option<String>,
        pub column_name: String,
        pub column_comment: Option<String>,
        pub column_type: String,
        pub constraint_type: Option<String>,
        pub constraint_name: Option<String>,
        pub primary_key_columns: Option<Vec<i16>>,
        pub foreign_table: Option<String>,
        pub foreign_key_columns: Option<Vec<i16>>,
    }

    let data = result
        .iter()
        .map(|row| TableInfo {
            table_oid: row.get(0),
            schema_name: row.get(1),
            table_name: row.get(2),
            table_comment: row.get(3),
            column_name: row.get(4),
            column_comment: row.get(5),
            column_type: row.get(6),
            constraint_type: row.get(7),
            constraint_name: row.get(8),
            primary_key_columns: row.get(9),
            foreign_table: row.get(10),
            foreign_key_columns: row.get(11),
        })
        .collect::<Vec<TableInfo>>();

    Ok((StatusCode::OK, Json(data)).into_response())
}
