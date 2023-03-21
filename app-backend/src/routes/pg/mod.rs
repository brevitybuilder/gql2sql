use axum::{
    middleware,
    routing::{get, post},
    Router,
};

use crate::{server::ApiContext, utils::middleware::is_service};

mod add_column;
mod add_column_index;
mod add_table;
mod get_database_schema;
mod get_table_schema;
mod rls;
mod update_column;
mod update_table;

pub fn router(context: ApiContext) -> Router {
    Router::new()
        .route("/column", post(add_column::add_column))
        .route("/update-column", post(update_column::update_column))
        .route(
            "/add-column-index",
            post(add_column_index::add_column_index),
        )
        .route("/table", post(add_table::add_table))
        .route("/table", get(get_database_schema::get_database_schema))
        .route("/table/:table_id", get(get_table_schema::get_table_schema))
        .route("/update-table", post(update_table::update_table))
        .with_state(context)
        .route_layer(middleware::from_fn(is_service))
}
