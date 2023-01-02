#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate dotenv;

use std::collections::BTreeMap;
use dotenv::dotenv;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use actix_web::{web, App, HttpResponse, HttpServer, middleware};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Query {
    query: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryResponse {
    data: sqlx::types::JsonValue,
    meta: Option<BTreeMap<String, String>>,
}

async fn graphql(payload: web::Json<Query>, db_pool: web::Data<Pool<Postgres>>) -> HttpResponse {
    let mut meta = BTreeMap::new();
    let start = std::time::Instant::now();
    let gqlast = graphql_parser::query::parse_query::<&str>(&payload.query).unwrap();
    meta.insert("parse".to_string(), start.elapsed().as_micros().to_string());
    let start = std::time::Instant::now();
    let query = gql2sql::gql2sql(gqlast).unwrap().to_string();
    meta.insert("transform".to_string(), start.elapsed().as_micros().to_string());
    let start = std::time::Instant::now();
    let pool = db_pool.get_ref();
    let value: (sqlx::types::JsonValue,) = sqlx::query_as(&query).fetch_one(pool).await.unwrap();
    meta.insert("execute".to_string(), start.elapsed().as_micros().to_string());
    meta.insert("query".to_string(), query);
    HttpResponse::Ok().json(QueryResponse { data: value.0, meta: Some(meta) })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")).await.unwrap();

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .app_data(web::JsonConfig::default().limit(4096)) // <- limit size of the payload (global configuration)
            .app_data(web::Data::new(pool.clone()))
            .service(web::resource("/graphql").route(web::post().to(graphql)))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
