extern crate dotenv;

use std::collections::BTreeMap;
use futures::future::join_all;
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
    data: BTreeMap<String, sqlx::types::JsonValue>,
    meta: Option<BTreeMap<String, String>>,
}

async fn graphql(payload: web::Json<Query>, db_pool: web::Data<Pool<Postgres>>) -> HttpResponse {
    let start = std::time::Instant::now();
    let gqlast = graphql_parser::query::parse_query::<&str>(&payload.query).unwrap();
    println!("Parsed in {}ms", start.elapsed().as_millis());
    let start = std::time::Instant::now();
    let statements = gql2sql::gql2sql(gqlast).unwrap();
    println!("Transformed in {}ms", start.elapsed().as_millis());
    let start = std::time::Instant::now();
    let mut data = BTreeMap::new();
    let mut meta = BTreeMap::new();
    let queries = join_all(statements
        .iter()
        .map(|(key, statement)| async {
            let pool = db_pool.get_ref();
            let value: (sqlx::types::JsonValue,) = sqlx::query_as(&statement.to_string()).fetch_one(pool).await.unwrap();
            (key.to_string(), value.0)
        })).await;
    for statement in statements {
        let (key, statement) = statement;
        meta.insert(key, statement.to_string());
    }
    for (key, value) in queries {
        data.insert(key, value);
    }
    println!("executed in {}ms", start.elapsed().as_millis());
    HttpResponse::Ok().json(QueryResponse { data, meta: Some(meta) })
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
            .app_data(web::JsonConfig::default().limit(4096)) // <- limit size of the payload (global configuration)
            .app_data(web::Data::new(pool.clone()))
            .service(web::resource("/graphql").route(web::post().to(graphql)))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
