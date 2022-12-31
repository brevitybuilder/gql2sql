extern crate dotenv;

use dotenv::dotenv;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder, middleware};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Query {
    query: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryResponse {
    data: sqlx::types::JsonValue,
}

async fn graphql(payload: web::Json<Query>, db_pool: web::Data<Pool<Postgres>>) -> HttpResponse {
    let start = std::time::Instant::now();
    let gqlast = graphql_parser::query::parse_query::<&str>(&payload.query).unwrap();
    println!("Parsed in {}ms", start.elapsed().as_millis());
    let start = std::time::Instant::now();
    let statements = gql2sql::gql2sql(gqlast).unwrap();
    println!("Transformed in {}ms", start.elapsed().as_millis());
    let start = std::time::Instant::now();
    let queries = statements
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    println!("Stringified in {}ms", start.elapsed().as_millis());
    let pool = db_pool.get_ref();
    let start = std::time::Instant::now();
    let value: (sqlx::types::JsonValue,) = sqlx::query_as(&queries[0]).fetch_one(pool).await.unwrap();
    println!("fetched in {}ms", start.elapsed().as_millis());
    HttpResponse::Ok().json(QueryResponse { data: value.0 })
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
