#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use lambda_http::{run, service_fn, Body, Error, Request, RequestExt, Response};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
struct Query {
    query: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryResponse {
    data: sqlx::types::JsonValue,
    meta: Option<BTreeMap<String, String>>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"))
        .await
        .expect("can't connect to database");
    let pool_ref = &pool;

    let handler_closure = move |event: Request| async move {
        if let Some(payload) = event.payload::<Query>().unwrap() {
            let gqlast = graphql_parser::query::parse_query::<String>(&payload.query).unwrap();
            let query = gql2sql::gql2sql(gqlast).unwrap().to_string();
            let value: (sqlx::types::JsonValue,) =
                sqlx::query_as(&query).fetch_one(pool_ref).await.unwrap();
            Result::<Response<Body>, Error>::Ok(
                Response::builder()
                    .status(200)
                    .body(Body::Text(
                        serde_json::to_string(&QueryResponse {
                            data: value.0,
                            meta: None,
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
        } else {
            // Return something that implements IntoResponse.
            // It will be serialized to the right response event automatically by the runtime
            let resp = Response::builder()
                .status(400)
                .header("content-type", "text/plain")
                .body("missing body".into())
                .map_err(Box::new)?;
            Result::<Response<Body>, Error>::Ok(resp)
        }
    };

    run(service_fn(handler_closure)).await?;
    Ok(())
}
