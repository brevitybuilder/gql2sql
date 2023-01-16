#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use lambda_http::{run, service_fn, Body, Error, Request, RequestExt, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::{PgArguments, PgPoolOptions};
use sqlx::Arguments;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
struct Query {
    query: String,
    variables: Option<Value>,
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
            let (statement, params) = gql2sql::gql2sql(gqlast).unwrap();
            let mut args = vec![];
            if let Some(Value::Object(mut map)) = payload.variables {
                if let Some(params) = params {
                    params.into_iter().for_each(|p| {
                        args.push(map.remove(&p).unwrap_or(Value::Null));
                    });
                }
            }

            let mut pg_args = PgArguments::default();
            args.into_iter().for_each(|a| match a {
                Value::String(s) => {
                    println!("string: {}", s);
                    pg_args.add(s);
                }
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        pg_args.add(i);
                    } else if let Some(f) = n.as_f64() {
                        pg_args.add(f);
                    }
                }
                Value::Bool(b) => pg_args.add(b),
                Value::Null => pg_args.add::<Option<String>>(None),
                _ => panic!("Unsupported type"),
            });

            let value = sqlx::query_scalar_with(&statement.to_string(), pg_args)
                .fetch_one(pool_ref)
                .await
                .unwrap();

            Result::<Response<Body>, Error>::Ok(
                Response::builder()
                    .status(200)
                    .body(Body::Text(
                        serde_json::to_string(&QueryResponse {
                            data: value,
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
