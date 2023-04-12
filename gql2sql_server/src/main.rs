#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts, State},
    http::{request::Parts, StatusCode},
    response::AppendHeaders,
    routing::post,
    Json, Router,
};
use dotenvy::dotenv;
use http::{
    header::{HeaderName, AUTHORIZATION},
    HeaderValue,
};
use serde::{Deserialize, Serialize};
use serde_json::{value::to_raw_value, Value};
use sqlx::Arguments;
use sqlx::Row;
use sqlx::{postgres::PgPoolOptions, PgPool};
use sqlx::{
    postgres::{PgArguments, PgRow},
    FromRow,
};
use std::collections::BTreeMap;
use std::{iter::once, net::SocketAddr};
use tower_http::{
    compression::CompressionLayer,
    propagate_header::PropagateHeaderLayer,
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse, TraceLayer},
    validate_request::ValidateRequestHeaderLayer,
    LatencyUnit,
};
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Query {
    query: String,
    variables: Option<Value>,
    operation_name: Option<String>,
}

#[derive(Deserialize)]
struct QueryResponse {
    data: Box<sqlx::types::JsonRawValue>,
}

impl FromRow<'_, PgRow> for QueryResponse {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(QueryResponse {
            data: to_raw_value::<&sqlx::types::JsonRawValue>(&row.get(0)).unwrap(),
        })
    }
}

#[derive(Serialize)]
struct APIResponse {
    data: Box<sqlx::types::JsonRawValue>,
    meta: Option<BTreeMap<String, String>>,
}

async fn graphql(
    State(pool): State<PgPool>,
    Json(payload): Json<Query>,
) -> Result<
    (
        AppendHeaders<[(HeaderName, HeaderValue); 1]>,
        Json<APIResponse>,
    ),
    (StatusCode, String),
> {
    let mut meta = BTreeMap::new();
    let start = std::time::Instant::now();
    let gqlast = async_graphql_parser::parse_query(&payload.query).unwrap();
    meta.insert("parse".to_string(), start.elapsed().as_millis().to_string());
    let start = std::time::Instant::now();
    let (statement, mut args) =
        gql2sql::gql2sql(gqlast, &payload.variables, payload.operation_name).unwrap();
    meta.insert(
        "transform".to_string(),
        start.elapsed().as_millis().to_string(),
    );
    let start = std::time::Instant::now();

    let mut pg_args = PgArguments::default();
    if let Some(args) = args {
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
    }

    let value: QueryResponse = sqlx::query_as_with(&statement.to_string(), pg_args)
        .fetch_one(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    meta.insert(
        "execute".to_string(),
        start.elapsed().as_millis().to_string(),
    );

    meta.insert("query".to_string(), statement.to_string());
    Ok((
        AppendHeaders([(
            HeaderName::from_static("vary"),
            HeaderValue::from_static("accept-encoding"),
        )]),
        Json(APIResponse {
            data: value.data,
            meta: Some(meta),
        }),
    ))
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "example_tokio_postgres=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let db_connection_str = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:password@localhost".to_string());

    // setup connection pool
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_connection_str)
        .await
        .expect("can't connect to database");

    // build our application with some routes
    let app = Router::new()
        .route("/graphql", post(graphql))
        // Mark the `Authorization` request header as sensitive so it doesn't show in logs
        .layer(SetSensitiveRequestHeadersLayer::new(once(AUTHORIZATION)))
        // High level logging of requests and responses
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(
                    DefaultOnResponse::new()
                        .level(Level::INFO)
                        .latency_unit(LatencyUnit::Millis),
                ),
        )
        // Propagate `X-Request-Id`s from requests to responses
        .layer(PropagateHeaderLayer::new(HeaderName::from_static(
            "x-request-id",
        )))
        // Accept only application/json, application/* and */* in a request's ACCEPT header
        .layer(ValidateRequestHeaderLayer::accept("application/json"))
        .layer(CompressionLayer::new().br(true).gzip(true))
        .with_state(pool);

    // run it with hyper
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// we can also write a custom extractor that grabs a connection from the pool
// which setup is appropriate depends on your application
struct DatabaseConnection(sqlx::pool::PoolConnection<sqlx::Postgres>);

#[async_trait]
impl<S> FromRequestParts<S> for DatabaseConnection
where
    PgPool: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let pool = PgPool::from_ref(state);

        let conn = pool.acquire().await.map_err(internal_error)?;

        Ok(Self(conn))
    }
}

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
