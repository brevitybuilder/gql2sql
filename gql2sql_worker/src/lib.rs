use std::fmt::format;

use async_graphql_parser::parse_query;
use gql2sql::gql2sql as gql2sql_rs;
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use worker::{event, Env, Fetch, Request, RequestInit, Response, Result, Router};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Query {
    query: String,
    variables: Option<Value>,
    operation_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Gql2sql {
    query: String,
    vars: Option<Vec<Value>>,
    tags: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct SqlPayload {
    query: String,
    params: Option<Vec<Value>>,
}

#[derive(Serialize)]
struct Extensions {
    tags: Option<Vec<String>>,
}

#[derive(Serialize)]
struct QueryResult {
    data: Box<serde_json::value::RawValue>,
    extensions: Option<Extensions>,
}

#[derive(Deserialize)]
struct DataResponse {
    data: Box<serde_json::value::RawValue>,
}

#[derive(Deserialize)]
struct SqlResponse {
    rows: Vec<DataResponse>,
}

#[event(fetch)]
pub async fn main(request: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    // Optionally, get more helpful error messages written to the console in the case of a panic.
    let router = Router::new();
    router
        .get("/", |_, _| Response::ok("Nothing to see here"))
        .post_async("/graphql", |mut req, _| async move {
            let body = req.json::<Query>().await?;
            let gqlast = parse_query(&body.query).unwrap();
            let (statement, params, tags) =
                gql2sql_rs(gqlast, &body.variables, body.operation_name).unwrap();
            let mut fetch_headers = worker::Headers::new();
            fetch_headers.set(
                "Neon-Connection-String",
                "postgres://nick:MvRE0dSpsKI1@ep-cold-violet-821289.us-west-2.aws.neon.tech/neondb",
            )?;
            fetch_headers.set("Content-Type", "application/json")?;
            let payload = SqlPayload {
                query: statement.to_string(),
                params,
            };
            let mut resp = Fetch::Request(Request::new_with_init(
                "https://ep-cold-violet-821289.us-west-2.aws.neon.tech/sql",
                RequestInit::new()
                    .with_method(worker::Method::Post)
                    .with_headers(fetch_headers)
                    .with_body(Some(serde_json::to_string(&payload)?.into())),
            )?)
            .send()
            .await?;
            let data = resp.json::<SqlResponse>().await?;
            let rows = data.rows;
            let first_row = rows.into_iter().next().ok_or("No rows returned")?;
            let resp = Response::from_json(&QueryResult {
                data: first_row.data,
                extensions: Some(Extensions { tags }),
            })?;
            Ok(resp)
        })
        .get("/worker-version", |_, ctx| {
            let version = ctx.var("WORKERS_RS_VERSION")?.to_string();
            Response::ok(version)
        })
        .run(request, env)
        .await
}
