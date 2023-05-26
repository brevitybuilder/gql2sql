use async_graphql_parser::parse_query;
use gql2sql::gql2sql as gql2sql_rs;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use worker::{event, Cache, Env, Fetch, Request, RequestInit, Response, Result, Router};

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

#[derive(Deserialize)]
struct SqlResponse {
    rows: Box<serde_json::value::RawValue>,
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
            let (statement, params, _tags) =
                gql2sql_rs(gqlast, &body.variables, body.operation_name).unwrap();
            let mut fetch_headers = worker::Headers::new();
            fetch_headers.set(
                "Neon-Connection-String",
                "postgres://nick:MvRE0dSpsKI1@ep-withered-paper-740685.us-west-2.aws.neon.tech/neondb",
            )?;
            fetch_headers.set(
                "Content-Type",
                "application/json"
            )?;
            let payload = SqlPayload {
                query: statement.to_string(),
                params,
            };
            let mut resp = Fetch::Request(Request::new_with_init(
                "https://ep-withered-paper-740685.us-west-2.aws.neon.tech/sql",
                RequestInit::new()
                    .with_method(worker::Method::Post)
                    .with_headers(fetch_headers)
                    .with_body(Some(serde_json::to_string(&payload)?.into())),
            )?)
            .send()
            .await?;
            let data = resp.json::<SqlResponse>().await?;
            let resp = Response::from_json(&data.rows)?;
            Ok(resp)
        })
        .get("/worker-version", |_, ctx| {
            let version = ctx.var("WORKERS_RS_VERSION")?.to_string();
            Response::ok(version)
        })
        .run(request, env)
        .await
}
