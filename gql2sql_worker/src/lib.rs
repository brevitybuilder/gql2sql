use serde::{Deserialize, Serialize};
use gql2sql::gql2sql as gql2sql_rs;
use graphql_parser::query::parse_query;
use worker::*;

#[derive(Debug, Serialize, Deserialize)]
struct Query {
    query: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Gql2sql {
	query: String,
	vars: Option<Vec<String>>,
}

#[event(fetch)]
pub async fn main(req: Request, env: Env, _ctx: worker::Context) -> Result<Response> {
    // Optionally, get more helpful error messages written to the console in the case of a panic.
    let router = Router::new();
    router
        .get("/", |_, _| Response::ok("Hello from Workers!"))
        .post_async("/graphql", |mut req, _| async move {
						let body = req.json::<Query>().await?;
						let gqlast = parse_query::<&str>(&body.query).unwrap();
						let (statement, params) = gql2sql_rs(gqlast).unwrap();
						Response::ok(serde_json::to_string(&Gql2sql {
							query: statement.to_string(),
							vars: params,
						}).unwrap())
        })
        .get("/worker-version", |_, ctx| {
            let version = ctx.var("WORKERS_RS_VERSION")?.to_string();
            Response::ok(version)
        })
        .run(req, env)
        .await
}
