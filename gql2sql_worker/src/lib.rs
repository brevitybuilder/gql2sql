use async_graphql_parser::parse_query;
use gql2sql::gql2sql as gql2sql_rs;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use worker::{event, Cache, Env, Request, Response, Result, Router};

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

#[event(fetch)]
pub async fn main(request: Request, env: Env, ctx: worker::Context) -> Result<Response> {
    // Optionally, get more helpful error messages written to the console in the case of a panic.
    let router = Router::new();
    let reponse = router
        .get("/", |_, _| Response::ok("Nothing to see here"))
        .post_async("/graphql", |mut req, _| async move {
            let cache = Cache::default();
            if let Some(response) = cache.get(&req, false).await? {
                return Ok(response);
            }
            let body = req.json::<Query>().await?;
            let gqlast = parse_query(&body.query).unwrap();
            let (statement, params, tags) =
                gql2sql_rs(gqlast, &body.variables, body.operation_name).unwrap();
            let mut resp = Response::from_json(&Gql2sql {
                query: statement.to_string(),
                vars: params,
								tags,
            })?;
            resp.headers_mut().set("cache-control", "max-age=86400")?;
            //
            Ok(resp)
        })
        .get("/worker-version", |_, ctx| {
            let version = ctx.var("WORKERS_RS_VERSION")?.to_string();
            Response::ok(version)
        })
        .run(request.clone().unwrap(), env)
        .await;

    if let Ok(mut resp) = reponse {
        let cache = Cache::default();
        let cloned = resp.cloned()?;
        ctx.wait_until(async move {
            let _ = cache.put(&request, cloned).await;
        });
        return Ok(resp);
    }
    reponse
}
