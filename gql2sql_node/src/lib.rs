use async_graphql_parser::parse_query;
use gql2sql::gql2sql as gql2sql_rs;
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub struct Args {
  pub query: String,
  pub variables: Option<Value>,
  pub operation_name: Option<String>,
}

#[derive(Serialize)]
pub struct GqlResult {
  pub sql: String,
  pub params: Option<Vec<Value>>,
  pub tags: Option<Vec<String>>,
  #[serde(rename = "isMutation")]
  pub is_mutation: bool,
}

#[napi]
#[must_use]
pub fn gql2sql(mut args: String) -> anyhow::Result<String> {
  let Args {
    query,
    variables,
    operation_name,
  } = serde_json::from_str(&mut args)?;
  let ast = parse_query(query)?;
  let (sql, params, tags, is_mutation) = gql2sql_rs(ast, &variables, operation_name)?;
  let result = GqlResult {
    sql: sql.to_string(),
    params,
    tags,
    is_mutation,
  };
  serde_json::to_string(&result).map_err(|e| anyhow::anyhow!(e))
}
