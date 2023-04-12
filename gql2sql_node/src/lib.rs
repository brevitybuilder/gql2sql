use async_graphql_parser::parse_query;
use gql2sql::gql2sql as gql2sql_rs;
use napi_derive::napi;
use serde_json::Value;

#[napi(object)]
pub struct GqlResult {
  pub sql: String,
  pub params: Option<Vec<Value>>,
}

#[napi]
pub fn gql2sql(query: String, vars: Option<Value>) -> Option<GqlResult> {
  if let Ok(ast) = parse_query(&query) {
    if let Ok((sql, params)) = gql2sql_rs(ast, &vars, None) {
      return Some(GqlResult {
        sql: sql.to_string(),
        params,
      });
    }
  }
  return None;
}
