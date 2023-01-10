use gql2sql::gql2sql as gql2sql_rs;
use graphql_parser::query::parse_query;
use napi_derive::napi;

#[napi]
pub fn gql2sql(query: String) -> String {
  let gqlast = parse_query::<&str>(&query).unwrap();
  let (statement, _params) = gql2sql_rs(gqlast).unwrap();
  statement.to_string()
}
