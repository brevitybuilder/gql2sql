use gql2sql::gql2sql as gql2sql_rs;
use graphql_parser::query::parse_query;
use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub fn gql2sql(buf: Buffer) -> Buffer {
  let code = unsafe { std::str::from_utf8_unchecked(&buf) };
  let gqlast = parse_query::<&str>(code).expect("Failed to parse query");
  let (statement, _params) = gql2sql_rs(gqlast).expect("Failed to convert query");
  statement.to_string().into_bytes().into()
}
