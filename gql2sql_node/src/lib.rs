use cached::proc_macro::cached;
use gql2sql::gql2sql as gql2sql_rs;
use async_graphql_parser::parse_query;
use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub fn gql2sql(buf: Buffer) -> Buffer {
  let code = unsafe { std::str::from_utf8_unchecked(&buf) };
  gql2sql_inner(code.to_owned()).into_bytes().into()
}

#[cached(size = 10, time = 3600)]
pub fn gql2sql_inner(code: String) -> String {
  let gqlast = parse_query(&code).expect("Failed to parse query");
  let (statement, _params) = gql2sql_rs(gqlast, &None, None).expect("Failed to convert query");
  statement.to_string()
}
