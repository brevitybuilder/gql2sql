#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use napi_derive::napi;
use graphql_parser::query::parse_query;
use gql2sql::gql2sql as gql2sql_rs;

#[napi]
pub fn gql2sql(query: String) -> String {
    let gqlast = parse_query::<&str>(&query).unwrap();
    let statement = gql2sql_rs(gqlast).unwrap();
    statement.to_string()
}

