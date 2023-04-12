use async_graphql_parser::parse_query;
use deno_bindgen::deno_bindgen;
use gql2sql::gql2sql as gql2sql_rs;

#[deno_bindgen]
pub fn gql2sql(code: &str) -> String {
    let gqlast = parse_query(code).expect("Failed to parse query");
    let (statement, _params) = gql2sql_rs(gqlast, &None, None).expect("Failed to convert query");
    statement.to_string()
}
