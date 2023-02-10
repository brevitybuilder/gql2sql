use deno_bindgen::deno_bindgen;
use gql2sql::gql2sql as gql2sql_rs;
use graphql_parser::query::parse_query;

#[deno_bindgen]
pub fn gql2sql(code: &str) -> String {
    let gqlast = parse_query::<&str>(&code).expect("Failed to parse query");
    let (statement, _params) = gql2sql_rs(gqlast).expect("Failed to convert query");
    statement.to_string()
}
