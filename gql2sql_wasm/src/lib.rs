use gql2sql::gql2sql as gql2sql_rs;
use graphql_parser::query::parse_query;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn gql2sql(query: String) -> String {
    let gqlast = parse_query::<String>(&query).unwrap();
    gql2sql_rs(gqlast).unwrap().to_string()
}
