use gql2sql::gql2sql as gql2sql_rs;
use graphql_parser::query::parse_query;
use wasm_bindgen::prelude::*;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
pub fn gql2sql(query: &str) -> JsValue {
    let gqlast = parse_query::<String>(query).unwrap();
    let statements = gql2sql_rs(gqlast).unwrap();
    let queries = statements
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();

    serde_wasm_bindgen::to_value(&queries).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(true, true);
    }
}
