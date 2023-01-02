use gql2sql::gql2sql as gql2sql_rs;
use graphql_parser::query::parse_query;
use wasm_bindgen::prelude::*;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
pub fn gql2sql(query: &str) -> JsValue {
    let gqlast = parse_query::<&str>(query).unwrap();
    let query = gql2sql_rs(gqlast).unwrap().to_string();
    serde_wasm_bindgen::to_value(&query).unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(true, true);
    }
}
