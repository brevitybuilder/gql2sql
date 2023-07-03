mod utils;

use async_graphql_parser::parse_query;
use gql2sql::gql2sql as gql2sql_rs;
use simd_json::{OwnedValue as Value};
use utils::set_panic_hook;
use wasm_bindgen::prelude::*;
use serde::{Serialize, Deserialize};

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[derive(Deserialize)]
pub struct Args {
    pub query: String,
    pub vars: Option<Value>,
}

#[derive(Serialize)]
pub struct GqlResult {
    pub sql: String,
    pub params: Option<Vec<Value>>,
    pub tags: Option<Vec<String>>,
}

#[wasm_bindgen]
pub fn gql2sql(mut args: String) -> Result<String, JsError> {
    set_panic_hook();
    let Args { query, vars } = unsafe { simd_json::from_str(&mut args)? };
    let ast = parse_query(query)?;
    let (sql, params, tags) = gql2sql_rs(ast, &vars, None).map_err(|e| JsError::new(&e.to_string()))?;
    let result = GqlResult {
        sql: sql.to_string(),
        params,
        tags,
    };
    Ok(simd_json::to_string(&result)?)
}
