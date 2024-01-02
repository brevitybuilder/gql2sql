mod utils;

use async_graphql_parser::parse_query;
use gql2sql::{detect_date, gql2sql as gql2sql_rs};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utils::set_panic_hook;
use wasm_bindgen::prelude::*;

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[derive(Deserialize)]
pub struct Args {
    pub query: String,
    pub variables: Option<Value>,
    pub operation_name: Option<String>,
}

#[derive(Serialize)]
pub struct GqlResult {
    pub sql: String,
    pub params: Option<Vec<Value>>,
    pub tags: Option<Vec<String>>,
    #[serde(rename = "isMutation")]
    pub is_mutation: bool,
}

#[wasm_bindgen]
pub fn gql2sql(args: String) -> Result<String, JsError> {
    set_panic_hook();
    let Args {
        query,
        variables,
        operation_name,
    } = serde_json::from_str(&args)?;
    let ast = parse_query(query)?;
    let (sql, params, tags, is_mutation) =
        gql2sql_rs(ast, &variables, operation_name).map_err(|e| JsError::new(&e.to_string()))?;
    let params = params.map(|o| {
        o.into_iter()
            .map(|a| match a {
                Value::String(s) => {
                    if let Some(date) = detect_date(&s) {
                        Value::String(date)
                    } else {
                        Value::String(s)
                    }
                }
                Value::Null => Value::Null,
                Value::Number(s) => Value::Number(s),
                Value::Bool(s) => Value::Bool(s),
                Value::Object(obj) => Value::String(serde_json::to_string(&obj).unwrap()),
                Value::Array(list) => Value::String(serde_json::to_string(&list).unwrap()),
            })
            .collect()
    });
    let result = GqlResult {
        sql: sql.to_string(),
        params,
        tags,
        is_mutation,
    };
    Ok(serde_json::to_string(&result)?)
}
