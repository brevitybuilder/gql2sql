use gql2sql::gql2sql as gql2sql_rs;
use async_graphql_parser::parse_query;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn gql2sql(query: String) -> String {
    let gqlast = parse_query(&query).unwrap();
    let (statement, _params) = gql2sql_rs(gqlast, &None, None).unwrap();
    statement.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn basic() {
        let query = r#"query App {
            app: App(filter: { id: { eq: "345810043118026832" } }, order: { name: ASC }) {
                id
                components @relation(table: "Component", field: ["appId"], references: ["id"]) {
                    id
                    pageMeta @relation(table: "PageMeta", field: ["componentId"], references: ["id"], single: true) {
                        id
                        path
                    }
                    elements(order: { order: ASC }) @relation(table: "Element", field: ["componentParentId"], references: ["id"]) {
                        id
                        name
                    }
                }
            }
            Component_aggregate(filter: { appId: { eq: "345810043118026832" } }) {
                count
                min {
                createdAt
                }
            }
        }"#;
        let result = r#"SELECT json_build_object('app', (SELECT coalesce(json_agg(row_to_json((SELECT "root" FROM (SELECT "base"."id", "components") AS "root"))), '[]') AS "root" FROM (SELECT * FROM "App" WHERE "id" = '345810043118026832' ORDER BY "name" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(row_to_json((SELECT "root" FROM (SELECT "base.Component"."id", "pageMeta", "elements") AS "root"))), '[]') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id") AS "base.Component" LEFT JOIN LATERAL (SELECT row_to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."id", "base.Component.PageMeta"."path") AS "root")) AS "pageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(row_to_json((SELECT "root" FROM (SELECT "base.Component.Element"."id", "base.Component.Element"."name") AS "root"))), '[]') AS "elements" FROM (SELECT * FROM "Element" WHERE "Element"."componentParentId" = "base.Component"."id" ORDER BY "order" ASC) AS "base.Component.Element") AS "root.Element" ON ('true')) AS "root.Component" ON ('true')), 'Component_aggregate', (SELECT json_build_object('count', COUNT(*), 'min', json_build_object('createdAt', MIN("createdAt"))) AS "root" FROM (SELECT * FROM "Component" WHERE "appId" = '345810043118026832') AS "base")) AS "data""#;
        let generated = gql2sql(query.to_owned());
        assert_eq!(generated, result.to_owned());
        println!("Statements:\n'{}'", generated);
    }
}
