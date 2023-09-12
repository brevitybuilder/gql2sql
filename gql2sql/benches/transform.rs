#![allow(
    clippy::too_many_lines,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

use async_graphql_parser::parse_query;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gql2sql::gql2sql;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("parse", |b| {
        b.iter(|| {
            parse_query(black_box(
                r#"query App {
                App(filter: { id: { eq: "345810043118026832" } }) {
                    id
                    components @relation(table: "Component", field: ["appId"], references: ["id"]) {
                        id
                    }
                }
            }"#,
            ))
        });
    });
    let gqlast = parse_query(
        r#"query App {
                App(filter: { id: { eq: "345810043118026832" } }) {
                    id
                    components @relation(table: "Component", field: ["appId"], references: ["id"]) {
                        id
                    }
                }
            }"#,
    )
    .unwrap();
    c.bench_function("transform", |b| {
        b.iter(|| gql2sql(black_box(gqlast.clone()), &None, Some("App".to_string())));
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
