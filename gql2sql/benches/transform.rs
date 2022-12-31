use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gql2sql::gql2sql;
use graphql_parser::query::parse_query;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("parse", |b| {
        b.iter(|| {
            parse_query::<&str>(black_box(
                r#"query App {
                App(filter: { id: { eq: "345810043118026832" } }) {
                    id
                    components @relation(table: "Component", field: ["appId"], references: ["id"]) {
                        id
                    }
                }
            }"#,
            ))
        })
    });
    let gqlast = parse_query::<String>(
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
        b.iter(|| gql2sql(black_box(gqlast.clone())))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
