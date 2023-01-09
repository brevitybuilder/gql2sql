use anyhow::anyhow;
use graphql_parser::query::{Definition, Document, Field, OperationDefinition, Selection};
use graphql_parser::schema::{Text, Value as GqlValue};
use sqlparser::ast::{
    Assignment, BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, Join,
    JoinConstraint, JoinOperator, ObjectName, Offset, OffsetRows, OrderByExpr, Query, Select,
    SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value, Values,
    WildcardAdditionalOptions,
};
use std::collections::BTreeMap;

fn get_value<'a, T: Text<'a>>(value: &GqlValue<'a, T>) -> Value {
    match value {
        GqlValue::Variable(v) => Value::Placeholder(v.as_ref().into()),
        GqlValue::Null => Value::Null,
        GqlValue::String(s) => Value::SingleQuotedString(s.clone()),
        GqlValue::Int(i) => Value::Number(i.as_i64().expect("Number to be an i64").to_string(), false),
        GqlValue::Float(f) => Value::Number(f.to_string(), false),
        GqlValue::Boolean(b) => Value::Boolean(b.to_owned()),
        GqlValue::Enum(e) => Value::SingleQuotedString(e.as_ref().into()),
        GqlValue::List(_l) => unimplemented!(),
        GqlValue::Object(_o) => unimplemented!(),
    }
}

fn get_op(op: &str) -> BinaryOperator {
    match op {
        "eq" | "equals" => BinaryOperator::Eq,
        "neq" | "not_equals" => BinaryOperator::NotEq,
        "lt" | "less_than" => BinaryOperator::Lt,
        "lte" | "less_than_or_equals" => BinaryOperator::LtEq,
        "gt" | "greater_than" => BinaryOperator::Gt,
        "gte" | "greater_than_or_equals" => BinaryOperator::GtEq,
        _ => unimplemented!(),
    }
}

fn get_expr<'a, T: Text<'a>>(left: Expr, args: &GqlValue<'a, T>) -> Option<Expr> {
    if let GqlValue::Object(o) = args {
        return match o.len() {
            0 => None,
            1 => {
                let (op, value) = o.iter().next().expect("list to have one item");
                let right_value = get_value(value);
                match op.as_ref() {
                    "like" => Some(Expr::Like {
                        negated: false,
                        expr: Box::new(left),
                        pattern: Box::new(Expr::Value(right_value)),
                        escape_char: None,
                    }),
                    "ilike" => Some(Expr::ILike {
                        negated: false,
                        expr: Box::new(left),
                        pattern: Box::new(Expr::Value(right_value)),
                        escape_char: None,
                    }),
                    _ => {
                        let op = get_op(op.as_ref());
                        Some(Expr::BinaryOp {
                            left: Box::new(left),
                            op,
                            right: Box::new(Expr::Value(right_value)),
                        })
                    }
                }
            }
            _ => {
                let mut conditions: Vec<Expr> = o
                    .iter()
                    .rev()
                    .map(|(op, v)| {
                        let op = get_op(op.as_ref());
                        let value = get_value(v);
                        Expr::BinaryOp {
                            left: Box::new(left.clone()),
                            op,
                            right: Box::new(Expr::Value(value)),
                        }
                    })
                    .collect();
                let mut last_expr = conditions.remove(0);
                for condition in conditions {
                    let expr = Expr::BinaryOp {
                        left: Box::new(condition),
                        op: BinaryOperator::And,
                        right: Box::new(last_expr),
                    };
                    last_expr = expr;
                }
                Some(last_expr)
            }
        };
    };
    None
}

fn get_filter<'a, T: Text<'a>>(args: &BTreeMap<T::Value, GqlValue<'a, T>>) -> Option<Expr> {
    match args.len() {
        0 => None,
        1 => {
            let (key, value) = args.iter().next().expect("list to have one item");
            match (key.as_ref(), value) {
                ("or", GqlValue::List(list)) => match list.len() {
                    0 => None,
                    1 => match list.get(0).expect("list to have one item") {
                        GqlValue::Object(o) => get_filter(o),
                        _ => None,
                    },
                    _ => {
                        let mut conditions: Vec<Expr> = list
                            .iter()
                            .map(|v| match v {
                                GqlValue::Object(o) => get_filter(o),
                                _ => None,
                            })
                            .filter_map(|v| v)
                            .collect();
                        let mut last_expr = conditions.remove(0);
                        for condition in conditions {
                            let expr = Expr::BinaryOp {
                                left: Box::new(condition),
                                op: BinaryOperator::Or,
                                right: Box::new(last_expr),
                            };
                            last_expr = expr;
                        }
                        Some(last_expr)
                    }
                },
                ("and", GqlValue::List(list)) => match list.len() {
                    0 => None,
                    1 => match list.get(0).expect("list to have one item") {
                        GqlValue::Object(o) => get_filter(o),
                        _ => None,
                    },
                    _ => {
                        let mut conditions: Vec<Expr> = list
                            .iter()
                            .map(|v| match v {
                                GqlValue::Object(o) => get_filter(o),
                                _ => None,
                            })
                            .filter_map(|v| v)
                            .collect();
                        let mut last_expr = conditions.remove(0);
                        for condition in conditions {
                            let expr = Expr::BinaryOp {
                                left: Box::new(condition),
                                op: BinaryOperator::And,
                                right: Box::new(last_expr),
                            };
                            last_expr = expr;
                        }
                        Some(last_expr)
                    }
                },
                _ => {
                    let left = Expr::Identifier(Ident {
                        value: key.as_ref().to_owned(),
                        quote_style: Some('"'),
                    });
                    get_expr(left, value)
                }
            }
        }
        _ => {
            let mut conditions: Vec<Expr> = args
                .iter()
                .rev()
                .map_while(|(key, value)| {
                    get_expr(
                        Expr::Identifier(Ident {
                            value: key.as_ref().into(),
                            quote_style: Some('"'),
                        }),
                        value,
                    )
                })
                .collect();
            let mut last_expr = conditions.remove(0);
            for condition in conditions {
                let expr = Expr::BinaryOp {
                    left: Box::new(condition),
                    op: BinaryOperator::And,
                    right: Box::new(last_expr),
                };
                last_expr = expr;
            }
            Some(last_expr)
        }
    }
}

fn get_agg_query<'a, T: Text<'a>>(
    aggs: Vec<FunctionArg>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
) -> SetExpr {
    SetExpr::Select(Box::new(Select {
        distinct: false,
        top: None,
        into: None,
        projection: vec![SelectItem::ExprWithAlias {
            alias: Ident {
                value: "root".into(),
                quote_style: Some('"'),
            },
            expr: Expr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: "json_build_object".to_string(),
                    quote_style: None,
                }]),
                args: aggs,
                over: None,
                distinct: false,
                special: false,
            }),
        }],
        from,
        lateral_views: Vec::new(),
        selection,
        group_by: Vec::new(),
        cluster_by: Vec::new(),
        distribute_by: Vec::new(),
        sort_by: Vec::new(),
        having: None,
        qualify: None,
    }))
}

fn get_root_query<'a, T: Text<'a>>(
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    is_single: bool,
    alias: &T::Value,
) -> SetExpr {
    let mut base = Expr::Function(Function {
        name: ObjectName(vec![Ident {
            value: "row_to_json".to_string(),
            quote_style: None,
        }]),
        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(
            Box::new(Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(Select {
                    distinct: false,
                    top: None,
                    projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: "root".to_string(),
                        quote_style: Some('"'),
                    }))],
                    into: None,
                    from: vec![TableWithJoins {
                        relation: TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(Query {
                                with: None,
                                body: Box::new(SetExpr::Select(Box::new(Select {
                                    distinct: false,
                                    top: None,
                                    projection,
                                    into: None,
                                    from: vec![],
                                    lateral_views: vec![],
                                    selection: None,
                                    group_by: vec![],
                                    cluster_by: vec![],
                                    distribute_by: vec![],
                                    sort_by: vec![],
                                    having: None,
                                    qualify: None,
                                }))),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                                fetch: None,
                                lock: None,
                            }),
                            alias: Some(TableAlias {
                                name: Ident {
                                    value: "root".to_string(),
                                    quote_style: Some('"'),
                                },
                                columns: vec![],
                            }),
                        },
                        joins: vec![],
                    }],
                    lateral_views: vec![],
                    selection: None,
                    group_by: vec![],
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    qualify: None,
                }))),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
                lock: None,
            }),
        )))],
        over: None,
        distinct: false,
        special: false,
    });
    if is_single == false {
        base = Expr::Function(Function {
            over: None,
            distinct: false,
            special: false,
            name: ObjectName(vec![Ident {
                value: "coalesce".to_string(),
                quote_style: None,
            }]),
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    distinct: false,
                    over: None,
                    special: false,
                    name: ObjectName(vec![Ident {
                        value: "json_agg".to_string(),
                        quote_style: None,
                    }]),
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(base))],
                }))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString("[]".to_string()),
                ))),
            ],
        });
    }
    SetExpr::Select(Box::new(Select {
        distinct: false,
        top: None,
        projection: vec![SelectItem::ExprWithAlias {
            alias: Ident {
                value: alias.as_ref().into(),
                quote_style: Some('"'),
            },
            expr: base,
        }],
        into: None,
        from,
        lateral_views: Vec::new(),
        selection,
        group_by: Vec::new(),
        cluster_by: Vec::new(),
        distribute_by: Vec::new(),
        sort_by: Vec::new(),
        having: None,
        qualify: None,
    }))
}

fn get_agg_filter_projection<'a, T: Text<'a>>(field: &Field<'a, T>) -> SelectItem {
    match field.name.as_ref() {
        "count" => SelectItem::UnnamedExpr(Expr::Value(Value::Number("1".to_string(), false))),
        _ => SelectItem::Wildcard(WildcardAdditionalOptions::default()),
    }
}

fn get_agg_agg_projection<'a, T: Text<'a>>(field: &Field<'a, T>) -> Vec<FunctionArg> {
    let name = field.name.as_ref();
    match name {
        "count" => {
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(field.name.as_ref().into()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: name.to_uppercase().to_string(),
                        quote_style: None,
                    }]),
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
                    over: None,
                    distinct: false,
                    special: false,
                }))),
            ]
        }
        "min" | "max" | "avg" => {
            let projection = field
                .selection_set
                .items
                .iter()
                .map(|arg| {
                    if let Selection::Field(field) = arg {
                        vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString(field.name.as_ref().into()),
                            ))),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                                name: ObjectName(vec![Ident {
                                    value: name.to_uppercase().to_string(),
                                    quote_style: None,
                                }]),
                                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(Ident {
                                        value: field.name.as_ref().into(),
                                        quote_style: Some('"'),
                                    }),
                                ))],
                                over: None,
                                distinct: false,
                                special: false,
                            }))),
                        ]
                    } else {
                        unreachable!()
                    }
                })
                .flatten()
                .collect();
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(field.name.as_ref().into()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: "json_build_object".to_string(),
                        quote_style: None,
                    }]),
                    args: projection,
                    over: None,
                    distinct: false,
                    special: false,
                }))),
            ]
        }
        _ => vec![],
    }
}

fn get_aggregate_projection<'a, T: Text<'a>>(
    items: &Vec<Selection<'a, T>>,
    _path: &str,
) -> Vec<FunctionArg> {
    let mut projection = Vec::new();
    let mut aggs = Vec::new();
    for selection in items {
        match selection {
            Selection::Field(field) => {
                projection.push(get_agg_filter_projection(field));
                aggs.extend(get_agg_agg_projection(field));
            }
            Selection::FragmentSpread(_) => unimplemented!(),
            Selection::InlineFragment(_) => unimplemented!(),
        }
    }
    aggs
}

fn get_projection<'a, T: Text<'a>>(
    items: &Vec<Selection<'a, T>>,
    relation: &str,
    path: Option<&str>,
) -> (Vec<SelectItem>, Vec<Join>) {
    let mut projection = Vec::new();
    let mut joins = Vec::new();
    for selection in items {
        match selection {
            Selection::Field(field) => {
                if !field.selection_set.items.is_empty() {
                    let (selection, order_by, mut first, after) = parse_args(&field.arguments);
                    let (relation, fk, pk, is_single) = get_relation(field);
                    if is_single {
                        first = Some(Expr::Value(Value::Number("1".to_string(), false)));
                    }
                    let sub_path =
                        path.map_or_else(|| relation.clone(), |v| v.to_string() + "." + &relation);
                    let (sub_projection, sub_joins) =
                        get_projection(&field.selection_set.items, &relation, Some(&sub_path));
                    let join_filter = Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            Ident {
                                value: relation.clone(),
                                quote_style: Some('"'),
                            },
                            Ident {
                                value: fk.clone(),
                                quote_style: Some('"'),
                            },
                        ])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            Ident {
                                value: path.map_or("base".to_string(), |v| v.to_string()),
                                quote_style: Some('"'),
                            },
                            Ident {
                                value: pk.clone(),
                                quote_style: Some('"'),
                            },
                        ])),
                    };
                    let sub_query = get_filter_query(
                        Some(selection.map_or(join_filter.clone(), |s| Expr::BinaryOp {
                            left: Box::new(join_filter),
                            op: BinaryOperator::And,
                            right: Box::new(s),
                        })),
                        order_by,
                        first,
                        after,
                        relation.clone(),
                    );
                    joins.push(Join {
                        relation: TableFactor::Derived {
                            lateral: true,
                            subquery: Box::new(Query {
                                with: None,
                                body: Box::new(get_root_query::<T>(
                                    sub_projection,
                                    vec![TableWithJoins {
                                        relation: TableFactor::Derived {
                                            lateral: false,
                                            subquery: Box::new(sub_query),
                                            alias: Some(TableAlias {
                                                name: Ident {
                                                    value: sub_path.clone(),
                                                    quote_style: Some('"'),
                                                },
                                                columns: vec![],
                                            }),
                                        },
                                        joins: sub_joins,
                                    }],
                                    None,
                                    is_single,
                                    &field.name,
                                )),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                                fetch: None,
                                lock: None,
                            }),
                            alias: Some(TableAlias {
                                name: Ident {
                                    value: "root.".to_owned() + &relation,
                                    quote_style: Some('"'),
                                },
                                columns: vec![],
                            }),
                        },
                        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(Expr::Nested(
                            Box::new(Expr::Value(Value::SingleQuotedString("true".to_string()))),
                        ))),
                    });
                    projection.push(SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: field.name.as_ref().into(),
                        quote_style: Some('"'),
                    })));
                } else {
                    match &field.alias {
                        Some(alias) => {
                            projection.push(SelectItem::ExprWithAlias {
                                expr: path.map_or_else(
                                    || {
                                        Expr::Identifier(Ident {
                                            value: field.name.as_ref().into(),
                                            quote_style: Some('"'),
                                        })
                                    },
                                    |path| {
                                        Expr::CompoundIdentifier(vec![
                                            Ident {
                                                value: path.to_string(),
                                                quote_style: Some('"'),
                                            },
                                            Ident {
                                                value: field.name.as_ref().into(),
                                                quote_style: Some('"'),
                                            },
                                        ])
                                    },
                                ),
                                alias: Ident {
                                    value: alias.as_ref().into(),
                                    quote_style: Some('"'),
                                },
                            });
                        }
                        None => {
                            let name = field.name.as_ref().into();
                            if name == "__typename" {
                                projection.push(SelectItem::ExprWithAlias {
                                    alias: Ident {
                                        value: name,
                                        quote_style: Some('"'),
                                    },
                                    expr: Expr::Value(Value::SingleQuotedString(
                                        relation.to_string(),
                                    )),
                                });
                            } else {
                                projection.push(SelectItem::UnnamedExpr(path.map_or_else(
                                    || {
                                        Expr::Identifier(Ident {
                                            value: name.clone(),
                                            quote_style: Some('"'),
                                        })
                                    },
                                    |path| {
                                        Expr::CompoundIdentifier(vec![
                                            Ident {
                                                value: path.to_string(),
                                                quote_style: Some('"'),
                                            },
                                            Ident {
                                                value: name.clone(),
                                                quote_style: Some('"'),
                                            },
                                        ])
                                    },
                                )));
                            }
                        }
                    }
                }
            }
            _ => unimplemented!(),
        }
    }
    (projection, joins)
}

fn value_to_string<'a, T: Text<'a>>(value: &GqlValue<'a, T>) -> String {
    match value {
        GqlValue::String(s) => s.clone(),
        GqlValue::Int(i) => i.as_i64().expect("int to be an i64").to_string(),
        GqlValue::Float(f) => f.to_string(),
        GqlValue::Boolean(b) => b.to_string(),
        GqlValue::Enum(e) => e.as_ref().into(),
        GqlValue::List(l) => l
            .iter()
            .map(|v| value_to_string(v))
            .collect::<Vec<String>>()
            .join(","),
        GqlValue::Null => "null".to_string(),
        _ => unimplemented!(),
    }
}

fn get_relation<'a, T: Text<'a>>(field: &Field<'a, T>) -> (String, String, String, bool) {
    let mut relation: String = field.name.as_ref().into();
    let mut fk = relation.clone() + "_id";
    let mut pk = "id".into();
    let mut is_single = false;
    for directive in &field.directives {
        let name: &str = directive.name.as_ref();
        if name == "relation" {
            for argument in &directive.arguments {
                match argument.0.as_ref() {
                    "table" => relation = value_to_string(&argument.1),
                    "field" => fk = value_to_string(&argument.1),
                    "references" => pk = value_to_string(&argument.1),
                    "single" => {
                        if let GqlValue::Boolean(b) = &argument.1 {
                            is_single = *b;
                        }
                    }
                    _ => unimplemented!(),
                }
            }
        }
    }
    (relation, fk, pk, is_single)
}

fn get_filter_query(
    selection: Option<Expr>,
    order_by: Vec<OrderByExpr>,
    first: Option<Expr>,
    after: Option<Offset>,
    table_name: String,
) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: false,
            top: None,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: table_name,
                        quote_style: Some('"'),
                    }]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            selection,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            qualify: None,
        }))),
        order_by,
        limit: first,
        offset: after,
        fetch: None,
        lock: None,
    }
}

fn get_order<'a, T: Text<'a>>(order: &BTreeMap<T::Value, GqlValue<'a, T>>) -> Vec<OrderByExpr> {
    let mut order_by = vec![];
    for (key, value) in order.iter() {
        order_by.push(OrderByExpr {
            expr: Expr::Identifier(Ident {
                value: key.as_ref().into(),
                quote_style: Some('"'),
            }),
            asc: match value {
                GqlValue::String(s) => Some(s == "ASC"),
                GqlValue::Enum(e) => {
                    let s: &str = e.as_ref();
                    Some(s == "ASC")
                }
                _ => unimplemented!(),
            },
            nulls_first: None,
        });
    }
    order_by
}

fn parse_args<'a, T: Text<'a>>(
    arguments: &Vec<(T::Value, GqlValue<'a, T>)>,
) -> (Option<Expr>, Vec<OrderByExpr>, Option<Expr>, Option<Offset>) {
    let mut selection = None;
    let mut order_by = vec![];
    let mut first = None;
    let mut after = None;
    for argument in arguments {
        let (key, value) = argument;
        match (key.as_ref(), value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(filter);
            }
            ("order", GqlValue::Object(order)) => {
                order_by = get_order(order);
            }
            ("first", GqlValue::Int(count)) => {
                first = Some(Expr::Value(Value::Number(
                    count.as_i64().expect("int to be an i64").to_string(),
                    false,
                )));
            }
            ("after", GqlValue::Int(count)) => {
                after = Some(Offset {
                    value: Expr::Value(Value::Number(count.as_i64().expect("int to be an i64").to_string(), false)),
                    rows: OffsetRows::None,
                });
            }
            _ => {}
        }
    }
    (selection, order_by, first, after)
}

fn get_mutation_columns<'a, T: Text<'a>>(
    arguments: &Vec<(T::Value, GqlValue<'a, T>)>,
) -> (Vec<Ident>, Vec<Vec<Expr>>) {
    let mut columns = vec![];
    let mut rows = vec![];
    for argument in arguments {
        let (key, value) = argument;
        match (key.as_ref(), value) {
            ("data", GqlValue::Object(data)) => {
                let mut row = vec![];
                for (key, value) in data.iter() {
                    columns.push(Ident {
                        value: key.as_ref().into(),
                        quote_style: Some('"'),
                    });
                    row.push(Expr::Value(get_value(value)));
                }
                rows.push(row);
            }
            ("data", GqlValue::List(list)) => {
                if list.len() == 0 {
                    continue;
                }
                for (i, item) in list.iter().enumerate() {
                    let mut row = vec![];
                    if let GqlValue::Object(data) = item {
                        for (key, value) in data.iter() {
                            if i == 0 {
                                columns.push(Ident {
                                    value: key.as_ref().into(),
                                    quote_style: Some('"'),
                                });
                            }
                            row.push(Expr::Value(get_value(value)));
                        }
                    }
                    rows.push(row);
                }
            }
            _ => todo!(),
        }
    }
    (columns, rows)
}

fn get_mutation_assignments<'a, T: Text<'a>>(
    arguments: &Vec<(T::Value, GqlValue<'a, T>)>,
) -> (Option<Expr>, Vec<Assignment>) {
    let mut selection = None;
    let mut assignments = vec![];
    for argument in arguments {
        let (key, value) = argument;
        match (key.as_ref(), value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(filter);
            }
            ("set", GqlValue::Object(data)) => {
                for (key, value) in data.iter() {
                    assignments.push(Assignment {
                        id: vec![Ident {
                            value: key.as_ref().into(),
                            quote_style: Some('"'),
                        }],
                        value: Expr::Value(get_value(value)),
                    });
                }
            }
            ("inc" | "increment", GqlValue::Object(data)) => {
                for (key, value) in data.iter() {
                    let column_ident = Ident {
                        value: key.as_ref().into(),
                        quote_style: Some('"'),
                    };
                    assignments.push(Assignment {
                        id: vec![column_ident.clone()],
                        value: Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(column_ident)),
                            op: BinaryOperator::Plus,
                            right: Box::new(Expr::Value(get_value(value))),
                        },
                    });
                }
            }
            _ => todo!(),
        }
    }
    (selection, assignments)
}

pub fn gql2sql<'a, T: Text<'a>>(ast: Document<'a, T>) -> Result<Statement, anyhow::Error> {
    let mut statements = Vec::new();
    for definition in ast.definitions {
        match definition {
            Definition::Operation(operation) => match operation {
                OperationDefinition::Query(query) => {
                    for selection in query.selection_set.items {
                        match selection {
                            Selection::Field(field) => {
                                let mut name = field.name.as_ref();
                                let key = field.alias.map_or_else(
                                    || field.name.as_ref().into(),
                                    |alias| alias.as_ref().into(),
                                );
                                let (selection, order_by, first, after) =
                                    parse_args(&field.arguments);
                                if name.ends_with("_aggregate") {
                                    name = &name[..name.len() - 10];
                                    let aggs = get_aggregate_projection(
                                        &field.selection_set.items,
                                        "base",
                                    );
                                    let base_query = get_filter_query(
                                        selection,
                                        order_by,
                                        first,
                                        after,
                                        name.to_owned(),
                                    );
                                    statements.push((
                                        key,
                                        Query {
                                            with: None,
                                            body: Box::new(get_agg_query::<&str>(
                                                aggs,
                                                vec![TableWithJoins {
                                                    relation: TableFactor::Derived {
                                                        lateral: false,
                                                        subquery: Box::new(base_query),
                                                        alias: Some(TableAlias {
                                                            name: Ident {
                                                                value: "base".to_string(),
                                                                quote_style: Some('"'),
                                                            },
                                                            columns: vec![],
                                                        }),
                                                    },
                                                    joins: vec![],
                                                }],
                                                None,
                                            )),
                                            order_by: vec![],
                                            limit: None,
                                            offset: None,
                                            fetch: None,
                                            lock: None,
                                        },
                                    ));
                                } else {
                                    let (projection, joins) = get_projection(
                                        &field.selection_set.items,
                                        name,
                                        Some("base"),
                                    );
                                    let base_query = get_filter_query(
                                        selection,
                                        order_by,
                                        first,
                                        after,
                                        name.to_owned(),
                                    );
                                    statements.push((
                                        key,
                                        Query {
                                            with: None,
                                            body: Box::new(get_root_query::<&str>(
                                                projection,
                                                vec![TableWithJoins {
                                                    relation: TableFactor::Derived {
                                                        lateral: false,
                                                        subquery: Box::new(base_query),
                                                        alias: Some(TableAlias {
                                                            name: Ident {
                                                                value: "base".to_string(),
                                                                quote_style: Some('"'),
                                                            },
                                                            columns: vec![],
                                                        }),
                                                    },
                                                    joins,
                                                }],
                                                None,
                                                false,
                                                &"root",
                                            )),
                                            order_by: vec![],
                                            limit: None,
                                            offset: None,
                                            fetch: None,
                                            lock: None,
                                        },
                                    ));
                                };
                            }
                            Selection::FragmentSpread(_) => unimplemented!(),
                            Selection::InlineFragment(_) => unimplemented!(),
                        }
                    }
                    return Ok(Statement::Query(Box::new(Query {
                        with: None,
                        body: Box::new(SetExpr::Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            into: None,
                            projection: vec![SelectItem::ExprWithAlias {
                                alias: Ident {
                                    value: "data".into(),
                                    quote_style: Some('"'),
                                },
                                expr: Expr::Function(Function {
                                    name: ObjectName(vec![Ident {
                                        value: "json_build_object".to_string(),
                                        quote_style: None,
                                    }]),
                                    args: statements
                                        .into_iter()
                                        .map(|(key, query)| {
                                            vec![
                                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                    Expr::Value(Value::SingleQuotedString(key)),
                                                )),
                                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                    Expr::Subquery(Box::new(query)),
                                                )),
                                            ]
                                        })
                                        .flatten()
                                        .collect(),
                                    over: None,
                                    distinct: false,
                                    special: false,
                                }),
                            }],
                            from: vec![],
                            lateral_views: Vec::new(),
                            selection: None,
                            group_by: Vec::new(),
                            cluster_by: Vec::new(),
                            distribute_by: Vec::new(),
                            sort_by: Vec::new(),
                            having: None,
                            qualify: None,
                        }))),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                        lock: None,
                    })));
                }
                OperationDefinition::Mutation(mutation) => {
                    for selection in mutation.selection_set.items {
                        match selection {
                            Selection::Field(field) => {
                                let mut name = field.name.as_ref();
                                // let key = field.alias.map_or_else(
                                //     || field.name.as_ref().into(),
                                //     |alias| alias.as_ref().into(),
                                // );
                                if name.starts_with("insert_") {
                                    name = &name[7..];
                                    let (columns, rows) = get_mutation_columns(&field.arguments);
                                    let (projection, _) =
                                        get_projection(&field.selection_set.items, name, None);
                                    return Ok(Statement::Insert {
                                        or: None,
                                        into: true,
                                        table_name: ObjectName(vec![Ident {
                                            value: name.to_string(),
                                            quote_style: Some('"'),
                                        }]),
                                        columns,
                                        overwrite: false,
                                        source: Box::new(Query {
                                            with: None,
                                            body: Box::new(SetExpr::Values(Values {
                                                explicit_row: false,
                                                rows,
                                            })),
                                            order_by: vec![],
                                            limit: None,
                                            offset: None,
                                            fetch: None,
                                            lock: None,
                                        }),
                                        partitioned: None,
                                        after_columns: vec![],
                                        table: false,
                                        on: None,
                                        returning: Some(projection),
                                    });
                                } else if name.starts_with("update_") {
                                    name = &name[7..];
                                    let (projection, _) =
                                        get_projection(&field.selection_set.items, name, None);
                                    let (selection, assignments) =
                                        get_mutation_assignments(&field.arguments);
                                    return Ok(Statement::Update {
                                        table: TableWithJoins {
                                            relation: TableFactor::Table {
                                                name: ObjectName(vec![Ident {
                                                    value: name.to_string(),
                                                    quote_style: Some('"'),
                                                }]),
                                                alias: None,
                                                args: None,
                                                with_hints: vec![],
                                            },
                                            joins: vec![],
                                        },
                                        assignments,
                                        from: None,
                                        selection,
                                        returning: Some(projection),
                                    });
                                }
                            }
                            Selection::FragmentSpread(_) => unimplemented!(),
                            Selection::InlineFragment(_) => unimplemented!(),
                        }
                    }
                }
                OperationDefinition::Subscription(_) => unimplemented!(),
                OperationDefinition::SelectionSet(_) => todo!(),
            },
            Definition::Fragment(_) => unimplemented!(),
        }
    }
    Err(anyhow!("No operation found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphql_parser::query::parse_query;
    use pretty_assertions::assert_eq;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn query() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query App {
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
            }"#,
        )?.clone();
        let sql = r#"SELECT json_build_object('app', (SELECT coalesce(json_agg(row_to_json((SELECT "root" FROM (SELECT "base"."id", "components") AS "root"))), '[]') AS "root" FROM (SELECT * FROM "App" WHERE "id" = '345810043118026832' ORDER BY "name" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(row_to_json((SELECT "root" FROM (SELECT "base.Component"."id", "pageMeta", "elements") AS "root"))), '[]') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id") AS "base.Component" LEFT JOIN LATERAL (SELECT row_to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."id", "base.Component.PageMeta"."path") AS "root")) AS "pageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(row_to_json((SELECT "root" FROM (SELECT "base.Component.Element"."id", "base.Component.Element"."name") AS "root"))), '[]') AS "elements" FROM (SELECT * FROM "Element" WHERE "Element"."componentParentId" = "base.Component"."id" ORDER BY "order" ASC) AS "base.Component.Element") AS "root.Element" ON ('true')) AS "root.Component" ON ('true')), 'Component_aggregate', (SELECT json_build_object('count', COUNT(*), 'min', json_build_object('createdAt', MIN("createdAt"))) AS "root" FROM (SELECT * FROM "Component" WHERE "appId" = '345810043118026832') AS "base")) AS "data""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql).unwrap();
        let statement = gql2sql(gqlast)?;
        assert_eq!(vec![statement.clone()], sqlast);
        println!("Statements:\n'{}'", statement.to_string());
        Ok(())
    }

    #[test]
    fn mutation_insert() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"mutation insertVillains {
                insert_Villain(data: [
                    { name: "Ronan the Accuser" },
                    { name: "Red Skull" },
                    { name: "The Vulture" },
                ]) { id name }
            }"#,
        )?
        .clone();
        let sql = r#"INSERT INTO "Villain" ("name") VALUES ('Ronan the Accuser'), ('Red Skull'), ('The Vulture') RETURNING "id", "name""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        let statement = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement.to_string());
        assert_eq!(vec![statement.clone()], sqlast);
        Ok(())
    }

    #[test]
    fn mutation_update() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"mutation updateHero {
                update_Hero(
                    filter: { secret_identity: { eq: "Sam Wilson" }},
                    set: {
                        name: "Captain America",
                    }
                    increment: {
                        number_of_movies: 1
                    }
                ) {
                    id
                    name
                    secret_identity
                    number_of_movies
                }
            }"#,
        )?
        .clone();
        let sql = r#"UPDATE "Hero" SET "name" = 'Captain America', "number_of_movies" = "number_of_movies" + 1 WHERE "secret_identity" = 'Sam Wilson' RETURNING "id", "name", "secret_identity", "number_of_movies""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        let statement = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement.to_string());
        assert_eq!(vec![statement.clone()], sqlast);
        Ok(())
    }
}
