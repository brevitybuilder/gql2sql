mod consts;

use crate::consts::*;
use anyhow::anyhow;
use graphql_parser::query::{
    Definition, Document, Field, OperationDefinition, Selection, TypeCondition,
};
use graphql_parser::schema::{Directive, Text, Type, Value as GqlValue};
use sqlparser::ast::{
    Assignment, BinaryOperator, DataType, Expr, Function, FunctionArg, FunctionArgExpr, Ident,
    Join, JoinConstraint, JoinOperator, ObjectName, Offset, OffsetRows, OrderByExpr, Query, Select,
    SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value, Values,
    WildcardAdditionalOptions,
};
use std::collections::BTreeMap;
use std::iter::zip;

fn get_value<'a, T: Text<'a>>(
    value: &GqlValue<'a, T>,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> Value {
    match value {
        GqlValue::Variable(v) => {
            let (index, _data_type) = parameters.get(v.as_ref()).expect("variable not found");
            Value::Placeholder(format!("${}", index))
        }
        GqlValue::Null => Value::Null,
        GqlValue::String(s) => Value::SingleQuotedString(s.clone()),
        GqlValue::Int(i) => {
            Value::Number(i.as_i64().expect("Number to be an i64").to_string(), false)
        }
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

fn get_expr<'a, T: Text<'a>>(
    left: Expr,
    args: &GqlValue<'a, T>,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> Option<Expr> {
    if let GqlValue::Object(o) = args {
        return match o.len() {
            0 => None,
            1 => {
                let (op, value) = o.iter().next().expect("list to have one item");
                let right_value = get_value(value, parameters);
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
                        let value = get_value(v, parameters);
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

fn handle_filter_arg<'a, T: Text<'a>>(
    key: &<T as Text<'a>>::Value,
    value: &GqlValue<'a, T>,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> Option<Expr> {
    match (key.as_ref(), value) {
        ("OR" | "or", GqlValue::List(list)) => match list.len() {
            0 => None,
            1 => match list.get(0).expect("list to have one item") {
                GqlValue::Object(o) => get_filter(o, parameters),
                _ => None,
            },
            _ => {
                let mut conditions: Vec<Expr> = list
                    .iter()
                    .filter_map(|v| match v {
                        GqlValue::Object(o) => get_filter(o, parameters),
                        _ => None,
                    })
                    .collect();
                let mut last_expr = conditions.remove(0);
                for condition in conditions {
                    let expr = Expr::BinaryOp {
                        left: Box::new(last_expr),
                        op: BinaryOperator::Or,
                        right: Box::new(condition),
                    };
                    last_expr = expr;
                }
                Some(Expr::Nested(Box::new(last_expr)))
            }
        },
        ("and", GqlValue::List(list)) => match list.len() {
            0 => None,
            1 => match list.get(0).expect("list to have one item") {
                GqlValue::Object(o) => get_filter(o, parameters),
                _ => None,
            },
            _ => {
                let mut conditions: Vec<Expr> = list
                    .iter()
                    .filter_map(|v| match v {
                        GqlValue::Object(o) => get_filter(o, parameters),
                        _ => None,
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
        },
        _ => {
            let left = Expr::Identifier(Ident {
                value: key.as_ref().to_owned(),
                quote_style: Some(QUOTE_CHAR),
            });
            get_expr(left, value, parameters)
        }
    }
}

fn get_filter<'a, T: Text<'a>>(
    args: &BTreeMap<T::Value, GqlValue<'a, T>>,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> Option<Expr> {
    match args.len() {
        0 => None,
        1 => {
            let (key, value) = args.iter().next().expect("list to have one item");
            handle_filter_arg(key, value, parameters)
        }
        _ => {
            let mut conditions: Vec<Expr> = args
                .into_iter()
                .rev()
                .map_while(|(key, value)| handle_filter_arg(key, value, parameters))
                .collect();
            if conditions.is_empty() {
                return None;
            }
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
                value: ROOT_LABEL.into(),
                quote_style: Some(QUOTE_CHAR),
            },
            expr: Expr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: JSON_BUILD_OBJECT.to_string(),
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
    merges: Vec<Merge>,
    is_single: bool,
    alias: &T::Value,
) -> SetExpr {
    let mut base = Expr::Function(Function {
        name: ObjectName(vec![Ident {
            value: TO_JSON.to_string(),
            quote_style: None,
        }]),
        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(
            Box::new(Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(Select {
                    distinct: false,
                    top: None,
                    projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: ROOT_LABEL.to_string(),
                        quote_style: Some(QUOTE_CHAR),
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
                                locks: vec![],
                            }),
                            alias: Some(TableAlias {
                                name: Ident {
                                    value: ROOT_LABEL.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
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
                locks: vec![],
            }),
        )))],
        over: None,
        distinct: false,
        special: false,
    });
    if !merges.is_empty() {
        base = Expr::BinaryOp {
            left: Box::new(Expr::Cast {
                expr: Box::new(base),
                data_type: DataType::Custom(
                    ObjectName(vec![Ident {
                        value: "jsonb".to_string(),
                        quote_style: None,
                    }]),
                    vec![],
                ),
            }),
            op: BinaryOperator::StringConcat,
            right: Box::new(Expr::Case {
                operand: None,
                conditions: merges.iter().map(|m| m.condition.clone()).collect(),
                results: merges.iter().map(|m| m.expr.clone()).collect(),
                else_result: Some(Box::new(Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: "jsonb_build_object".to_string(),
                        quote_style: None,
                    }]),
                    args: vec![],
                    over: None,
                    distinct: false,
                    special: false,
                }))),
            }),
        };
    }
    if !is_single {
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
                        value: JSON_AGG.to_string(),
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
                quote_style: Some(QUOTE_CHAR),
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
                        value: name.to_uppercase(),
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
                .flat_map(|arg| {
                    if let Selection::Field(field) = arg {
                        vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString(field.name.as_ref().into()),
                            ))),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                                name: ObjectName(vec![Ident {
                                    value: name.to_uppercase(),
                                    quote_style: None,
                                }]),
                                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(Ident {
                                        value: field.name.as_ref().into(),
                                        quote_style: Some(QUOTE_CHAR),
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
                .collect();
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(field.name.as_ref().into()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: JSON_BUILD_OBJECT.to_string(),
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

fn get_join<'a, T: Text<'a>>(
    arguments: &Vec<(T::Value, GqlValue<'a, T>)>,
    directives: &Vec<Directive<'a, T>>,
    selection_items: &Vec<Selection<'a, T>>,
    path: Option<&str>,
    name: &T::Value,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> Join {
    let (selection, distinct, order_by, mut first, after) = parse_args(arguments, parameters);
    let (relation, fks, pks, is_single) = get_relation(directives);
    if is_single {
        first = Some(Expr::Value(Value::Number("1".to_string(), false)));
    }
    let sub_path = path.map_or_else(|| relation.clone(), |v| v.to_string() + "." + &relation);
    let (sub_projection, sub_joins, merges) =
        get_projection(selection_items, &relation, Some(&sub_path), parameters);
    let join_filter = zip(pks, fks)
        .map(|(pk, fk)| Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident {
                    value: relation.clone(),
                    quote_style: Some(QUOTE_CHAR),
                },
                Ident {
                    value: fk,
                    quote_style: Some(QUOTE_CHAR),
                },
            ])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                Ident {
                    value: path.map_or("base".to_string(), |v| v.to_string()),
                    quote_style: Some(QUOTE_CHAR),
                },
                Ident {
                    value: pk,
                    quote_style: Some(QUOTE_CHAR),
                },
            ])),
        })
        .reduce(|acc, expr| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::And,
            right: Box::new(expr),
        })
        .unwrap_or(Expr::Value(Value::Boolean(true)));
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
        distinct,
    );
    Join {
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
                                    value: sub_path,
                                    quote_style: Some(QUOTE_CHAR),
                                },
                                columns: vec![],
                            }),
                        },
                        joins: sub_joins,
                    }],
                    None,
                    merges,
                    is_single,
                    name,
                )),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
                locks: vec![],
            }),
            alias: Some(TableAlias {
                name: Ident {
                    value: "root.".to_owned() + &relation,
                    quote_style: Some(QUOTE_CHAR),
                },
                columns: vec![],
            }),
        },
        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(Expr::Nested(Box::new(
            Expr::Value(Value::SingleQuotedString("true".to_string())),
        )))),
    }
}

struct Merge {
    condition: Expr,
    expr: Expr,
}

fn get_static<'a, T: Text<'a>>(
    name: &T::Value,
    directives: &Vec<Directive<'a, T>>,
) -> Option<SelectItem> {
    for directive in directives {
        let directive_name: &str = directive.name.as_ref();
        if directive_name == "static" {
            let value = directive
                .arguments
                .iter()
                .find(|(name, _)| name.as_ref() == "value")
                .map(|(_, value)| match value {
                    GqlValue::String(value) => value.to_string(),
                    GqlValue::Int(value) => {
                        value.as_i64().expect("value is not an int").to_string()
                    }
                    GqlValue::Float(value) => value.to_string(),
                    GqlValue::Boolean(value) => value.to_string(),
                    _ => unreachable!(),
                })
                .unwrap_or_else(|| "".to_string());
            return Some(SelectItem::ExprWithAlias {
                expr: Expr::Value(Value::SingleQuotedString(value)),
                alias: Ident {
                    value: name.as_ref().to_string(),
                    quote_style: Some(QUOTE_CHAR),
                },
            });
        }
    }
    None
}

fn get_projection<'a, T: Text<'a>>(
    items: &Vec<Selection<'a, T>>,
    relation: &str,
    path: Option<&str>,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> (Vec<SelectItem>, Vec<Join>, Vec<Merge>) {
    let mut projection = Vec::new();
    let mut joins = Vec::new();
    let mut merges = Vec::new();
    for selection in items {
        match selection {
            Selection::Field(field) => {
                if !field.selection_set.items.is_empty() {
                    let join = get_join(
                        &field.arguments,
                        &field.directives,
                        &field.selection_set.items,
                        path,
                        &field.name,
                        parameters,
                    );
                    joins.push(join);
                    projection.push(SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: field.name.as_ref().into(),
                        quote_style: Some(QUOTE_CHAR),
                    })));
                } else {
                    if let Some(value) = get_static(&field.name, &field.directives) {
                        projection.push(value);
                        continue;
                    }
                    match &field.alias {
                        Some(alias) => {
                            projection.push(SelectItem::ExprWithAlias {
                                expr: path.map_or_else(
                                    || {
                                        Expr::Identifier(Ident {
                                            value: field.name.as_ref().into(),
                                            quote_style: Some(QUOTE_CHAR),
                                        })
                                    },
                                    |path| {
                                        Expr::CompoundIdentifier(vec![
                                            Ident {
                                                value: path.to_string(),
                                                quote_style: Some(QUOTE_CHAR),
                                            },
                                            Ident {
                                                value: field.name.as_ref().into(),
                                                quote_style: Some(QUOTE_CHAR),
                                            },
                                        ])
                                    },
                                ),
                                alias: Ident {
                                    value: alias.as_ref().into(),
                                    quote_style: Some(QUOTE_CHAR),
                                },
                            });
                        }
                        None => {
                            let name = field.name.as_ref().into();
                            if name == "__typename" {
                                projection.push(SelectItem::ExprWithAlias {
                                    alias: Ident {
                                        value: name,
                                        quote_style: Some(QUOTE_CHAR),
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
                                            quote_style: Some(QUOTE_CHAR),
                                        })
                                    },
                                    |path| {
                                        Expr::CompoundIdentifier(vec![
                                            Ident {
                                                value: path.to_string(),
                                                quote_style: Some(QUOTE_CHAR),
                                            },
                                            Ident {
                                                value: name.clone(),
                                                quote_style: Some(QUOTE_CHAR),
                                            },
                                        ])
                                    },
                                )));
                            }
                        }
                    }
                }
            }
            Selection::InlineFragment(frag) => {
                if let Some(type_condition) = &frag.type_condition {
                    let TypeCondition::On(name) = type_condition;
                    let (relation, _fks, _pks, _is_single) = get_relation(&frag.directives);
                    let join = get_join(
                        &vec![],
                        &frag.directives,
                        &frag.selection_set.items,
                        path,
                        name,
                        parameters,
                    );
                    joins.push(join);
                    merges.push(Merge {
                        expr: Expr::Function(Function {
                            name: ObjectName(vec![Ident {
                                value: TO_JSONB.to_string(),
                                quote_style: None,
                            }]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident {
                                    value: name.as_ref().into(),
                                    quote_style: Some(QUOTE_CHAR),
                                }),
                            ))],
                            over: None,
                            distinct: false,
                            special: false,
                        }),
                        condition: Expr::IsNotNull(Box::new(Expr::CompoundIdentifier(vec![
                            Ident {
                                value: "root.".to_string() + &relation,
                                quote_style: Some(QUOTE_CHAR),
                            },
                            Ident {
                                value: relation.to_string(),
                                quote_style: Some(QUOTE_CHAR),
                            },
                        ]))),
                    })
                }
            }
            Selection::FragmentSpread(_) => unimplemented!(),
        }
    }
    (projection, joins, merges)
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

fn get_relation<'a, T: Text<'a>>(
    directives: &Vec<Directive<'a, T>>,
) -> (String, Vec<String>, Vec<String>, bool) {
    let mut relation: String = "".to_string();
    let mut fk = vec![];
    let mut pk = vec![];
    let mut is_single = false;
    for directive in directives {
        let name: &str = directive.name.as_ref();
        if name == "relation" {
            for argument in &directive.arguments {
                match argument.0.as_ref() {
                    "table" => relation = value_to_string(&argument.1),
                    "field" => {
                        fk = match &argument.1 {
                            GqlValue::String(s) => vec![s.clone()],
                            GqlValue::List(e) => e
                                .iter()
                                .map(|v| value_to_string(v))
                                .collect::<Vec<String>>(),
                            _ => unimplemented!(),
                        }
                    }
                    "references" => {
                        pk = match &argument.1 {
                            GqlValue::String(s) => vec![s.clone()],
                            GqlValue::List(e) => e
                                .iter()
                                .map(|v| value_to_string(v))
                                .collect::<Vec<String>>(),
                            _ => unimplemented!(),
                        }
                    }
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
    mut order_by: Vec<OrderByExpr>,
    first: Option<Expr>,
    after: Option<Offset>,
    table_name: String,
    distinct: Option<Vec<String>>,
) -> Query {
    let mut projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];
    let is_distinct = distinct.is_some();
    if let Some(distinct) = distinct {
        let columns = distinct
            .into_iter()
            .map(|s| Value::DoubleQuotedString(s).to_string())
            .collect::<Vec<String>>();
        projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
            value: ON.to_owned() + " (" + &columns.join(",") + ") *",
            quote_style: None,
        }))];
        columns.into_iter().rev().for_each(|c| {
            order_by.insert(
                0,
                OrderByExpr {
                    expr: Expr::Identifier(Ident {
                        value: c,
                        quote_style: None,
                    }),
                    asc: Some(true),
                    nulls_first: None,
                },
            );
        });
    }
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: is_distinct,
            top: None,
            projection,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: table_name,
                        quote_style: Some(QUOTE_CHAR),
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
        locks: vec![],
    }
}

fn get_order<'a, T: Text<'a>>(
    order: &BTreeMap<T::Value, GqlValue<'a, T>>,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> Vec<OrderByExpr> {
    if order.contains_key("expr") && order.contains_key("dir") {
        let mut asc = None;
        if let Some(dir) = order.get("dir") {
            match dir {
                GqlValue::String(s) => {
                    asc = Some(s == "ASC");
                }
                GqlValue::Enum(e) => {
                    let s: &str = e.as_ref();
                    asc = Some(s == "ASC");
                }
                _ => unimplemented!(),
            }
        }
        if let Some(expr) = order.get("expr") {
            match expr {
                GqlValue::String(s) => {
                    return vec![OrderByExpr {
                        expr: Expr::Identifier(Ident {
                            value: s.clone(),
                            quote_style: Some(QUOTE_CHAR),
                        }),
                        asc,
                        nulls_first: None,
                    }];
                }
                GqlValue::Object(args) => {
                    if let Some(expression) = get_filter(args, parameters) {
                        return vec![OrderByExpr {
                            expr: expression,
                            asc,
                            nulls_first: None,
                        }];
                    }
                }
                _ => unimplemented!(),
            }
        }
    }
    let mut order_by = vec![];
    for (key, value) in order.iter() {
        match value {
            GqlValue::String(s) => {
                order_by.push(OrderByExpr {
                    expr: Expr::Identifier(Ident {
                        value: key.as_ref().into(),
                        quote_style: Some(QUOTE_CHAR),
                    }),
                    asc: Some(s == "ASC"),
                    nulls_first: None,
                });
            }
            GqlValue::Enum(e) => {
                let s: &str = e.as_ref();
                order_by.push(OrderByExpr {
                    expr: Expr::Identifier(Ident {
                        value: key.as_ref().into(),
                        quote_style: Some(QUOTE_CHAR),
                    }),
                    asc: Some(s == "ASC"),
                    nulls_first: None,
                });
            }
            _ => unimplemented!(),
        }
    }
    order_by
}

fn get_distinct<'a, T: Text<'a>>(distinct: &Vec<GqlValue<'a, T>>) -> Option<Vec<String>> {
    let values: Vec<String> = distinct
        .iter()
        .flat_map(|v| match v {
            GqlValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn parse_args<'a, T: Text<'a>>(
    arguments: &Vec<(T::Value, GqlValue<'a, T>)>,
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> (
    Option<Expr>,
    Option<Vec<String>>,
    Vec<OrderByExpr>,
    Option<Expr>,
    Option<Offset>,
) {
    let mut selection = None;
    let mut order_by = vec![];
    let mut distinct = None;
    let mut first = None;
    let mut after = None;
    for argument in arguments {
        let (key, value) = argument;
        match (key.as_ref(), value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(filter, parameters);
            }
            ("distinct", GqlValue::List(d)) => {
                distinct = get_distinct(d);
            }
            ("order", GqlValue::Object(order)) => {
                order_by = get_order(order, parameters);
            }
            ("order", GqlValue::List(list)) => {
                list.into_iter()
                    .filter_map(|v| match v {
                        GqlValue::Object(o) => Some(o),
                        _ => None,
                    })
                    .for_each(|o| order_by.append(&mut get_order(&o, parameters)));
            }
            ("first", GqlValue::Int(count)) => {
                first = Some(Expr::Value(Value::Number(
                    count.as_i64().expect("int to be an i64").to_string(),
                    false,
                )));
            }
            ("after", GqlValue::Int(count)) => {
                after = Some(Offset {
                    value: Expr::Value(Value::Number(
                        count.as_i64().expect("int to be an i64").to_string(),
                        false,
                    )),
                    rows: OffsetRows::None,
                });
            }
            _ => {}
        }
    }
    (selection, distinct, order_by, first, after)
}

fn get_mutation_columns<'a, T: Text<'a>>(
    arguments: &Vec<(T::Value, GqlValue<'a, T>)>,
    parameters: &BTreeMap<String, (usize, DataType)>,
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
                        quote_style: Some(QUOTE_CHAR),
                    });
                    row.push(Expr::Value(get_value(value, parameters)));
                }
                rows.push(row);
            }
            ("data", GqlValue::List(list)) => {
                if list.is_empty() {
                    continue;
                }
                for (i, item) in list.iter().enumerate() {
                    let mut row = vec![];
                    if let GqlValue::Object(data) = item {
                        for (key, value) in data.iter() {
                            if i == 0 {
                                columns.push(Ident {
                                    value: key.as_ref().into(),
                                    quote_style: Some(QUOTE_CHAR),
                                });
                            }
                            row.push(Expr::Value(get_value(value, parameters)));
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
    parameters: &BTreeMap<String, (usize, DataType)>,
) -> (Option<Expr>, Vec<Assignment>) {
    let mut selection = None;
    let mut assignments = vec![];
    for argument in arguments {
        let (key, value) = argument;
        match (key.as_ref(), value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(filter, parameters);
            }
            ("set", GqlValue::Object(data)) => {
                for (key, value) in data.iter() {
                    assignments.push(Assignment {
                        id: vec![Ident {
                            value: key.as_ref().into(),
                            quote_style: Some(QUOTE_CHAR),
                        }],
                        value: Expr::Value(get_value(value, parameters)),
                    });
                }
            }
            ("inc" | "increment", GqlValue::Object(data)) => {
                for (key, value) in data.iter() {
                    let column_ident = Ident {
                        value: key.as_ref().into(),
                        quote_style: Some(QUOTE_CHAR),
                    };
                    assignments.push(Assignment {
                        id: vec![column_ident.clone()],
                        value: Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(column_ident)),
                            op: BinaryOperator::Plus,
                            right: Box::new(Expr::Value(get_value(value, parameters))),
                        },
                    });
                }
            }
            _ => todo!(),
        }
    }
    (selection, assignments)
}

fn get_data_type<'a, T: Text<'a>>(var_type: &Type<'a, T>) -> DataType {
    match var_type {
        Type::NamedType(name) => match name.as_ref() {
            "String" => DataType::Text,
            "Int" => DataType::Int(None),
            "Float" => DataType::Float(None),
            "Boolean" => DataType::Boolean,
            "ID" => DataType::Text,
            _ => todo!(),
        },
        Type::NonNullType(inner) => get_data_type(inner),
        Type::ListType(_) => todo!(),
    }
}

fn get_sorted_json_params(parameters: &BTreeMap<String, (usize, DataType)>) -> Vec<String> {
    let mut list = parameters
        .iter()
        .map(|(k, v)| (k.to_owned(), v.0))
        .collect::<Vec<(String, usize)>>();
    list.sort_by(|a, b| a.1.cmp(&b.1));
    list.into_iter().map(|(k, _)| k).collect()
}

pub fn gql2sql<'a, T: Text<'a>>(
    ast: Document<'a, T>,
) -> Result<(Statement, Option<Vec<String>>), anyhow::Error> {
    let mut statements = Vec::new();
    let mut parameters: BTreeMap<String, (usize, DataType)> = BTreeMap::new();
    for definition in ast.definitions {
        match definition {
            Definition::Operation(operation) => match operation {
                OperationDefinition::Query(query) => {
                    for (i, param) in query.variable_definitions.into_iter().enumerate() {
                        parameters.insert(
                            param.name.as_ref().into(),
                            (i + 1, get_data_type(&param.var_type)),
                        );
                    }
                    for selection in query.selection_set.items {
                        match selection {
                            Selection::Field(field) => {
                                let mut name = field.name.as_ref();
                                let key = field.alias.map_or_else(
                                    || field.name.as_ref().into(),
                                    |alias| alias.as_ref().into(),
                                );
                                let (selection, distinct, order_by, mut first, after) =
                                    parse_args(&field.arguments, &parameters);
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
                                        distinct,
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
                                                                quote_style: Some(QUOTE_CHAR),
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
                                            locks: vec![],
                                        },
                                    ));
                                } else {
                                    let mut is_single = false;
                                    if name.ends_with("_one") {
                                        name = &name[..name.len() - 4];
                                        first = Some(Expr::Value(Value::Number(
                                            "1".to_string(),
                                            false,
                                        )));
                                        is_single = true;
                                    }
                                    let (projection, joins, merges) = get_projection(
                                        &field.selection_set.items,
                                        name,
                                        Some("base"),
                                        &parameters,
                                    );
                                    let base_query = get_filter_query(
                                        selection,
                                        order_by,
                                        first,
                                        after,
                                        name.to_owned(),
                                        distinct,
                                    );
                                    let root_query = get_root_query::<&str>(
                                        projection,
                                        vec![TableWithJoins {
                                            relation: TableFactor::Derived {
                                                lateral: false,
                                                subquery: Box::new(base_query),
                                                alias: Some(TableAlias {
                                                    name: Ident {
                                                        value: "base".to_string(),
                                                        quote_style: Some(QUOTE_CHAR),
                                                    },
                                                    columns: vec![],
                                                }),
                                            },
                                            joins,
                                        }],
                                        None,
                                        merges,
                                        is_single,
                                        &ROOT_LABEL,
                                    );
                                    statements.push((
                                        key,
                                        Query {
                                            with: None,
                                            body: Box::new(root_query),
                                            order_by: vec![],
                                            limit: None,
                                            offset: None,
                                            fetch: None,
                                            locks: vec![],
                                        },
                                    ));
                                };
                            }
                            Selection::FragmentSpread(_) => unimplemented!(),
                            Selection::InlineFragment(_) => unimplemented!(),
                        }
                    }
                    let statement = Statement::Query(Box::new(Query {
                        with: None,
                        body: Box::new(SetExpr::Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            into: None,
                            projection: vec![SelectItem::ExprWithAlias {
                                alias: Ident {
                                    value: DATA_LABEL.into(),
                                    quote_style: Some(QUOTE_CHAR),
                                },
                                expr: Expr::Function(Function {
                                    name: ObjectName(vec![Ident {
                                        value: JSON_BUILD_OBJECT.to_string(),
                                        quote_style: None,
                                    }]),
                                    args: statements
                                        .into_iter()
                                        .flat_map(|(key, query)| {
                                            vec![
                                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                    Expr::Value(Value::SingleQuotedString(key)),
                                                )),
                                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                    Expr::Subquery(Box::new(query)),
                                                )),
                                            ]
                                        })
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
                        locks: vec![],
                    }));
                    if !parameters.is_empty() {
                        return Ok((statement, Some(get_sorted_json_params(&parameters))));
                    }
                    return Ok((statement, None));
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
                                    let (columns, rows) =
                                        get_mutation_columns(&field.arguments, &parameters);
                                    let (projection, _, _) = get_projection(
                                        &field.selection_set.items,
                                        name,
                                        None,
                                        &parameters,
                                    );
                                    return Ok((
                                        Statement::Insert {
                                            or: None,
                                            into: true,
                                            table_name: ObjectName(vec![Ident {
                                                value: name.to_string(),
                                                quote_style: Some(QUOTE_CHAR),
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
                                                locks: vec![],
                                            }),
                                            partitioned: None,
                                            after_columns: vec![],
                                            table: false,
                                            on: None,
                                            returning: Some(projection),
                                        },
                                        None,
                                    ));
                                } else if name.starts_with("update_") {
                                    name = &name[7..];
                                    let (projection, _, _) = get_projection(
                                        &field.selection_set.items,
                                        name,
                                        None,
                                        &parameters,
                                    );
                                    let (selection, assignments) =
                                        get_mutation_assignments(&field.arguments, &parameters);
                                    return Ok((
                                        Statement::Update {
                                            table: TableWithJoins {
                                                relation: TableFactor::Table {
                                                    name: ObjectName(vec![Ident {
                                                        value: name.to_string(),
                                                        quote_style: Some(QUOTE_CHAR),
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
                                        },
                                        None,
                                    ));
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
        let sql = r#"SELECT json_build_object('app', (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base"."id", "components") AS "root"))), '[]') AS "root" FROM (SELECT * FROM "App" WHERE "id" = '345810043118026832' ORDER BY "name" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component"."id", "pageMeta", "elements") AS "root"))), '[]') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id") AS "base.Component" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."id", "base.Component.PageMeta"."path") AS "root")) AS "pageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.Element"."id", "base.Component.Element"."name") AS "root"))), '[]') AS "elements" FROM (SELECT * FROM "Element" WHERE "Element"."componentParentId" = "base.Component"."id" ORDER BY "order" ASC) AS "base.Component.Element") AS "root.Element" ON ('true')) AS "root.Component" ON ('true')), 'Component_aggregate', (SELECT json_build_object('count', COUNT(*), 'min', json_build_object('createdAt', MIN("createdAt"))) AS "root" FROM (SELECT * FROM "Component" WHERE "appId" = '345810043118026832') AS "base")) AS "data""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql).unwrap();
        let (statement, _params) = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement);
        assert_eq!(vec![statement], sqlast);
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
        let (statement, _params) = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement);
        assert_eq!(vec![statement], sqlast);
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
        let (statement, _params) = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement);
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn query_mega() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query GetApp($orgId: String!, $appId: String!, $branch: String!) {
      app: App_one(
        filter: {
          orgId: { eq: $orgId }
          id: { eq: $appId }
          branch: { eq: $branch }
        }
      ) {
        orgId
        id
        branch
        name
        description
        theme
        favicon
        customCSS
        analytics
        customDomain
        components
          @relation(
            table: "Component"
            field: ["appId", "branch"]
            references: ["id", "branch"]
          ) {
          id
          branch
          ... on PageMeta
            @relation(
              table: "PageMeta"
              field: ["componentId", "branch"]
              references: ["id", "branch"]
              single: true
            ) {
            title
            description
            path
            socialImage
            urlParams
            loader
            protection
            maxAge
            sMaxAge
            staleWhileRevalidate
          }
          ... on ComponentMeta
            @relation(
              table: "ComponentMeta"
              field: ["componentId", "branch"]
              references: ["id", "branch"]
              single: true
            ) {
            title
            sources
              @relation(
                table: "Source"
                field: ["componentId", "branch"]
                references: ["id", "branch"]
              ) {
              id
              branch
              name
              provider
              description
              template
              instanceTemplate
              outputType
              source
              sourceProp
              componentId
              utilityId
              component(order: { order: ASC })
                @relation(
                  table: "Element"
                  field: ["id", "branch"]
                  references: ["componentId", "branch"]
                  single: true
                ) {
                id
                branch
                name
                kind
                source
                styles
                props
                order
                conditions
              }
              utility
                @relation(
                  table: "Utility"
                  field: ["id", "branch"]
                  references: ["componentId", "branch"]
                  single: true
                ) {
                id
                branch
                name
                kind
                kindId
                data
              }
            }
            events @relation(table: "Event", field: ["componentMetaId", "branch"], references: ["id", "branch"]) {
                id
                branch
                name
                label
                help
                type
            }
          }
        }
        connections @relation(table: "Connection", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          kind
          prodUrl
          mutationSchema @relation(table: "Schema", field: ["mutationConnectionId", "branch"], references: ["id", "branch"], single: true) {
            id
            branch
            schema
          }
          endpoints @relation(table: "Endpoint", field: ["connectionId", "branch"], references: ["id", "branch"]) {
            id
            branch
            name
            method
            path
            responseSchemaId
            headers @relation(table: "Header", field: ["parentEndpointId", "branch"], references: ["id", "branch"]) {
              id
              branch
              key
              value
              dynamic
            }
            search @relation(table: "Search", field: ["endpointId", "branch"], references: ["id", "branch"]) {
              id
              branch
              key
              value
              dynamic
            }
          }
          headers @relation(table: "Header", field: ["parentConnectionId", "branch"], references: ["id", "branch"]) {
            id
            branch
            key
            value
            dynamic
          }
        }
        layouts @relation(table: "Layout", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          source
          kind
          styles
          props
        }
        plugins @relation(table: "Plugin", field: ["appId", "branch"], references: ["id", "branch"]) {
          instanceId
          kind
        }
        schemas @relation(table: "Schema", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          schema
        }
        styles @relation(table: "Style", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          kind
          styles
          isDefault
        }
        workflows @relation(table: "Workflow", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          args
          steps(order: { order: ASC }) @relation(table: "Step", field: ["workflowId", "branch"], references: ["id", "branch"]) {
            id
            branch
            parentId
            kind
            kindId
            data
            order
          }
        }
      }
    }
"#,
        )?
        .clone();
        let sql = r#"UPDATE "Hero" SET "name" = 'Captain America', "number_of_movies" = "number_of_movies" + 1 WHERE "secret_identity" = 'Sam Wilson' RETURNING "id", "name", "secret_identity", "number_of_movies""#;
        let dialect = PostgreSqlDialect {};
        let _sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, _params) = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement);
        // assert_eq!(statements, sqlast);
        Ok(())
    }

    #[test]
    fn query_frag() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query GetApp($componentId: String!) {
                component: Component_one(filter: { id : { eq: $componentId } }) {
                   id
                   branch
                   ... on ComponentMeta @relation(
                        table: "ComponentMeta"
                        field: ["componentId", "branch"]
                        references: ["id", "branch"]
                        single: true
                    ) {
                     title
                   }
                }
            }"#,
        )?
        .clone();
        // println!("ast {:#?}", gqlast);
        let sql = r#"SELECT json_build_object('component', (SELECT to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch") AS "root"))::jsonb || CASE WHEN "root.ComponentMeta"."ComponentMeta" IS NOT NULL THEN to_jsonb("ComponentMeta") ELSE jsonb_build_object() END AS "root" FROM (SELECT * FROM "Component" WHERE "id" = $1 LIMIT 1) AS "base" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.ComponentMeta"."title") AS "root")) AS "ComponentMeta" FROM (SELECT * FROM "ComponentMeta" WHERE "ComponentMeta"."componentId" = "base"."id" AND "ComponentMeta"."branch" = "base"."branch" LIMIT 1) AS "base.ComponentMeta") AS "root.ComponentMeta" ON ('true'))) AS "data""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, _params) = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement);
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn query_static() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query GetApp($componentId: String!) {
                component: Component_one(filter: { id : { eq: $componentId } }) {
                   id
                   branch
                   kind @static(value: "page")
                }
            }"#,
        )?
        .clone();
        let sql = r#"SELECT json_build_object('component', (SELECT to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch", 'page' AS "kind") AS "root")) AS "root" FROM (SELECT * FROM "Component" WHERE "id" = $1 LIMIT 1) AS "base")) AS "data""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, _params) = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement);
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn query_distinct() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query GetApp($componentId: String!, $branch: String!) {
                component: Component_one(filter: { id : { eq: $componentId }, or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }, order: [{ expr: { branch: { eq: $branch } }, dir: DESC }], distinct: ["id"]) {
                   id
                   branch
                   kind @static(value: "page")
                }
            }"#,
        )?
        .clone();
        let sql = r#"SELECT json_build_object('component', (SELECT to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch", 'page' AS "kind") AS "root")) AS "root" FROM (SELECT DISTINCT ON ("id") * FROM "Component" WHERE "id" = $1 AND ("branch" = $2 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $2 DESC LIMIT 1) AS "base")) AS "data""#;
        let (statement, _params) = gql2sql(gqlast)?;
        println!("Statements:\n'{}'", statement);
        assert_eq!(statement.to_string(), sql);
        Ok(())
    }

    #[test]
    fn query_ast() -> Result<(), anyhow::Error> {
        let sql = r#"
            SELECT
            DISTINCT ON (column1)
            column2
            FROM
            table_name
            WHERE
            column1 = 'value'
            AND (column2 = 'value' OR column3 = 'value')
            ORDER BY
            column1,
            column2;
        "#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        println!("ast:\n{:#?}", sqlast);
        println!("back:\n{:#?}", sqlast[0].to_string());
        Ok(())
    }
}
