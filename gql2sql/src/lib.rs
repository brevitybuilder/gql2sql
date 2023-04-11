mod consts;

use crate::consts::*;
use anyhow::anyhow;
use async_graphql_parser::{
    types::{
        BaseType, Directive, DocumentOperations, ExecutableDocument, Field, OperationDefinition,
        OperationType, Selection, Type, TypeCondition,
    },
    Positioned,
};
use async_graphql_value::{Name, Value as GqlValue};
use indexmap::IndexMap;
use sqlparser::ast::{
    Assignment, BinaryOperator, Cte, DataType, Expr, Function, FunctionArg, FunctionArgExpr, Ident,
    Join, JoinConstraint, JoinOperator, ObjectName, Offset, OffsetRows, OrderByExpr, Query, Select,
    SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value, Values,
    WildcardAdditionalOptions, With,
};
use std::iter::zip;

type AnyResult<T> = anyhow::Result<T>;

fn get_value<'a>(value: &'a GqlValue, parameters: &IndexMap<&'a str, DataType>) -> AnyResult<Expr> {
    match value {
        GqlValue::Variable(v) => {
            let index = parameters
                .get_index_of(v.as_ref())
                .ok_or(anyhow!("variable not found"))?;
            Ok(Expr::Value(Value::Placeholder(format!("${}", index))))
        }
        GqlValue::Null => Ok(Expr::Value(Value::Null)),
        GqlValue::String(s) => Ok(Expr::Value(Value::SingleQuotedString(s.clone()))),
        GqlValue::Number(f) => Ok(Expr::Value(Value::Number(f.to_string(), false))),
        GqlValue::Boolean(b) => Ok(Expr::Value(Value::Boolean(b.to_owned()))),
        GqlValue::Enum(e) => Ok(Expr::Value(Value::SingleQuotedString(e.as_ref().into()))),
        GqlValue::List(_l) => Err(anyhow!("list not supported")),
        GqlValue::Binary(_b) => Err(anyhow!("binary not supported")),
        GqlValue::Object(o) => {
            if o.contains_key("_parentRef") {
                if let Some(GqlValue::String(s)) = o.get("_parentRef") {
                    return Ok(Expr::CompoundIdentifier(vec![
                        Ident::with_quote(QUOTE_CHAR, BASE.to_owned()),
                        Ident::with_quote(QUOTE_CHAR, s),
                    ]));
                }
            }
            Err(anyhow!("object not supported"))
        }
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

fn get_expr<'a>(
    left: Expr,
    args: &'a GqlValue,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<Option<Expr>> {
    if let GqlValue::Object(o) = args {
        return match o.len() {
            0 => Ok(None),
            1 => {
                let (op, value) = o.iter().next().ok_or(anyhow!("list to have one item"))?;
                let right_value = get_value(value, parameters)?;
                match op.as_str() {
                    "like" => Ok(Some(Expr::Like {
                        negated: false,
                        expr: Box::new(left),
                        pattern: Box::new(right_value),
                        escape_char: None,
                    })),
                    "ilike" => Ok(Some(Expr::ILike {
                        negated: false,
                        expr: Box::new(left),
                        pattern: Box::new(right_value),
                        escape_char: None,
                    })),
                    _ => {
                        let op = get_op(op.as_ref());
                        Ok(Some(Expr::BinaryOp {
                            left: Box::new(left),
                            op,
                            right: Box::new(right_value),
                        }))
                    }
                }
            }
            _ => {
                let mut conditions: Vec<Expr> = o
                    .iter()
                    .rev()
                    .map(|(op, v)| {
                        let op = get_op(op.as_ref());
                        let value = get_value(v, parameters)?;
                        Ok(Expr::BinaryOp {
                            left: Box::new(left.clone()),
                            op,
                            right: Box::new(value),
                        })
                    })
                    .filter_map(|v: AnyResult<Expr>| v.ok())
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
                Ok(Some(last_expr))
            }
        };
    };
    Ok(None)
}

fn handle_filter_arg<'a>(
    key: &'a str,
    value: &GqlValue,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<Option<Expr>> {
    let value = match (key, value) {
        ("OR" | "or", GqlValue::List(list)) => match list.len() {
            0 => None,
            1 => match list.get(0).ok_or(anyhow!("list to have one item"))? {
                GqlValue::Object(o) => get_filter(o, parameters)?,
                _ => None,
            },
            _ => {
                let mut conditions: Vec<Expr> = list
                    .iter()
                    .map(|v| match v {
                        GqlValue::Object(o) => get_filter(o, parameters),
                        _ => Ok(None),
                    })
                    .filter_map(|v| v.ok().flatten())
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
        ("AND" | "and", GqlValue::List(list)) => match list.len() {
            0 => None,
            1 => match list.get(0).expect("list to have one item") {
                GqlValue::Object(o) => get_filter(o, parameters)?,
                _ => None,
            },
            _ => {
                let mut conditions: Vec<Expr> = list
                    .iter()
                    .map(|v| match v {
                        GqlValue::Object(o) => get_filter(o, parameters),
                        _ => Ok(None),
                    })
                    .filter_map(|v| v.ok().flatten())
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
                value: key.to_owned(),
                quote_style: Some(QUOTE_CHAR),
            });
            get_expr(left, value, parameters)?
        }
    };
    Ok(value)
}

fn get_filter<'a>(
    args: &IndexMap<Name, GqlValue>,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<Option<Expr>> {
    let value = match args.len() {
        0 => None,
        1 => {
            let (key, value) = args.iter().next().expect("list to have one item");
            handle_filter_arg(key.as_str(), value, parameters)?
        }
        _ => {
            let mut conditions: Vec<Expr> = args
                .into_iter()
                .rev()
                .map(|(key, value)| handle_filter_arg(key, value, parameters))
                .filter_map(|v| v.ok().flatten())
                .collect();
            if conditions.is_empty() {
                return Ok(None);
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
    };
    Ok(value)
}

fn get_agg_query<'a>(
    aggs: Vec<FunctionArg>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    alias: &'a str,
) -> SetExpr {
    SetExpr::Select(Box::new(Select {
        distinct: false,
        top: None,
        into: None,
        projection: vec![SelectItem::ExprWithAlias {
            alias: Ident {
                value: alias.to_string(),
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

fn get_root_query<'a>(
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    merges: Vec<Merge>,
    is_single: bool,
    alias: &'a str,
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
                value: alias.to_string(),
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

fn get_agg_agg_projection<'a>(field: &'a Field) -> Vec<FunctionArg> {
    let name = field.name.node.as_ref();
    match name {
        "count" => {
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(field.name.node.to_string()),
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
                .node
                .items
                .iter()
                .flat_map(|arg| {
                    if let Selection::Field(field) = &arg.node {
                        let field = &field.node;
                        vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString(field.name.node.to_string()),
                            ))),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                                name: ObjectName(vec![Ident {
                                    value: name.to_uppercase(),
                                    quote_style: None,
                                }]),
                                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(Ident {
                                        value: field.name.node.to_string(),
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
                    Value::SingleQuotedString(field.name.node.to_string()),
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

fn get_aggregate_projection<'a>(items: &'a Vec<Positioned<Selection>>) -> Vec<FunctionArg> {
    let mut aggs = Vec::new();
    for selection in items {
        match &selection.node {
            Selection::Field(field) => {
                aggs.extend(get_agg_agg_projection(&field.node));
            }
            Selection::FragmentSpread(_) => unimplemented!(),
            Selection::InlineFragment(_) => unimplemented!(),
        }
    }
    aggs
}

fn get_join<'a>(
    arguments: &Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    directives: &Vec<Positioned<Directive>>,
    selection_items: &Vec<Positioned<Selection>>,
    path: Option<&'a str>,
    name: &'a str,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<Join> {
    let (selection, distinct, distinct_order, order_by, mut first, after) =
        parse_args(arguments, parameters)?;
    let (relation, fks, pks, is_single, is_aggregate) = get_relation(directives);
    if is_single {
        first = Some(Expr::Value(Value::Number("1".to_string(), false)));
    }
    let sub_path = path.map_or_else(|| relation.clone(), |v| v.to_string() + "." + &relation);
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
                    value: path.map_or(BASE.to_string(), |v| v.to_string()),
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
        });

    let sub_query = get_filter_query(
        selection.map_or_else(
            || join_filter.clone(),
            |s| {
                Some(join_filter.clone().map_or_else(
                    || s.clone(),
                    |jf| Expr::BinaryOp {
                        left: Box::new(jf),
                        op: BinaryOperator::And,
                        right: Box::new(s.clone()),
                    },
                ))
            },
        ),
        order_by,
        first,
        after,
        relation.clone(),
        distinct,
        distinct_order,
    );
    if is_aggregate {
        let aggs = get_aggregate_projection(selection_items);
        Ok(Join {
            relation: TableFactor::Derived {
                lateral: true,
                subquery: Box::new(Query {
                    with: None,
                    body: Box::new(get_agg_query(
                        aggs,
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
                            joins: vec![],
                        }],
                        None,
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
        })
    } else {
        let (sub_projection, sub_joins, merges) =
            get_projection(selection_items, &relation, Some(&sub_path), parameters)?;
        Ok(Join {
            relation: TableFactor::Derived {
                lateral: true,
                subquery: Box::new(Query {
                    with: None,
                    body: Box::new(get_root_query(
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
        })
    }
}

struct Merge {
    condition: Expr,
    expr: Expr,
}

fn get_static<'a>(name: &'a str, directives: &Vec<Positioned<Directive>>) -> Option<SelectItem> {
    for p_directive in directives {
        let directive = &p_directive.node;
        let directive_name: &str = directive.name.node.as_ref();
        if directive_name == "static" {
            let value = directive
                .arguments
                .iter()
                .find(|(name, _)| name.node.as_ref() == "value")
                .map(|(_, value)| match &value.node {
                    GqlValue::String(value) => value.to_string(),
                    GqlValue::Number(value) => {
                        value.as_i64().expect("value is not an int").to_string()
                    }
                    GqlValue::Boolean(value) => value.to_string(),
                    _ => unreachable!(),
                })
                .unwrap_or_else(|| "".to_string());
            return Some(SelectItem::ExprWithAlias {
                expr: Expr::Value(Value::SingleQuotedString(value)),
                alias: Ident {
                    value: name.to_string(),
                    quote_style: Some(QUOTE_CHAR),
                },
            });
        }
    }
    None
}

fn get_projection<'a>(
    items: &Vec<Positioned<Selection>>,
    relation: &'a str,
    path: Option<&'a str>,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<(Vec<SelectItem>, Vec<Join>, Vec<Merge>)> {
    let mut projection = Vec::new();
    let mut joins = Vec::new();
    let mut merges = Vec::new();
    for selection in items {
        let selection = &selection.node;
        match selection {
            Selection::Field(field) => {
                let field = &field.node;
                if !field.selection_set.node.items.is_empty() {
                    let join = get_join(
                        &field.arguments,
                        &field.directives,
                        &field.selection_set.node.items,
                        path,
                        &field.name.node,
                        parameters,
                    )?;
                    joins.push(join);
                    projection.push(SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: field.name.node.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    })));
                } else {
                    if let Some(value) = get_static(&field.name.node, &field.directives) {
                        projection.push(value);
                        continue;
                    }
                    match &field.alias {
                        Some(alias) => {
                            projection.push(SelectItem::ExprWithAlias {
                                expr: path.map_or_else(
                                    || {
                                        Expr::Identifier(Ident {
                                            value: field.name.node.to_string(),
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
                                                value: field.name.node.to_string(),
                                                quote_style: Some(QUOTE_CHAR),
                                            },
                                        ])
                                    },
                                ),
                                alias: Ident {
                                    value: alias.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
                                },
                            });
                        }
                        None => {
                            let name = field.name.node.to_string();
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
                let frag = &frag.node;
                if let Some(type_condition) = &frag.type_condition {
                    let name = &type_condition.node.on.node;
                    let args = frag
                        .directives
                        .iter()
                        .find(|d| d.node.name.node.as_ref() == "relation");
                    let (relation, _fks, _pks, _is_single, _is_aggregate) =
                        get_relation(&frag.directives);
                    let join = get_join(
                        args.map_or(&vec![], |dir| &dir.node.arguments),
                        &frag.directives,
                        &frag.selection_set.node.items,
                        path,
                        name,
                        parameters,
                    )?;
                    joins.push(join);
                    merges.push(Merge {
                        expr: Expr::Function(Function {
                            name: ObjectName(vec![Ident {
                                value: TO_JSONB.to_string(),
                                quote_style: None,
                            }]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(Ident {
                                    value: name.to_string(),
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
    Ok((projection, joins, merges))
}

fn value_to_string(value: &GqlValue) -> String {
    match value {
        GqlValue::String(s) => s.clone(),
        GqlValue::Number(f) => f.to_string(),
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

fn get_relation<'a>(
    directives: &Vec<Positioned<Directive>>,
) -> (String, Vec<String>, Vec<String>, bool, bool) {
    let mut relation: String = "".to_owned();
    let mut fk = vec![];
    let mut pk = vec![];
    let mut is_single = false;
    let mut is_aggregate = false;
    if let Some(p_directive) = directives
        .iter()
        .find(|d| d.node.name.node.as_str() == "relation")
    {
        let directive = &p_directive.node;
        let name = directive.name.node.as_str();
        if name == "relation" {
            for (name, value) in &directive.arguments {
                let name = name.node.as_str();
                let value = &value.node;
                match name {
                    "table" => relation = value_to_string(value),
                    "field" | "fields" => {
                        fk = match &value {
                            GqlValue::String(s) => vec![s.clone()],
                            GqlValue::List(e) => e
                                .iter()
                                .map(|v| value_to_string(v))
                                .collect::<Vec<String>>(),
                            _ => unimplemented!(),
                        }
                    }
                    "reference" | "references" => {
                        pk = match value {
                            GqlValue::String(s) => vec![s.clone()],
                            GqlValue::List(e) => e
                                .iter()
                                .map(|v| value_to_string(v))
                                .collect::<Vec<String>>(),
                            _ => unimplemented!(),
                        }
                    }
                    "single" => {
                        if let GqlValue::Boolean(b) = value {
                            is_single = *b;
                        }
                    }
                    "aggregate" => {
                        if let GqlValue::Boolean(b) = value {
                            is_aggregate = *b;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    (relation, fk, pk, is_single, is_aggregate)
}

fn get_filter_query<'a>(
    selection: Option<Expr>,
    order_by: Vec<OrderByExpr>,
    first: Option<Expr>,
    after: Option<Offset>,
    table_name: String,
    distinct: Option<Vec<&'a str>>,
    distinct_order: Option<Vec<OrderByExpr>>,
) -> Query {
    let mut projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];
    let is_distinct = distinct.is_some();
    let has_distinct_order = distinct_order.is_some();
    let mut distinct_order_by = distinct_order.unwrap_or_else(|| order_by.clone());
    if let Some(distinct) = distinct {
        let columns = distinct
            .into_iter()
            .map(|s| Value::DoubleQuotedString(s.to_string()).to_string())
            .collect::<Vec<String>>();
        projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
            value: ON.to_owned() + " (" + &columns.join(",") + ") *",
            quote_style: None,
        }))];
        columns.into_iter().rev().for_each(|c| {
            distinct_order_by.insert(
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
    let q = Query {
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
        order_by: distinct_order_by,
        limit: first,
        offset: after,
        fetch: None,
        locks: vec![],
    };
    if has_distinct_order && order_by.len() > 0 {
        Query {
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
                into: None,
                from: vec![TableWithJoins {
                    relation: TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(q),
                        alias: Some(TableAlias {
                            name: Ident {
                                value: "sorter".to_string(),
                                quote_style: None,
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
            order_by,
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
        }
    } else {
        q
    }
}

fn get_order<'a>(
    order: &IndexMap<Name, GqlValue>,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<Vec<OrderByExpr>> {
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
                    return Ok(vec![OrderByExpr {
                        expr: Expr::Identifier(Ident {
                            value: s.clone(),
                            quote_style: Some(QUOTE_CHAR),
                        }),
                        asc,
                        nulls_first: None,
                    }]);
                }
                GqlValue::Object(args) => {
                    if let Some(expression) = get_filter(args, parameters)? {
                        return Ok(vec![OrderByExpr {
                            expr: expression,
                            asc,
                            nulls_first: None,
                        }]);
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
                        value: key.as_str().to_owned(),
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
                        value: key.as_str().to_owned(),
                        quote_style: Some(QUOTE_CHAR),
                    }),
                    asc: Some(s == "ASC"),
                    nulls_first: None,
                });
            }
            _ => unimplemented!(),
        }
    }
    Ok(order_by)
}

fn get_distinct<'a>(distinct: &'a Vec<GqlValue>) -> Option<Vec<&'a str>> {
    let values: Vec<&'a str> = distinct
        .iter()
        .flat_map(|v| match v {
            GqlValue::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn parse_args<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<(
    Option<Expr>,
    Option<Vec<&'a str>>,
    Option<Vec<OrderByExpr>>,
    Vec<OrderByExpr>,
    Option<Expr>,
    Option<Offset>,
)> {
    let mut selection = None;
    let mut order_by = vec![];
    let mut distinct = None;
    let mut distinct_order = None;
    let mut first = None;
    let mut after = None;
    for argument in arguments {
        let (p_key, p_value) = argument;
        let key = p_key.node.as_str();
        let value = &p_value.node;
        match (key, value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(filter, parameters)?;
            }
            ("distinct", GqlValue::Object(d)) => {
                if let Some(GqlValue::List(list)) = d.get("on") {
                    distinct = get_distinct(list);
                }
                match d.get("order") {
                    Some(GqlValue::Object(order)) => {
                        distinct_order = Some(get_order(order, parameters)?);
                    }
                    Some(GqlValue::List(list)) => {
                        let order = list
                            .into_iter()
                            .filter_map(|v| match v {
                                GqlValue::Object(o) => Some(o),
                                _ => None,
                            })
                            .map(|o| get_order(&o, parameters))
                            .collect::<AnyResult<Vec<Vec<OrderByExpr>>>>()?;
                        distinct_order = Some(order.into_iter().flatten().collect());
                    }
                    _ => unimplemented!(),
                }
            }
            ("order", GqlValue::Object(order)) => {
                order_by = get_order(order, parameters)?;
            }
            ("order", GqlValue::List(list)) => {
                let items = list
                    .into_iter()
                    .filter_map(|v| match v {
                        GqlValue::Object(o) => Some(o),
                        _ => None,
                    })
                    .map(|o| get_order(&o, parameters))
                    .collect::<AnyResult<Vec<Vec<OrderByExpr>>>>()?;
                order_by.append(items.into_iter().flatten().collect::<Vec<OrderByExpr>>().as_mut())
            }
            ("first", GqlValue::Number(count)) => {
                first = Some(Expr::Value(Value::Number(
                    count.as_i64().expect("int to be an i64").to_string(),
                    false,
                )));
            }
            ("after", GqlValue::Number(count)) => {
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
    Ok((selection, distinct, distinct_order, order_by, first, after))
}

fn get_mutation_columns<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<(Vec<Ident>, Vec<Vec<Expr>>)> {
    let mut columns = vec![];
    let mut rows = vec![];
    for argument in arguments {
        let (key, value) = argument;
        let (key, value) = (&key.node, &value.node);
        match (key.as_ref(), value) {
            ("data", GqlValue::Object(data)) => {
                let mut row = vec![];
                for (key, value) in data.iter() {
                    columns.push(Ident {
                        value: key.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    });
                    row.push(get_value(value, parameters)? );
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
                                    value: key.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
                                });
                            }
                            row.push(get_value(value, parameters)?);
                        }
                    }
                    rows.push(row);
                }
            }
            _ => todo!(),
        }
    }
    Ok((columns, rows))
}

fn get_mutation_assignments<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    parameters: &IndexMap<&'a str, DataType>,
) -> AnyResult<(Option<Expr>, Vec<Assignment>)> {
    let mut selection = None;
    let mut assignments = vec![];
    for argument in arguments {
        let (p_key, p_value) = argument;
        let (key, value) = (&p_key.node, &p_value.node);
        match (key.as_ref(), value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(filter, parameters)?;
            }
            ("set", GqlValue::Object(data)) => {
                for (key, value) in data.iter() {
                    assignments.push(Assignment {
                        id: vec![Ident {
                            value: key.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        }],
                        value: get_value(value, parameters)?,
                    });
                }
            }
            ("inc" | "increment", GqlValue::Object(data)) => {
                for (key, value) in data.iter() {
                    let column_ident = Ident {
                        value: key.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    };
                    assignments.push(Assignment {
                        id: vec![column_ident.clone()],
                        value: Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(column_ident)),
                            op: BinaryOperator::Plus,
                            right: Box::new(get_value(value, parameters)?),
                        },
                    });
                }
            }
            _ => todo!(),
        }
    }
    Ok((selection, assignments))
}

fn get_data_type(var_type: &Type) -> DataType {
    match var_type.base {
        BaseType::Named(ref name) => match name.as_str() {
            "Int" => DataType::Int(None),
            "Float" => DataType::Float(None),
            "String" => DataType::Text,
            "Boolean" => DataType::Boolean,
            "ID" => DataType::Text,
            _ => unimplemented!(),
        },
        BaseType::List(_) => unimplemented!(),
    }
}

pub fn parse_query_meta<'a>(field: &'a Field) -> (&'a str, &'a str, bool, bool) {
    let mut is_aggregate = false;
    let mut is_single = false;
    let mut name = field.name.node.as_str();
    let key = field
        .alias
        .as_ref()
        .map_or_else(|| field.name.node.as_str(), |alias| alias.node.as_str());

    if name.ends_with("_aggregate") {
        name = &name[..name.len() - 10];
        is_aggregate = true;
    } else if name.ends_with("_one") {
        name = &name[..name.len() - 4];
        is_single = true;
    }

    if let Some(p_directive) = field
        .directives
        .iter()
        .find(|directive| directive.node.name.node.as_str() == "meta")
    {
        let directive = &p_directive.node;
        directive.arguments.iter().for_each(|(arg_name, argument)| {
            let arg_name = arg_name.node.as_str();
            if arg_name == "table" {
                if let GqlValue::String(table) = &argument.node {
                    name = table.as_ref();
                }
            } else if arg_name == "aggregate" {
                if let GqlValue::Boolean(aggregate) = &argument.node {
                    is_aggregate = *aggregate;
                }
            } else if arg_name == "single" {
                if let GqlValue::Boolean(single) = &argument.node {
                    is_single = *single;
                }
            }
        });
    }

    if is_aggregate && is_single {
        panic!("Query cannot be both aggregate and single");
    }

    return (name, key, is_aggregate, is_single);
}

pub fn parse_mutation_meta<'a>(field: &'a Field) -> (&'a str, &'a str, bool, bool) {
    let mut is_insert = false;
    let mut is_update = false;
    let mut name = field.name.node.as_ref();
    let key = field
        .alias
        .as_ref()
        .map_or_else(|| field.name.node.as_str(), |alias| alias.node.as_str());

    if name.starts_with("insert_") {
        name = &name[7..];
        is_insert = true;
    } else if name.starts_with("update_") {
        name = &name[7..];
        is_update = true;
    }

    if let Some(p_directive) = field
        .directives
        .iter()
        .find(|directive| directive.node.name.node.as_str() == "meta")
    {
        let directive = &p_directive.node;
        directive.arguments.iter().for_each(|(arg_name, argument)| {
            let arg_name = arg_name.node.as_str();
            if arg_name == "table" {
                if let GqlValue::String(table) = &argument.node {
                    name = table.as_ref();
                }
            } else if arg_name == "insert" {
                if let GqlValue::Boolean(aggregate) = &argument.node {
                    is_insert = *aggregate;
                }
            } else if arg_name == "update" {
                if let GqlValue::Boolean(single) = &argument.node {
                    is_update = *single;
                }
            }
        });
    }

    if is_insert && is_update {
        panic!("Mutation can not be both insert and update");
    }

    return (name, key, is_insert, is_update);
}

pub fn wrap_mutation<'a>(key: &'a str, value: Statement) -> Statement {
    Statement::Query(Box::new(Query {
        with: Some(With {
            cte_tables: vec![Cte {
                alias: TableAlias {
                    name: Ident {
                        value: "result".to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    },
                    columns: vec![],
                },
                query: Box::new(Query {
                    with: None,
                    body: Box::new(SetExpr::Insert(value)),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    locks: vec![],
                }),
                from: None,
            }],
            recursive: false,
        }),
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: false,
            top: None,
            into: None,
            projection: vec![SelectItem::UnnamedExpr(Expr::Function(Function {
                name: ObjectName(vec![Ident {
                    value: JSON_BUILD_OBJECT.to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        Value::SingleQuotedString(DATA_LABEL.to_string()),
                    ))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                        name: ObjectName(vec![Ident {
                            value: JSON_BUILD_OBJECT.to_string(),
                            quote_style: None,
                        }]),
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString(key.to_string()),
                            ))),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(Box::new(
                                Query {
                                    with: None,
                                    body: Box::new(SetExpr::Select(Box::new(Select {
                                        distinct: false,
                                        top: None,
                                        projection: vec![SelectItem::UnnamedExpr(Expr::Function(
                                            Function {
                                                over: None,
                                                distinct: false,
                                                special: false,
                                                name: ObjectName(vec![Ident {
                                                    value: "coalesce".to_string(),
                                                    quote_style: None,
                                                }]),
                                                args: vec![
                                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                        Expr::Function(Function {
                                                            name: ObjectName(vec![Ident {
                                                                value: JSON_AGG.to_string(),
                                                                quote_style: None,
                                                            }]),
                                                            args: vec![FunctionArg::Unnamed(
                                                                FunctionArgExpr::Expr(
                                                                    Expr::Identifier(Ident {
                                                                        value: "result".to_string(),
                                                                        quote_style: Some(
                                                                            QUOTE_CHAR,
                                                                        ),
                                                                    }),
                                                                ),
                                                            )],
                                                            over: None,
                                                            distinct: false,
                                                            special: false,
                                                        }),
                                                    )),
                                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                        Expr::Value(Value::SingleQuotedString(
                                                            "[]".to_string(),
                                                        )),
                                                    )),
                                                ],
                                            },
                                        ))],
                                        into: None,
                                        from: vec![TableWithJoins {
                                            relation: TableFactor::Table {
                                                name: ObjectName(vec![Ident {
                                                    value: "result".to_string(),
                                                    quote_style: Some(QUOTE_CHAR),
                                                }]),
                                                alias: None,
                                                args: None,
                                                with_hints: vec![],
                                            },
                                            joins: vec![],
                                        }],
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
                                },
                            )))),
                        ],
                        over: None,
                        distinct: false,
                        special: false,
                    }))),
                ],
                over: None,
                distinct: false,
                special: false,
            }))],
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
    }))
}

pub fn gql2sql<'a>(
    ast: ExecutableDocument,
    operation_name: Option<&'a str>,
) -> Result<(Statement, Option<Vec<String>>), anyhow::Error> {
    let mut statements = Vec::new();
    let mut parameters: IndexMap<&str, DataType> = IndexMap::new();
    let operation = match ast.operations {
        DocumentOperations::Single(operation) => operation.node,
        DocumentOperations::Multiple(map) => {
            if let Some(name) = operation_name {
                map.get(name)
                    .ok_or_else(|| anyhow::anyhow!("Operation {} not found in the document", name))?
                    .node.clone()
            } else {
                map.values().next().ok_or_else(|| {
                    anyhow::anyhow!("No operation found in the document, please specify one")
                })?.node.clone()
            }
        }
    };

    match operation.ty {
        OperationType::Query => {
            for  param in operation.variable_definitions.iter() {
                let ptype = &param.node.var_type.node;
                parameters.insert(param.node.name.node.as_str(), get_data_type(&ptype));
            }
            for selection in &operation.selection_set.node.items {
                match &selection.node {
                    Selection::Field(p_field) => {
                        let field = &p_field.node;
                        let (name, key, is_aggregate, is_single) = parse_query_meta(field);
                        let (selection, distinct, distinct_order, order_by, mut first, after) =
                            parse_args(&field.arguments, &parameters)?;
                        if is_single {
                            first = Some(Expr::Value(Value::Number("1".to_string(), false)));
                        }
                        let base_query = get_filter_query(
                            selection,
                            order_by,
                            first,
                            after,
                            name.to_owned(),
                            distinct,
                            distinct_order,
                        );
                        if is_aggregate {
                            let aggs = get_aggregate_projection(&field.selection_set.node.items);
                            statements.push((
                                key,
                                Query {
                                    with: None,
                                    body: Box::new(get_agg_query(
                                        aggs,
                                        vec![TableWithJoins {
                                            relation: TableFactor::Derived {
                                                lateral: false,
                                                subquery: Box::new(base_query),
                                                alias: Some(TableAlias {
                                                    name: Ident {
                                                        value: BASE.to_string(),
                                                        quote_style: Some(QUOTE_CHAR),
                                                    },
                                                    columns: vec![],
                                                }),
                                            },
                                            joins: vec![],
                                        }],
                                        None,
                                        &ROOT_LABEL,
                                    )),
                                    order_by: vec![],
                                    limit: None,
                                    offset: None,
                                    fetch: None,
                                    locks: vec![],
                                },
                            ));
                        } else {
                            let (projection, joins, merges) = get_projection(
                                &field.selection_set.node.items,
                                &name,
                                Some(BASE),
                                &parameters,
                            )?;
                            let root_query = get_root_query(
                                projection,
                                vec![TableWithJoins {
                                    relation: TableFactor::Derived {
                                        lateral: false,
                                        subquery: Box::new(base_query),
                                        alias: Some(TableAlias {
                                            name: Ident {
                                                value: BASE.to_string(),
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
        }
        OperationType::Mutation => {
            for selection in operation.selection_set.node.items {
                match &selection.node {
                    Selection::Field(p_field) => {
                        let field = &p_field.node;
                        let (name, key, is_insert, is_update) = parse_mutation_meta(field);
                        if is_insert {
                            let (columns, rows) =
                                get_mutation_columns(&field.arguments, &parameters)?;
                            let (projection, _, _) = get_projection(
                                &field.selection_set.node.items,
                                name,
                                None,
                                &parameters,
                            )?;
                            return Ok((
                                wrap_mutation(
                                    key,
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
                                ),
                                None,
                            ));
                        } else if is_update {
                            let (projection, _, _) = get_projection(
                                &field.selection_set.node.items,
                                &name,
                                None,
                                &parameters,
                            )?;
                            let (selection, assignments) =
                                get_mutation_assignments(&field.arguments, &parameters)?;
                            return Ok((
                                wrap_mutation(
                                    key,
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
                                ),
                                None,
                            ));
                        }
                    }
                    Selection::FragmentSpread(_) => unimplemented!(),
                    Selection::InlineFragment(_) => unimplemented!(),
                }
            }
        }
        OperationType::Subscription => unimplemented!(),
    }
    Err(anyhow!("No operation found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql_parser::parse_query;
    use pretty_assertions::assert_eq;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn query() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query App {
                app(filter: { id: { eq: "345810043118026832" } }, order: { name: ASC }) @meta(table: "App") {
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
            }
            query Another {
                Component_aggregate(filter: { appId: { eq: "345810043118026832" } }) {
                  count
                  min {
                    createdAt
                  }
                }
            }
        "#,
        )?.clone();
        let sql = r#"SELECT json_build_object('app', (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base"."id", "components") AS "root"))), '[]') AS "root" FROM (SELECT * FROM "App" WHERE "id" = '345810043118026832' ORDER BY "name" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component"."id", "pageMeta", "elements") AS "root"))), '[]') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id") AS "base.Component" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."id", "base.Component.PageMeta"."path") AS "root")) AS "pageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.Element"."id", "base.Component.Element"."name") AS "root"))), '[]') AS "elements" FROM (SELECT * FROM "Element" WHERE "Element"."componentParentId" = "base.Component"."id" ORDER BY "order" ASC) AS "base.Component.Element") AS "root.Element" ON ('true')) AS "root.Component" ON ('true')), 'Component_aggregate', (SELECT json_build_object('count', COUNT(*), 'min', json_build_object('createdAt', MIN("createdAt"))) AS "root" FROM (SELECT * FROM "Component" WHERE "appId" = '345810043118026832') AS "base")) AS "data""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql).unwrap();
        let (statement, _params) = gql2sql(gqlast, Some("App"))?;
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn mutation_insert() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"mutation insertVillains {
                insert(data: [
                    { name: "Ronan the Accuser" },
                    { name: "Red Skull" },
                    { name: "The Vulture" },
                ]) @meta(table: "Villain", insert: true) { id name }
            }"#,
        )?
        .clone();
        let sql = r#"WITH "result" as (INSERT INTO "Villain" ("name") VALUES ('Ronan the Accuser'), ('Red Skull'), ('The Vulture') RETURNING "id", "name") SELECT json_build_object('data', json_build_object('insert', (SELECT coalesce(json_agg("result"), '[]') FROM "result")))"#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, _params) = gql2sql(gqlast, None)?;
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn mutation_update() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"mutation updateHero {
                update(
                    filter: { secret_identity: { eq: "Sam Wilson" }},
                    set: {
                        name: "Captain America",
                    }
                    increment: {
                        number_of_movies: 1
                    }
                ) @meta(table: "Hero", update: true) {
                    id
                    name
                    secret_identity
                    number_of_movies
                }
            }"#,
        )?
        .clone();
        let sql = r#"WITH "result" AS (UPDATE "Hero" SET "name" = 'Captain America', "number_of_movies" = "number_of_movies" + 1 WHERE "secret_identity" = 'Sam Wilson' RETURNING "id", "name", "secret_identity", "number_of_movies") SELECT json_build_object('data', json_build_object('update', (SELECT coalesce(json_agg("result"), '[]') FROM "result")))"#;
        // let dialect = PostgreSqlDialect {};
        // let sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, _params) = gql2sql(gqlast, None)?;
        assert_eq!(statement.to_string(), sql);
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
        let (statement, _params) = gql2sql(gqlast, None)?;
        // assert_eq!(statements, sqlast);
        Ok(())
    }

    #[test]
    fn query_frag() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query GetApp($componentId: String!, $branch: String!) {
                component: Component_one(filter: { id : { eq: $componentId } }) {
                   id
                   branch
                   ... on ComponentMeta @relation(
                        table: "ComponentMeta"
                        field: ["componentId"]
                        references: ["id"]
                        single: true
                        filter: {
                                or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }]
                        }
                    ) {
                     title
                   }
                }
            }"#,
        )?
        .clone();
        let sql = r#"SELECT json_build_object('component', (SELECT CAST(to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch") AS "root")) AS jsonb) || CASE WHEN "root.ComponentMeta"."ComponentMeta" IS NOT NULL THEN to_jsonb("ComponentMeta") ELSE jsonb_build_object() END AS "root" FROM (SELECT * FROM "Component" WHERE "id" = $1 LIMIT 1) AS "base" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.ComponentMeta"."title") AS "root")) AS "ComponentMeta" FROM (SELECT * FROM "ComponentMeta" WHERE "ComponentMeta"."componentId" = "base"."id" AND ("branch" = $2 OR "branch" = 'main') LIMIT 1) AS "base.ComponentMeta") AS "root.ComponentMeta" ON ('true'))) AS "data""#;
        let (statement, _params) = gql2sql(gqlast, None)?;
        assert_eq!(sql, statement.to_string());
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
        let (statement, _params) = gql2sql(gqlast, None)?;
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn query_distinct() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query GetApp($componentId: String!, $branch: String!) {
                component: Component_one(
                    filter: {
                        id : { eq: $componentId },
                        or: [
                            { branch: { eq: $branch } },
                            { branch: { eq: "main" } }
                        ]
                    },
                    order: [
                        { orderKey: ASC }
                    ],
                    distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
                ) {
                   id
                   branch
                   kind @static(value: "page")
                   stuff(filter: { componentId: { eq: { _parentRef: "id" } } }) @relation(table: "Stuff") {
                     id
                   }
                }
            }"#,
        )?
        .clone();
        let sql = r#"SELECT json_build_object('component', (SELECT to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch", 'page' AS "kind", "stuff") AS "root")) AS "root" FROM (SELECT * FROM (SELECT DISTINCT ON ("id") * FROM "Component" WHERE "id" = $1 AND ("branch" = $2 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $2 DESC LIMIT 1) AS sorter ORDER BY "orderKey" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Stuff"."id") AS "root"))), '[]') AS "stuff" FROM (SELECT * FROM "Stuff" WHERE "componentId" = "base"."id") AS "base.Stuff") AS "root.Stuff" ON ('true'))) AS "data""#;
        let (statement, _params) = gql2sql(gqlast, None)?;
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
        Ok(())
    }

    #[test]
    fn query_sub_agg() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query GetData {
                testing @meta(table: "UcwtYEtmmpXagcpcRiYKC") {
                    id
                    created_at
                    updated_at
                    anothers @relation(table: "N8Ag4Vgad4rYwcRmMJhGR", fields: ["id"], reference:["xb8nemrkchVQgxkXkCPhE"], aggregate: true) {
                        count
                    }
                    stuff @relation(table: "iYrk3kyTqaDQrLgjDaE9n", fields: ["eT86hgrpFB49r7N6AXz63"], references: ["id"], single: true) {
                        id
                    }
                }
            }"#,
        )?
        .clone();
        // let sql = r#""#;
        let (statement, _params) = gql2sql(gqlast, None)?;
        // assert_eq!(statement.to_string(), sql);
        Ok(())
    }

    #[test]
    fn query_json_arg() -> Result<(), anyhow::Error> {
        let gqlast = parse_query::<&str>(
            r#"query BrevityQuery($order_getProfileList: tMjcdRYigDTiKeFcdfciTM_Order) {
                getProfileList(order: $order_getProfileList) @meta(table: "tMjcdRYigDTiKeFcdfciTM") {
                    id
                }
            }"#,
        )?
        .clone();
        // let sql = r#""#;
        let (statement, _params) = gql2sql(gqlast, None)?;
        // assert_eq!(statement.to_string(), sql);
        Ok(())
    }
}
