mod consts;

use crate::consts::{
    BASE, DATA_LABEL, JSON_AGG, JSON_BUILD_OBJECT, ON, QUOTE_CHAR, ROOT_LABEL, TO_JSON, TO_JSONB,
};
use anyhow::anyhow;
use async_graphql_parser::{
    types::{
        Directive, DocumentOperations, ExecutableDocument, Field, OperationType, Selection,
        VariableDefinition,
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

type JsonValue = serde_json::Value;
type AnyResult<T> = anyhow::Result<T>;

fn get_value<'a>(value: &'a GqlValue, sql_vars: &'a IndexMap<Name, JsonValue>) -> AnyResult<Expr> {
    match value {
        GqlValue::Variable(v) => {
            let index = sql_vars
                .get_index_of(v)
                .map(|i| i + 1)
                .ok_or(anyhow!("variable not found"))?;
            Ok(Expr::Value(Value::Placeholder(format!("${index}"))))
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

fn get_op(op: &str) -> AnyResult<BinaryOperator> {
    let value = match op {
        "eq" | "equals" => BinaryOperator::Eq,
        "neq" | "not_equals" => BinaryOperator::NotEq,
        "lt" | "less_than" => BinaryOperator::Lt,
        "lte" | "less_than_or_equals" => BinaryOperator::LtEq,
        "gt" | "greater_than" => BinaryOperator::Gt,
        "gte" | "greater_than_or_equals" => BinaryOperator::GtEq,
        _ => {
            return Err(anyhow!("operator not supported: {}", op));
        }
    };
    Ok(value)
}

fn get_expr<'a>(
    left: Expr,
    args: &'a GqlValue,
    variables: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<Option<Expr>> {
    if let GqlValue::Object(o) = args {
        return match o.len() {
            0 => Ok(None),
            1 => {
                let (op, value) = o.iter().next().ok_or(anyhow!("list to have one item"))?;
                let right_value = get_value(value, variables)?;
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
                        let op = get_op(op.as_ref())?;
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
                        let op = get_op(op.as_ref())?;
                        let value = get_value(v, variables)?;
                        Ok(Expr::BinaryOp {
                            left: Box::new(left.clone()),
                            op,
                            right: Box::new(value),
                        })
                    })
                    .collect::<AnyResult<Vec<Expr>>>()?;
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
    variables: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<Option<Expr>> {
    let value = match (key, value) {
        ("OR" | "or", GqlValue::List(list)) => match list.len() {
            0 => None,
            1 => match list.get(0).ok_or(anyhow!("list to have one item"))? {
                GqlValue::Object(o) => get_filter(o, variables)?,
                _ => None,
            },
            _ => {
                let mut conditions = list
                    .iter()
                    .map(|v| match v {
                        GqlValue::Object(o) => get_filter(o, variables),
                        _ => Ok(None),
                    })
                    .collect::<AnyResult<Vec<Option<Expr>>>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<Expr>>();

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
                GqlValue::Object(o) => get_filter(o, variables)?,
                _ => None,
            },
            _ => {
                let mut conditions = list
                    .iter()
                    .map(|v| match v {
                        GqlValue::Object(o) => get_filter(o, variables),
                        _ => Ok(None),
                    })
                    .collect::<AnyResult<Vec<Option<Expr>>>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<Expr>>();
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
            get_expr(left, value, variables)?
        }
    };
    Ok(value)
}

fn get_filter<'a>(
    args: &IndexMap<Name, GqlValue>,
    variables: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<Option<Expr>> {
    let value = match args.len() {
        0 => None,
        1 => {
            let (key, value) = args.iter().next().expect("list to have one item");
            handle_filter_arg(key.as_str(), value, variables)?
        }
        _ => {
            let mut conditions: Vec<Expr> = args
                .into_iter()
                .rev()
                .map(|(key, value)| handle_filter_arg(key, value, variables))
                .collect::<AnyResult<Vec<Option<Expr>>>>()?
                .into_iter()
                .flatten()
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
    merges: &[Merge],
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
                        vec![]
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

fn get_aggregate_projection<'a>(
    items: &'a Vec<Positioned<Selection>>,
) -> AnyResult<Vec<FunctionArg>> {
    let mut aggs = Vec::new();
    for selection in items {
        match &selection.node {
            Selection::Field(field) => {
                aggs.extend(get_agg_agg_projection(&field.node));
            }
            Selection::FragmentSpread(_) => {
                return Err(anyhow!(
                    "Fragment spread is not supported in aggregate query"
                ));
            }
            Selection::InlineFragment(_) => {
                return Err(anyhow!(
                    "Inline fragment is not supported in aggregate query"
                ));
            }
        }
    }
    Ok(aggs)
}

fn get_join<'a>(
    arguments: &Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    directives: &Vec<Positioned<Directive>>,
    selection_items: &Vec<Positioned<Selection>>,
    path: Option<&'a str>,
    name: &'a str,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<Join> {
    let (selection, distinct, distinct_order, order_by, mut first, after) =
        parse_args(arguments, variables, sql_vars)?;
    let (relation, fks, pks, is_single, is_aggregate) = get_relation(directives, sql_vars)?;
    if is_single {
        first = Some(Expr::Value(Value::Number("1".to_string(), false)));
    }
    let sub_path = path.map_or_else(|| relation.to_string(), |v| format!("{v}.{relation}"));
    let join_filter = zip(pks, fks)
        .map(|(pk, fk)| Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                Ident {
                    value: relation.to_string(),
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
                    value: path.map_or(BASE.to_string(), std::string::ToString::to_string),
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
        &relation,
        distinct,
        distinct_order,
    );
    if is_aggregate {
        let aggs = get_aggregate_projection(selection_items)?;
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
        let (sub_projection, sub_joins, merges) = get_projection(
            selection_items,
            &relation,
            Some(&sub_path),
            variables,
            sql_vars,
        )?;
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
                        &merges,
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

fn get_static<'a>(
    name: &'a str,
    directives: &Vec<Positioned<Directive>>,
    sql_vars: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<Option<SelectItem>> {
    for p_directive in directives {
        let directive = &p_directive.node;
        let directive_name: &str = directive.name.node.as_ref();
        if directive_name == "static" {
            let (_, value) = directive
                .arguments
                .iter()
                .find(|(name, _)| name.node.as_ref() == "value")
                .ok_or_else(|| anyhow!("static value not found"))?;
            let value = match &value.node {
                GqlValue::String(value) => value.to_string(),
                GqlValue::Number(value) => value.as_i64().expect("value is not an int").to_string(),
                GqlValue::Variable(name) => {
                    if let Some(value) = sql_vars.get(name) {
                        value.to_string()
                    } else {
                        return Err(anyhow!("variable not found: {}", name));
                    }
                }
                GqlValue::Boolean(value) => value.to_string(),
                _ => {
                    return Err(anyhow!("static value is not a string"));
                }
            };
            return Ok(Some(SelectItem::ExprWithAlias {
                expr: Expr::Value(Value::SingleQuotedString(value)),
                alias: Ident {
                    value: name.to_string(),
                    quote_style: Some(QUOTE_CHAR),
                },
            }));
        }
    }
    Ok(None)
}

fn get_projection<'a>(
    items: &Vec<Positioned<Selection>>,
    relation: &'a str,
    path: Option<&'a str>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a IndexMap<Name, JsonValue>,
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
                        variables,
                        sql_vars,
                    )?;
                    joins.push(join);
                    projection.push(SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: field.name.node.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    })));
                } else {
                    if let Some(value) = get_static(&field.name.node, &field.directives, sql_vars)?
                    {
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
                        .find(|d| d.node.name.node.as_ref() == "args");
                    let (relation, _fks, _pks, _is_single, _is_aggregate) =
                        get_relation(&frag.directives, sql_vars)?;
                    let join = get_join(
                        args.map_or(&vec![], |dir| &dir.node.arguments),
                        &frag.directives,
                        &frag.selection_set.node.items,
                        path,
                        name,
                        variables,
                        sql_vars,
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
                    });
                }
            }
            Selection::FragmentSpread(_) => {
                return Err(anyhow!("Fragment spread is not supported"));
            }
        }
    }
    Ok((projection, joins, merges))
}

fn value_to_string<'a>(
    value: &'a GqlValue,
    sql_vars: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<String> {
    let output = match value {
        GqlValue::String(s) => s.clone(),
        GqlValue::Number(f) => f.to_string(),
        GqlValue::Boolean(b) => b.to_string(),
        GqlValue::Enum(e) => e.to_string(),
        GqlValue::List(l) => l
            .iter()
            .map(|l| value_to_string(l, sql_vars))
            .collect::<AnyResult<Vec<String>>>()?
            .join(","),
        GqlValue::Null => "null".to_owned(),
        GqlValue::Object(obj) => serde_json::to_string(obj).unwrap(),
        GqlValue::Variable(name) => {
            if let Some(value) = sql_vars.get(name) {
                value.to_string()
            } else {
                return Err(anyhow!("Variable {} is not defined", name));
            }
        }
        GqlValue::Binary(_) => {
            return Err(anyhow!("Binary value is not supported"));
        }
    };
    Ok(output)
}

fn get_relation<'a>(
    directives: &'a [Positioned<Directive>],
    sql_vars: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<(String, Vec<String>, Vec<String>, bool, bool)> {
    let mut relation: String = String::new();
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
                    "table" => relation = value_to_string(value, sql_vars)?,
                    "field" | "fields" => {
                        fk = match &value {
                            GqlValue::String(s) => vec![s.clone()],
                            GqlValue::List(e) => e
                                .iter()
                                .map(|l| value_to_string(l, sql_vars))
                                .collect::<AnyResult<Vec<String>>>()?,
                            _ => {
                                return Err(anyhow!("Invalid value for field in relation"));
                            }
                        }
                    }
                    "reference" | "references" => {
                        pk = match value {
                            GqlValue::String(s) => vec![s.clone()],
                            GqlValue::List(e) => e
                                .iter()
                                .map(|l| value_to_string(l, sql_vars))
                                .collect::<AnyResult<Vec<String>>>()?,
                            _ => {
                                return Err(anyhow!("Invalid value for reference in relation"));
                            }
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
    Ok((relation, fk, pk, is_single, is_aggregate))
}

fn get_filter_query<'a>(
    selection: Option<Expr>,
    order_by: Vec<OrderByExpr>,
    first: Option<Expr>,
    after: Option<Offset>,
    table_name: &'a str,
    distinct: Option<Vec<String>>,
    distinct_order: Option<Vec<OrderByExpr>>,
) -> Query {
    let mut projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];
    let is_distinct = distinct.is_some();
    let has_distinct_order = distinct_order.is_some();
    let mut distinct_order_by = distinct_order.unwrap_or_else(|| order_by.clone());
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
                        value: table_name.to_string(),
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
    if has_distinct_order && !order_by.is_empty() {
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
    sql_vars: &'a IndexMap<Name, JsonValue>,
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
                GqlValue::Variable(v) => {
                    if let Some(JsonValue::String(s)) = sql_vars.get(v) {
                        asc = Some(s == "ASC");
                    }
                }
                _ => {
                    return Err(anyhow!("Invalid value for order direction"));
                }
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
                    if let Some(expression) = get_filter(args, sql_vars)? {
                        return Ok(vec![OrderByExpr {
                            expr: expression,
                            asc,
                            nulls_first: None,
                        }]);
                    }
                }
                GqlValue::Variable(v) => {
                    if let Some(JsonValue::String(s)) = sql_vars.get(v) {
                        return Ok(vec![OrderByExpr {
                            expr: Expr::Identifier(Ident {
                                value: s.clone(),
                                quote_style: Some(QUOTE_CHAR),
                            }),
                            asc,
                            nulls_first: None,
                        }]);
                    }
                }
                _ => {
                    return Err(anyhow!("Invalid value for order expression"));
                }
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
            GqlValue::Variable(name) => {
                let index = sql_vars
                    .get_index_of(name)
                    .map(|i| i + 1)
                    .ok_or(anyhow!("variable not found"))?;
                order_by.push(OrderByExpr {
                    expr: Expr::Value(Value::Placeholder(format!("${index}"))),
                    asc: None,
                    nulls_first: None,
                });
            }
            _ => return Err(anyhow!("Invalid value for order expression")),
        }
    }
    Ok(order_by)
}

fn get_distinct(distinct: Vec<GqlValue>) -> Option<Vec<String>> {
    let values: Vec<String> = distinct
        .iter()
        .filter_map(|v| match v {
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

fn flatten<'a>(
    name: Name,
    value: &JsonValue,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
) -> GqlValue {
    match value {
        JsonValue::Null => GqlValue::Null,
        JsonValue::Bool(b) => {
            sql_vars.insert(name.clone(), JsonValue::Bool(*b));
            GqlValue::Variable(name)
        }
        JsonValue::String(s) => {
            sql_vars.insert(name.clone(), JsonValue::String(s.clone()));
            GqlValue::Variable(name)
        }
        JsonValue::Number(n) => {
            sql_vars.insert(name.clone(), JsonValue::Number(n.clone()));
            GqlValue::Variable(name)
        }
        JsonValue::Array(list) => {
            let new_list = list
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let new_name = format!("{name}_{i}");
                    flatten(Name::new(new_name), v, sql_vars)
                })
                .collect();
            GqlValue::List(new_list)
        }
        JsonValue::Object(o) => {
            let mut out = IndexMap::with_capacity(o.len());
            for (k, v) in o {
                let new_name = format!("{name}_{k}");
                let name = Name::new(new_name);
                let key = Name::new(k);
                let new_value = flatten(name, v, sql_vars);
                out.insert(key, new_value);
            }
            GqlValue::Object(out)
        }
    }
}

fn flatten_variables<'a>(
    variables: &'a Option<JsonValue>,
    definitions: Vec<Positioned<VariableDefinition>>,
) -> (IndexMap<Name, GqlValue>, IndexMap<Name, JsonValue>) {
    let mut sql_vars = IndexMap::new();
    let mut parameters = IndexMap::with_capacity(definitions.len());
    if let Some(JsonValue::Object(map)) = variables {
        for def in definitions {
            let def = def.node;
            let name = def.name.node;
            if let Some(value) = map.get(name.as_str()) {
                let new_value = flatten(name.clone(), value, &mut sql_vars);
                parameters.insert(name, new_value);
            }
        }
    }
    (parameters, sql_vars)
}

fn parse_args<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<(
    Option<Expr>,
    Option<Vec<String>>,
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
        let mut value = p_value.node.clone();
        if let GqlValue::Variable(ref name) = value {
            if let Some(new_value) = variables.get(name) {
                value = new_value.clone()
            }
        }
        match (key, value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(&filter, sql_vars)?;
            }
            ("distinct", GqlValue::Object(d)) => {
                if let Some(GqlValue::List(list)) = d.get("on") {
                    distinct = get_distinct(list.clone());
                }
                match d.get("order") {
                    Some(GqlValue::Object(order)) => {
                        distinct_order = Some(get_order(order, sql_vars)?);
                    }
                    Some(GqlValue::List(list)) => {
                        let order = list
                            .iter()
                            .filter_map(|v| match v {
                                GqlValue::Object(o) => Some(o),
                                _ => None,
                            })
                            .map(|o| get_order(o, sql_vars))
                            .collect::<AnyResult<Vec<Vec<OrderByExpr>>>>()?;
                        distinct_order = Some(order.into_iter().flatten().collect());
                    }
                    _ => {
                        return Err(anyhow!("Invalid value for distinct order"));
                    }
                }
            }
            ("order", GqlValue::Object(order)) => {
                order_by = get_order(&order, sql_vars)?;
            }
            ("order", GqlValue::List(list)) => {
                let items = list
                    .iter()
                    .filter_map(|v| match v {
                        GqlValue::Object(o) => Some(o),
                        _ => None,
                    })
                    .map(|o| get_order(o, sql_vars))
                    .collect::<AnyResult<Vec<Vec<OrderByExpr>>>>()?;
                order_by.append(
                    items
                        .into_iter()
                        .flatten()
                        .collect::<Vec<OrderByExpr>>()
                        .as_mut(),
                );
            }
            ("first", GqlValue::Variable(name)) => {
                first = Some(get_value(&GqlValue::Variable(name), sql_vars)?);
            }
            ("first", GqlValue::Number(count)) => {
                first = Some(Expr::Value(Value::Number(
                    count.as_i64().expect("int to be an i64").to_string(),
                    false,
                )));
            }
            ("after", GqlValue::Variable(name)) => {
                after = Some(Offset {
                    value: get_value(&GqlValue::Variable(name), sql_vars)?,
                    rows: OffsetRows::None,
                });
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
            _ => {
                return Err(anyhow!("Invalid argument for: {}", key));
            }
        }
    }
    Ok((selection, distinct, distinct_order, order_by, first, after))
}

fn get_mutation_columns<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<(Vec<Ident>, Vec<Vec<Expr>>)> {
    let mut columns = vec![];
    let mut rows = vec![];
    for argument in arguments {
        let (key, value) = argument;
        let (key, mut value) = (&key.node, &value.node);
        if let GqlValue::Variable(name) = value {
            if let Some(new_value) = variables.get(name) {
                value = new_value
            }
        }
        match (key.as_ref(), value) {
            ("data", GqlValue::Object(data)) => {
                let mut row = vec![];
                for (key, value) in data.iter() {
                    columns.push(Ident {
                        value: key.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    });
                    row.push(get_value(value, sql_vars)?);
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
                            row.push(get_value(value, sql_vars)?);
                        }
                    }
                    rows.push(row);
                }
            }
            _ => continue,
        }
    }
    Ok((columns, rows))
}

fn get_mutation_assignments<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a IndexMap<Name, JsonValue>,
) -> AnyResult<(Option<Expr>, Vec<Assignment>)> {
    let mut selection = None;
    let mut assignments = vec![];
    for argument in arguments {
        let (p_key, p_value) = argument;
        let (key, mut value) = (&p_key.node, &p_value.node);
        if let GqlValue::Variable(name) = value {
            if let Some(new_value) = variables.get(name) {
                value = new_value
            }
        }
        match (key.as_ref(), value) {
            ("filter" | "where", GqlValue::Object(filter)) => {
                selection = get_filter(filter, sql_vars)?;
            }
            ("set", GqlValue::Object(data)) => {
                for (key, value) in data.iter() {
                    assignments.push(Assignment {
                        id: vec![Ident {
                            value: key.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        }],
                        value: get_value(value, sql_vars)?,
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
                            right: Box::new(get_value(value, sql_vars)?),
                        },
                    });
                }
            }
            _ => todo!(),
        }
    }
    Ok((selection, assignments))
}

pub fn parse_query_meta<'a>(field: &'a Field) -> AnyResult<(&'a str, &'a str, bool, bool)> {
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
        return Err(anyhow!("Query cannot be both aggregate and single"));
    }

    Ok((name, key, is_aggregate, is_single))
}

#[must_use]
pub fn parse_mutation_meta<'a>(
    field: &'a Field,
) -> AnyResult<(&'a str, &'a str, bool, bool, bool)> {
    let mut is_insert = false;
    let mut is_update = false;
    let mut is_delete = false;
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
    } else if name.starts_with("delete_") {
        name = &name[7..];
        is_delete = true;
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
            } else if arg_name == "delete" {
                if let GqlValue::Boolean(single) = &argument.node {
                    is_delete = *single;
                }
            }
        });
    }

    if is_insert && is_update {
        return Err(anyhow!("Mutation cannot be both insert and update"));
    } else if is_insert && is_delete {
        return Err(anyhow!("Mutation cannot be both insert and delete"));
    } else if is_update && is_delete {
        return Err(anyhow!("Mutation cannot be both update and delete"));
    }

    Ok((name, key, is_insert, is_update, is_delete))
}

#[must_use]
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
    variables: &Option<JsonValue>,
    operation_name: Option<String>,
) -> AnyResult<(Statement, Option<Vec<JsonValue>>)> {
    let mut statements = Vec::new();
    let operation = match ast.operations {
        DocumentOperations::Single(operation) => operation.node,
        DocumentOperations::Multiple(map) => {
            if let Some(name) = operation_name {
                map.get(name.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Operation {} not found in the document", name))?
                    .node
                    .clone()
            } else {
                map.values()
                    .next()
                    .ok_or_else(|| {
                        anyhow::anyhow!("No operation found in the document, please specify one")
                    })?
                    .node
                    .clone()
            }
        }
    };

    let (variables, sql_vars) = flatten_variables(variables, operation.variable_definitions);

    match operation.ty {
        OperationType::Query => {
            for selection in &operation.selection_set.node.items {
                match &selection.node {
                    Selection::Field(p_field) => {
                        let field = &p_field.node;
                        let (name, key, is_aggregate, is_single) = parse_query_meta(field)?;
                        let (selection, distinct, distinct_order, order_by, mut first, after) =
                            parse_args(&field.arguments, &variables, &sql_vars)?;
                        if is_single {
                            first = Some(Expr::Value(Value::Number("1".to_string(), false)));
                        }
                        let base_query = get_filter_query(
                            selection,
                            order_by,
                            first,
                            after,
                            name,
                            distinct,
                            distinct_order,
                        );
                        if is_aggregate {
                            let aggs = get_aggregate_projection(&field.selection_set.node.items)?;
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
                                        ROOT_LABEL,
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
                                name,
                                Some(BASE),
                                &variables,
                                &sql_vars,
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
                                &merges,
                                is_single,
                                ROOT_LABEL,
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
                    Selection::FragmentSpread(_) => {
                        return Err(anyhow::anyhow!("Fragment not supported"))
                    }
                    Selection::InlineFragment(_) => {
                        return Err(anyhow::anyhow!("Fragment not supported"))
                    }
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
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                            Value::SingleQuotedString(key.to_string()),
                                        ))),
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
            let params = if sql_vars.is_empty() {
                None
            } else {
                Some(sql_vars.into_values().collect())
            };
            return Ok((statement, params));
        }
        OperationType::Mutation => {
            for selection in operation.selection_set.node.items {
                match &selection.node {
                    Selection::Field(p_field) => {
                        let field = &p_field.node;
                        let (name, key, is_insert, is_update, is_delete) =
                            parse_mutation_meta(field)?;
                        if is_insert {
                            let (columns, rows) =
                                get_mutation_columns(&field.arguments, &variables, &sql_vars)?;
                            let (projection, _, _) = get_projection(
                                &field.selection_set.node.items,
                                name,
                                None,
                                &variables,
                                &sql_vars,
                            )?;
                            let params = if sql_vars.is_empty() {
                                None
                            } else {
                                Some(sql_vars.into_values().collect())
                            };
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
                                params,
                            ));
                        } else if is_update {
                            let (projection, _, _) = get_projection(
                                &field.selection_set.node.items,
                                name,
                                None,
                                &variables,
                                &sql_vars,
                            )?;
                            let (selection, assignments) =
                                get_mutation_assignments(&field.arguments, &variables, &sql_vars)?;
                            let params = if sql_vars.is_empty() {
                                None
                            } else {
                                Some(sql_vars.into_values().collect())
                            };
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
                                params,
                            ));
                        } else if is_delete {
                            let (projection, _, _) = get_projection(
                                &field.selection_set.node.items,
                                name,
                                None,
                                &variables,
                                &sql_vars,
                            )?;
                            let (selection, _) =
                                get_mutation_assignments(&field.arguments, &variables, &sql_vars)?;
                            let params = if sql_vars.is_empty() {
                                None
                            } else {
                                Some(sql_vars.into_values().collect())
                            };
                            return Ok((
                                wrap_mutation(
                                    key,
                                    Statement::Delete {
                                        table_name: TableFactor::Table {
                                            name: ObjectName(vec![Ident {
                                                value: name.to_string(),
                                                quote_style: Some(QUOTE_CHAR),
                                            }]),
                                            alias: None,
                                            args: None,
                                            with_hints: vec![],
                                        },
                                        using: None,
                                        selection,
                                        returning: Some(projection),
                                    },
                                ),
                                params,
                            ));
                        }
                    }
                    Selection::FragmentSpread(_) => {
                        return Err(anyhow::anyhow!("Fragment not supported"))
                    }
                    Selection::InlineFragment(_) => {
                        return Err(anyhow::anyhow!("Fragment not supported"))
                    }
                }
            }
        }
        OperationType::Subscription => return Err(anyhow::anyhow!("Subscription not supported")),
    }
    Err(anyhow!("No operation found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql_parser::parse_query;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn query() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
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
        )?;
        let sql = r#"SELECT json_build_object('app', (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base"."id", "components") AS "root"))), '[]') AS "root" FROM (SELECT * FROM "App" WHERE "id" = '345810043118026832' ORDER BY "name" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component"."id", "pageMeta", "elements") AS "root"))), '[]') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id") AS "base.Component" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."id", "base.Component.PageMeta"."path") AS "root")) AS "pageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.Element"."id", "base.Component.Element"."name") AS "root"))), '[]') AS "elements" FROM (SELECT * FROM "Element" WHERE "Element"."componentParentId" = "base.Component"."id" ORDER BY "order" ASC) AS "base.Component.Element") AS "root.Element" ON ('true')) AS "root.Component" ON ('true')), 'Component_aggregate', (SELECT json_build_object('count', COUNT(*), 'min', json_build_object('createdAt', MIN("createdAt"))) AS "root" FROM (SELECT * FROM "Component" WHERE "appId" = '345810043118026832') AS "base")) AS "data""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql).unwrap();
        let (statement, _params) = gql2sql(gqlast, &None, Some("App".to_owned()))?;
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn mutation_insert() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"mutation insertVillains($data: [Villain_insert_input!]!) {
                insert(data: $data) @meta(table: "Villain", insert: true) { id name }
            }"#,
        )?;
        let sql = r#"WITH "result" as (INSERT INTO "Villain" ("name") VALUES ($1), ($2), ($3) RETURNING "id", "name") SELECT json_build_object('data', json_build_object('insert', (SELECT coalesce(json_agg("result"), '[]') FROM "result")))"#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, params) = gql2sql(
            gqlast,
            &Some(json!({
                "data": [
                    { "name": "Ronan the Accuser" },
                    { "name": "Red Skull" },
                    { "name": "The Vulture" }
                ]
            })),
            None,
        )?;
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn mutation_update() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
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
        )?;
        let sql = r#"WITH "result" AS (UPDATE "Hero" SET "name" = 'Captain America', "number_of_movies" = "number_of_movies" + 1 WHERE "secret_identity" = 'Sam Wilson' RETURNING "id", "name", "secret_identity", "number_of_movies") SELECT json_build_object('data', json_build_object('update', (SELECT coalesce(json_agg("result"), '[]') FROM "result")))"#;
        // let dialect = PostgreSqlDialect {};
        // let sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, _params) = gql2sql(gqlast, &None, None)?;
        assert_eq!(statement.to_string(), sql);
        Ok(())
    }

    #[test]
    fn query_mega() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
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
        )?;
        let sql = r#"UPDATE "Hero" SET "name" = 'Captain America', "number_of_movies" = "number_of_movies" + 1 WHERE "secret_identity" = 'Sam Wilson' RETURNING "id", "name", "secret_identity", "number_of_movies""#;
        let dialect = PostgreSqlDialect {};
        let _sqlast = Parser::parse_sql(&dialect, sql)?;
        let (_statement, _params) = gql2sql(
            gqlast,
            &Some(json!({
                "orgId": "org",
                "appId": "app",
                "branch": "branch"
            })),
            None,
        )?;
        // assert_eq!(statements, sqlast);
        Ok(())
    }

    #[test]
    fn query_frag() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query GetApp($componentId: String!, $branch: String!) {
                component: Component_one(filter: { id : { eq: $componentId } }) {
                   id
                   branch
                   ... on ComponentMeta @relation(
                        table: "ComponentMeta"
                        field: ["componentId"]
                        references: ["id"]
                        single: true
                    ) @args(
                        filter: {
                                or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }]
                        }
                    ) {
                     title
                   }
                }
            }"#,
        )?;
        let sql = r#"SELECT json_build_object('component', (SELECT CAST(to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch") AS "root")) AS jsonb) || CASE WHEN "root.ComponentMeta"."ComponentMeta" IS NOT NULL THEN to_jsonb("ComponentMeta") ELSE jsonb_build_object() END AS "root" FROM (SELECT * FROM "Component" WHERE "id" = $1 LIMIT 1) AS "base" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.ComponentMeta"."title") AS "root")) AS "ComponentMeta" FROM (SELECT * FROM "ComponentMeta" WHERE "ComponentMeta"."componentId" = "base"."id" AND ("branch" = $2 OR "branch" = 'main') LIMIT 1) AS "base.ComponentMeta") AS "root.ComponentMeta" ON ('true'))) AS "data""#;
        let (statement, _params) = gql2sql(
            gqlast,
            &Some(json!({
                "componentId": "comp",
                "branch": "branch"
            })),
            None,
        )?;
        assert_eq!(sql, statement.to_string());
        Ok(())
    }

    #[test]
    fn query_static() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query GetApp($componentId: String!) {
                component: Component_one(filter: { id : { eq: $componentId } }) {
                   id
                   branch
                   kind @static(value: "page")
                }
            }"#,
        )?;
        let sql = r#"SELECT json_build_object('component', (SELECT to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch", 'page' AS "kind") AS "root")) AS "root" FROM (SELECT * FROM "Component" WHERE "id" = $1 LIMIT 1) AS "base")) AS "data""#;
        let dialect = PostgreSqlDialect {};
        let sqlast = Parser::parse_sql(&dialect, sql)?;
        let (statement, _params) = gql2sql(
            gqlast,
            &Some(json!({
                "componentId": "fake"
            })),
            None,
        )?;
        assert_eq!(vec![statement], sqlast);
        Ok(())
    }

    #[test]
    fn query_distinct() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
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
        )?;
        let sql = r#"SELECT json_build_object('component', (SELECT to_json((SELECT "root" FROM (SELECT "base"."id", "base"."branch", 'page' AS "kind", "stuff") AS "root")) AS "root" FROM (SELECT * FROM (SELECT DISTINCT ON ("id") * FROM "Component" WHERE "id" = $1 AND ("branch" = $2 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $2 DESC LIMIT 1) AS sorter ORDER BY "orderKey" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Stuff"."id") AS "root"))), '[]') AS "stuff" FROM (SELECT * FROM "Stuff" WHERE "componentId" = "base"."id") AS "base.Stuff") AS "root.Stuff" ON ('true'))) AS "data""#;
        let (statement, _params) = gql2sql(
            gqlast,
            &Some(json!({
                "componentId": "fake",
                "branch": "branch",
            })),
            None,
        )?;
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
        let _sqlast = Parser::parse_sql(&dialect, sql)?;
        Ok(())
    }

    #[test]
    fn query_sub_agg() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
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
        )?;
        // let sql = r#""#;
        let (_statement, _params) = gql2sql(gqlast, &None, None)?;
        // assert_eq!(statement.to_string(), sql);
        Ok(())
    }

    #[test]
    fn query_json_arg() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query GetData($first: Int!, $after: Int!, $order: OrderBy!) {
                agg @meta(table: "tMjcdRYigDTiKeFcdfciTM", aggregate: true) {
                    count
                }
                tMjcdRYigDTiKeFcdfciTM(first: $first, after: $after, order: $order) @meta(table: "tMjcdRYigDTiKeFcdfciTM") {
                    id: id
                    created_at: created_at
                    updated_at: updated_at
                    bio: cbCFaUbBCaDQxRy3c7wPWi
                    username: cF6dbD8zBdHUJeiPaNg8Ex,
                    posts @relation(table: "tgwxVQxPi8EnKDwwWpJxbi", fields: ["cYzdX9KBkrpyJyRqg7hrJd"], references: ["id"], aggregate: true) { count }
                }
            }"#,
        )?;
        // let sql = r#""#;
        let (statement, params) = gql2sql(
            gqlast,
            &Some(json!({
                "first": 10,
                "after": 0,
                "order": {
                    "created_at": "DESC"
                }
            })),
            None,
        )?;
        println!("{:?}", statement.to_string());
        println!("{params:?}");
        // assert_eq!(statement.to_string(), sql);
        Ok(())
    }
}
