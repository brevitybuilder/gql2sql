#![allow(
    clippy::too_many_arguments,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::too_many_lines,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

mod consts;

use crate::consts::{
    BASE, DATA_LABEL, JSONB_AGG, JSONB_BUILD_ARRAY, JSONB_BUILD_OBJECT, ON, QUOTE_CHAR, ROOT_LABEL,
    TO_JSONB,
};
use anyhow::anyhow;
use async_graphql_parser::{
    types::{
        Directive, DocumentOperations, ExecutableDocument, Field, OperationType, Selection,
        VariableDefinition,
    },
    Positioned,
};
use async_graphql_value::{
    indexmap::{IndexMap, IndexSet},
    Name, Value as GqlValue,
};
use consts::{ID, TYPENAME};
use lazy_static::lazy_static;
use regex::Regex;
use sqlparser::ast::{
    Assignment, BinaryOperator, ConflictTarget, Cte, DataType, Delete, DoUpdate, Expr, FromTable,
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr,
    Ident, Insert, Join, JoinConstraint, JoinOperator, ObjectName, Offset, OffsetRows, OnConflict,
    OnConflictAction, OnInsert, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement,
    TableAlias, TableFactor, TableWithJoins, Value, Values, WildcardAdditionalOptions, With,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::{
    fmt::{Debug, Formatter},
    iter::zip,
};

type JsonValue = serde_json::Value;
type AnyResult<T> = anyhow::Result<T>;

#[must_use]
pub fn detect_date(text: &str) -> Option<String> {
    lazy_static! {
        static ref RE: Regex = Regex::new(
            r"^((?:(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d+)?))(Z|[\+-]\d{2}:\d{2})?)$"
        )
        .expect("Failed to compile regex");
    }
    if RE.is_match(text) {
        if text.contains('Z')
            || text.contains('+')
            || text.chars().nth_back(5).unwrap_or('T') == '-'
        {
            return Some(text.to_owned());
        } else if text.contains('.') {
            let date_str = text.to_owned() + "Z";
            return Some(date_str);
        }
        let date_str = text.to_owned() + ".000Z";
        return Some(date_str);
    }
    None
}

fn value_to_type(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => String::new(),
        JsonValue::Bool(_) => "::boolean".to_owned(),
        JsonValue::Number(_) => "::numeric".to_owned(),
        JsonValue::String(s) => {
            if detect_date(s).is_some() {
                "::timestamptz".to_owned()
            } else {
                "::text".to_owned()
            }
        }
        JsonValue::Array(_) | JsonValue::Object(_) => "::jsonb".to_owned(),
    }
}

fn get_value<'a>(
    value: &'a GqlValue,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
) -> AnyResult<Expr> {
    match value {
        GqlValue::Variable(v) => {
            if sql_vars.contains_key(v) {
                let var_value = sql_vars
                    .get(v)
                    .expect("variable not found, gaurded by contains");
                if let JsonValue::Null = var_value {
                    return Ok(Expr::Value(Value::Null));
                }
                let param_cast = value_to_type(var_value);
                let (i, _) = final_vars.insert_full(v.clone());
                return Ok(Expr::Value(Value::Placeholder(format!(
                    "${}{param_cast}",
                    i + 1,
                ))));
            }
            Ok(Expr::Value(Value::Null))
        }
        GqlValue::Null => Ok(Expr::Value(Value::Null)),
        GqlValue::String(s) => Ok(Expr::Value(Value::SingleQuotedString(s.clone()))),
        GqlValue::Number(f) => Ok(Expr::Value(Value::Number(f.to_string(), false))),
        GqlValue::Boolean(b) => Ok(Expr::Value(Value::Boolean(b.to_owned()))),
        GqlValue::Enum(e) => Ok(Expr::Value(Value::SingleQuotedString(e.as_ref().into()))),
        GqlValue::Binary(_b) => Err(anyhow!("binary not supported")),
        GqlValue::List(l) => Ok(Expr::Function(Function {
            within_group: vec![],
            name: ObjectName(vec![Ident::new(JSONB_BUILD_ARRAY)]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                clauses: vec![],
                args: l
                    .iter()
                    .map(|v| {
                        let value = get_value(v, sql_vars, final_vars).unwrap();
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(value))
                    })
                    .collect::<Vec<FunctionArg>>(),
            }),
            over: None,
            filter: None,
            null_treatment: None,
        })),
        GqlValue::Object(o) => {
            if o.contains_key("_parentRef") {
                if let Some(GqlValue::String(s)) = o.get("_parentRef") {
                    return Ok(Expr::CompoundIdentifier(vec![
                        Ident::with_quote(QUOTE_CHAR, BASE.to_owned()),
                        Ident::with_quote(QUOTE_CHAR, s),
                    ]));
                }
            }
            Ok(Expr::Function(Function {
                within_group: vec![],
                name: ObjectName(vec![Ident::new(JSONB_BUILD_OBJECT)]),
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    clauses: vec![],
                    args: o
                        .into_iter()
                        .flat_map(|(k, v)| {
                            let value = get_value(v, sql_vars, final_vars).unwrap();
                            vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                    Value::SingleQuotedString(k.to_string()),
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(value)),
                            ]
                        })
                        .collect::<Vec<FunctionArg>>(),
                }),
                over: None,
                filter: None,
                null_treatment: None,
            }))
        }
    }
}

fn get_logical_operator(op: &str) -> AnyResult<BinaryOperator> {
    let value = match op {
        "AND" => BinaryOperator::And,
        "OR" => BinaryOperator::Or,
        _ => {
            return Err(anyhow!("logical operator not supported: {}", op));
        }
    };
    Ok(value)
}

fn get_op(op: &str) -> BinaryOperator {
    match op {
        "eq" | "equals" => BinaryOperator::Eq,
        "neq" | "not_equals" => BinaryOperator::NotEq,
        "lt" | "less_than" => BinaryOperator::Lt,
        "lte" | "less_than_or_equals" => BinaryOperator::LtEq,
        "gt" | "greater_than" => BinaryOperator::Gt,
        "gte" | "greater_than_or_equals" => BinaryOperator::GtEq,
        _ => BinaryOperator::Custom(op.to_owned()),
    }
}

fn get_expr<'a>(
    left: Expr,
    operator: &'a str,
    value: &'a GqlValue,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
) -> AnyResult<Option<Expr>> {
    match operator {
        "like" => Ok(Some(Expr::Like {
            negated: false,
            expr: Box::new(left),
            pattern: Box::new(get_value(value, sql_vars, final_vars)?),
            escape_char: None,
        })),
        "ilike" => Ok(Some(Expr::ILike {
            negated: false,
            expr: Box::new(left),
            pattern: Box::new(get_value(value, sql_vars, final_vars)?),
            escape_char: None,
        })),
        "null" => Ok(Some(Expr::IsNull(Box::new(left)))),
        "not_null" => Ok(Some(Expr::IsNotNull(Box::new(left)))),
        "in" => {
            let list: Result<Vec<_>, _> = if let GqlValue::List(v) = value {
                v.into_iter()
                    .map(|v| get_value(v, sql_vars, final_vars))
                    .collect()
            } else {
                Ok(vec![get_value(value, sql_vars, final_vars)?])
            };
            let list = list?;
            if list.is_empty() {
                return Ok(Some(Expr::Value(Value::Boolean(false))));
            }
            Ok(Some(Expr::InList {
                expr: Box::new(left),
                list,
                negated: false,
            }))
        }
        "not_in" => {
            let list: Result<Vec<_>, _> = if let GqlValue::List(v) = value {
                v.into_iter()
                    .map(|v| get_value(v, sql_vars, final_vars))
                    .collect()
            } else {
                Ok(vec![get_value(value, sql_vars, final_vars)?])
            };
            let list = list?;
            if list.is_empty() {
                return Ok(Some(Expr::Value(Value::Boolean(true))));
            }
            Ok(Some(Expr::InList {
                expr: Box::new(left),
                list,
                negated: true,
            }))
        }
        _ => {
            let mut right_value = get_value(value, sql_vars, final_vars)?;
            let op = get_op(operator);
            if let Expr::Value(Value::Null) = right_value {
                if op == BinaryOperator::Eq {
                    return Ok(Some(Expr::IsNull(Box::new(left))));
                } else if op == BinaryOperator::NotEq {
                    return Ok(Some(Expr::IsNotNull(Box::new(left))));
                }
            }
            if op == BinaryOperator::NotEq {
                right_value = Expr::BinaryOp {
                    left: Box::new(right_value),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::IsNull(Box::new(left.clone()))),
                };
            }
            Ok(Some(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right_value),
            }))
        }
    }
}

fn get_string_or_variable(
    value: &GqlValue,
    variables: &IndexMap<Name, JsonValue>,
) -> AnyResult<String> {
    match value {
        GqlValue::Variable(v) => {
            if let Some(JsonValue::String(s)) = variables.get(v) {
                Ok(s.clone())
            } else {
                Err(anyhow!("variable not found"))
            }
        }
        GqlValue::String(s) => Ok(s.clone()),
        _ => Err(anyhow!("value not supported")),
    }
}

fn get_filter(
    args: &IndexMap<Name, GqlValue>,
    sql_vars: &mut IndexMap<Name, JsonValue>,
    final_vars: &mut IndexSet<Name>,
) -> AnyResult<(Option<Expr>, Option<IndexSet<Tag>>)> {
    let mut tags = IndexSet::new();
    let field = args
        .get("field")
        .map(|v| get_string_or_variable(v, sql_vars))
        .ok_or(anyhow!("field not found"))??;
    let operator = args
        .get("operator")
        .map(|v| get_string_or_variable(v, sql_vars))
        .ok_or(anyhow!("operator not found"))??;
    let ignore_null = args.get("ignoreEmpty").is_some_and(|v| match v {
        GqlValue::Boolean(b) => *b,
        GqlValue::Variable(v) => match sql_vars.get(v) {
            Some(JsonValue::Bool(b)) => *b,
            _ => false,
        },
        _ => false,
    });

    let value = args.get("value").unwrap_or_else(|| &GqlValue::Null);
    if operator == "eq" {
        if let Ok(value) = get_string_or_variable(value, sql_vars) {
            tags.insert(Tag {
                key: field.clone(),
                value: Some(value),
            });
        }
    }
    let left = Expr::Identifier(Ident {
        value: field,
        quote_style: Some(QUOTE_CHAR),
    });
    let primary = if ignore_null && !should_add_filter(value, sql_vars) {
        None
    } else {
        get_expr(left, operator.as_str(), value, sql_vars, final_vars)?
    };
    if args.contains_key("children") {
        if let Some(GqlValue::List(children)) = args.get("children") {
            let op = if let Some(val) = args.get("logicalOperator") {
                let op_name = get_string_or_variable(val, sql_vars)?;
                get_logical_operator(op_name.to_uppercase().as_str())?
            } else {
                BinaryOperator::And
            };
            if let Some(filters) = children
                .iter()
                .map(|v| match v {
                    GqlValue::Object(o) => {
                        if let Ok((item, new_tags)) = get_filter(o, sql_vars, final_vars) {
                            if let Some(new_tags) = new_tags {
                                tags.extend(new_tags);
                            }
                            return item;
                        }
                        None
                    }
                    _ => None,
                })
                .fold(primary, |acc: Option<Expr>, item| {
                    if let Some(acc) = acc {
                        let item = item.unwrap_or_else(|| Expr::Value(Value::Boolean(true)));
                        let expr = Expr::BinaryOp {
                            left: Box::new(acc),
                            op: op.clone(),
                            right: Box::new(item),
                        };
                        Some(expr)
                    } else {
                        None
                    }
                })
            {
                if tags.is_empty() {
                    return Ok((Some(Expr::Nested(Box::new(filters))), None));
                }
                return Ok((Some(Expr::Nested(Box::new(filters))), Some(tags)));
            }
            return Ok((None, None));
        }
    } else if !tags.is_empty() {
        return Ok((primary, Some(tags)));
    } else {
        return Ok((primary, None));
    }
    Ok((None, None))
}

fn get_agg_query(
    aggs: Vec<FunctionArg>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    alias: &str,
    group_by: Option<Vec<(String, Expr)>>,
) -> SetExpr {
    SetExpr::Select(Box::new(Select {
        window_before_qualify: false,
        connect_by: None,
        value_table_mode: None,
        distinct: None,
        named_window: vec![],
        top: None,
        into: None,
        projection: vec![SelectItem::ExprWithAlias {
            alias: Ident {
                value: alias.to_string(),
                quote_style: Some(QUOTE_CHAR),
            },
            expr: Expr::Function(Function {
                within_group: vec![],
                name: ObjectName(vec![Ident {
                    value: JSONB_BUILD_OBJECT.to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    clauses: vec![],
                    args: aggs,
                }),
                over: None,
                filter: None,
                null_treatment: None,
            }),
        }],
        from,
        lateral_views: vec![],
        selection,
        group_by: GroupByExpr::Expressions(
            group_by
                .unwrap_or_else(|| vec![])
                .into_iter()
                .map(|(_, expr)| expr)
                .collect::<Vec<_>>(),
        ),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        qualify: None,
    }))
}

fn get_root_query(
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    merges: &[Merge],
    is_single: bool,
    alias: &str,
) -> SetExpr {
    let mut base = Expr::Function(Function {
        within_group: vec![],
        name: ObjectName(vec![Ident {
            value: TO_JSONB.to_string(),
            quote_style: None,
        }]),
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            clauses: vec![],
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(
                Box::new(Query {
                    for_clause: None,
                    limit_by: vec![],
                    with: None,
                    body: Box::new(SetExpr::Select(Box::new(Select {
                        window_before_qualify: false,
                        connect_by: None,
                        value_table_mode: None,
                        distinct: None,
                        named_window: vec![],
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
                                    for_clause: None,
                                    limit_by: vec![],
                                    with: None,
                                    body: Box::new(SetExpr::Select(Box::new(Select {
                                        window_before_qualify: false,
                                        connect_by: None,
                                        value_table_mode: None,
                                        distinct: None,
                                        named_window: vec![],
                                        top: None,
                                        projection,
                                        into: None,
                                        from: vec![],
                                        lateral_views: vec![],
                                        selection: None,
                                        group_by: GroupByExpr::Expressions(vec![]),
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
                        group_by: GroupByExpr::Expressions(vec![]),
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
        }),
        over: None,
        filter: None,
        null_treatment: None,
    });
    if !merges.is_empty() {
        base = Expr::BinaryOp {
            left: Box::new(Expr::Cast {
                kind: sqlparser::ast::CastKind::Cast,
                format: None,
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
                    within_group: vec![],
                    name: ObjectName(vec![Ident {
                        value: "jsonb_build_object".to_string(),
                        quote_style: None,
                    }]),
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        clauses: vec![],
                        args: vec![],
                    }),
                    over: None,
                    filter: None,
                    null_treatment: None,
                }))),
            }),
        };
    }
    if !is_single {
        base = Expr::Function(Function {
            within_group: vec![],
            over: None,
            name: ObjectName(vec![Ident {
                value: "coalesce".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                clauses: vec![],
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                        within_group: vec![],
                        over: None,
                        name: ObjectName(vec![Ident {
                            value: JSONB_AGG.to_string(),
                            quote_style: None,
                        }]),
                        args: FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            clauses: vec![],
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(base))],
                        }),
                        filter: None,
                        null_treatment: None,
                    }))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        Value::SingleQuotedString("[]".to_string()),
                    ))),
                ],
            }),
            filter: None,
            null_treatment: None,
        });
    }
    SetExpr::Select(Box::new(Select {
        window_before_qualify: false,
        connect_by: None,
        value_table_mode: None,
        distinct: None,
        named_window: vec![],
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
        lateral_views: vec![],
        selection,
        group_by: GroupByExpr::Expressions(vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        qualify: None,
    }))
}

fn get_agg_agg_projection(field: &Field, table_name: &str) -> Vec<FunctionArg> {
    let name = field.name.node.as_ref();
    match name {
        "__typename" => {
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(field.name.node.to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    within_group: vec![],
                    name: ObjectName(vec![Ident {
                        value: "MIN".to_string(),
                        quote_style: None,
                    }]),
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        clauses: vec![],
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(format!("{table_name}_Agg")),
                        )))],
                    }),
                    over: None,
                    filter: None,
                    null_treatment: None,
                }))),
            ]
        }
        "count" => {
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(field.name.node.to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    within_group: vec![],
                    name: ObjectName(vec![Ident {
                        value: name.to_uppercase(),
                        quote_style: None,
                    }]),
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        clauses: vec![],
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
                    }),
                    over: None,
                    filter: None,
                    null_treatment: None,
                }))),
            ]
        }
        "min" | "max" | "avg" | "sum" => {
            let projection = field
                .selection_set
                .node
                .items
                .iter()
                .flat_map(|arg| {
                    if let Selection::Field(field) = &arg.node {
                        let field = &field.node;
                        let field_name = field.name.node.as_ref();
                        match field_name {
                            "__typename" => {
                                vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                        Value::SingleQuotedString(field_name.to_string()),
                                    ))),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
                                        Function {
                                            within_group: vec![],
                                            name: ObjectName(vec![Ident {
                                                value: "MIN".to_string(),
                                                quote_style: None,
                                            }]),
                                            args: FunctionArguments::List(FunctionArgumentList {
                                                duplicate_treatment: None,
                                                clauses: vec![],
                                                args: vec![FunctionArg::Unnamed(
                                                    FunctionArgExpr::Expr(Expr::Value(
                                                        Value::SingleQuotedString(format!(
                                                            "{table_name}_AggCol"
                                                        )),
                                                    )),
                                                )],
                                            }),
                                            over: None,
                                            filter: None,
                                            null_treatment: None,
                                        },
                                    ))),
                                ]
                            }
                            _ => {
                                vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                        Value::SingleQuotedString(field_name.to_string()),
                                    ))),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
                                        Function {
                                            within_group: vec![],
                                            name: ObjectName(vec![Ident {
                                                value: name.to_uppercase(),
                                                quote_style: None,
                                            }]),
                                            args: FunctionArguments::List(FunctionArgumentList {
                                                duplicate_treatment: None,
                                                clauses: vec![],
                                                args: vec![FunctionArg::Unnamed(
                                                    FunctionArgExpr::Expr(Expr::Identifier(
                                                        Ident {
                                                            value: field_name.to_string(),
                                                            quote_style: Some(QUOTE_CHAR),
                                                        },
                                                    )),
                                                )],
                                            }),
                                            over: None,
                                            filter: None,
                                            null_treatment: None,
                                        },
                                    ))),
                                ]
                            }
                        }
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
                    within_group: vec![],
                    name: ObjectName(vec![Ident {
                        value: JSONB_BUILD_OBJECT.to_string(),
                        quote_style: None,
                    }]),
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        clauses: vec![],
                        args: projection,
                    }),
                    over: None,
                    filter: None,
                    null_treatment: None,
                }))),
            ]
        }
        _ => vec![],
    }
}

fn get_aggregate_projection<'a>(
    items: &'a Vec<Positioned<Selection>>,
    table_name: &'a str,
    group_by: Option<Vec<(String, Expr)>>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
    tags: &mut IndexMap<String, IndexSet<Tag>>,
) -> AnyResult<Vec<FunctionArg>> {
    let mut aggs = if group_by.is_some() {
        let value = items.iter().find_map(|s| {
            if let Selection::Field(f) = &s.node {
                if f.node.name.node.as_ref() == "value" {
                    Some(&f.node)
                } else {
                    None
                }
            } else {
                None
            }
        });
        if let Some(value) = &value {
            vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString("value".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
            within_group: vec![],
                    name: ObjectName(vec![Ident {
                        value: JSONB_BUILD_OBJECT.to_owned(),
                        quote_style: None,
                    }]),
                    args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    clauses: vec![],
                    args: value
                        .selection_set
                        .node
                        .items
                        .iter()
                        .flat_map(|ss| {
                            if let Selection::Field(field) = &ss.node {
                                let name = field.node.name.node.as_ref().to_string();

                                let this_group = group_by
                                    .clone()
                                    .unwrap_or_else(|| vec![])
                                    .into_iter()
                                    .find(|(key, _expr)| key == &name);
                                if this_group.is_none() {
                                    return Ok::<Vec<FunctionArg>, anyhow::Error>(vec![]);
                                }
                                let (group_key, _group_expr) = this_group.unwrap();
                                if field.node.directives.is_empty() {
                                    Ok(vec![
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                            Value::SingleQuotedString(name.clone()),
                                        ))),
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                            Expr::Identifier(Ident {
                                                value: name,
                                                quote_style: Some(QUOTE_CHAR),
                                            }),
                                        )),
                                    ])
                                } else {
                                    let (
                                        relation,
                                        _fks,
                                        _pks,
                                        _is_single,
                                        _is_aggregate,
                                        _is_many,
                                        _schema_name,
                                    ) = get_relation(&field.node.directives, sql_vars, final_vars)?;
                                    let (projection, joins, _merges) = get_projection(
                                        &field.node.selection_set.node.items,
                                        &relation,
                                        None,
                                        variables,
                                        sql_vars,
                                        final_vars,
                                        tags,
                                    )?;

                                    let query = SetExpr::Select(Box::new(Select {
        window_before_qualify: false,
        connect_by: None,
                                        value_table_mode: None,
                                        distinct: None,
                                        named_window: vec![],
                                        top: None,
                                        projection,
                                        into: None,
                                        from: vec![TableWithJoins {
                                            relation: TableFactor::Derived {
                                                lateral: false,
                                                subquery: Box::new(Query {
                                                    with: None,
                                                    body: Box::new(SetExpr::Select(Box::new(
                                                        Select {
        window_before_qualify: false,
        connect_by: None,
                                                            distinct: None,
                                                            top: None,
                                                            projection: vec![SelectItem::Wildcard(
                                                                WildcardAdditionalOptions {
                                                                    opt_ilike: None,
                                                                    opt_exclude: None,
                                                                    opt_except: None,
                                                                    opt_rename: None,
                                                                    opt_replace: None,
                                                                },
                                                            )],
                                                            into: None,
                                                            from: vec![TableWithJoins {
                                                                relation: TableFactor::Table {
                                                                    name: ObjectName(vec![Ident {
                                                                        value: relation.to_string(),
                                                                        quote_style: Some(
                                                                            QUOTE_CHAR,
                                                                        ),
                                                                    }]),
                                                                    alias: None,
                                                                    args: None,
                                                                    with_hints: vec![],
                                                                    version: None,
                                                                    partitions: vec![],
                                                                },
                                                                joins: vec![],
                                                            }],
                                                            lateral_views: vec![],
                                                            selection: Some(Expr::BinaryOp {
                                                                left: Box::new(Expr::Identifier(
                                                                    Ident {
                                                                        value: "id".to_string(),
                                                                        quote_style: Some(
                                                                            QUOTE_CHAR,
                                                                        ),
                                                                    },
                                                                )),
                                                                op: BinaryOperator::Eq,
                                                                right: Box::new(Expr::Identifier(
                                                                    Ident {
                                                                        value: group_key,
                                                                        quote_style: Some(
                                                                            QUOTE_CHAR,
                                                                        ),
                                                                    },
                                                                )),
                                                            }),
                                                            group_by: GroupByExpr::Expressions(
                                                                vec![],
                                                            ),
                                                            cluster_by: vec![],
                                                            distribute_by: vec![],
                                                            sort_by: vec![],
                                                            having: None,
                                                            named_window: vec![],
                                                            qualify: None,
                                                            value_table_mode: None,
                                                        },
                                                    ))),
                                                    order_by: vec![],
                                                    limit: None,
                                                    limit_by: vec![],
                                                    offset: None,
                                                    fetch: None,
                                                    locks: vec![],
                                                    for_clause: None,
                                                }),
                                                alias: Some(TableAlias {
                                                    name: Ident {
                                                        value: "AGG".to_string(),
                                                        quote_style: Some(QUOTE_CHAR),
                                                    },
                                                    columns: vec![],
                                                }),
                                            },
                                            joins,
                                        }],
                                        lateral_views: vec![],
                                        selection: None,
                                        group_by: GroupByExpr::Expressions(vec![]),
                                        cluster_by: vec![],
                                        distribute_by: vec![],
                                        sort_by: vec![],
                                        having: None,
                                        qualify: None,
                                    }));

                                    Ok(vec![
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                            Value::SingleQuotedString(name),
                                        ))),
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                            Expr::Function(Function {
            within_group: vec![],
                                                name: ObjectName(vec![Ident {
                                                    value: TO_JSONB.to_owned(),
                                                    quote_style: None,
                                                }]),
                                                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    clauses: vec![],
                    args: vec![FunctionArg::Unnamed(
                                                    FunctionArgExpr::Expr(Expr::Subquery(
                                                        Box::new(Query {
                                                            with: None,
                                                            body: Box::new(SetExpr::Select(
                                                                Box::new(Select {
        window_before_qualify: false,
        connect_by: None,
                                                                    distinct: None,
                                                                    top: None,
                                                                    projection: vec![SelectItem::UnnamedExpr(Expr::Value(Value::DoubleQuotedString(BASE.to_string())))],
                                                                    into: None,
                                                                    from: vec![TableWithJoins {
                                                                        relation: TableFactor::Derived { lateral: false, subquery: Box::new(Query {
                                                                            with: None, body: Box::new(query), order_by: vec![], limit: None, limit_by: vec![], offset: None, fetch: None, locks: vec![], for_clause: None
                                                                        }), alias: Some(TableAlias { name: Ident { value: BASE.to_string(), quote_style: Some(QUOTE_CHAR) }, columns: vec![] }) },
                                                                        joins: vec![],
                                                                    }],
                                                                    lateral_views: vec![],
                                                                    selection: None,
                                                                    group_by: GroupByExpr::Expressions(vec![]),
                                                                    cluster_by: vec![],
                                                                    distribute_by: vec![],
                                                                    sort_by: vec![],
                                                                    having: None,
                                                                    named_window: vec![],
                                                                    qualify: None,
                                                                    value_table_mode: None,
                                                                }),
                                                            )),
                                                            order_by: vec![],
                                                            limit: None,
                                                            limit_by: vec![],
                                                            offset: None,
                                                            fetch: None,
                                                            locks: vec![],
                                                            for_clause: None,
                                                        }),
                                                    )),
                                                )],
                                                }),
                                                filter: None,
                                                null_treatment: None,
                                                over: None,
                                            }),
                                        )),
                                    ])
                                }
                            } else {
                                Ok(vec![])
                            }
                        })
                        .flatten()
                        .collect::<Vec<_>>(),
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                }))),
            ]
        } else {
            vec![]
        }
    } else {
        vec![]
    };
    // let mut aggs = vec![];
    for selection in items {
        match &selection.node {
            Selection::Field(field) => {
                if field.node.name.node.as_ref() == "value" {
                    continue;
                }
                aggs.extend(get_agg_agg_projection(&field.node, table_name));
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
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    directives: &'a [Positioned<Directive>],
    selection_items: &'a Vec<Positioned<Selection>>,
    path: Option<&'a str>,
    name: &'a str,
    kind: &'a str,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
    parent: &'a str,
    tags: &'a mut IndexMap<String, IndexSet<Tag>>,
) -> AnyResult<Join> {
    let (selection, distinct, distinct_order, order_by, mut first, after, keys, group_by) =
        parse_args(arguments, variables, sql_vars, final_vars)?;
    let (relation, fks, pks, is_single, is_aggregate, is_many, schema_name) =
        get_relation(directives, sql_vars, final_vars)?;
    if is_single {
        first = Some(Expr::Value(Value::Number("1".to_string(), false)));
    }
    if let Some(keys) = keys {
        tags.insert(relation.clone(), keys.into_iter().collect());
    } else {
        tags.insert(relation.clone(), IndexSet::new());
    };

    let table_name = schema_name.as_ref().map_or_else(
        || {
            ObjectName(vec![Ident {
                value: relation.to_string(),
                quote_style: Some(QUOTE_CHAR),
            }])
        },
        |schema_name| {
            ObjectName(vec![
                Ident {
                    value: schema_name.clone(),
                    quote_style: Some(QUOTE_CHAR),
                },
                Ident {
                    value: relation.to_string(),
                    quote_style: Some(QUOTE_CHAR),
                },
            ])
        },
    );

    let sub_path = path.map_or_else(|| relation.to_string(), |v| format!("{v}.{relation}"));
    let mut additional_select_items = vec![];
    let mut join_name = None;
    if is_many {
        let (a, b) = if relation.as_str() < parent {
            (relation.as_str(), parent)
        } else {
            (parent, relation.as_str())
        };
        join_name = Some(format!("_{a}To{b}"));
    }
    let join_filter = join_name.as_ref().map_or_else(
        || {
            zip(pks, fks)
                .map(|(pk, fk)| {
                    additional_select_items.push(SelectItem::UnnamedExpr(
                        Expr::CompoundIdentifier(vec![
                            Ident {
                                value: sub_path.to_string(),
                                quote_style: Some(QUOTE_CHAR),
                            },
                            Ident {
                                value: fk.clone(),
                                quote_style: Some(QUOTE_CHAR),
                            },
                        ]),
                    ));
                    let mut new_tags = IndexSet::new();
                    if let Some(table_tags) = tags.get(parent) {
                        for tag in table_tags {
                            if tag.key == pk {
                                new_tags.insert(Tag {
                                    key: fk.clone(),
                                    value: tag.value.clone(),
                                });
                            } else if tag.key == fk {
                                new_tags.insert(Tag {
                                    key: pk.clone(),
                                    value: tag.value.clone(),
                                });
                            } else {
                                new_tags.insert(Tag {
                                    key: pk.clone(),
                                    value: None,
                                });
                            }
                        }
                    } else {
                        new_tags.insert(Tag {
                            key: pk.clone(),
                            value: None,
                        });
                    }
                    if let Some(v) = tags.get_mut(name) {
                        v.extend(new_tags);
                    } else {
                        tags.insert(relation.clone(), new_tags);
                    };
                    let mut identifier = vec![
                        Ident {
                            value: relation.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                        Ident {
                            value: fk,
                            quote_style: Some(QUOTE_CHAR),
                        },
                    ];
                    if let Some(schema_name) = schema_name.as_ref() {
                        identifier.insert(
                            0,
                            Ident {
                                value: schema_name.clone(),
                                quote_style: Some(QUOTE_CHAR),
                            },
                        );
                    }
                    Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(identifier)),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            Ident {
                                value: path
                                    .map_or(BASE.to_string(), std::string::ToString::to_string),
                                quote_style: Some(QUOTE_CHAR),
                            },
                            Ident {
                                value: pk,
                                quote_style: Some(QUOTE_CHAR),
                            },
                        ])),
                    }
                })
                .reduce(|acc, expr| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: BinaryOperator::And,
                    right: Box::new(expr),
                })
        },
        |join_name| {
            let (join_col, value_col) = if relation.as_str() < parent {
                ("A", "B")
            } else {
                ("B", "A")
            };
            Some(Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![
                        Ident {
                            value: join_name.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                        Ident {
                            value: join_col.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                    ])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::CompoundIdentifier(vec![
                        Ident {
                            value: relation.clone(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                        Ident {
                            value: "id".to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                    ])),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::CompoundIdentifier(vec![
                        Ident {
                            value: join_name.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                        Ident {
                            value: value_col.to_string(),
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
                            value: "id".to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                    ])),
                }),
            })
        },
    );

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
        join_name.map_or_else(
            || vec![table_name.clone()],
            |name| {
                vec![
                    table_name.clone(),
                    ObjectName(vec![Ident {
                        value: name,
                        quote_style: Some(QUOTE_CHAR),
                    }]),
                ]
            },
        ),
        distinct,
        distinct_order,
    );
    if is_aggregate {
        let aggs = get_aggregate_projection(
            selection_items,
            kind,
            group_by.clone(),
            variables,
            sql_vars,
            final_vars,
            tags,
        )?;
        Ok(Join {
            relation: TableFactor::Derived {
                lateral: true,
                subquery: Box::new(Query {
                    for_clause: None,
                    limit_by: vec![],
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
                        group_by,
                    )),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                    locks: vec![],
                }),
                alias: Some(TableAlias {
                    name: Ident {
                        value: format!("{name}.{relation}"),
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
            final_vars,
            tags,
        )?;
        additional_select_items.extend(sub_projection);
        Ok(Join {
            relation: TableFactor::Derived {
                lateral: true,
                subquery: Box::new(Query {
                    for_clause: None,
                    limit_by: vec![],
                    with: None,
                    body: Box::new(get_root_query(
                        additional_select_items,
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
                        value: format!("{name}.{relation}"),
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

fn parse_skip<'a>(directive: &'a Directive, sql_vars: &'a IndexMap<Name, JsonValue>) -> bool {
    if let Some((_, value_pos)) = directive.arguments.iter().find(|&arg| arg.0.node == "if") {
        let value = &value_pos.node;
        match value {
            GqlValue::Variable(v) => {
                if sql_vars.contains_key(v) {
                    let var_value = sql_vars
                        .get(v)
                        .expect("variable not found, gaurded by contains");
                    if let JsonValue::Bool(b) = var_value {
                        return *b;
                    }
                    return false;
                }
                return false;
            }
            GqlValue::Boolean(b) => {
                return *b;
            }
            _ => {
                return false;
            }
        }
    }
    false
}

fn has_skip<'a>(field: &'a Field, sql_vars: &'a IndexMap<Name, JsonValue>) -> bool {
    if let Some(directive) = field
        .directives
        .iter()
        .find(|&x| x.node.name.node == "skip")
    {
        return parse_skip(&directive.node, sql_vars);
    }
    false
}

fn get_projection<'a>(
    items: &'a Vec<Positioned<Selection>>,
    relation: &'a str,
    path: Option<&'a str>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
    tags: &mut IndexMap<String, IndexSet<Tag>>,
) -> AnyResult<(Vec<SelectItem>, Vec<Join>, Vec<Merge>)> {
    let mut projection = vec![];
    let mut joins = vec![];
    let mut merges = vec![];
    for selection in items {
        let selection = &selection.node;
        match selection {
            Selection::Field(field) => {
                let field = &field.node;
                if has_skip(field, sql_vars) {
                    continue;
                }
                if field.selection_set.node.items.is_empty() {
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
                } else if field.selection_set.node.items.len() == 1
                    && field.directives.is_empty()
                    && field.selection_set.node.items.first().map_or(false, |f| {
                        if let Selection::Field(f) = &f.node {
                            f.node.name.node.to_string() == ID.to_string()
                        } else {
                            false
                        }
                    })
                {
                    let name = field.name.node.to_string();
                    let alias = match &field.alias {
                        Some(alias) => alias.node.to_string(),
                        None => name.to_string(),
                    };
                    /*
                     * */
                    projection.push(SelectItem::ExprWithAlias {
                        expr: Expr::Case {
                            operand: None,
                            conditions: vec![Expr::IsNotNull(Box::new(Expr::Identifier(Ident {
                                value: name.to_string(),
                                quote_style: Some(QUOTE_CHAR),
                            })))],
                            results: vec![Expr::Function(Function {
                                within_group: vec![],
                                name: ObjectName(vec![Ident {
                                    value: JSONB_BUILD_OBJECT.to_string(),
                                    quote_style: None,
                                }]),
                                args: FunctionArguments::List(FunctionArgumentList {
                                    duplicate_treatment: None,
                                    clauses: vec![],
                                    args: vec![
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                            Value::SingleQuotedString(ID.to_string()),
                                        ))),
                                        FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                            Expr::Identifier(Ident {
                                                value: name.to_string(),
                                                quote_style: Some(QUOTE_CHAR),
                                            }),
                                        )),
                                    ],
                                }),
                                over: None,
                                filter: None,
                                null_treatment: None,
                            })],
                            else_result: Some(Box::new(Expr::Value(Value::Null))),
                        },
                        alias: Ident {
                            value: alias,
                            quote_style: Some(QUOTE_CHAR),
                        },
                    });
                } else {
                    let mut hasher = DefaultHasher::new();
                    let arg_bytes = serde_json::to_vec(&field.arguments)?;
                    hasher.write(&arg_bytes);
                    let hash_str = format!("{:x}", hasher.finish());
                    let kind = field.name.node.as_ref();
                    let name = format!("join.{}.{}", kind, &hash_str[..13]);
                    let join = get_join(
                        &field.arguments,
                        &field.directives,
                        &field.selection_set.node.items,
                        path,
                        &name,
                        kind,
                        variables,
                        sql_vars,
                        final_vars,
                        relation,
                        tags,
                    )?;
                    joins.push(join);
                    match &field.alias {
                        Some(alias) => {
                            projection.push(SelectItem::ExprWithAlias {
                                expr: Expr::Identifier(Ident {
                                    value: name,
                                    quote_style: Some(QUOTE_CHAR),
                                }),
                                alias: Ident {
                                    value: alias.node.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
                                },
                            });
                        }
                        None => {
                            projection.push(SelectItem::ExprWithAlias {
                                expr: Expr::Identifier(Ident {
                                    value: name,
                                    quote_style: Some(QUOTE_CHAR),
                                }),
                                alias: Ident {
                                    value: field.name.node.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
                                },
                            });
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
                    let (relation, _fks, _pks, _is_single, _is_aggregate, _is_many, schema_name) =
                        get_relation(&frag.directives, sql_vars, final_vars)?;
                    let join = get_join(
                        args.map_or(&vec![], |dir| &dir.node.arguments),
                        &frag.directives,
                        &frag.selection_set.node.items,
                        path,
                        name,
                        &relation,
                        variables,
                        sql_vars,
                        final_vars,
                        &relation,
                        tags,
                    )?;
                    joins.push(join);
                    let table_name = schema_name.map_or_else(
                        || relation.to_string(),
                        |schema_name| schema_name + "." + &relation,
                    );
                    merges.push(Merge {
                        expr: Expr::Function(Function {
                            within_group: vec![],
                            name: ObjectName(vec![Ident {
                                value: TO_JSONB.to_string(),
                                quote_style: None,
                            }]),
                            args: FunctionArguments::List(FunctionArgumentList {
                                duplicate_treatment: None,
                                clauses: vec![],
                                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(Ident {
                                        value: name.to_string(),
                                        quote_style: Some(QUOTE_CHAR),
                                    }),
                                ))],
                            }),
                            over: None,
                            filter: None,
                            null_treatment: None,
                        }),
                        condition: Expr::IsNotNull(Box::new(Expr::CompoundIdentifier(vec![
                            Ident {
                                value: format!("{name}.{relation}"),
                                quote_style: Some(QUOTE_CHAR),
                            },
                            Ident {
                                value: table_name,
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
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
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
                match value {
                    JsonValue::String(s) => s.clone(),
                    _ => value.to_string(),
                }
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
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    _final_vars: &'a IndexSet<Name>,
) -> AnyResult<(
    String,
    Vec<String>,
    Vec<String>,
    bool,
    bool,
    bool,
    Option<String>,
)> {
    let mut relation: String = String::new();
    let mut fk = vec![];
    let mut pk = vec![];
    let mut is_single = false;
    let mut is_aggregate = false;
    let mut is_many = false;
    let mut schema_name = None;
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
                    "schema" => schema_name = Some(value_to_string(value, sql_vars)?),
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
                    "many" => {
                        if let GqlValue::Boolean(b) = value {
                            is_many = *b;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok((
        relation,
        fk,
        pk,
        is_single,
        is_aggregate,
        is_many,
        schema_name,
    ))
}

fn get_filter_query(
    selection: Option<Expr>,
    order_by: Vec<OrderByExpr>,
    first: Option<Expr>,
    after: Option<Offset>,
    table_names: Vec<ObjectName>,
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
        for_clause: None,
        limit_by: vec![],
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            window_before_qualify: false,
            connect_by: None,
            value_table_mode: None,
            distinct: if is_distinct {
                Some(sqlparser::ast::Distinct::Distinct)
            } else {
                None
            },
            named_window: vec![],
            top: None,
            projection,
            into: None,
            from: table_names
                .into_iter()
                .map(|table_name| TableWithJoins {
                    relation: TableFactor::Table {
                        partitions: vec![],
                        version: None,
                        name: table_name,
                        alias: None,
                        args: None,
                        with_hints: vec![],
                    },
                    joins: vec![],
                })
                .collect(),
            lateral_views: vec![],
            selection: selection.map(|s| {
                if let Expr::Nested(nested) = s {
                    *nested
                } else {
                    s
                }
            }),
            group_by: GroupByExpr::Expressions(vec![]),
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
            for_clause: None,
            limit_by: vec![],
            with: None,
            body: Box::new(SetExpr::Select(Box::new(Select {
                window_before_qualify: false,
                connect_by: None,
                value_table_mode: None,
                distinct: None,
                named_window: vec![],
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
                group_by: GroupByExpr::Expressions(vec![]),
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
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
) -> AnyResult<Vec<OrderByExpr>> {
    if order.contains_key("field") && order.contains_key("direction") {
        let direction =
            value_to_string(order.get("direction").unwrap_or(&GqlValue::Null), sql_vars)?;
        let field = value_to_string(order.get("field").unwrap_or(&GqlValue::Null), sql_vars)?;
        return Ok(vec![OrderByExpr {
            expr: Expr::Identifier(Ident {
                value: field.clone(),
                quote_style: Some(QUOTE_CHAR),
            }),
            asc: Some(direction == "ASC"),
            nulls_first: None,
        }]);
    } else if order.contains_key("expr") && order.contains_key("dir") {
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
                    if let (Some(expression), _) = get_filter(args, sql_vars, final_vars)? {
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
    for (key, mut value) in order {
        if let GqlValue::Variable(name) = value {
            if let Some(new_value) = variables.get(name) {
                value = new_value;
            }
        }
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
                if let JsonValue::String(value) = sql_vars.get(name).unwrap_or(&JsonValue::Null) {
                    order_by.push(OrderByExpr {
                        expr: Expr::Identifier(Ident {
                            value: key.as_str().to_owned(),
                            quote_style: Some(QUOTE_CHAR),
                        }),
                        asc: Some(value == "ASC"),
                        nulls_first: None,
                    });
                }
            }
            _ => return Err(anyhow!("Invalid value for order expression")),
        }
    }
    Ok(order_by)
}

fn get_distinct(
    distinct: &[GqlValue],
    variables: &IndexMap<Name, JsonValue>,
) -> Option<Vec<String>> {
    let values: Vec<String> = distinct
        .iter()
        .filter_map(|v| get_string_or_variable(v, variables).ok())
        .collect();

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn flatten(name: Name, value: &JsonValue, sql_vars: &mut IndexMap<Name, JsonValue>) -> GqlValue {
    match value {
        JsonValue::Null => GqlValue::Null,
        JsonValue::Bool(s) => {
            sql_vars.insert(name.clone(), JsonValue::Bool(*s));
            GqlValue::Variable(name)
        }
        JsonValue::Number(s) => {
            sql_vars.insert(name.clone(), JsonValue::Number(s.clone()));
            GqlValue::Variable(name)
        }
        JsonValue::String(s) => {
            if s == "ASC" || s == "DESC" {
                return GqlValue::Enum(Name::new(s.clone()));
            }
            sql_vars.insert(name.clone(), JsonValue::String(s.clone()));
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

fn flatten_variables(
    variables: &Option<JsonValue>,
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

fn should_add_filter<'a>(value: &'a GqlValue, sql_vars: &'a mut IndexMap<Name, JsonValue>) -> bool {
    match &value {
        GqlValue::Null => false,
        GqlValue::List(v) => !v.is_empty(),
        GqlValue::Variable(v) => {
            let val = sql_vars.get(v);
            match val {
                None => false,
                Some(JsonValue::Null) => false,
                Some(JsonValue::Array(v)) => !v.is_empty(),
                _ => true,
            }
        }
        _ => true,
    }
}

fn parse_args<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
) -> AnyResult<(
    Option<Expr>,
    Option<Vec<String>>,
    Option<Vec<OrderByExpr>>,
    Vec<OrderByExpr>,
    Option<Expr>,
    Option<Offset>,
    Option<IndexSet<Tag>>,
    Option<Vec<(String, Expr)>>,
)> {
    let mut selection = None;
    let mut order_by = vec![];
    let mut distinct = None;
    let mut distinct_order = None;
    let mut first = None;
    let mut after = None;
    let mut keys = None;
    let mut group_by = None;
    for argument in arguments {
        let (p_key, p_value) = argument;
        let key = p_key.node.as_str();
        let mut value = p_value.node.clone();
        if let GqlValue::Variable(ref name) = value {
            if let Some(new_value) = variables.get(name) {
                value = new_value.clone();
                if let GqlValue::Null = value {
                    if !["id", "email", "A", "B"].contains(&key) {
                        continue;
                    }
                }
            }
        }
        match (key, value) {
            ("id" | "email" | "A" | "B", value) => {
                let new_selection;
                if should_add_filter(&value, sql_vars) {
                    new_selection = get_expr(
                        Expr::Identifier(Ident {
                            value: key.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        }),
                        "eq",
                        &value,
                        sql_vars,
                        final_vars,
                    )?;
                } else {
                    new_selection = Some(Expr::Value(Value::Boolean(false)));
                }
                if selection.is_some() && new_selection.is_some() {
                    selection = Some(Expr::BinaryOp {
                        left: Box::new(selection.expect("gaurded by condition")),
                        op: BinaryOperator::And,
                        right: Box::new(new_selection.expect("gaurded by condition")),
                    });
                } else {
                    selection = new_selection;
                }
            }
            ("filter" | "where", GqlValue::Object(filter)) => {
                // keys = get_filter_key(&filter, sql_vars)?;
                (selection, keys) = get_filter(&filter, sql_vars, final_vars)?;
            }
            ("distinct", GqlValue::Object(d)) => {
                if let Some(GqlValue::List(list)) = d.get("on") {
                    distinct = get_distinct(list, &sql_vars);
                }
                match d.get("order") {
                    Some(GqlValue::Object(order)) => {
                        distinct_order = Some(get_order(order, variables, sql_vars, final_vars)?);
                    }
                    Some(GqlValue::List(list)) => {
                        let order = list
                            .iter()
                            .filter_map(|v| match v {
                                GqlValue::Object(o) => Some(o),
                                _ => None,
                            })
                            .map(|o| get_order(o, variables, sql_vars, final_vars))
                            .collect::<AnyResult<Vec<Vec<OrderByExpr>>>>()?;
                        distinct_order = Some(order.into_iter().flatten().collect());
                    }
                    _ => {
                        return Err(anyhow!("Invalid value for distinct order"));
                    }
                }
            }
            ("order", GqlValue::Object(order)) => {
                order_by = get_order(&order, variables, sql_vars, final_vars)?;
            }
            ("order", GqlValue::List(list)) => {
                let items = list
                    .iter()
                    .filter_map(|v| match v {
                        GqlValue::Object(o) => Some(o),
                        _ => None,
                    })
                    .map(|o| get_order(o, variables, sql_vars, final_vars))
                    .collect::<AnyResult<Vec<Vec<OrderByExpr>>>>()?;
                order_by.append(
                    items
                        .into_iter()
                        .flatten()
                        .collect::<Vec<OrderByExpr>>()
                        .as_mut(),
                );
            }
            ("first" | "limit", GqlValue::Variable(name)) => {
                first = Some(get_value(&GqlValue::Variable(name), sql_vars, final_vars)?);
            }
            ("first" | "limit", GqlValue::Number(count)) => {
                first = Some(Expr::Value(Value::Number(
                    count.as_i64().expect("int to be an i64").to_string(),
                    false,
                )));
            }
            ("after" | "offset", GqlValue::Variable(name)) => {
                after = Some(Offset {
                    value: get_value(&GqlValue::Variable(name), sql_vars, final_vars)?,
                    rows: OffsetRows::None,
                });
            }
            ("after" | "offset", GqlValue::Number(count)) => {
                after = Some(Offset {
                    value: Expr::Value(Value::Number(
                        count.as_i64().expect("int to be an i64").to_string(),
                        false,
                    )),
                    rows: OffsetRows::None,
                });
            }
            ("group_by" | "groupBy", GqlValue::List(list)) => {
                let items = list
                    .into_iter()
                    .filter_map(|v| {
                        get_string_or_variable(&v, &sql_vars)
                            .map(|v| (v.clone(), Expr::Value(Value::DoubleQuotedString(v))))
                            .ok()
                    })
                    .collect::<Vec<_>>();
                group_by = Some(items);
            }
            _ => {
                return Err(anyhow!("Invalid argument for: {}", key));
            }
        }
    }
    Ok((
        selection,
        distinct,
        distinct_order,
        order_by,
        first,
        after,
        keys,
        group_by,
    ))
}

fn get_mutation_columns<'a>(
    arguments: &'a Vec<(Positioned<Name>, Positioned<GqlValue>)>,
    variables: &'a IndexMap<Name, GqlValue>,
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
) -> AnyResult<(Vec<Ident>, Vec<Vec<Expr>>)> {
    let mut columns = vec![];
    let mut rows = vec![];
    for argument in arguments {
        let (key, value) = argument;
        let (key, mut value) = (&key.node, &value.node);
        if let GqlValue::Variable(name) = value {
            if let Some(new_value) = variables.get(name) {
                value = new_value;
                if let GqlValue::Null = value {
                    continue;
                }
            }
        }
        match (key.as_ref(), value) {
            ("data", GqlValue::Object(data)) => {
                let mut row = vec![];
                for (key, value) in data {
                    columns.push(Ident {
                        value: key.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    });
                    row.push(get_value(value, sql_vars, final_vars)?);
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
                        for (key, value) in data {
                            if i == 0 {
                                columns.push(Ident {
                                    value: key.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
                                });
                            }
                            row.push(get_value(value, sql_vars, final_vars)?);
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
    sql_vars: &'a mut IndexMap<Name, JsonValue>,
    final_vars: &'a mut IndexSet<Name>,
    has_updated_at_directive: bool,
) -> AnyResult<(Option<Expr>, Vec<Assignment>)> {
    let mut selection = None;
    let mut assignments = vec![];
    if has_updated_at_directive {
        assignments.push(Assignment {
            id: vec![Ident {
                value: "updated_at".to_string(),
                quote_style: Some(QUOTE_CHAR),
            }],
            value: Expr::Function(Function {
                within_group: vec![],
                name: ObjectName(vec![Ident {
                    value: "now".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    clauses: vec![],
                    args: vec![],
                }),
                over: None,
                filter: None,
                null_treatment: None,
            }),
        });
    }
    for argument in arguments {
        let (p_key, p_value) = argument;
        let (key, mut value) = (&p_key.node, &p_value.node);
        if let GqlValue::Variable(name) = value {
            if let Some(new_value) = variables.get(name) {
                value = new_value;
                if let GqlValue::Null = value {
                    continue;
                }
            }
        }
        match (key.as_ref(), value) {
            ("id" | "email" | "A" | "B", value) => {
                let new_selection = get_expr(
                    Expr::Identifier(Ident {
                        value: key.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    }),
                    "eq",
                    value,
                    sql_vars,
                    final_vars,
                )?;
                if selection.is_some() && new_selection.is_some() {
                    selection = Some(Expr::BinaryOp {
                        left: Box::new(selection.expect("gaurded by condition")),
                        op: BinaryOperator::And,
                        right: Box::new(new_selection.expect("gaurded by condition")),
                    });
                } else {
                    selection = new_selection;
                }
            }
            ("filter" | "where", GqlValue::Object(filter)) => {
                (selection, _) = get_filter(filter, sql_vars, final_vars)?;
            }
            ("set", GqlValue::Object(data)) => {
                for (key, value) in data {
                    assignments.push(Assignment {
                        id: vec![Ident {
                            value: key.to_string(),
                            quote_style: Some(QUOTE_CHAR),
                        }],
                        value: get_value(value, sql_vars, final_vars)?,
                    });
                }
            }
            ("inc" | "increment", GqlValue::Object(data)) => {
                for (key, value) in data {
                    let column_ident = Ident {
                        value: key.to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    };
                    assignments.push(Assignment {
                        id: vec![column_ident.clone()],
                        value: Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(column_ident)),
                            op: BinaryOperator::Plus,
                            right: Box::new(get_value(value, sql_vars, final_vars)?),
                        },
                    });
                }
            }
            _ => return Err(anyhow!("Invalid argument for update at: {}", key)),
        }
    }
    Ok((
        selection.or_else(|| Some(Expr::Value(Value::Boolean(false)))),
        assignments,
    ))
}

pub fn parse_query_meta(field: &Field) -> AnyResult<(&str, &str, bool, bool, Option<&str>)> {
    let mut is_aggregate = false;
    let mut is_single = false;
    let mut name = field.name.node.as_str();
    let mut schema_name = None;
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
            } else if arg_name == "schema" {
                if let GqlValue::String(schema) = &argument.node {
                    schema_name = Some(schema.as_ref());
                }
            }
        });
    }

    if is_aggregate && is_single {
        return Err(anyhow!("Query cannot be both aggregate and single"));
    }

    Ok((name, key, is_aggregate, is_single, schema_name))
}

pub fn parse_mutation_meta(
    field: &Field,
) -> AnyResult<(&str, &str, bool, bool, bool, bool, Option<&str>)> {
    let mut is_insert = false;
    let mut is_update = false;
    let mut is_delete = false;
    let mut is_single = false;
    let mut schema_name = None;
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
                if let GqlValue::Boolean(insert) = &argument.node {
                    is_insert = *insert;
                }
            } else if arg_name == "update" {
                if let GqlValue::Boolean(update) = &argument.node {
                    is_update = *update;
                }
            } else if arg_name == "delete" {
                if let GqlValue::Boolean(delete) = &argument.node {
                    is_delete = *delete;
                }
            } else if arg_name == "single" {
                if let GqlValue::Boolean(delete) = &argument.node {
                    is_single = *delete;
                }
            } else if arg_name == "schema" {
                if let GqlValue::String(schema) = &argument.node {
                    schema_name = Some(schema.as_ref());
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

    Ok((
        name,
        key,
        is_insert,
        is_update,
        is_delete,
        is_single,
        schema_name,
    ))
}

#[must_use]
pub fn wrap_mutation(key: &str, value: Statement, is_single: bool) -> Statement {
    let mut base = Expr::Function(Function {
        within_group: vec![],
        over: None,
        name: ObjectName(vec![Ident {
            value: "coalesce".to_string(),
            quote_style: None,
        }]),
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            clauses: vec![],
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                    within_group: vec![],
                    name: ObjectName(vec![Ident {
                        value: JSONB_AGG.to_string(),
                        quote_style: None,
                    }]),
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        clauses: vec![],
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Identifier(Ident {
                                value: "result".to_string(),
                                quote_style: Some(QUOTE_CHAR),
                            }),
                        ))],
                    }),
                    over: None,
                    filter: None,
                    null_treatment: None,
                }))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString("[]".to_string()),
                ))),
            ],
        }),
        filter: None,
        null_treatment: None,
    });
    if is_single {
        base = Expr::BinaryOp {
            left: Box::new(base),
            op: BinaryOperator::Custom("->".to_string()),
            right: Box::new(Expr::Value(Value::Number("0".to_string(), false))),
        }
    }
    Statement::Query(Box::new(Query {
        for_clause: None,
        limit_by: vec![],
        with: Some(With {
            cte_tables: vec![Cte {
                materialized: None,
                alias: TableAlias {
                    name: Ident {
                        value: "result".to_string(),
                        quote_style: Some(QUOTE_CHAR),
                    },
                    columns: vec![],
                },
                query: Box::new(Query {
                    for_clause: None,
                    limit_by: vec![],
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
            window_before_qualify: false,
            connect_by: None,
            value_table_mode: None,
            distinct: None,
            named_window: vec![],
            top: None,
            into: None,
            projection: vec![SelectItem::ExprWithAlias {
                expr: Expr::Function(Function {
                    within_group: vec![],
                    name: ObjectName(vec![Ident {
                        value: JSONB_BUILD_OBJECT.to_string(),
                        quote_style: None,
                    }]),
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        clauses: vec![],
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString(key.to_string()),
                            ))),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(Box::new(
                                Query {
                                    for_clause: None,
                                    limit_by: vec![],
                                    with: None,
                                    body: Box::new(SetExpr::Select(Box::new(Select {
                                        window_before_qualify: false,
                                        connect_by: None,
                                        value_table_mode: None,
                                        distinct: None,
                                        named_window: vec![],
                                        top: None,
                                        projection: vec![SelectItem::UnnamedExpr(base)],
                                        into: None,
                                        from: vec![TableWithJoins {
                                            relation: TableFactor::Table {
                                                partitions: vec![],
                                                version: None,
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
                                        lateral_views: vec![],
                                        selection: None,
                                        group_by: GroupByExpr::Expressions(vec![]),
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
                                },
                            )))),
                        ],
                    }),
                    over: None,
                    filter: None,
                    null_treatment: None,
                }),
                alias: Ident {
                    value: DATA_LABEL.to_string(),
                    quote_style: Some(QUOTE_CHAR),
                },
            }],
            from: vec![],
            lateral_views: vec![],
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
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
    }))
}

#[derive(PartialEq, Eq, Hash)]
struct Tag {
    key: String,
    value: Option<String>,
}

impl Debug for Tag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.value.is_some() {
            return write!(f, "{}:{}", self.key, self.value.as_ref().expect("is_some"));
        }
        write!(f, "{}", self.key)
    }
}

impl ToString for Tag {
    fn to_string(&self) -> String {
        if self.value.is_some() {
            return format!("{}:{}", self.key, self.value.as_ref().expect("is_some"));
        }
        self.key.clone()
    }
}

pub fn gql2sql(
    ast: ExecutableDocument,
    variables: &Option<JsonValue>,
    operation_name: Option<String>,
) -> AnyResult<(Statement, Option<Vec<JsonValue>>, Option<Vec<String>>, bool)> {
    let mut statements = vec![];
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

    let (variables, mut sql_vars) = flatten_variables(variables, operation.variable_definitions);
    let mut tags: IndexMap<String, IndexSet<Tag>> = IndexMap::new();
    let mut final_vars: IndexSet<Name> = IndexSet::new();

    match operation.ty {
        OperationType::Query => {
            for selection in &operation.selection_set.node.items {
                match &selection.node {
                    Selection::Field(p_field) => {
                        let field = &p_field.node;
                        if has_skip(field, &sql_vars) {
                            continue;
                        }
                        let (name, key, is_aggregate, is_single, schema_name) =
                            parse_query_meta(field)?;

                        let (
                            selection,
                            distinct,
                            distinct_order,
                            order_by,
                            mut first,
                            after,
                            keys,
                            group_by,
                        ) = parse_args(
                            &field.arguments,
                            &variables,
                            &mut sql_vars,
                            &mut final_vars,
                        )?;
                        if is_single {
                            first = Some(Expr::Value(Value::Number("1".to_string(), false)));
                        }
                        if let Some(keys) = keys {
                            tags.insert(name.to_string(), keys.into_iter().collect());
                        } else {
                            tags.insert(name.to_string(), IndexSet::new());
                        };
                        let table_name = schema_name.map_or_else(
                            || {
                                ObjectName(vec![Ident {
                                    value: name.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
                                }])
                            },
                            |schema_name| {
                                ObjectName(vec![
                                    Ident {
                                        value: schema_name.to_string(),
                                        quote_style: Some(QUOTE_CHAR),
                                    },
                                    Ident {
                                        value: name.to_string(),
                                        quote_style: Some(QUOTE_CHAR),
                                    },
                                ])
                            },
                        );
                        let base_query = get_filter_query(
                            selection,
                            order_by,
                            first,
                            after,
                            vec![table_name],
                            distinct,
                            distinct_order,
                        );
                        if is_aggregate {
                            let aggs = get_aggregate_projection(
                                &field.selection_set.node.items,
                                name,
                                group_by.clone(),
                                &variables,
                                &mut sql_vars,
                                &mut final_vars,
                                &mut tags,
                            )?;
                            let subquery = Query {
                                for_clause: None,
                                limit_by: vec![],
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
                                    group_by.clone(),
                                )),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                                fetch: None,
                                locks: vec![],
                            };
                            // TODO: Do I need to be deleted?
                            if group_by.is_some() {
                                // find-me
                                statements.push((
                                    key,
                                    Expr::Subquery(Box::new(Query {
                                        with: None,
                                        body: Box::new(SetExpr::Select(Box::new(Select {
                                            window_before_qualify: false,
                                            connect_by: None,
                                            distinct: None,
                                            top: None,
                                            projection: vec![SelectItem::UnnamedExpr(
                                                Expr::Function(Function {
                                                    within_group: vec![],
                                                    name: ObjectName(vec![Ident {
                                                        value: JSONB_AGG.to_owned(),
                                                        quote_style: None,
                                                    }]),
                                                    args: FunctionArguments::List(
                                                        FunctionArgumentList {
                                                            duplicate_treatment: None,
                                                            clauses: vec![],
                                                            args: vec![FunctionArg::Unnamed(
                                                                FunctionArgExpr::Expr(
                                                                    Expr::CompoundIdentifier(vec![
                                                                        Ident {
                                                                            value: "T".to_owned(),
                                                                            quote_style: Some(
                                                                                QUOTE_CHAR,
                                                                            ),
                                                                        },
                                                                        Ident {
                                                                            value: ROOT_LABEL
                                                                                .to_owned(),
                                                                            quote_style: Some(
                                                                                QUOTE_CHAR,
                                                                            ),
                                                                        },
                                                                    ]),
                                                                ),
                                                            )],
                                                        },
                                                    ),
                                                    filter: None,
                                                    null_treatment: None,
                                                    over: None,
                                                }),
                                            )],
                                            into: None,
                                            from: vec![TableWithJoins {
                                                relation: TableFactor::Derived {
                                                    lateral: false,
                                                    subquery: Box::new(subquery),
                                                    alias: Some(TableAlias {
                                                        name: Ident {
                                                            value: "T".to_owned(),
                                                            quote_style: Some(QUOTE_CHAR),
                                                        },
                                                        columns: vec![],
                                                    }),
                                                },
                                                joins: vec![],
                                            }],
                                            lateral_views: vec![],
                                            selection: None,
                                            group_by: GroupByExpr::Expressions(vec![]),
                                            cluster_by: vec![],
                                            distribute_by: vec![],
                                            sort_by: vec![],
                                            having: None,
                                            named_window: vec![],
                                            qualify: None,
                                            value_table_mode: None,
                                        }))),
                                        order_by: vec![],
                                        limit: None,
                                        limit_by: vec![],
                                        offset: None,
                                        fetch: None,
                                        locks: vec![],
                                        for_clause: None,
                                    })),
                                ));
                                // statements.push((
                                //     key,
                                //     Expr::Function(Function {
                                //         order_by: vec![],
                                //         name: ObjectName(vec![Ident {
                                //             value: JSONB_AGG.to_string(),
                                //             quote_style: None,
                                //         }]),
                                //         args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(

                                //             Expr::Function(Function {
                                //                 name: ObjectName(vec![Ident {
                                //                     value: TO_JSONB.to_string(),
                                //                     quote_style: None,
                                //                 }]),
                                //                 args: vec![FunctionArg::Unnamed(
                                //                     FunctionArgExpr::Expr(Expr::Subquery(
                                //                         Box::new(Query {
                                //                             body: Box::new(SetExpr::Select(
                                //                                 Box::new(Select {
                                //                                     distinct: None,
                                //                                     top: None,
                                //                                     projection: vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                                //                                         value: ROOT_LABEL.to_string(),
                                //                                         quote_style: Some(QUOTE_CHAR),
                                //                                     }))],
                                //                                     // find me
                                //                                     into: None,
                                //                                     from: vec![TableWithJoins {
                                //                                         relation: TableFactor::Derived { lateral: false, subquery: Box::new(subquery) , alias: Some(TableAlias { name: Ident { value: ROOT_LABEL.to_string(), quote_style: Some(QUOTE_CHAR) }, columns: vec![] }) },
                                //                                         joins: vec![],
                                //                                     }],
                                //                                     lateral_views: vec![],
                                //                                     selection: None,
                                //                                     group_by: GroupByExpr::Expressions(vec![]),
                                //                                     cluster_by: vec![],
                                //                                     distribute_by: vec![],
                                //                                     sort_by: vec![],
                                //                                     having: None,
                                //                                     named_window: vec![],
                                //                                     qualify: None,
                                //                                     value_table_mode: None,
                                //                                 }),
                                //                             )),
                                //                             for_clause: None,
                                //                             limit_by: vec![],
                                //                             with: None,
                                //                             order_by: vec![],
                                //                             limit: None,
                                //                             offset: None,
                                //                             fetch: None,
                                //                             locks: vec![],
                                //                         }),
                                //                     )),
                                //                 )],
                                //                 filter: None,
                                //                 null_treatment: None,
                                //                 over: None,
                                //                 distinct: false,
                                //                 special: false,
                                //                 order_by: vec![],
                                //             }),
                                //         ))],
                                //         over: None,
                                //         distinct: false,
                                //         special: false,
                                //         filter: None,
                                //         null_treatment: None,
                                //     }),
                                // ));
                            } else {
                                statements.push((key, Expr::Subquery(Box::new(subquery))));
                            }
                        } else {
                            let (projection, joins, merges) = get_projection(
                                &field.selection_set.node.items,
                                name,
                                Some(BASE),
                                &variables,
                                &mut sql_vars,
                                &mut final_vars,
                                &mut tags,
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
                                Expr::Subquery(Box::new(Query {
                                    for_clause: None,
                                    limit_by: vec![],
                                    with: None,
                                    body: Box::new(root_query),
                                    order_by: vec![],
                                    limit: None,
                                    offset: None,
                                    fetch: None,
                                    locks: vec![],
                                })),
                            ));
                        };
                    }
                    Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                        return Err(anyhow::anyhow!("Fragment not supported"))
                    }
                }
            }
            let statement = Statement::Query(Box::new(Query {
                for_clause: None,
                limit_by: vec![],
                with: None,
                body: Box::new(SetExpr::Select(Box::new(Select {
                    window_before_qualify: false,
                    connect_by: None,
                    value_table_mode: None,
                    distinct: None,
                    named_window: vec![],
                    top: None,
                    into: None,
                    projection: vec![SelectItem::ExprWithAlias {
                        alias: Ident {
                            value: DATA_LABEL.into(),
                            quote_style: Some(QUOTE_CHAR),
                        },
                        expr: Expr::Function(Function {
                            within_group: vec![],
                            name: ObjectName(vec![Ident {
                                value: JSONB_BUILD_OBJECT.to_string(),
                                quote_style: None,
                            }]),
                            args: FunctionArguments::List(FunctionArgumentList {
                                duplicate_treatment: None,
                                clauses: vec![],
                                args: statements
                                    .into_iter()
                                    .flat_map(|(key, query)| {
                                        vec![
                                            FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                                Expr::Value(Value::SingleQuotedString(
                                                    key.to_string(),
                                                )),
                                            )),
                                            FunctionArg::Unnamed(FunctionArgExpr::Expr(query)),
                                        ]
                                    })
                                    .collect(),
                            }),
                            over: None,
                            filter: None,
                            null_treatment: None,
                        }),
                    }],
                    from: vec![],
                    lateral_views: vec![],
                    selection: None,
                    group_by: GroupByExpr::Expressions(vec![]),
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
            }));
            let params = if final_vars.is_empty() {
                None
            } else {
                Some(
                    final_vars
                        .into_iter()
                        .filter_map(|n| sql_vars.swap_remove(&n))
                        .collect(),
                )
            };
            if tags.is_empty() {
                return Ok((statement, params, None, false));
            }
            let mut sub_tags = tags
                .into_iter()
                .flat_map(|(key, values)| {
                    if values.is_empty() {
                        return vec![format!("type:{key}")];
                    }
                    values
                        .into_iter()
                        .map(|v| format!("type:{key}:{}", v.to_string()))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<String>>();
            sub_tags.sort_unstable();
            return Ok((statement, params, Some(sub_tags), false));
        }
        OperationType::Mutation => {
            for selection in operation.selection_set.node.items {
                match &selection.node {
                    Selection::Field(p_field) => {
                        let field = &p_field.node;
                        let (name, key, is_insert, is_update, is_delete, is_single, schema_name) =
                            parse_mutation_meta(field)?;

                        let table_name = schema_name.map_or_else(
                            || {
                                ObjectName(vec![Ident {
                                    value: name.to_string(),
                                    quote_style: Some(QUOTE_CHAR),
                                }])
                            },
                            |schema_name| {
                                ObjectName(vec![
                                    Ident {
                                        value: schema_name.to_string(),
                                        quote_style: Some(QUOTE_CHAR),
                                    },
                                    Ident {
                                        value: name.to_string(),
                                        quote_style: Some(QUOTE_CHAR),
                                    },
                                ])
                            },
                        );
                        if is_insert {
                            let (columns, rows) = get_mutation_columns(
                                &field.arguments,
                                &variables,
                                &mut sql_vars,
                                &mut final_vars,
                            )?;
                            // let (projection, _, _) = get_projection(
                            //     &field.selection_set.node.items,
                            //     name,
                            //     None,
                            //     &variables,
                            //     &mut sql_vars,
                            //     &mut final_vars,
                            //     &mut tags,
                            // )?;
                            if rows.is_empty() {
                                return Ok((
                                    Statement::Query(Box::new(Query {
                                        for_clause: None,
                                        limit_by: vec![],
                                        with: None,
                                        body: Box::new(SetExpr::Select(Box::new(Select {
                                            window_before_qualify: false,
                                            connect_by: None,
                                            value_table_mode: None,
                                            distinct: None,
                                            named_window: vec![],
                                            top: None,
                                            into: None,
                                            projection: vec![SelectItem::ExprWithAlias {
                                                expr: Expr::Function(Function {
                                                    within_group: vec![],
                                                    name: ObjectName(vec![Ident {
                                                        value: JSONB_BUILD_OBJECT.to_string(),
                                                        quote_style: None,
                                                    }]),
                                                    args: FunctionArguments::List(
                                                        FunctionArgumentList {
                                                            duplicate_treatment: None,
                                                            clauses: vec![],
                                                            args: vec![
                                                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                                                    Value::SingleQuotedString(key.to_string()),
                                                                ))),
                                                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(Function {
                                                                    within_group: vec![],
                                                                    name: ObjectName(vec![Ident {
                                                                        value: JSONB_BUILD_ARRAY.to_string(),
                                                                        quote_style: None,
                                                                    }]),
                                                                    args: FunctionArguments::List(
                                                                        FunctionArgumentList {
                                                                            duplicate_treatment: None,
                                                                            clauses: vec![],
                                                                            args: vec![],
                                                                        },
                                                                    ),
                                                                    over: None,
                                                                    filter: None,
                                                                    null_treatment: None,
                                                                }))),
                        ],
                                                        },
                                                    ),
                                                    over: None,
                                                    filter: None,
                                                    null_treatment: None,
                                                }),
                                                alias: Ident {
                                                    value: DATA_LABEL.to_string(),
                                                    quote_style: Some(QUOTE_CHAR),
                                                },
                                            }],
                                            from: vec![],
                                            lateral_views: vec![],
                                            selection: None,
                                            group_by: GroupByExpr::Expressions(vec![]),
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
                                    })),
                                    None,
                                    None,
                                    false,
                                ));
                            }
                            let params = if final_vars.is_empty() {
                                None
                            } else {
                                Some(
                                    final_vars
                                        .into_iter()
                                        .filter_map(|n| sql_vars.swap_remove(&n))
                                        .collect(),
                                )
                            };
                            let is_potential_upsert = columns.contains(&Ident {
                                value: "id".to_owned(),
                                quote_style: Some(QUOTE_CHAR),
                            });
                            return Ok((
                                wrap_mutation(
                                    key,
                                    Statement::Insert(Insert {
                                        insert_alias: None,
                                        ignore: false,
                                        priority: None,
                                        replace_into: false,
                                        table_alias: None,
                                        or: None,
                                        into: true,
                                        table_name,
                                        columns: columns.clone(),
                                        overwrite: false,
                                        source: Some(Box::new(Query {
                                            for_clause: None,
                                            limit_by: vec![],
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
                                        })),
                                        partitioned: None,
                                        after_columns: vec![],
                                        table: false,
                                        on: if is_potential_upsert {
                                            Some(OnInsert::OnConflict(OnConflict {
                                                conflict_target: Some(ConflictTarget::Columns(
                                                    vec![Ident {
                                                        value: "id".to_owned(),
                                                        quote_style: Some(QUOTE_CHAR),
                                                    }],
                                                )),
                                                action: OnConflictAction::DoUpdate(DoUpdate {
                                                    assignments: columns
                                                        .iter()
                                                        .filter_map(|c| {
                                                            if c.value == "id" {
                                                                return None;
                                                            }
                                                            Some(Assignment {
                                                                id: vec![c.clone()],
                                                                value: Expr::CompoundIdentifier(
                                                                    vec![
                                                                        Ident::new("EXCLUDED"),
                                                                        c.clone(),
                                                                    ],
                                                                ),
                                                            })
                                                        })
                                                        .collect(),
                                                    selection: None,
                                                }),
                                            }))
                                        } else {
                                            None
                                        },
                                        returning: Some(vec![
                                            SelectItem::ExprWithAlias {
                                                alias: Ident {
                                                    value: TYPENAME.to_string(),
                                                    quote_style: Some(QUOTE_CHAR),
                                                },
                                                expr: Expr::Value(Value::SingleQuotedString(
                                                    name.to_owned(),
                                                )),
                                            },
                                            SelectItem::Wildcard(
                                                WildcardAdditionalOptions::default(),
                                            ),
                                        ]),
                                    }),
                                    is_single,
                                ),
                                params,
                                None,
                                true,
                            ));
                        } else if is_update {
                            let has_updated_at_directive = field
                                .directives
                                .iter()
                                .any(|d| d.node.name.node == "updatedAt");
                            let (selection, assignments) = get_mutation_assignments(
                                &field.arguments,
                                &variables,
                                &mut sql_vars,
                                &mut final_vars,
                                has_updated_at_directive,
                            )?;
                            let params = if final_vars.is_empty() {
                                None
                            } else {
                                Some(
                                    final_vars
                                        .into_iter()
                                        .filter_map(|n| sql_vars.swap_remove(&n))
                                        .collect(),
                                )
                            };
                            return Ok((
                                wrap_mutation(
                                    key,
                                    Statement::Update {
                                        table: TableWithJoins {
                                            relation: TableFactor::Table {
                                                partitions: vec![],
                                                version: None,
                                                name: table_name,
                                                alias: None,
                                                args: None,
                                                with_hints: vec![],
                                            },
                                            joins: vec![],
                                        },
                                        assignments,
                                        from: None,
                                        selection,
                                        returning: Some(vec![
                                            SelectItem::ExprWithAlias {
                                                alias: Ident {
                                                    value: TYPENAME.to_string(),
                                                    quote_style: Some(QUOTE_CHAR),
                                                },
                                                expr: Expr::Value(Value::SingleQuotedString(
                                                    name.to_owned(),
                                                )),
                                            },
                                            SelectItem::Wildcard(
                                                WildcardAdditionalOptions::default(),
                                            ),
                                        ]),
                                    },
                                    is_single,
                                ),
                                params,
                                None,
                                true,
                            ));
                        } else if is_delete {
                            let (selection, _) = get_mutation_assignments(
                                &field.arguments,
                                &variables,
                                &mut sql_vars,
                                &mut final_vars,
                                false,
                            )?;
                            let params = if final_vars.is_empty() {
                                None
                            } else {
                                Some(
                                    final_vars
                                        .into_iter()
                                        .filter_map(|n| sql_vars.swap_remove(&n))
                                        .collect(),
                                )
                            };
                            return Ok((
                                wrap_mutation(
                                    key,
                                    Statement::Delete(Delete {
                                        limit: None,
                                        order_by: vec![],
                                        tables: vec![],
                                        from: FromTable::WithFromKeyword(vec![TableWithJoins {
                                            relation: TableFactor::Table {
                                                partitions: vec![],
                                                version: None,
                                                name: table_name,
                                                alias: None,
                                                args: None,
                                                with_hints: vec![],
                                            },
                                            joins: vec![],
                                        }]),
                                        using: None,
                                        selection,
                                        returning: Some(vec![
                                            SelectItem::ExprWithAlias {
                                                alias: Ident {
                                                    value: TYPENAME.to_string(),
                                                    quote_style: Some(QUOTE_CHAR),
                                                },
                                                expr: Expr::Value(Value::SingleQuotedString(
                                                    name.to_owned(),
                                                )),
                                            },
                                            SelectItem::Wildcard(
                                                WildcardAdditionalOptions::default(),
                                            ),
                                        ]),
                                    }),
                                    is_single,
                                ),
                                params,
                                None,
                                true,
                            ));
                        }
                    }
                    Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
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

    use insta::assert_snapshot;
    use serde_json::json;

    #[test]
    fn simple() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query App {
                app(filter: { field: "id", operator: "eq", value: "345810043118026832" }, order: { name: ASC }) @meta(table: "App") {
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
                Component_aggregate(filter: { field: "appId", operator: "eq", value: "345810043118026832" }) {
                  count
                  min {
                    createdAt
                  }
                }
            }
            query Another {
                Component_aggregate(filter: { field: "appId", operator: "eq", value: "345810043118026832" }) {
                  count
                  min {
                    createdAt
                  }
                }
            }
        "#,
        )?;
        let (statement, _params, _tags, _is_mutation) =
            gql2sql(gqlast, &None, Some("App".to_owned()))?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn id_ignore() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query App($id: String) {
                app(id: $id) @meta(table: "App") {
                    id
                }
            }
        "#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "id": null
            })),
            Some("App".to_owned()),
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn simple_ignore() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query App($filter: Filter) {
                app(filter: $filter, order: { name: ASC }) @meta(table: "App") {
                    id
                }
            }
        "#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "filter": {
                    "field": "id",
                    "operator": "eq",
                    "value": null,
                    "ignoreEmpty": true,
                    "children": [{
                        "field": "other",
                        "operator": "gte",
                        "value": null,
                        "ignoreEmpty": true,
                    }]
                }
            })),
            Some("App".to_owned()),
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn mutation_insert() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"mutation insertVillains($data: [Villain_insert_input!]!) {
                insert(data: $data) @meta(table: "Villain", insert: true, schema: "auth") { id name }
            }"#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "data": [
                    { "name": "Ronan the Accuser", "id": "1" },
                    { "name": "Red Skull", "id": "2" },
                    { "name": "The Vulture", "id": "3" }
                ]
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn mutation_empty_insert() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"mutation insertVillains($data: [Villain_insert_input!]!) {
                insert(data: $data) @meta(table: "Villain", insert: true, schema: "auth") { id name }
            }"#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "data": [
                ]
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn mutation_update() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"mutation updateHero {
                update(
                    filter: { field: "secret_identity", operator: "eq", value: "Sam Wilson" },
                    set: {
                        name: "Captain America",
                    }
                    increment: {
                        number_of_movies: 1
                    }
                ) @meta(table: "Hero", update: true, schema: "auth") @updatedAt {
                    id
                    name
                    secret_identity
                    number_of_movies
                }
            }"#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(gqlast, &None, None)?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_mega() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query GetApp($orgId: String!, $appId: String!, $branch: String!) {
      app: App_one(
        filter: {
          field: "orgId",
          operator: "eq",
          value: $orgId,
          logicalOperator: "AND",
          children: [
            { field: "id", operator: "eq", value: $appId },
            { field: "branch", operator: "eq", value: $branch }
          ]
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
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "orgId": "org",
                "appId": "app",
                "branch": "branch"
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_frag() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query GetApp($componentId: String!, $branch: String!) {
                component: Component_one(filter: { field: "id", operator: "eq", value: $componentId }) {
                   id
                   branch
                   ... on ComponentMeta @relation(
                        table: "ComponentMeta"
                        field: ["componentId"]
                        references: ["id"]
                        single: true
                    ) @args(
                        filter: {
                          field: "branch"
                          operator: "eq",
                          value: $branch,
                          logicalOperator: "OR",
                          children: [
                            { field: "branch", operator: "eq", value: "main" }
                          ]
                        }
                    ) {
                     title
                   }
                }
            }"#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "componentId": "comp",
                "branch": "branch"
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_static() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query GetApp($componentId: String!) {
                component: Component_one(filter: { field: "id", operator: "eq", value: $componentId }) {
                   id
                   branch
                   kind @static(value: "page")
                }
            }"#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "componentId": "fake"
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_distinct() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"query GetApp($componentId: String!, $branch: String!) {
                component: Component_one(
                    filter: {
                        field: "id",
                        operator: "eq",
                        value: $componentId
                        logicalOperator: "AND",
                        children: [
                            { field: "branch", operator: "eq", value: $branch, logicalOperator: "OR", children: [
                                { field: "branch", operator: "eq", value: "main" }
                            ]}
                        ]
                    },
                    order: [
                        { orderKey: ASC }
                    ],
                    distinct: { on: ["id"], order: [{ expr: { field: "branch", operator: "eq", value: $branch }, dir: DESC }] }
                ) {
                   id
                   branch
                   kind @static(value: "page")
                   stuff(filter: { field: "componentId", operator: "eq", value: { _parentRef: "id" } }) @relation(table: "Stuff") {
                     id
                   }
                }
            }"#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "componentId": "fake",
                "branch": "branch",
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
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
                        __typename
                        count
                        avg {
                          __typename
                          value
                        }
                    }
                    stuff @relation(table: "iYrk3kyTqaDQrLgjDaE9n", fields: ["eT86hgrpFB49r7N6AXz63"], references: ["id"], single: true) {
                        id
                    }
                }
            }"#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(gqlast, &None, None)?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_schema_arg() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
              query GetSession($sessionToken: String!) {
    session(
        filter: {
            field: "sessionToken"
            operator: "eq"
            value: $sessionToken
        }
    ) @meta(table: "sessions", single: true, schema: "auth") {
        sessionToken
        userId
        expires
        user2: user
            @relation(
                table: "users"
                field: ["id"]
                references: ["userId"]
                single: true
                schema: "auth"
            ) {
            id
            name
            email
            emailVerified
            image
        }
    }
}
            "#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
              "sessionToken": "fake"
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_wrap_arg() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
                mutation CreateVerificationToken($data: [VerificationToken!]!) {
                    insert(data: $data)
                        @meta(table: "verification_tokens", insert: true, schema: "auth", single: true) {
                        identifier
                        token
                        expires
                    }
                }
            "#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
            "data": [{
                "identifier": "nick@brevity.io",
                "token": "da978cc2c1e0e7b61e1be31b2e3979af576e494d68bd6f5dc156084d9924ee12",
                "expires": "2023-04-26T21:38:26"
                }]
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_json_arg() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
                query BrevityQuery($order_getTodoList: tXY7bJTNXP7RAhLFGybN4d_Order, $filter: tXY7bJTNXP7RAhLFGybN4d_Filter) {
                getTodoList(order: $order_getTodoList, filter: $filter) @meta(table: "tXY7bJTNXP7RAhLFGybN4d") {
                    id
                    cJ9jmpnjfYhRbCQBpWAzB8
                    cPQdcYiWcPWWVeKVniUMjy
                }
                }
            "#,
        )?;
        // let sql = r#""#;
        let (_statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "order_getTodoList": {
                    "cPQdcYiWcPWWVeKVniUMjy": "ASC"
                },
                "filter": null
            })),
            None,
        )?;
        // assert_eq!(statement.to_string(), sql);
        Ok(())
    }

    #[test]
    fn query_simple_filter() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
                query Test($id: String!) {
                    record(id: $id) @meta(table: "Record") {
                        id
                        name
                        age
                    }
                }
            "#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "id": "fake"
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_many_to_many() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
                query ManyToMany($id: String!) {
                    currentUser(id: $id) @meta(table: "User") {
                        id
                        lists @relation(table: "wrHJEgwMUmdJ3eWtPLPk8", many: true) {
                            id
                        }
                    }
                }
            "#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
                "id": "fake"
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn query_andre() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
            query BrevityQuery($id_getH33iDwNVqqMxAnVEgPaThById: ID) {
            getH33iDwNVqqMxAnVEgPaThById(id: $id_getH33iDwNVqqMxAnVEgPaThById)
                @meta(table: "H33iDwNVqqMxAnVEgPaTh", single: true) {
                d8GJJg9DjNehPAeJcpTjM
                Fjjm3XAhyDmbhzymrrkRT_Aggregate
                @relation(
                    table: "Fjjm3XAhyDmbhzymrrkRT"
                    fields: ["id"]
                    aggregate: true
                    references: ["TbFeY8XVMaYnkQjDPWMkb_id"]
                ) {
                avg {
                    XF4f6Qrhk86AX6dFWjYDt
                }
                }
                q6pJYTjmbprTNRdqG9Jrw
                egeyQ33H3z4EqzcRVFchV
                HYWfawTyxPNUf9a4DAH79
                H33iDwNVqqMxAnVEgPaTh_by_MdYg7jdht8ByhnKdfXBAb
                @relation(
                    table: "MdYg7jdht8ByhnKdfXBAb"
                    fields: ["id"]
                    single: true
                    references: ["MiyNcUJzKGJgQ9BERD8fr_id"]
                ) {
                H6hp6JGhzgPTYmLYwLk8P
                id
                }
                zFjEBPkLYmEAxLHrt3N4B
                LJDX6neXAYeXt9aVWxTRk
                FwpKpCegQH4EkzbjbNqVn
                ayipLT8iKHNTdhmiVqmxq
                Mr3R877DKbWTNWRzmEjxE_Aggregate
                @relation(many: true, table: "Mr3R877DKbWTNWRzmEjxE", aggregate: true) {
                count
                }
                r7xwAFrckDaVLwPzUAADB
                H33iDwNVqqMxAnVEgPaTh_by_User
                @relation(
                    table: "User"
                    fields: ["id"]
                    single: true
                    references: ["Gb8jAGqGDbYqfeqDDxKUF_id"]
                ) {
                gnHezR9MdBFH9kCthN3aB
                created_at
                id
                }
                id
            }
            }
            "#,
        )?;
        let (statement, params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({
              "id_getH33iDwNVqqMxAnVEgPaThById": "HAzqFfhQGbaB6WKBr6LA7"
            })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        assert_snapshot!(serde_json::to_string_pretty(&params)?);
        Ok(())
    }

    #[test]
    fn mutation_delete() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
            mutation DeleteVerificationToken(
                $identifier: String!
                $token: String!
                ) {
                delete(
                    filter: {
                    field: "identifier"
                    operator: "eq"
                    value: $identifier
                    logicalOperator: "AND"
                    children: [{ field: "token", operator: "eq", value: $token }]
                    }
                ) @meta(table: "verification_tokens", delete: true, schema: "auth") {
                    identifier
                    token
                    expires
                }
            }
            "#,
        )?;
        let (statement, _params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({ "token": "12345", "identifier": "fake@email.com" })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        Ok(())
    }

    #[test]
    fn mutation_image() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
            mutation Update($id: String!, $set: dogUpdateInput!) {
                update(
                  filter: {
                    field: "id"
                    operator: "eq"
                    value: $id
                  }
                  set: $set
                ) @meta(table: "WFqGH6dk8MpxfpHXh7awi", update: true) {
                  id
                }
              }
            "#,
        )?;
        let (statement, params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(
                json!({"id":"ffj9ACLQqpzjyh8yNFeQ6","set":{"updated_at":"2023-06-06T19:41:47+00:00","ynWfqMzGjjVQYzbKx4rMX":"DOGGY","QYtpTcmJCe6zfCHWwpNjR":"MYDOG","a8heQgUMyFync44JACwKA":{"src":"https://assets.brevity.io/uploads/jwy1g8rs7bxr9ptkaf6sy/lp_image-1685987665741.png","width":588,"height":1280}}}),
            ),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        assert_snapshot!(serde_json::to_string_pretty(&params)?);
        Ok(())
    }
    #[test]
    fn nested_query() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
                query BrevityQuery($id_getU7BBKiUwTgwiWMcgUYA4CById: ID) {
                getU7BBKiUwTgwiWMcgUYA4CById(id: $id_getU7BBKiUwTgwiWMcgUYA4CById) @meta(table: "U7BBKiUwTgwiWMcgUYA4C", single: true) {
                    BtaHL8fRtKFw8gDJULFYp
                    WFqGH6dk8MpxfpHXh7awi_by_U7BBKiUwTgwiWMcgUYA4C @relation(table: "WFqGH6dk8MpxfpHXh7awi", fields: ["MHPB9NP84gr3eXBmBfbxh_id"], references: ["id"]) {
                    ynWfqMzGjjVQYzbKx4rMX
                    QYtpTcmJCe6zfCHWwpNjR
                    MHPB9NP84gr3eXBmBfbxh_id @relation(table: "U7BBKiUwTgwiWMcgUYA4C", fields: ["id"], single: true, references: ["MHPB9NP84gr3eXBmBfbxh_id"]) {
                        id
                        __typename
                    }
                    id
                    }
                    id
                }
                }
            "#,
        )?;
        let (statement, params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({ "id_getU7BBKiUwTgwiWMcgUYA4CById": "piWkMrFFXgdQBBkzf84MD" })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        assert_snapshot!(serde_json::to_string_pretty(&params)?);
        Ok(())
    }
    #[test]
    fn group_by_query() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
                query BrevityQuery($groupBy: [String]) {
                    Event(filter: { field: "xVAFwi3LkLnRYqtkV3e9A_id", operator: "eq", value: "ge3xraXEcwPTF6hJxLXC7" }, groupBy: $groupBy) @meta(table: "LC4PdkWrXEq6PnJNF98RE", aggregate: true) {
                        value {
                          W3htYNGnCaJp4MAp6p6c9_id @relation(table: "AQfNfkgxq4iLcAhkdNAWf", fields: ["id"], references: ["W3htYNGnCaJp4MAp6p6c9_id"], single: true) {
                            id
                            name: QJ3MwMUiXqrkPwb88eW8g
                          }
                          t473xCb8nhWCxX7Ag7k6q_id @relation(table: "fTgjFRxYgaj3qHriEdQi3", fields: ["id"], references: ["t473xCb8nhWCxX7Ag7k6q_id"], single: true) {
                            id
                            title: tcGyWe4CLwhpTJp4krApd
                          }
                        }
                        count
                    }
                }
            "#,
        )?;
        let (statement, params, _tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!({ "groupBy": ["W3htYNGnCaJp4MAp6p6c9_id", "t473xCb8nhWCxX7Ag7k6q_id"] })),
            None,
        )?;
        assert_snapshot!(statement.to_string());
        assert_snapshot!(serde_json::to_string_pretty(&params)?);
        Ok(())
    }
    #[test]
    fn nested_playground() -> Result<(), anyhow::Error> {
        let gqlast = parse_query(
            r#"
            query BrevityDBQuery(
  $playbook_id: String!
  $template_BahPd_id_order: [boardcolumn_Order]
  $playbook_LFc9r_id_order: [boardcolumn_Order]
  $playbook_playbook_id_order: [playbookstandard_Order]
  $boardrow_row_id_filter: boardcell_Filter
  $boardrow_row_id_distinct: boardcell_Distinct
  $workflows_Kdda9_id_order: [approvalworkflow_cqaw9_Order]
) {
  playbook: getplaybookById(id: $playbook_id)
    @meta(table: "playbook", single: true) {
    __typename
    id
    name
    created_at
    updated_at
    folder_UEiw4_id {
      id
    }
    workspace_VTGmA_id {
      id
    }
    ownercolumn_3hJaT_id
      @relation(
        table: "boardcolumn"
        fields: ["id"]
        single: true
        references: ["ownercolumn_3hJaT_id"]
      ) {
      __typename
      id
      created_at
      name_CwYar
      type_Hbagk
      updated_at
      order_WUkJE
      width_xJ846
      board_3jDDw_id {
        id
      }
      required_4jaR4
      isdefault_KCmRr
      temporary_4NyhY
      playbook_LFc9r_id {
        id
      }
      template_BahPd_id {
        id
      }
    }
    playbook_template_id
      @relation(
        table: "template"
        fields: ["id"]
        single: true
        references: ["playbook_template_id"]
      ) {
      __typename
      id
      title
      created_at
      updated_at
      organization_organization_id {
        id
      }
      defaultdescriptioncolumn_kMT8n_id {
        id
      }
      template_BahPd_id(order: $template_BahPd_id_order)
        @relation(
          table: "boardcolumn"
          fields: ["template_BahPd_id"]
          references: ["id"]
        ) {
        __typename
        id
        created_at
        name_CwYar
        type_Hbagk
        updated_at
        order_WUkJE
        width_xJ846
        board_3jDDw_id {
          id
        }
        required_4jaR4
        isdefault_KCmRr
        temporary_4NyhY
        playbook_LFc9r_id {
          id
        }
        template_BahPd_id {
          id
        }
        column_Xdjyz_id
          @relation(
            table: "boardcolumnoptions_mrX6T"
            fields: ["column_Xdjyz_id"]
            references: ["id"]
          ) {
          __typename
          id
          created_at
          name_bFeAf
          updated_at
          column_Xdjyz_id {
            id
          }
        }
      }
    }
    statuscolumn_fFigH_id
      @relation(
        table: "boardcolumn"
        fields: ["id"]
        single: true
        references: ["statuscolumn_fFigH_id"]
      ) {
      __typename
      id
      created_at
      name_CwYar
      type_Hbagk
      updated_at
      order_WUkJE
      width_xJ846
      board_3jDDw_id {
        id
      }
      required_4jaR4
      isdefault_KCmRr
      temporary_4NyhY
      playbook_LFc9r_id {
        id
      }
      template_BahPd_id {
        id
      }
    }
    duedatecolumn_Qajep_id
      @relation(
        table: "boardcolumn"
        fields: ["id"]
        single: true
        references: ["duedatecolumn_Qajep_id"]
      ) {
      __typename
      id
      created_at
      name_CwYar
      type_Hbagk
      updated_at
      order_WUkJE
      width_xJ846
      board_3jDDw_id {
        id
      }
      required_4jaR4
      isdefault_KCmRr
      temporary_4NyhY
      playbook_LFc9r_id {
        id
      }
      template_BahPd_id {
        id
      }
    }
    descriptioncolumn_nNkVP_id
      @relation(
        table: "boardcolumn"
        fields: ["id"]
        single: true
        references: ["descriptioncolumn_nNkVP_id"]
      ) {
      __typename
      id
      created_at
      name_CwYar
      type_Hbagk
      updated_at
      order_WUkJE
      width_xJ846
      board_3jDDw_id {
        id
      }
      required_4jaR4
      isdefault_KCmRr
      temporary_4NyhY
      playbook_LFc9r_id {
        id
      }
      template_BahPd_id
        @relation(
          table: "template"
          fields: ["id"]
          single: true
          references: ["template_BahPd_id"]
        ) {
        __typename
        id
        title
        created_at
        updated_at
        organization_organization_id {
          id
        }
        defaultdescriptioncolumn_kMT8n_id {
          id
        }
      }
    }
    playbook_id
      @relation(
        table: "folderitem"
        fields: ["playbook_id"]
        single: true
        references: ["id"]
      ) {
      __typename
      id
      board_id {
        id
      }
      folder_id {
        id
      }
      created_at
      updated_at
      playbook_id {
        id
      }
      okr_ByTYz_id {
        id
      }
      dashboard_zzwnp_id {
        id
      }
    }
    playbook_LFc9r_id(order: $playbook_LFc9r_id_order)
      @relation(
        table: "boardcolumn"
        fields: ["playbook_LFc9r_id"]
        references: ["id"]
      ) {
      __typename
      id
      created_at
      name_CwYar
      type_Hbagk
      updated_at
      order_WUkJE
      width_xJ846
      board_3jDDw_id {
        id
      }
      required_4jaR4
      isdefault_KCmRr
      temporary_4NyhY
      playbook_LFc9r_id {
        id
      }
      template_BahPd_id
        @relation(
          table: "template"
          fields: ["id"]
          single: true
          references: ["template_BahPd_id"]
        ) {
        __typename
        id
        title
        created_at
        updated_at
        organization_organization_id {
          id
        }
        defaultdescriptioncolumn_kMT8n_id {
          id
        }
      }
      column_Xdjyz_id
        @relation(
          table: "boardcolumnoptions_mrX6T"
          fields: ["column_Xdjyz_id"]
          references: ["id"]
        ) {
        __typename
        id
        created_at
        name_bFeAf
        updated_at
        column_Xdjyz_id {
          id
        }
      }
    }
    playbook_playbook_id(order: $playbook_playbook_id_order)
      @relation(
        table: "playbookstandard"
        fields: ["playbook_playbook_id"]
        references: ["id"]
      ) {
      __typename
      id
      created_at
      updated_at
      row_DBUfb_id
        @relation(
          table: "boardrow"
          fields: ["id"]
          single: true
          references: ["row_DBUfb_id"]
        ) {
        __typename
        id
        created_at
        updated_at
        board_mkGmp_id {
          id
        }
        playbook_t7raV_id {
          id
        }
        template_xdeiM_id {
          id
        }
        defaultdescriptionvalue_tHbN3_id {
          id
        }
        boardrow_row_id(
          filter: $boardrow_row_id_filter
          distinct: $boardrow_row_id_distinct
        )
          @relation(
            table: "boardcell"
            fields: ["boardrow_row_id"]
            references: ["id"]
          ) {
          __typename
          id
          created_at
          updated_at
          boardrow_row_id {
            id
          }
          datevalue_hd3CD
          status_HVqrA_id {
            id
          }
          textvalue_ahacc
          numberfield_YfkE7
          playbook_EqwVY_id {
            id
          }
          selectvalue_q66xK
          uservalue_faXth_id {
            id
          }
          parentboard_BWMCH_id {
            id
          }
          boardcolumn_column_id {
            id
          }
          multiselectvalue_bHY6V
        }
      }
      status_d9CPd_id {
        id
      }
      duedate_cRCB8_id {
        id
      }
      ownercell_TyVaR_id {
        id
      }
      playbook_playbook_id {
        id
      }
      playbookrow_VJ6Vw_id
        @relation(
          table: "approvalworkflow_cqaw9"
          fields: ["id"]
          single: true
          references: ["playbookrow_VJ6Vw_id"]
        ) {
        __typename
        id
        created_at
        name_HKxd6
        updated_at
        workflows_Kdda9_id {
          id
        }
      }
    }
    workflows_Kdda9_id(order: $workflows_Kdda9_id_order)
      @relation(
        table: "approvalworkflow_cqaw9"
        fields: ["workflows_Kdda9_id"]
        references: ["id"]
      ) {
      __typename
      id
      created_at
      name_HKxd6
      updated_at
      workflows_Kdda9_id {
        id
      }
    }
  }
}
            "#,
        )?;
        let (statement, params, tags, _is_mutation) = gql2sql(
            gqlast,
            &Some(json!(
            {
              "playbook_id": "PMxiGmJ4eyndrdp3J3Li6",
              "template_BahPd_id_order": [
                {
                  "id": "ASC",
                  "field": "created_at",
                  "direction": "ASC"
                }
              ],
              "playbook_LFc9r_id_order": [
                {
                  "id": "ASC",
                  "field": "created_at",
                  "direction": "ASC"
                }
              ],
              "playbook_playbook_id_order": [
                {
                  "id": "ASC",
                  "field": "created_at",
                  "direction": "ASC"
                }
              ],
              "boardrow_row_id_filter": {
                "id": "filter_YipDb8gGjkbHRpLfbGBNt",
                "field": "playbook_EqwVY_id",
                "value": null,
                "children": [
                  {
                    "id": "filter_L6NRaeg8JXzdDFdtFePdc",
                    "field": "playbook_EqwVY_id",
                    "value": "PMxiGmJ4eyndrdp3J3Li6",
                    "children": [],
                    "operator": "eq",
                    "logicalOperator": "AND"
                  }
                ],
                "operator": "null",
                "logicalOperator": "OR"
              },
              "boardrow_row_id_distinct": {
                "on": [
                  "boardrow_row_id",
                  "boardcolumn_column_id"
                ],
                "order": [
                  {
                    "id": "ASC",
                    "field": "created_at",
                    "direction": "DESC"
                  }
                ]
              },
              "workflows_Kdda9_id_order": [
                {
                  "id": "ASC",
                  "field": "created_at",
                  "direction": "ASC"
                }
              ]
            }
                        )),
            None,
        )?;

        println!("query: {statement}");
        println!("vars: {}", serde_json::to_string_pretty(&params)?);
        println!("tags: {}", serde_json::to_string_pretty(&tags)?);
        // assert_snapshot!(statement.to_string());
        // assert_snapshot!();
        Ok(())
    }
}
