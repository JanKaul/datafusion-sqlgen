use crate::RelToSql;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_sql::sqlparser::ast::{
    Expr as SQLExpr, Ident, ObjectName, Offset, OffsetRows, OrderByExpr, Query, Select, SelectItem,
    SetExpr, TableFactor, TableWithJoins, Value,
};

impl RelToSql {
    pub fn plan_to_query(&self, plan: &LogicalPlan, query: Option<Query>) -> Result<Query> {
        let query = match query {
            None => Query {
                body: SetExpr::Select(Box::new(Select {
                    distinct: false,
                    top: None,
                    projection: vec![],
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
                })),
                with: None,
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
                lock: None,
            },
            Some(s) => s,
        };
        match plan {
            LogicalPlan::Sort(sort) => {
                let order_by_expr = sort
                    .expr
                    .iter()
                    .map(|x| match x {
                        Expr::Sort {
                            expr,
                            asc,
                            nulls_first,
                        } => Ok(OrderByExpr {
                            expr: self.logical_expr_to_sql_expr(expr)?,
                            asc: Some(*asc),
                            nulls_first: Some(*nulls_first),
                        }),
                        e => Ok(OrderByExpr {
                            expr: self.logical_expr_to_sql_expr(&e)?,
                            asc: None,
                            nulls_first: None,
                        }),
                    })
                    .collect::<Result<_>>()?;
                self.plan_to_query(
                    &sort.input,
                    Some(Query {
                        order_by: order_by_expr,
                        ..query
                    }),
                )
            }
            LogicalPlan::Projection(projection) => {
                let select = match query.body {
                    SetExpr::Select(select) => Ok(SetExpr::Select(Box::new(Select {
                        projection: projection
                            .expr
                            .iter()
                            .map(|x| match x {
                                Expr::Wildcard => Ok(SelectItem::Wildcard),
                                Expr::QualifiedWildcard { qualifier } => {
                                    Ok(SelectItem::QualifiedWildcard(ObjectName(
                                        qualifier
                                            .split(".")
                                            .map(|x| Ident {
                                                value: x.to_string(),
                                                quote_style: None,
                                            })
                                            .collect(),
                                    )))
                                }
                                Expr::Alias(e, alias) => {
                                    self.logical_expr_to_sql_expr(&e).map(|y| {
                                        SelectItem::ExprWithAlias {
                                            expr: y,
                                            alias: Ident {
                                                value: alias.to_string(),
                                                quote_style: None,
                                            },
                                        }
                                    })
                                }
                                e => self
                                    .logical_expr_to_sql_expr(&e)
                                    .map(|y| SelectItem::UnnamedExpr(y)),
                            })
                            .collect::<Result<_>>()?,
                        ..*select
                    }))),
                    e => Err(DataFusionError::Plan(format!(
                        "Cannot perform projection on {:#?}, it needs to be a select.",
                        e
                    ))),
                }?;
                self.plan_to_query(
                    &projection.input,
                    Some(Query {
                        body: select,
                        ..query
                    }),
                )
            }
            LogicalPlan::Filter(filter) => {
                let select = match query.body {
                    SetExpr::Select(select) => Ok(SetExpr::Select(Box::new(Select {
                        selection: Some(self.logical_expr_to_sql_expr(&filter.predicate)?),
                        ..*select
                    }))),
                    e => Err(DataFusionError::Plan(format!(
                        "Cannot perform projection on {:#?}, it needs to be a select.",
                        e
                    ))),
                }?;
                self.plan_to_query(
                    &filter.input,
                    Some(Query {
                        body: select,
                        ..query
                    }),
                )
            }
            LogicalPlan::Distinct(distinct) => {
                let select = match query.body {
                    SetExpr::Select(select) => Ok(SetExpr::Select(Box::new(Select {
                        distinct: true,
                        ..*select
                    }))),
                    e => Err(DataFusionError::Plan(format!(
                        "Cannot perform projection on {:#?}, it needs to be a select.",
                        e
                    ))),
                }?;
                self.plan_to_query(
                    &distinct.input,
                    Some(Query {
                        body: select,
                        ..query
                    }),
                )
            }
            LogicalPlan::TableScan(table_scan) => {
                let select = match query.body {
                    SetExpr::Select(select) => Ok(SetExpr::Select(Box::new(Select {
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: ObjectName(vec![Ident {
                                    value: table_scan.table_name.to_string(),
                                    quote_style: None,
                                }]),
                                alias: None,
                                args: None,
                                with_hints: vec![],
                            },
                            joins: vec![],
                        }],
                        ..*select
                    }))),
                    e => Err(DataFusionError::Plan(format!(
                        "Cannot perform projection on {:#?}, it needs to be a select.",
                        e
                    ))),
                }?;
                Ok(Query {
                    body: select,
                    ..query
                })
            }
            LogicalPlan::Limit(limit) => {
                let lim = limit
                    .fetch
                    .map(|x| SQLExpr::Value(Value::Number(format!("{}", x), false)));
                let offset = limit.skip.map(|x| Offset {
                    value: SQLExpr::Value(Value::Number(format!("{}", x), false)),
                    rows: OffsetRows::Rows,
                });
                self.plan_to_query(
                    &limit.input,
                    Some(Query {
                        limit: lim,
                        offset: offset,
                        ..query
                    }),
                )
            }
            LogicalPlan::EmptyRelation(_) => Ok(Query { ..query }),
            p => Err(DataFusionError::Plan(format!(
                "Could not convert plan {:#?} to sql.",
                p
            ))),
        }
    }
}
