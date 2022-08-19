use crate::RelToSql;
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Expr;
use datafusion_expr::Operator;
use datafusion_sql::sqlparser::ast::{
    BinaryOperator, FunctionArg, FunctionArgExpr, ObjectName, Value,
};
use datafusion_sql::sqlparser::ast::{DataType as SQLDataType, Expr as SQLExpr};
use datafusion_sql::sqlparser::ast::{Function, Ident};
use datafusion_sql::sqlparser::parser::ParserError;

impl RelToSql {
    pub fn logical_expr_to_sql_expr(&self, expr: &Expr) -> Result<SQLExpr> {
        match expr {
            Expr::Column(column) => match &column.relation {
                Some(table) => Ok(SQLExpr::CompoundIdentifier(vec![
                    Ident {
                        value: table.clone(),
                        quote_style: None,
                    },
                    Ident {
                        value: column.name.clone(),
                        quote_style: None,
                    },
                ])),
                None => Ok(SQLExpr::Identifier(Ident {
                    value: column.name.clone(),
                    quote_style: None,
                })),
            },
            Expr::Literal(lit) => literal_to_sql_value(lit).map(|x| SQLExpr::Value(x)),
            Expr::BinaryExpr { left, op, right } => match op {
                Operator::IsDistinctFrom => {
                    let left = self.logical_expr_to_sql_expr(left)?;
                    let right = self.logical_expr_to_sql_expr(right)?;
                    Ok(SQLExpr::IsDistinctFrom(Box::new(left), Box::new(right)))
                }
                Operator::IsNotDistinctFrom => {
                    let left = self.logical_expr_to_sql_expr(left)?;
                    let right = self.logical_expr_to_sql_expr(right)?;
                    Ok(SQLExpr::IsNotDistinctFrom(Box::new(left), Box::new(right)))
                }
                op => {
                    let left = self.logical_expr_to_sql_expr(left)?;
                    let right = self.logical_expr_to_sql_expr(right)?;
                    let op = binary_operator_to_sql(op)?;
                    Ok(SQLExpr::BinaryOp {
                        left: Box::new(left),
                        right: Box::new(right),
                        op: op,
                    })
                }
            },
            Expr::Cast { expr, data_type } => Ok(SQLExpr::Cast {
                expr: Box::new(self.logical_expr_to_sql_expr(expr)?),
                data_type: datatype_to_sql(data_type)?,
            }),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.logical_expr_to_sql_expr(expr)?;
                let low = self.logical_expr_to_sql_expr(low)?;
                let high = self.logical_expr_to_sql_expr(high)?;
                Ok(SQLExpr::Between {
                    expr: Box::new(expr),
                    negated: *negated,
                    low: Box::new(low),
                    high: Box::new(high),
                })
            }
            Expr::ScalarFunction { fun, args } => {
                let args = args
                    .iter()
                    .map(|x| {
                        Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            self.logical_expr_to_sql_expr(x)?,
                        )))
                    })
                    .collect::<Result<_>>()?;
                Ok(SQLExpr::Function(Function {
                    name: ObjectName(vec![Ident {
                        value: fun.to_string(),
                        quote_style: None,
                    }]),
                    args: args,
                    over: None,
                    distinct: false,
                }))
            }
            Expr::GetIndexedField { expr, key } => {
                let expr = self.logical_expr_to_sql_expr(expr)?;
                let index = literal_to_sql_value(key)?;
                Ok(SQLExpr::ArrayIndex {
                    obj: Box::new(expr),
                    indexes: vec![SQLExpr::Value(index)],
                })
            }
            e => Err(DataFusionError::SQL(ParserError::ParserError(format!(
                "Could not convert expression {:?} to sql expression.",
                e
            )))),
        }
    }
}

#[inline]
fn literal_to_sql_value(lit: &ScalarValue) -> Result<Value> {
    match lit {
        ScalarValue::Null => Ok(Value::Null),
        ScalarValue::Boolean(bool) => {
            bool.map(|x| Value::Boolean(x))
                .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                    "Could not convert boolean {:#?} to sql boolean.",
                    bool
                ))))
        }
        ScalarValue::Int8(number) => {
            number
                .map(|x| Value::Number(x.to_string(), false))
                .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                    "Could not convert int8 {:#?} to sql number.",
                    number
                ))))
        }
        ScalarValue::Int16(number) => {
            number
                .map(|x| Value::Number(x.to_string(), false))
                .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                    "Could not convert int16 {:#?} to sql number.",
                    number
                ))))
        }
        ScalarValue::Int32(number) => {
            number
                .map(|x| Value::Number(x.to_string(), false))
                .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                    "Could not convert int32 {:#?} to sql number.",
                    number
                ))))
        }
        ScalarValue::Int64(number) => {
            number
                .map(|x| Value::Number(x.to_string(), false))
                .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                    "Could not convert int64 {:#?} to sql number.",
                    number
                ))))
        }
        ScalarValue::Float32(number) => {
            number
                .map(|x| Value::Number(x.to_string(), false))
                .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                    "Could not convert float32 {:#?} to sql number.",
                    number
                ))))
        }
        ScalarValue::Float64(number) => {
            number
                .map(|x| Value::Number(x.to_string(), false))
                .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                    "Could not convert float64 {:#?} to sql number.",
                    number
                ))))
        }
        ScalarValue::Utf8(string) => string
            .as_ref()
            .map(|x| Value::SingleQuotedString(x.clone()))
            .ok_or(DataFusionError::SQL(ParserError::ParserError(format!(
                "Could not convert string {:#?} to sql string.",
                string
            )))),
        e => Err(DataFusionError::SQL(ParserError::ParserError(format!(
            "Could not convert expression {:#?} to sql expression.",
            e
        )))),
    }
}

#[inline]
fn binary_operator_to_sql(op: &Operator) -> Result<BinaryOperator> {
    match op {
        Operator::Gt => Ok(BinaryOperator::Gt),
        Operator::GtEq => Ok(BinaryOperator::GtEq),
        Operator::Lt => Ok(BinaryOperator::Lt),
        Operator::LtEq => Ok(BinaryOperator::LtEq),
        Operator::Eq => Ok(BinaryOperator::Eq),
        Operator::NotEq => Ok(BinaryOperator::NotEq),
        Operator::Plus => Ok(BinaryOperator::Plus),
        Operator::Minus => Ok(BinaryOperator::Minus),
        Operator::Multiply => Ok(BinaryOperator::Multiply),
        Operator::Divide => Ok(BinaryOperator::Divide),
        Operator::Modulo => Ok(BinaryOperator::Modulo),
        Operator::And => Ok(BinaryOperator::And),
        Operator::Or => Ok(BinaryOperator::Or),
        Operator::Like => Ok(BinaryOperator::Like),
        Operator::NotLike => Ok(BinaryOperator::NotLike),
        Operator::RegexMatch => Ok(BinaryOperator::PGRegexMatch),
        Operator::RegexIMatch => Ok(BinaryOperator::PGRegexIMatch),
        Operator::RegexNotMatch => Ok(BinaryOperator::PGRegexNotMatch),
        Operator::RegexNotIMatch => Ok(BinaryOperator::PGRegexNotIMatch),
        Operator::BitwiseAnd => Ok(BinaryOperator::BitwiseAnd),
        Operator::BitwiseOr => Ok(BinaryOperator::BitwiseOr),
        Operator::StringConcat => Ok(BinaryOperator::StringConcat),
        e => Err(DataFusionError::SQL(ParserError::ParserError(format!(
            "Could not convert operator {:#?} to sql operator.",
            e
        )))),
    }
}

fn datatype_to_sql(data_type: &DataType) -> Result<SQLDataType> {
    match data_type {
        DataType::Boolean => Ok(SQLDataType::Boolean),
        DataType::Int8 => Ok(SQLDataType::TinyInt(None)),
        DataType::Int16 => Ok(SQLDataType::SmallInt(None)),
        DataType::Int32 => Ok(SQLDataType::Int(None)),
        DataType::Int64 => Ok(SQLDataType::BigInt(None)),
        DataType::Float16 => Ok(SQLDataType::Float(Some(16))),
        DataType::Float32 => Ok(SQLDataType::Float(Some(32))),
        DataType::Float64 => Ok(SQLDataType::Float(Some(64))),
        DataType::UInt8 => Ok(SQLDataType::UnsignedTinyInt(None)),
        DataType::UInt16 => Ok(SQLDataType::UnsignedSmallInt(None)),
        DataType::UInt32 => Ok(SQLDataType::UnsignedInt(None)),
        DataType::UInt64 => Ok(SQLDataType::UnsignedBigInt(None)),
        DataType::Utf8 => Ok(SQLDataType::String),
        d => Err(DataFusionError::SQL(ParserError::ParserError(format!(
            "Could not convert datatype {:#?} to sql datatype.",
            d
        )))),
    }
}
