use datafusion_common::{DataFusionError, Result};
use datafusion_sql::sqlparser::{
    ast::{BinaryOperator, DataType as SQLDataType, Expr, FunctionArg, FunctionArgExpr, Value},
    keywords::Keyword,
    parser::ParserError,
    tokenizer::{Token, Whitespace, Word},
};
use itertools::Itertools;

pub(crate) fn generate_expr(expr: &Expr) -> Result<Vec<Token>> {
    match expr {
        Expr::Identifier(ident) => Ok(vec![Token::Word(Word {
            value: ident.value.clone(),
            quote_style: None,
            keyword: Keyword::NoKeyword,
        })]),
        Expr::CompoundIdentifier(idents) => Ok(idents
            .iter()
            .map(|x| {
                Token::Word(Word {
                    value: x.value.clone(),
                    quote_style: None,
                    keyword: Keyword::NoKeyword,
                })
            })
            .intersperse(Token::Period)
            .collect()),
        Expr::Value(value) => generate_value(value).map(|x| vec![x]),
        Expr::BinaryOp { left, op, right } => {
            let mut left = generate_expr(left)?;
            let mut right = generate_expr(right)?;
            let op = generate_op(op)?;
            left.push(Token::Whitespace(Whitespace::Space));
            left.push(op);
            left.push(Token::Whitespace(Whitespace::Space));
            left.append(&mut right);
            Ok(left)
        }
        Expr::Cast { expr, data_type } => {
            let mut expr = generate_expr(expr)?;
            let mut data_type = generate_datatype(data_type)?;
            let mut tokens = Vec::new();
            tokens.push(Token::Word(Word {
                value: "CAST".to_string(),
                quote_style: None,
                keyword: Keyword::CAST,
            }));
            tokens.push(Token::LParen);
            tokens.append(&mut expr);
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "AS".to_string(),
                quote_style: None,
                keyword: Keyword::AS,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.append(&mut data_type);
            tokens.push(Token::RParen);
            Ok(tokens)
        }
        Expr::IsDistinctFrom(left, right) => {
            let mut tokens = generate_expr(left)?;
            let mut right = generate_expr(right)?;
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "IS".to_string(),
                quote_style: None,
                keyword: Keyword::IS,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "DISTINCT".to_string(),
                quote_style: None,
                keyword: Keyword::DISTINCT,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "FROM".to_string(),
                quote_style: None,
                keyword: Keyword::FROM,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.append(&mut right);
            Ok(tokens)
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let mut tokens = generate_expr(left)?;
            let mut right = generate_expr(right)?;
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "IS".to_string(),
                quote_style: None,
                keyword: Keyword::IS,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "NOT".to_string(),
                quote_style: None,
                keyword: Keyword::NOT,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "DISTINCT".to_string(),
                quote_style: None,
                keyword: Keyword::DISTINCT,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "FROM".to_string(),
                quote_style: None,
                keyword: Keyword::FROM,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.append(&mut right);
            Ok(tokens)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let mut tokens = generate_expr(expr)?;
            let mut low = generate_expr(low)?;
            let mut high = generate_expr(high)?;
            if *negated {
                tokens.push(Token::Whitespace(Whitespace::Space));
                tokens.push(Token::Word(Word {
                    value: "NOT".to_string(),
                    quote_style: None,
                    keyword: Keyword::NOT,
                }));
            }
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "BETWEEN".to_string(),
                quote_style: None,
                keyword: Keyword::BETWEEN,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.append(&mut low);
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.push(Token::Word(Word {
                value: "AND".to_string(),
                quote_style: None,
                keyword: Keyword::AND,
            }));
            tokens.push(Token::Whitespace(Whitespace::Space));
            tokens.append(&mut high);
            Ok(tokens)
        }
        Expr::Function(fun) => {
            let args = fun
                .args
                .iter()
                .map(|x| match x {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => generate_expr(&expr),
                    a => Err(DataFusionError::SQL(ParserError::ParserError(format!(
                        "Could not generate sql for function argument {:#?}.",
                        a
                    )))),
                })
                .collect::<Result<Vec<Vec<Token>>>>()?;
            let mut tokens = Vec::new();
            tokens.push(Token::Word(Word {
                value: fun.name.to_string(),
                quote_style: None,
                keyword: Keyword::NoKeyword,
            }));
            tokens.push(Token::LParen);
            for mut arg in args {
                tokens.append(&mut arg);
                tokens.push(Token::Comma);
            }
            tokens.pop();
            tokens.push(Token::RParen);
            Ok(tokens)
        }
        Expr::ArrayIndex { obj, indexes } => {
            let expr = generate_expr(obj)?;
            let mut indexes = indexes
                .iter()
                .map(|x| generate_expr(x))
                .collect::<Result<Vec<Vec<Token>>>>()?
                .into_iter()
                .intersperse(vec![Token::Comma])
                .flatten()
                .collect::<Vec<Token>>();
            let mut tokens = expr;
            tokens.push(Token::LBracket);
            tokens.append(&mut indexes);
            tokens.push(Token::RBracket);
            Ok(tokens)
        }
        e => Err(DataFusionError::SQL(ParserError::ParserError(format!(
            "Could not generate sql for expression {:#?}.",
            e
        )))),
    }
}

#[inline]
fn generate_value(value: &Value) -> Result<Token> {
    match value {
        Value::Null => Ok(Token::Word(Word {
            value: "NULL".to_string(),
            quote_style: None,
            keyword: Keyword::NULL,
        })),
        Value::Boolean(bool) => Ok(Token::Word(match bool {
            true => Word {
                value: "TRUE".to_string(),
                quote_style: None,
                keyword: Keyword::TRUE,
            },
            false => Word {
                value: "FALSE".to_string(),
                quote_style: None,
                keyword: Keyword::FALSE,
            },
        })),
        Value::Number(string, bool) => Ok(Token::Number(string.to_string(), *bool)),
        Value::DoubleQuotedString(string) => Ok(Token::SingleQuotedString(string.to_string())),
        Value::SingleQuotedString(string) => Ok(Token::SingleQuotedString(string.to_string())),
        v => Err(DataFusionError::SQL(ParserError::ParserError(format!(
            "Could not generate sql for expression {:#?}.",
            v
        )))),
    }
}

#[inline]
fn generate_op(op: &BinaryOperator) -> Result<Token> {
    match op {
        BinaryOperator::Eq => Ok(Token::Eq),
        BinaryOperator::Gt => Ok(Token::Gt),
        BinaryOperator::GtEq => Ok(Token::GtEq),
        BinaryOperator::Lt => Ok(Token::Lt),
        BinaryOperator::LtEq => Ok(Token::LtEq),
        BinaryOperator::And => Ok(Token::Word(Word {
            value: "AND".to_string(),
            quote_style: None,
            keyword: Keyword::AND,
        })),
        BinaryOperator::Or => Ok(Token::Word(Word {
            value: "OR".to_string(),
            quote_style: None,
            keyword: Keyword::OR,
        })),
        BinaryOperator::Plus => Ok(Token::Plus),
        BinaryOperator::Minus => Ok(Token::Minus),
        BinaryOperator::Multiply => Ok(Token::Mul),
        BinaryOperator::Divide => Ok(Token::Div),
        BinaryOperator::Modulo => Ok(Token::Mod),
        BinaryOperator::BitwiseAnd => Ok(Token::Ampersand),
        BinaryOperator::BitwiseOr => Ok(Token::Pipe),
        BinaryOperator::BitwiseXor => Ok(Token::Caret),
        o => Err(DataFusionError::SQL(ParserError::ParserError(format!(
            "Could not generate sql for expression {:#?}.",
            o
        )))),
    }
}

#[inline]
fn generate_datatype(data_type: &SQLDataType) -> Result<Vec<Token>> {
    match data_type {
        SQLDataType::TinyInt(_) => Ok(vec![Token::Word(Word {
            value: "TINYINT".to_string(),
            quote_style: None,
            keyword: Keyword::TINYINT,
        })]),
        SQLDataType::SmallInt(_) => Ok(vec![Token::Word(Word {
            value: "SMALLINT".to_string(),
            quote_style: None,
            keyword: Keyword::SMALLINT,
        })]),
        SQLDataType::Int(_) => Ok(vec![Token::Word(Word {
            value: "INT".to_string(),
            quote_style: None,
            keyword: Keyword::INT,
        })]),
        SQLDataType::BigInt(_) => Ok(vec![Token::Word(Word {
            value: "BIGINT".to_string(),
            quote_style: None,
            keyword: Keyword::BIGINT,
        })]),
        SQLDataType::UnsignedTinyInt(_) => Ok(vec![
            Token::Word(Word {
                value: "UNSIGNED".to_string(),
                quote_style: None,
                keyword: Keyword::UNSIGNED,
            }),
            Token::Whitespace(Whitespace::Space),
            Token::Word(Word {
                value: "TINYINT".to_string(),
                quote_style: None,
                keyword: Keyword::TINYINT,
            }),
        ]),
        SQLDataType::UnsignedSmallInt(_) => Ok(vec![
            Token::Word(Word {
                value: "UNSIGNED".to_string(),
                quote_style: None,
                keyword: Keyword::UNSIGNED,
            }),
            Token::Whitespace(Whitespace::Space),
            Token::Word(Word {
                value: "SMALLINT".to_string(),
                quote_style: None,
                keyword: Keyword::SMALLINT,
            }),
        ]),
        SQLDataType::UnsignedInt(_) => Ok(vec![
            Token::Word(Word {
                value: "UNSIGNED".to_string(),
                quote_style: None,
                keyword: Keyword::UNSIGNED,
            }),
            Token::Whitespace(Whitespace::Space),
            Token::Word(Word {
                value: "INT".to_string(),
                quote_style: None,
                keyword: Keyword::INT,
            }),
        ]),
        SQLDataType::UnsignedBigInt(_) => Ok(vec![
            Token::Word(Word {
                value: "UNSIGNED".to_string(),
                quote_style: None,
                keyword: Keyword::UNSIGNED,
            }),
            Token::Whitespace(Whitespace::Space),
            Token::Word(Word {
                value: "BIGINT".to_string(),
                quote_style: None,
                keyword: Keyword::BIGINT,
            }),
        ]),
        SQLDataType::Boolean => Ok(vec![Token::Word(Word {
            value: "BOOLEAN".to_string(),
            quote_style: None,
            keyword: Keyword::BOOLEAN,
        })]),
        d => Err(DataFusionError::SQL(ParserError::ParserError(format!(
            "Could not generate sql for expression {:#?}.",
            d
        )))),
    }
}
