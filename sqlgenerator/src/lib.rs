use datafusion_common::{DataFusionError, Result};
use datafusion_sql::sqlparser::{
    ast::{OffsetRows, SelectItem, SetExpr, Statement, TableFactor},
    keywords::Keyword,
    parser::ParserError,
    tokenizer::{Token, Whitespace, Word},
};
use itertools::Itertools;

use crate::expr::generate_expr;

mod expr;

pub fn generate_sql(statements: Vec<Statement>) -> Result<Vec<String>> {
    Ok(generate_tokens(statements)?
        .into_iter()
        .map(|vec| vec.into_iter().map(|token| format!("{}", token)).collect())
        .collect())
}

fn generate_tokens(statements: Vec<Statement>) -> Result<Vec<Vec<Token>>> {
    statements
        .into_iter()
        .map(|statement| match statement {
            Statement::Query(query) => {
                let mut tokens = Vec::new();
                match query.body {
                    SetExpr::Select(select) => {
                        tokens.push(Token::Word(Word {
                            value: "SELECT".to_string(),
                            quote_style: None,
                            keyword: Keyword::SELECT,
                        }));
                        if select.distinct {
                            tokens.push(Token::Whitespace(Whitespace::Space));
                            tokens.push(Token::Word(Word {
                                value: "DISTINCT".to_string(),
                                quote_style: None,
                                keyword: Keyword::DISTINCT,
                            }));
                        }
                        tokens.push(Token::Whitespace(Whitespace::Space));
                        for item in select.projection {
                            match item {
                                SelectItem::UnnamedExpr(expr) => {
                                    let mut new = generate_expr(&expr)?;
                                    tokens.append(&mut new)
                                }
                                SelectItem::ExprWithAlias { expr, alias } => {
                                    let mut expr = generate_expr(&expr)?;
                                    tokens.append(&mut expr);
                                    tokens.push(Token::Whitespace(Whitespace::Space));
                                    tokens.push(Token::Word(Word {
                                        value: "AS".to_string(),
                                        quote_style: None,
                                        keyword: Keyword::AS,
                                    }));
                                    tokens.push(Token::Whitespace(Whitespace::Space));
                                    tokens.push(Token::Word(Word {
                                        value: alias.value,
                                        quote_style: None,
                                        keyword: Keyword::NoKeyword,
                                    }));
                                }
                                _ => (),
                            }
                            tokens.push(Token::Comma);
                            tokens.push(Token::Whitespace(Whitespace::Space));
                        }
                        tokens.pop();
                        tokens.pop();
                        tokens.push(Token::Whitespace(Whitespace::Space));
                        tokens.push(Token::Word(Word {
                            value: "FROM".to_string(),
                            quote_style: None,
                            keyword: Keyword::FROM,
                        }));
                        tokens.push(Token::Whitespace(Whitespace::Space));
                        for item in select.from {
                            match item.relation {
                                TableFactor::Table { name, .. } => {
                                    let mut new = name
                                        .0
                                        .into_iter()
                                        .map(|x| {
                                            Token::Word(Word {
                                                value: x.value,
                                                quote_style: None,
                                                keyword: Keyword::NoKeyword,
                                            })
                                        })
                                        .intersperse(Token::Period)
                                        .collect();
                                    tokens.append(&mut new)
                                }
                                _ => (),
                            }
                            tokens.push(Token::Comma);
                            tokens.push(Token::Whitespace(Whitespace::Space));
                        }
                        tokens.pop();
                        tokens.pop();
                        match select.selection {
                            Some(expr) => {
                                tokens.push(Token::Whitespace(Whitespace::Newline));
                                tokens.push(Token::Word(Word {
                                    value: "WHERE".to_string(),
                                    quote_style: None,
                                    keyword: Keyword::WHERE,
                                }));
                                tokens.push(Token::Whitespace(Whitespace::Space));
                                let mut filter = generate_expr(&expr)?;
                                tokens.append(&mut filter);
                            }
                            None => (),
                        }
                        Ok(())
                    }
                    q => Err(DataFusionError::SQL(ParserError::ParserError(format!(
                        "Could not generate sql for statement {:#?}.",
                        q
                    )))),
                }?;
                if !query.order_by.is_empty() {
                    tokens.push(Token::Whitespace(Whitespace::Newline));
                    tokens.push(Token::Word(Word {
                        value: "ORDER".to_string(),
                        quote_style: None,
                        keyword: Keyword::ORDER,
                    }));
                    tokens.push(Token::Whitespace(Whitespace::Space));
                    tokens.push(Token::Word(Word {
                        value: "BY".to_string(),
                        quote_style: None,
                        keyword: Keyword::BY,
                    }));
                    for order_by_expr in query.order_by {
                        tokens.push(Token::Whitespace(Whitespace::Space));
                        let mut new = generate_expr(&order_by_expr.expr)?;
                        tokens.append(&mut new);
                        match order_by_expr.asc {
                            Some(false) => tokens.push(Token::Word(Word {
                                value: "ASC".to_string(),
                                quote_style: None,
                                keyword: Keyword::ASC,
                            })),
                            _ => (),
                        }
                    }
                }
                match query.limit {
                    None => (),
                    Some(limit) => {
                        tokens.push(Token::Whitespace(Whitespace::Newline));
                        tokens.push(Token::Word(Word {
                            value: "LIMIT".to_string(),
                            quote_style: None,
                            keyword: Keyword::ORDER,
                        }));
                        tokens.push(Token::Whitespace(Whitespace::Space));
                        let mut limit = generate_expr(&limit)?;
                        tokens.append(&mut limit);
                    }
                }
                match query.offset {
                    None => (),
                    Some(offset) => {
                        tokens.push(Token::Whitespace(Whitespace::Newline));
                        tokens.push(Token::Word(Word {
                            value: "OFFSET".to_string(),
                            quote_style: None,
                            keyword: Keyword::ORDER,
                        }));
                        tokens.push(Token::Whitespace(Whitespace::Space));
                        let mut limit = generate_expr(&offset.value)?;
                        tokens.push(Token::Whitespace(Whitespace::Space));
                        match &offset.rows {
                            OffsetRows::None => (),
                            OffsetRows::Row => tokens.push(Token::Word(Word {
                                value: "ROW".to_string(),
                                quote_style: None,
                                keyword: Keyword::ORDER,
                            })),
                            OffsetRows::Rows => tokens.push(Token::Word(Word {
                                value: "ROWS".to_string(),
                                quote_style: None,
                                keyword: Keyword::ORDER,
                            })),
                        }
                        tokens.append(&mut limit);
                    }
                }
                tokens.push(Token::SemiColon);
                Ok(tokens)
            }
            s => Err(DataFusionError::SQL(ParserError::ParserError(format!(
                "Could not generate sql for statement {:#?}.",
                s
            )))),
        })
        .collect::<Result<Vec<Vec<Token>>>>()
}
