use miette::{Result, miette};

use std::{fmt, iter::Peekable};

use crate::lexer::{Lexer, Token};

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operator {
    Equal,
    NotEqual,
    And,
    Or,
}

impl Operator {
    fn precedence(&self) -> u8 {
        match self {
            Operator::Equal => 4,
            Operator::NotEqual => 4,
            Operator::And => 3,
            Operator::Or => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    BinaryOp {
        left: Box<Expression>,
        op: Operator,
        right: Box<Expression>,
    },
    Column(String),
    Literal(LiteralValue),
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::BinaryOp { left, op, right } => {
                write!(f, "({left} {op:?} {right})")
            }
            Expression::Column(name) => write!(f, "{name}"),
            Expression::Literal(value) => match value {
                LiteralValue::Number(num) => write!(f, "{num}"),
                LiteralValue::String(s) => write!(f, "\"{s}\""),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnList {
    All,
    Columns(Vec<String>),
}

#[derive(Debug, Clone)]
pub enum Statement {
    Create,
    Select {
        columns: ColumnList,
        table: String,
        r#where: Option<Expression>,
    },
    Update {
        table: String,
        columns: Vec<String>,
        values: Vec<String>,
    },
    Insert,
    Delete,
}

#[derive(Debug)]
pub enum ExecutionPlan {
    TableScan {
        table_name: String,
        columns: ColumnList,
        r#where: Option<Expression>,
    },
}

impl ExecutionPlan {
    pub fn from_statement(statement: Statement) -> Result<ExecutionPlan> {
        match statement {
            Statement::Select {
                columns,
                table,
                r#where,
            } => Ok(ExecutionPlan::TableScan {
                table_name: table,
                columns,
                r#where,
            }),
            _ => Err(miette!("Unsupported statement for execution plan")),
        }
    }
}

pub struct SqlParser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> SqlParser<'a> {
    pub fn new(query: &'a str) -> Self {
        Self {
            lexer: Lexer::new(query).peekable(),
        }
    }

    pub fn parse(&mut self) -> Result<Statement> {
        let Some(Ok(token)) = self.lexer.peek() else {
            return Err(miette!("Error occured while parsing"));
        };

        let statement = match token {
            Token::Keyword(keyword) => match *keyword {
                "SELECT" => self.parse_select_statement()?,
                _ => return Err(miette!("Unsupported keyword: {:?}", keyword)),
            },
            _ => return Err(miette!("Unexpected token: {:?}", token)),
        };

        Ok(statement)
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        self.expect_keyword("SELECT")?;

        let columns = self.parse_column_list()?;

        self.expect_keyword("FROM")?;
        let table = match self.lexer.next() {
            Some(Ok(Token::Identifier(table_name))) => table_name.to_string(),
            _ => return Err(miette!("Expected table name after FROM")),
        };

        let r#where: Option<Expression> = match self.expect_keyword("WHERE") {
            Ok(_) => {
                let expression = match self.parse_where_expression(0) {
                    Ok(expr) => expr,
                    Err(e) => panic!("Failed to parse where expression: {e}"),
                };

                Some(expression)
            }
            Err(_) => None,
        };

        Ok(Statement::Select {
            columns,
            table,
            r#where,
        })
    }

    fn parse_column_list(&mut self) -> Result<ColumnList> {
        if let Some(Ok(Token::Asterisk)) = self.lexer.peek() {
            self.lexer.next();
            return Ok(ColumnList::All);
        }

        let mut columns = Vec::new();

        loop {
            if let Some(Ok(Token::Identifier(column))) = self.lexer.next() {
                columns.push(column.to_string());
            } else {
                return Err(miette!("Expected column name"));
            }

            if let Some(Ok(Token::Comma)) = self.lexer.peek() {
                self.lexer.next();
            } else {
                break;
            }
        }

        Ok(ColumnList::Columns(columns))
    }

    fn parse_where_expression(&mut self, min_prec: u8) -> Result<Expression> {
        let mut lhs = self.parse_primary()?;

        while let Ok(op) = self.peek_binary_op() {
            if op.precedence() < min_prec {
                break;
            }
            // consume op
            self.lexer.next();

            let rhs = self.parse_where_expression(op.precedence() + 1)?;
            lhs = Expression::BinaryOp {
                left: Box::new(lhs),
                op,
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<Expression> {
        match self.lexer.next() {
            Some(Ok(Token::Identifier(identifier))) => {
                Ok(Expression::Column(identifier.to_string()))
            }
            Some(Ok(Token::Number(num))) => Ok(Expression::Literal(LiteralValue::Number(num))),
            Some(Ok(Token::String(s))) => {
                Ok(Expression::Literal(LiteralValue::String(s.to_string())))
            }
            Some(Ok(Token::Asterisk)) => Ok(Expression::Column("*".to_string())),
            token => Err(miette!("Expected primary expression, found: {:?}", token)),
        }
    }

    fn peek_binary_op(&mut self) -> Result<Operator> {
        match self.lexer.peek() {
            Some(Ok(Token::Equal)) => Ok(Operator::Equal),
            Some(Ok(Token::NotEqual)) => Ok(Operator::NotEqual),
            Some(Ok(Token::Identifier(identifier))) => {
                Err(miette!("Unexpected identifier: {:?}", identifier))
            }
            _ => Err(miette!("Expected binary operator, found none")),
        }
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<()> {
        match self.lexer.next() {
            Some(Ok(Token::Keyword(keyword))) if keyword == expected => Ok(()),
            Some(Ok(token)) => Err(miette!("Expected '{}', found: {:?}", expected, token)),
            Some(Err(e)) => Err(miette!("Lexer error: {:?}", e)),
            None => Err(miette!("Expected '{}', found end of input", expected)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let mut parser = SqlParser::new("SELECT * FROM users");
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select { columns, table, .. } => {
                assert!(matches!(columns, ColumnList::All));
                assert_eq!(table, "users".to_string());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_with_columns() {
        let mut parser = SqlParser::new("SELECT id, name FROM users");
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select { columns, table, .. } => {
                match columns {
                    ColumnList::Columns(cols) => {
                        assert_eq!(cols, vec!["id".to_string(), "name".to_string()]);
                    }
                    ColumnList::All => panic!("Expected specific columns, got All"),
                }
                assert_eq!(table, "users".to_string());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parser_select_with_where_clause() {
        let mut parser = SqlParser::new("SELECT * FROM users WHERE id = 1");
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select {
                columns,
                table,
                r#where,
            } => {
                assert!(matches!(columns, ColumnList::All));
                assert_eq!(table, "users".to_string());
                assert!(r#where.is_some());
                assert_eq!(
                    r#where,
                    Some(Expression::BinaryOp {
                        left: Box::new(Expression::Column("id".to_string())),
                        op: Operator::Equal,
                        right: Box::new(Expression::Literal(LiteralValue::Number(1.))),
                    })
                );
            }
            _ => panic!("Expected Select statement"),
        }
    }
}
