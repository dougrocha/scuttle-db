use miette::{Result, miette};

use std::{fmt, iter::Peekable};

use super::lexer::{Lexer, Token};
use crate::keyword::Keyword;

/// A literal value in SQL (number or string).
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    /// Numeric literal
    Number(f64),

    /// String literal
    String(String),
}

/// Binary comparison operators for WHERE clauses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operator {
    /// Equality (=)
    Equal,

    /// Inequality (!=)
    NotEqual,

    /// Logical AND (not yet used)
    And,

    /// Logical OR (not yet used)
    Or,

    /// Greater than (>)
    GreaterThan,

    /// Less than (<)
    LessThan,
}

impl Operator {
    /// Returns the precedence level of this operator.
    ///
    /// Higher numbers = higher precedence. Used for parsing expressions
    /// with correct operator associativity.
    fn precedence(&self) -> u8 {
        match self {
            Operator::Or => 2,
            Operator::And => 3,
            Operator::NotEqual => 4,
            Operator::Equal => 4,
            Operator::LessThan => 5,
            Operator::GreaterThan => 5,
        }
    }
}

/// An expression in a WHERE clause.
///
/// Expressions form a tree structure representing the filtering logic.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    BinaryOp {
        /// Left operand
        left: Box<Expression>,

        /// Operator
        op: Operator,

        /// Right operand
        right: Box<Expression>,
    },

    /// Column reference (e.g., `age`, `name`)
    Column(String),

    /// Literal value (e.g., `25`, `'Alice'`)
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

/// Column list in a SELECT statement.
#[derive(Debug, Clone)]
pub enum ColumnList {
    /// SELECT * (all columns)
    All,

    /// SELECT col1, col2, ... (specific columns)
    Columns(Vec<String>),
}

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement {
    /// CREATE statement (not yet implemented)
    Create,

    /// SELECT statement
    Select {
        /// Columns to select (* or specific list)
        columns: ColumnList,

        /// Table to select from
        table: String,

        /// Optional WHERE clause
        r#where: Option<Expression>,
    },

    /// UPDATE statement (not yet implemented)
    Update {
        /// Table to update
        table: String,

        /// Columns to update
        columns: Vec<String>,

        /// New values
        values: Vec<String>,
    },

    /// INSERT statement (not yet implemented)
    Insert,

    /// DELETE statement (not yet implemented)
    Delete,
}

impl Statement {
    /// Extracts the table name from this statement.
    pub fn table_name(&self) -> &str {
        match self {
            Statement::Select { table, .. } => table,
            Statement::Update { table, .. } => table,
            _ => panic!("NOT SUPPORTED YET"),
        }
    }
}

/// SQL parser that converts tokens into an AST.
///
/// Uses recursive descent parsing with a peekable token stream.
pub struct SqlParser<'a> {
    /// Token stream from the lexer
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> SqlParser<'a> {
    /// Creates a new parser for the given SQL query string.
    pub fn new(query: &'a str) -> Self {
        Self {
            lexer: Lexer::new(query).peekable(),
        }
    }

    /// Parses the query and returns the top-level AST node (Statement).
    pub fn parse(&mut self) -> Result<Statement> {
        let Some(Ok(token)) = self.lexer.peek() else {
            return Err(miette!("Error occured while parsing"));
        };

        let statement = match token {
            Token::Keyword(keyword) => match keyword {
                Keyword::Select => self.parse_select_statement()?,
                _ => return Err(miette!("Unsupported keyword: {:?}", keyword)),
            },
            _ => return Err(miette!("Unexpected token: {:?}", token)),
        };

        Ok(statement)
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Select)?;

        let columns = self.parse_column_list()?;

        self.expect_keyword(Keyword::From)?;
        let table = match self.lexer.next() {
            Some(Ok(Token::Identifier(table_name))) => table_name.to_string(),
            _ => return Err(miette!("Expected table name after FROM")),
        };

        let r#where: Option<Expression> = match self.expect_keyword(Keyword::Where) {
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
            Some(Ok(Token::GreaterThan)) => Ok(Operator::GreaterThan),
            Some(Ok(Token::LessThan)) => Ok(Operator::LessThan),
            Some(Ok(Token::Identifier(identifier))) => {
                Err(miette!("Unexpected identifier: {:?}", identifier))
            }
            _ => Err(miette!("Expected binary operator, found none")),
        }
    }

    fn expect_keyword(&mut self, expected: Keyword) -> Result<()> {
        match self.lexer.next() {
            Some(Ok(Token::Keyword(keyword))) if keyword == expected => Ok(()),
            Some(Ok(token)) => Err(miette!("Expected '{:?}', found: {:?}", expected, token)),
            Some(Err(e)) => Err(miette!("Lexer error: {:?}", e)),
            None => Err(miette!("Expected '{:?}', found end of input", expected)),
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

    #[test]
    fn test_parser_select_with_where_clause_gt() {
        let mut parser = SqlParser::new("SELECT * FROM users WHERE id > 1");
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
                        op: Operator::GreaterThan,
                        right: Box::new(Expression::Literal(LiteralValue::Number(1.))),
                    })
                );
            }
            _ => panic!("Expected Select statement"),
        }
    }
}
