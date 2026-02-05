use miette::{Result, miette};

use std::{fmt, iter::Peekable};

use super::lexer::{Lexer, Token};
use crate::keyword::Keyword;

/// A literal value in SQL
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

/// Binary comparison operators for WHERE clauses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operator {
    /// Equality (=)
    Equal,
    NotEqual,

    /// Logical AND
    And,
    /// Logical OR
    Or,

    /// Greater than (>)
    GreaterThan,
    GreaterThanEqual,

    /// Less than (<)
    LessThan,
    LessThanEqual,

    Add,
    Multiply,
    Divide,
    Subtract,
}

impl Operator {
    /// Returns the binding power (precedence) of this operator.
    ///
    /// This defines the "Order of Operations". Operators with a higher number
    /// bind tighter and are evaluated first.
    fn precedence(&self) -> u8 {
        match self {
            Operator::Or => 2,
            Operator::And => 3,
            Operator::NotEqual
            | Operator::Equal
            | Operator::LessThan
            | Operator::LessThanEqual
            | Operator::GreaterThan
            | Operator::GreaterThanEqual => 5,
            Operator::Add | Operator::Subtract => 7,
            Operator::Multiply | Operator::Divide => 10,
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
                LiteralValue::Float(num) => write!(f, "{num}"),
                LiteralValue::Integer(num) => write!(f, "{num}"),
                LiteralValue::String(s) => write!(f, "\"{s}\""),
                LiteralValue::Boolean(bool) => write!(f, "{}", bool.to_string().to_uppercase()),
                LiteralValue::Null => write!(f, "NULL"),
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

        let r#where = match self.expect_keyword(Keyword::Where) {
            Ok(_) => Some(self.parse_expression(0)?),
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

    fn parse_expression(&mut self, min_prec: u8) -> Result<Expression> {
        let mut lhs = self.parse_primary()?;

        while let Ok(op) = self.peek_binary_op() {
            if op.precedence() < min_prec {
                break;
            }
            // consume op
            self.lexer.next();

            let rhs = self.parse_expression(op.precedence() + 1)?;
            lhs = Expression::BinaryOp {
                left: Box::new(lhs),
                op,
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<Expression> {
        let token = self
            .lexer
            .next()
            .ok_or(miette!("Unexpected end of input"))??;

        let token = match token {
            Token::Boolean(b) => Expression::Literal(LiteralValue::Boolean(b)),
            Token::Integer(i) => Expression::Literal(LiteralValue::Integer(i)),
            Token::Float(f) => Expression::Literal(LiteralValue::Float(f)),
            Token::String(s) => Expression::Literal(LiteralValue::String(s.to_string())),

            Token::Identifier(i) => Expression::Column(i.to_string()),

            Token::Asterisk => Expression::Column("*".to_string()),

            Token::LeftParen => {
                let expr = self.parse_expression(0)?;

                match self.lexer.next() {
                    Some(Ok(Token::RightParen)) => expr,
                    Some(Ok(t)) => return Err(miette!("Expected ')', found {:?}", t)),
                    Some(Err(e)) => return Err(e),
                    None => return Err(miette!("Expected ')', found EOF")),
                }
            }

            t => {
                return Err(miette!("Expected a column or value, but found {:?}", t));
            }
        };

        Ok(token)
    }

    fn peek_binary_op(&mut self) -> Result<Operator> {
        let Some(Ok(token)) = self.lexer.peek() else {
            return Err(miette!("Unexpected end of input"));
        };

        match token {
            Token::Equal => Ok(Operator::Equal),
            Token::NotEqual => Ok(Operator::NotEqual),
            Token::GreaterThan => Ok(Operator::GreaterThan),
            Token::LessThan => Ok(Operator::LessThan),
            Token::GreaterThanEqual => Ok(Operator::GreaterThanEqual),
            Token::LessThanEqual => Ok(Operator::LessThanEqual),

            Token::Plus => Ok(Operator::Add),
            Token::Minus => Ok(Operator::Subtract),
            Token::Asterisk => Ok(Operator::Multiply),
            Token::Slash => Ok(Operator::Divide),

            Token::Keyword(Keyword::And) => Ok(Operator::And),
            Token::Keyword(Keyword::Or) => Ok(Operator::Or),

            t => Err(miette!("Expected a binary operator, but found {:?}", t)),
        }
    }

    fn expect_keyword(&mut self, expected: Keyword) -> Result<()> {
        let token = self
            .lexer
            .next()
            .ok_or(miette!("Unexpected end of input"))??;

        match token {
            Token::Keyword(keyword) if keyword == expected => Ok(()),
            found => Err(miette!(
                "Expected keyword {:?}, but found {:?}",
                expected,
                found
            )),
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
                        right: Box::new(Expression::Literal(LiteralValue::Integer(1))),
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
                        right: Box::new(Expression::Literal(LiteralValue::Integer(1))),
                    })
                );
            }
            _ => panic!("Expected Select statement"),
        }
    }
}
