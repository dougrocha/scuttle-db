use std::iter::Peekable;

pub(crate) use ast::*;
pub(crate) use literal_value::LiteralValue;
use miette::{Result, miette};
pub(crate) use operators::Operator;

use crate::{
    keyword::Keyword,
    sql::lexer::{Lexer, Token},
};

pub(crate) mod ast;
pub(crate) mod literal_value;
pub(crate) mod operators;

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

        let columns = self.parse_targets()?;

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
            targets: columns,
            table,
            r#where,
        })
    }

    fn parse_targets(&mut self) -> Result<TargetList> {
        let mut columns = Vec::new();

        while let Some(Ok(token)) = self.lexer.peek() {
            if matches!(token, Token::Keyword(Keyword::From)) {
                break;
            }

            let expr = self.parse_expression(0)?;

            if let Expression::Column(col) = &expr
                && col == "*"
            {
                columns.push(SelectTarget::Star);
            } else {
                let alias = match self.lexer.peek() {
                    Some(Ok(Token::Keyword(Keyword::As))) => {
                        // consume AS
                        self.lexer.next();
                        match self.lexer.next() {
                            Some(Ok(Token::Identifier(name))) => Some(name.to_string()),
                            Some(Ok(_)) => return Err(miette!("Expected identifier after AS")),
                            _ => return Err(miette!("Unexpected EOF")),
                        }
                    }
                    _ => None,
                };

                columns.push(SelectTarget::Expression {
                    expr: expr.clone(),
                    alias,
                });
            }

            if let Some(Ok(Token::Comma)) = self.lexer.peek() {
                self.lexer.next();
            }
        }

        Ok(columns)
    }

    fn parse_expression(&mut self, min_prec: u8) -> Result<Expression> {
        let mut lhs = self.parse_primary()?;

        while let Ok(op) = self.peek_binary_op() {
            if op.precedence() < min_prec {
                break;
            }
            // consume op
            // will consume NOT if op is 'ISNOT'
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

        let expr = match token {
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

        self.parse_is_postfix(expr)
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

    // Potentially parse "IS" postfix
    fn parse_is_postfix(&mut self, expr: Expression) -> Result<Expression> {
        // Check if next token is IS keyword
        if !matches!(self.lexer.peek(), Some(Ok(Token::Keyword(Keyword::Is)))) {
            return Ok(expr); // No IS, return expression as-is
        }

        self.lexer.next(); // Consume IS

        // Check for optional NOT
        let is_negated = if matches!(self.lexer.peek(), Some(Ok(Token::Keyword(Keyword::Not)))) {
            self.lexer.next(); // Consume NOT
            true
        } else {
            false
        };

        // Expect TRUE/FALSE/NULL
        match self.lexer.next() {
            Some(Ok(Token::Null)) => Ok(Expression::Is {
                expr: Box::new(expr),
                predicate: IsPredicate::Null,
                is_negated,
            }),
            Some(Ok(Token::Boolean(bool))) => Ok(Expression::Is {
                expr: Box::new(expr),
                predicate: if bool {
                    IsPredicate::True
                } else {
                    IsPredicate::False
                },
                is_negated,
            }),
            Some(Ok(t)) => Err(miette!("Expected TRUE/FALSE/NULL after IS, found {:?}", t)),
            Some(Err(e)) => Err(e),
            None => Err(miette!("Expected TRUE/FALSE/NULL after IS, found EOF")),
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

    /// Helper to parse a query and extract components
    fn parse(query: &str) -> Statement {
        let mut parser = SqlParser::new(query);
        parser.parse().expect("Failed to parse query")
    }

    /// Helper to parse and extract WHERE expression
    fn parse_where(query: &str) -> Expression {
        match parse(query) {
            Statement::Select {
                r#where: Some(expr),
                ..
            } => expr,
            _ => panic!("Expected SELECT with WHERE clause"),
        }
    }

    #[test]
    fn test_parse_select_all() {
        match parse("SELECT * FROM users") {
            Statement::Select {
                targets: columns,
                table,
                r#where,
            } => {
                assert_eq!(columns, vec![SelectTarget::Star]);
                assert_eq!(table, "users");
                assert!(r#where.is_none());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_columns() {
        match parse("SELECT id as \"Identity\", name AS firstName FROM users") {
            Statement::Select {
                targets: columns,
                table,
                ..
            } => {
                assert_eq!(
                    columns,
                    vec![
                        SelectTarget::Expression {
                            expr: Expression::Column("id".to_string()),
                            alias: Some("Identity".to_string()),
                        },
                        SelectTarget::Expression {
                            expr: Expression::Column("name".to_string()),
                            alias: Some("firstName".to_string()),
                        },
                    ]
                );
                assert_eq!(table, "users");
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_where_simple_comparison() {
        let expr = parse_where("SELECT * FROM users WHERE id = 1");
        assert_eq!(
            expr,
            Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: Operator::Equal,
                right: Box::new(Expression::Literal(LiteralValue::Integer(1))),
            }
        );
    }

    #[test]
    fn test_parse_where_operators() {
        // Greater than
        let expr = parse_where("SELECT * FROM users WHERE age > 18");
        assert!(matches!(
            expr,
            Expression::BinaryOp {
                op: Operator::GreaterThan,
                ..
            }
        ));

        // Less than
        let expr = parse_where("SELECT * FROM users WHERE age < 65");
        assert!(matches!(
            expr,
            Expression::BinaryOp {
                op: Operator::LessThan,
                ..
            }
        ));

        // Not equal
        let expr = parse_where("SELECT * FROM users WHERE status != 'active'");
        assert!(matches!(
            expr,
            Expression::BinaryOp {
                op: Operator::NotEqual,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_where_logical_operators() {
        // AND
        let expr = parse_where("SELECT * FROM users WHERE age > 18 AND status = 'active'");
        assert!(matches!(
            expr,
            Expression::BinaryOp {
                op: Operator::And,
                ..
            }
        ));

        // OR
        let expr = parse_where("SELECT * FROM users WHERE age < 18 OR age > 65");
        assert!(matches!(
            expr,
            Expression::BinaryOp {
                op: Operator::Or,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_where_arithmetic() {
        // Addition
        let expr = parse_where("SELECT * FROM users WHERE age + 5 > 30");
        if let Expression::BinaryOp {
            left,
            op: Operator::GreaterThan,
            ..
        } = expr
        {
            assert!(matches!(
                *left,
                Expression::BinaryOp {
                    op: Operator::Add,
                    ..
                }
            ));
        } else {
            panic!("Expected comparison with arithmetic");
        }

        // Multiplication (higher precedence)
        let expr = parse_where("SELECT * FROM users WHERE price * 2 > 100");
        if let Expression::BinaryOp {
            left,
            op: Operator::GreaterThan,
            ..
        } = expr
        {
            assert!(matches!(
                *left,
                Expression::BinaryOp {
                    op: Operator::Multiply,
                    ..
                }
            ));
        } else {
            panic!("Expected comparison with arithmetic");
        }
    }

    #[test]
    fn test_parse_where_parentheses() {
        let expr =
            parse_where("SELECT * FROM users WHERE (age > 18 AND age < 65) OR status = 'admin'");
        // Should parse with correct precedence
        assert!(matches!(
            expr,
            Expression::BinaryOp {
                op: Operator::Or,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_operator_precedence() {
        // Multiplication before addition: a + b * c should parse as a + (b * c)
        let expr = parse_where("SELECT * FROM t WHERE a + b * c > 10");
        if let Expression::BinaryOp {
            left,
            op: Operator::GreaterThan,
            ..
        } = expr
        {
            if let Expression::BinaryOp {
                left: a,
                op: Operator::Add,
                right: bc,
            } = *left
            {
                assert!(matches!(*a, Expression::Column(_)));
                assert!(matches!(
                    *bc,
                    Expression::BinaryOp {
                        op: Operator::Multiply,
                        ..
                    }
                ));
            } else {
                panic!("Expected a + (b * c) structure");
            }
        }
    }

    #[test]
    fn test_parse_literal_types() {
        // Integer
        let expr = parse_where("SELECT * FROM t WHERE x = 42");
        if let Expression::BinaryOp { right, .. } = expr {
            assert_eq!(*right, Expression::Literal(LiteralValue::Integer(42)));
        }

        // Float
        let expr = parse_where("SELECT * FROM t WHERE x = 3.15");
        if let Expression::BinaryOp { right, .. } = expr {
            assert_eq!(*right, Expression::Literal(LiteralValue::Float(3.15)));
        }

        // Boolean
        let expr = parse_where("SELECT * FROM t WHERE active = TRUE");
        if let Expression::BinaryOp { right, .. } = expr {
            assert_eq!(*right, Expression::Literal(LiteralValue::Boolean(true)));
        }
    }
}
