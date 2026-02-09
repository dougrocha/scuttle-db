use std::{borrow::Cow, iter::Peekable};

use miette::{Result, miette};

use crate::{
    DataType,
    sql::{
        lexer::{Lexer, Token},
        parser::{
            expression::IsPredicate,
            statement::{
                ColumnConstraint, ColumnDefinition, CreateStatement, FromClause, SelectStatement,
            },
        },
    },
};

pub(crate) use ast::*;
pub(crate) use keyword::Keyword;
pub(crate) use literal::Literal;
pub(crate) use operators::Operator;

pub(crate) mod ast;
pub(crate) mod keyword;
pub(crate) mod literal;
pub(crate) mod operators;

/// SQL parser that converts tokens into an AST.
///
/// Uses recursive descent parsing with a peekable token stream.
pub struct SqlParser<'src> {
    /// Token stream from the lexer
    lexer: Peekable<Lexer<'src>>,
}

impl<'src> SqlParser<'src> {
    /// Creates a new parser for the given SQL query string.
    pub fn new(query: &'src str) -> Self {
        Self {
            lexer: Lexer::new(query).peekable(),
        }
    }

    /// Parses the query and returns the top-level AST node (Statement).
    pub fn parse(&mut self) -> Result<Statement<'src>> {
        let token = self.peek_token()?;

        let statement = match token {
            Token::Keyword(keyword) => match keyword {
                Keyword::Select => self.parse_select_statement()?,
                Keyword::Create => self.parse_create_statement()?,
                _ => return Err(miette!("Unsupported keyword: {:?}", keyword)),
            },
            _ => return Err(miette!("Unexpected token: {:?}", token)),
        };

        Ok(statement)
    }

    fn parse_select_statement(&mut self) -> Result<Statement<'src>> {
        self.expect_keyword(Keyword::Select)?;

        let select_list = self.parse_targets()?;

        self.expect_keyword(Keyword::From)?;

        let table_name = self.expect_identifier()?;

        let where_clause = self
            .expect_keyword(Keyword::Where)
            .ok()
            .map(|_| self.parse_expression(0))
            .transpose()?;

        Ok(Statement::Select(SelectStatement {
            select_list,
            from_clause: FromClause { table_name },
            where_clause,
        }))
    }

    fn parse_create_statement(&mut self) -> Result<Statement<'src>> {
        self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Table)?;

        let table_name = self.expect_identifier()?;

        self.expect_token(Token::LeftParen)?;

        let mut columns = Vec::new();

        while !self.peek_is(Token::RightParen) {
            columns.push(self.parse_column_definition()?)
        }

        self.expect_token(Token::RightParen)?;

        Ok(Statement::Create(CreateStatement {
            table_name,
            if_not_exists: false,
            columns,
        }))
    }

    fn parse_targets(&mut self) -> Result<SelectList<'src>> {
        let mut columns = Vec::new();

        while !self.peek_keyword(Keyword::From) {
            let expr = self.parse_expression(0)?;

            if let Expression::Identifier(col) = &expr
                && col == "*"
            {
                columns.push(SelectTarget::Star);
                continue;
            }

            let alias = if self.consume_if(Token::Keyword(Keyword::As)) {
                Some(self.expect_identifier()?)
            } else if let Ok(Token::Identifier(_)) = self.peek_token() {
                Some(self.expect_identifier()?)
            } else {
                // no alias
                None
            };

            columns.push(SelectTarget::Expression { expr, alias });

            self.consume_if(Token::Comma);
        }

        Ok(SelectList(columns))
    }

    fn parse_expression(&mut self, min_prec: u8) -> Result<Expression<'src>> {
        let mut lhs = self.parse_primary()?;

        while let Ok(op) = self.peek_binary_op() {
            if op.precedence() < min_prec {
                break;
            }

            // consume op
            // and will consume NOT if op is 'ISNOT'
            self.next_token()?;

            let rhs = self.parse_expression(op.precedence() + 1)?;
            lhs = Expression::BinaryOp {
                left: Box::new(lhs),
                op,
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<Expression<'src>> {
        let expr = match self.next_token()? {
            Token::Keyword(kw) if kw.is_bool_literal() => {
                Expression::Literal(Literal::Bool(matches!(kw, Keyword::True)))
            }
            Token::Integer(i) => Expression::Literal(Literal::Int64(i)),
            Token::Float(f) => Expression::Literal(Literal::Float64(f)),
            Token::String(s) => Expression::Literal(Literal::Text(s)),
            Token::Identifier(i) => Expression::Identifier(i),
            Token::Asterisk => Expression::Identifier(Cow::from("*")),
            Token::LeftParen => {
                let expr = self.parse_expression(0)?;

                match self.next_token()? {
                    Token::RightParen => expr,
                    t => return Err(miette!("Expected ')', found {:?}", t)),
                }
            }
            t => {
                return Err(miette!("Expected a column or value, but found {:?}", t));
            }
        };

        self.parse_is_postfix(expr)
    }

    // Potentially parse "IS" postfix
    fn parse_is_postfix(&mut self, expr: Expression<'src>) -> Result<Expression<'src>> {
        if !self.consume_if(Token::Keyword(Keyword::Is)) {
            return Ok(expr);
        }

        let is_negated = self.consume_if(Token::Keyword(Keyword::Not));

        match self.next_token()? {
            Token::Keyword(kw @ (Keyword::True | Keyword::False | Keyword::Null)) => {
                let predicate = IsPredicate::try_from(kw).unwrap();
                Ok(Expression::Is {
                    expr: Box::new(expr),
                    predicate,
                    is_negated,
                })
            }
            t => Err(miette!("Expected TRUE/FALSE/NULL after IS, found {:?}", t)),
        }
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDefinition<'src>> {
        let name = self.expect_identifier()?;

        let Token::Keyword(data_type) = self.next_token()? else {
            return Err(miette!("Expected a column type."));
        };

        let data_type = match data_type {
            Keyword::Integer => DataType::Int64,
            Keyword::Float => DataType::Float64,
            Keyword::Varchar => {
                self.expect_token(Token::LeftParen)?;
                let size = self.expect_integer()?;
                let size: usize = size
                    .try_into()
                    .map_err(|_| miette!("VARCHAR size must be positive, got {}", size))?;
                self.expect_token(Token::RightParen)?;

                DataType::VarChar(size)
            }
            Keyword::Text => DataType::Text,
            Keyword::Timestamp => DataType::Timestamp,
            Keyword::Boolean => DataType::Bool,
            _ => return Err(miette!("Expected a column type.")),
        };

        let mut constraints = vec![];

        while !self.peek_is(Token::RightParen) && !self.peek_is(Token::Comma) {
            let constraint = match self.next_token()? {
                Token::Keyword(Keyword::Not) => {
                    if self.consume_if(Token::Keyword(Keyword::Null)) {
                        ColumnConstraint::NotNull
                    } else {
                        return Err(miette!("Expected 'NULL' after 'NOT'"));
                    }
                }
                Token::Keyword(Keyword::Primary) => {
                    if self.consume_if(Token::Keyword(Keyword::Key)) {
                        ColumnConstraint::PrimaryKey
                    } else {
                        return Err(miette!("Expected 'KEY' after 'PRIMARY'"));
                    }
                }
                Token::Keyword(Keyword::Unique) => ColumnConstraint::Unique,
                t => {
                    return Err(miette!(
                        "Unexpected token '{:?}' while parsing constraints",
                        t
                    ));
                }
            };

            constraints.push(constraint);
        }

        // optionally consume comma
        self.consume_if(Token::Comma);

        Ok(ColumnDefinition {
            name,
            data_type,
            constraints,
        })
    }

    fn peek_binary_op(&mut self) -> Result<Operator> {
        match self.peek_token()? {
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

    fn next_token(&mut self) -> Result<Token<'src>> {
        self.lexer
            .next()
            .transpose()?
            .ok_or_else(|| miette!("Unexpected end of input"))
    }

    fn expect_token(&mut self, expected: Token<'src>) -> Result<()> {
        let token = self.next_token()?;
        if token == expected {
            Ok(())
        } else {
            Err(miette!("Expected {:?}, found {:?}", expected, token))
        }
    }

    fn peek_token(&mut self) -> Result<&Token<'src>> {
        match self.lexer.peek() {
            Some(Ok(token)) => Ok(token),
            Some(Err(_)) => Err(miette!("Lexer error occurred")),
            None => Err(miette!("Unexpected end of input")),
        }
    }

    fn peek_is(&mut self, expected: Token) -> bool {
        matches!(self.lexer.peek(), Some(Ok(token)) if *token == expected)
    }

    fn peek_keyword(&mut self, expected: Keyword) -> bool {
        matches!(self.lexer.peek(), Some(Ok(Token::Keyword(kw))) if *kw == expected)
    }

    fn consume_if(&mut self, expected: Token) -> bool {
        if self.peek_is(expected) {
            self.lexer.next();
            true
        } else {
            false
        }
    }

    fn expect_identifier(&mut self) -> Result<Cow<'src, str>> {
        match self.next_token()? {
            Token::Identifier(ident) => Ok(ident),
            got => Err(miette!("Expected IDENTIFIER, but found {:?}", got)),
        }
    }

    fn expect_integer(&mut self) -> Result<i64> {
        match self.next_token()? {
            Token::Integer(n) => Ok(n),
            other => Err(miette!("Expected integer, found {:?}", other)),
        }
    }

    fn expect_float(&mut self) -> Result<f64> {
        match self.next_token()? {
            Token::Float(n) => Ok(n),
            other => Err(miette!("Expected integer, found {:?}", other)),
        }
    }

    fn expect_keyword(&mut self, expected: Keyword) -> Result<()> {
        match self.next_token()? {
            Token::Keyword(kw) if kw == expected => Ok(()),
            other => Err(miette!("Expected {:?}, found {:?}", expected, other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::{SelectStatement, statement::ColumnConstraint};

    use super::*;

    /// Helper to parse a query and extract components
    fn parse(query: &str) -> Statement<'_> {
        let mut parser = SqlParser::new(query);
        parser.parse().expect("Failed to parse query")
    }

    /// Helper to parse and extract WHERE expression
    fn parse_where(query: &str) -> Expression<'_> {
        match parse(query) {
            Statement::Select(SelectStatement {
                where_clause: Some(expr),
                ..
            }) => expr,
            _ => panic!("Expected SELECT with WHERE clause"),
        }
    }

    #[test]
    fn test_parse_select_all() {
        match parse("SELECT * FROM users") {
            Statement::Select(SelectStatement {
                select_list,
                from_clause,
                where_clause,
            }) => {
                assert_eq!(select_list.0, vec![SelectTarget::Star]);
                assert_eq!(
                    from_clause,
                    FromClause {
                        table_name: Cow::from("users"),
                    }
                );
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_multiple() {
        match parse("SELECT id, name FROM users") {
            Statement::Select(SelectStatement { select_list, .. }) => {
                assert_eq!(
                    select_list.0,
                    vec![
                        SelectTarget::Expression {
                            expr: Expression::Identifier(Cow::from("id")),
                            alias: None,
                        },
                        SelectTarget::Expression {
                            expr: Expression::Identifier(Cow::from("name")),
                            alias: None,
                        }
                    ]
                );
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_with_implicit_alias() {
        match parse("SELECT id user_id FROM users") {
            Statement::Select(SelectStatement { select_list, .. }) => {
                assert_eq!(
                    select_list.0,
                    vec![SelectTarget::Expression {
                        expr: Expression::Identifier(Cow::from("id")),
                        alias: Some(Cow::from("user_id")),
                    }]
                );
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_select_columns() {
        match parse("SELECT id as \"Identity\", name AS firstName FROM users") {
            Statement::Select(SelectStatement { select_list, .. }) => {
                assert_eq!(
                    select_list.0,
                    vec![
                        SelectTarget::Expression {
                            expr: Expression::Identifier(Cow::from("id")),
                            alias: Some(Cow::from("Identity")),
                        },
                        SelectTarget::Expression {
                            expr: Expression::Identifier(Cow::from("name")),
                            alias: Some(Cow::from("firstName")),
                        },
                    ]
                );
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
                left: Box::new(Expression::Identifier(Cow::from("id"))),
                op: Operator::Equal,
                right: Box::new(Expression::Literal(Literal::Int64(1))),
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
                assert!(matches!(*a, Expression::Identifier(_)));
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
            assert_eq!(*right, Expression::Literal(Literal::Int64(42)));
        }

        // Float
        let expr = parse_where("SELECT * FROM t WHERE x = 3.15");
        if let Expression::BinaryOp { right, .. } = expr {
            assert_eq!(*right, Expression::Literal(Literal::Float64(3.15)));
        }

        // Boolean
        let expr = parse_where("SELECT * FROM t WHERE active = TRUE");
        if let Expression::BinaryOp { right, .. } = expr {
            assert_eq!(*right, Expression::Literal(Literal::Bool(true)));
        }
    }

    #[test]
    fn test_parse_create_table() {
        match parse(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, active BOOL UNIQUE)",
        ) {
            Statement::Create(CreateStatement {
                table_name,
                if_not_exists,
                columns,
            }) => {
                assert_eq!(table_name, Cow::from("users"));
                assert!(!if_not_exists);
                assert_eq!(
                    columns,
                    vec![
                        ColumnDefinition {
                            name: Cow::from("id"),
                            data_type: DataType::Int64,
                            constraints: vec![ColumnConstraint::PrimaryKey],
                        },
                        ColumnDefinition {
                            name: Cow::from("name"),
                            data_type: DataType::Text,
                            constraints: vec![ColumnConstraint::NotNull],
                        },
                        ColumnDefinition {
                            name: Cow::from("active"),
                            data_type: DataType::Bool,
                            constraints: vec![ColumnConstraint::Unique],
                        },
                    ]
                );
            }
            _ => panic!("Expected CREATE statement"),
        }
    }
}
