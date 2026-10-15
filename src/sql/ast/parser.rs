use std::{borrow::Cow, iter::Peekable};

use miette::{Result, miette};

use crate::{
    ColumnDef, DataType, Value,
    db::table::column_def::ColumnConstraint,
    sql::{
        ast::{
            expression::{Expression, FunctionArgs},
            keyword::Keyword,
            operator::Operator,
            predicate::IsPredicate,
            statement::{
                CreateStatement, DeleteStatement, From, InsertStatement, JoinClause, JoinKind,
                OrderByItem, SelectStatement, Statement, TableRef, UpdateStatement,
            },
            target::{SelectList, SelectTarget},
        },
        lexer::{Lexer, Token},
    },
};

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
    pub fn parse(&mut self) -> Result<Statement> {
        let token = self.peek_token()?;

        let statement = match token {
            Token::Keyword(keyword) => match keyword {
                Keyword::Select => self.parse_select_statement()?,
                Keyword::Create => self.parse_create_statement()?,
                Keyword::Insert => self.parse_insert_statement()?,
                Keyword::Update => self.parse_update_statement()?,
                Keyword::Delete => self.parse_delete_statement()?,
                _ => return Err(miette!("Unsupported keyword: {:?}", keyword)),
            },
            _ => return Err(miette!("Unexpected token: {:?}", token)),
        };

        // Anything left over is a clause we don't understand. Reject it instead of
        // silently running a different query than the one that was written.
        self.consume_if(Token::SemiColon);
        if let Some(token) = self.lexer.next() {
            return Err(miette!("Unexpected {:?} after end of statement", token?));
        }

        Ok(statement)
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Select)?;

        let select_list = self.parse_targets()?;

        self.expect_keyword(Keyword::From)?;

        let from_clause = self.parse_from_clause()?;

        let where_clause = self.parse_where_clause()?;

        let mut group_by = Vec::new();
        if self.consume_if(Token::Keyword(Keyword::Group)) {
            self.expect_keyword(Keyword::By)?;
            loop {
                group_by.push(self.parse_expression(0)?);
                if !self.consume_if(Token::Comma) {
                    break;
                }
            }
        }

        let mut order_by = Vec::new();
        if self.consume_if(Token::Keyword(Keyword::Order)) {
            self.expect_keyword(Keyword::By)?;
            loop {
                let expr = self.parse_expression(0)?;
                let descending = if self.consume_if(Token::Keyword(Keyword::Desc)) {
                    true
                } else {
                    self.consume_if(Token::Keyword(Keyword::Asc));
                    false
                };
                order_by.push(OrderByItem { expr, descending });

                if !self.consume_if(Token::Comma) {
                    break;
                }
            }
        }

        let limit = if self.consume_if(Token::Keyword(Keyword::Limit)) {
            Some(self.expect_count("LIMIT")?)
        } else {
            None
        };

        let offset = if self.consume_if(Token::Keyword(Keyword::Offset)) {
            Some(self.expect_count("OFFSET")?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            select_list,
            from_clause,
            where_clause,
            group_by,
            order_by,
            limit,
            offset,
        }))
    }

    /// FROM <table_ref> { [INNER] JOIN | LEFT [OUTER] JOIN } <table_ref> ON <expr> ...
    ///
    /// # Errors
    /// RIGHT, FULL and CROSS joins are rejected. A join without ON is an error.
    fn parse_from_clause(&mut self) -> Result<From> {
        let table = self.parse_table_ref()?;
        let mut joins = Vec::new();

        loop {
            let kind = if self.consume_if(Token::Keyword(Keyword::Join)) {
                JoinKind::Inner
            } else if self.consume_if(Token::Keyword(Keyword::Inner)) {
                self.expect_keyword(Keyword::Join)?;
                JoinKind::Inner
            } else if self.consume_if(Token::Keyword(Keyword::Left)) {
                self.consume_if(Token::Keyword(Keyword::Outer));
                self.expect_keyword(Keyword::Join)?;
                JoinKind::Left
            } else if self.peek_keyword(Keyword::Right) {
                return Err(miette!("RIGHT JOIN is not supported"));
            } else if self.peek_keyword(Keyword::Full) {
                return Err(miette!("FULL JOIN is not supported"));
            } else if self.peek_keyword(Keyword::Cross) {
                return Err(miette!("CROSS JOIN is not supported"));
            } else {
                // FULL and CROSS are reserved keywords ONLY so they can be rejected here:
                // as identifiers, `users FULL JOIN orders` would parse as alias `full` plus
                // an INNER JOIN and silently return wrong rows.
                break;
            };

            let table = self.parse_table_ref()?;
            self.expect_keyword(Keyword::On)?;
            // parse_expression(0) stops at JOIN / INNER / LEFT / WHERE / GROUP, because
            // peek_binary_op() fails on them and the loop in parse_expression ends.
            let on = self.parse_expression(0)?;
            joins.push(JoinClause { kind, table, on });
        }

        Ok(From { table, joins })
    }

    /// `name`, `name alias` or `name AS alias`.
    ///
    /// The bare-alias form is taken when the next token is an Identifier. Keywords
    /// (WHERE, JOIN, LEFT, ...) are never identifiers, so they end the table ref.
    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let name = self.expect_identifier()?.to_string();
        let alias = if self.consume_if(Token::Keyword(Keyword::As)) {
            Some(self.expect_identifier()?.to_string())
        } else if let Ok(Token::Identifier(_)) = self.peek_token() {
            Some(self.expect_identifier()?.to_string())
        } else {
            None
        };
        Ok(TableRef { name, alias })
    }

    fn parse_update_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Update)?;

        let table_name = self.expect_identifier()?;

        self.expect_keyword(Keyword::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.expect_identifier()?.to_string();
            self.expect_token(Token::Equal)?;
            let value = self.parse_expression(0)?;
            assignments.push((column, value));

            if !self.consume_if(Token::Comma) {
                break;
            }
        }

        let where_clause = self.parse_where_clause()?;

        Ok(Statement::Update(UpdateStatement {
            table: table_name.to_string(),
            assignments,
            where_clause,
        }))
    }

    fn parse_delete_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        let table_name = self.expect_identifier()?;

        let where_clause = self.parse_where_clause()?;

        Ok(Statement::Delete(DeleteStatement {
            table: table_name.to_string(),
            where_clause,
        }))
    }

    /// Parses an optional `WHERE <expr>`.
    fn parse_where_clause(&mut self) -> Result<Option<Expression>> {
        if self.consume_if(Token::Keyword(Keyword::Where)) {
            Ok(Some(self.parse_expression(0)?))
        } else {
            Ok(None)
        }
    }

    fn parse_create_statement(&mut self) -> Result<Statement> {
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
            table_name: table_name.to_string(),
            columns,
        }))
    }

    fn parse_insert_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;

        let table_name = self.expect_identifier()?;

        let mut columns = None;
        if self.consume_if(Token::LeftParen) {
            let mut cols = Vec::new();
            while !self.consume_if(Token::RightParen) {
                self.consume_if(Token::Comma);

                cols.push(self.expect_identifier()?.to_string());
            }

            columns = Some(cols);
        }

        let mut values = Vec::new();
        if self.peek_keyword(Keyword::Values) {
            self.expect_keyword(Keyword::Values)?;

            loop {
                let mut row_vals = Vec::new();
                if self.consume_if(Token::LeftParen) {
                    while !self.consume_if(Token::RightParen) {
                        self.consume_if(Token::Comma);

                        row_vals.push(self.parse_expression(0)?);
                    }
                }

                values.push(row_vals);

                if !self.consume_if(Token::Comma) {
                    break;
                }
            }
        } else {
            return Err(miette!(
                "Expected either VALUES or SELECT, found {:?}",
                self.peek_token()?,
            ));
        }

        Ok(Statement::Insert(InsertStatement {
            table: table_name.to_string(),
            columns,
            source: values,
        }))
    }

    fn parse_targets(&mut self) -> Result<SelectList> {
        let mut columns = Vec::new();

        while !self.peek_keyword(Keyword::From) {
            // `a.*` lexes as Identifier("a.") followed by Asterisk. Handle it before
            // parse_expression, which would read it as a multiplication.
            if let Some(Ok(Token::Identifier(word))) = self.lexer.peek()
                && let Some(relation) = word.strip_suffix('.')
            {
                let relation = relation.to_string();
                self.lexer.next(); // the "a." word
                self.expect_token(Token::Asterisk)?;
                columns.push(SelectTarget::QualifiedStar(relation));
                self.consume_if(Token::Comma);
                continue;
            }

            let expr = self.parse_expression(0)?;

            if let Expression::Identifier(col) = &expr
                && col == "*"
            {
                columns.push(SelectTarget::Star);
                self.consume_if(Token::Comma);
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

            columns.push(SelectTarget::Expression {
                expr,
                alias: alias.map(|s| s.to_string()),
            });

            self.consume_if(Token::Comma);
        }

        Ok(SelectList(columns))
    }

    fn parse_expression(&mut self, min_prec: u8) -> Result<Expression> {
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

    fn parse_primary(&mut self) -> Result<Expression> {
        let expr = match self.next_token()? {
            Token::Keyword(kw) if kw.is_bool_literal() => {
                Expression::Literal(Value::from(matches!(kw, Keyword::True)))
            }
            Token::Keyword(Keyword::Null) => Expression::Literal(Value::Null),
            Token::Integer(i) => Expression::Literal(Value::Int64(i)),
            Token::Float(f) => Expression::Literal(Value::Float64(f)),
            Token::String(s) => Expression::Literal(Value::Text(s.to_string())),
            Token::Identifier(name) if self.peek_is(Token::LeftParen) => {
                self.parse_function_call(name.to_string())?
            }
            Token::Identifier(i) => Self::column_from_word(&i)?,
            Token::Asterisk => Expression::Identifier("*".to_string()),
            Token::LeftParen => {
                let expr = self.parse_expression(0)?;

                self.expect_token(Token::RightParen)?;

                expr
            }
            t => {
                return Err(miette!("Expected a column or value, but found {:?}", t));
            }
        };

        self.parse_is_postfix(expr)
    }

    /// Turns the lexer's word into a column reference.  The lexer does NOT stop at '.',
    /// so `u.name` arrives as one word.  Splits on the first '.'.
    ///
    /// Known limitation: the lexer drops quote information, so a quoted identifier
    /// that contains a dot (`"a.b"`) is also split.
    ///
    /// # Errors
    /// `a.`, `.a`, `a.b.c` -> `Invalid column reference "<word>"`.
    fn column_from_word(word: &str) -> Result<Expression> {
        match word.split_once('.') {
            None => Ok(Expression::Identifier(word.to_string())),
            Some((rel, col)) if !rel.is_empty() && !col.is_empty() && !col.contains('.') => {
                Ok(Expression::Qualified {
                    relation: rel.to_string(),
                    column: col.to_string(),
                })
            }
            Some(_) => Err(miette!("Invalid column reference \"{word}\"")),
        }
    }

    /// Parses the `(...)` after a function name.
    fn parse_function_call(&mut self, name: String) -> Result<Expression> {
        self.expect_token(Token::LeftParen)?;

        let args = if self.consume_if(Token::Asterisk) {
            self.expect_token(Token::RightParen)?;
            FunctionArgs::Star
        } else {
            let mut args = Vec::new();
            while !self.consume_if(Token::RightParen) {
                if !args.is_empty() {
                    self.expect_token(Token::Comma)?;
                }
                args.push(self.parse_expression(0)?);
            }
            FunctionArgs::List(args)
        };

        Ok(Expression::Function { name, args })
    }

    // Potentially parse "IS" postfix
    fn parse_is_postfix(&mut self, expr: Expression) -> Result<Expression> {
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

    fn parse_column_definition(&mut self) -> Result<ColumnDef> {
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
                Token::Keyword(Keyword::Null) => ColumnConstraint::Nullable,
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

        Ok(ColumnDef {
            name: name.to_string(),
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

    /// Expects a non-negative integer, as used by LIMIT and OFFSET.
    fn expect_count(&mut self, clause: &str) -> Result<u64> {
        let n = self.expect_integer()?;
        n.try_into()
            .map_err(|_| miette!("{clause} must not be negative, got {n}"))
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
    use super::*;

    /// Helper to parse a query and extract components
    fn parse(query: &str) -> Statement {
        let mut parser = SqlParser::new(query);
        parser.parse().expect("Failed to parse query")
    }

    /// Helper to parse and extract WHERE expression
    fn parse_where(query: &str) -> Expression {
        match parse(query) {
            Statement::Select(SelectStatement {
                where_clause: Some(expr),
                ..
            }) => expr,
            _ => panic!("Expected SELECT with WHERE clause"),
        }
    }

    /// Helper to build `left = right` with bare column names
    fn column_eq(left: &str, right: &str) -> Expression {
        Expression::BinaryOp {
            left: Box::new(Expression::Identifier(left.to_string())),
            op: Operator::Equal,
            right: Box::new(Expression::Identifier(right.to_string())),
        }
    }

    #[test]
    fn test_parse_from_alias_without_as() {
        assert_eq!(
            parse_select("SELECT * FROM users u").from_clause,
            From {
                table: TableRef {
                    name: "users".to_string(),
                    alias: Some("u".to_string()),
                },
                joins: vec![],
            }
        );
    }

    #[test]
    fn test_parse_from_alias_with_as() {
        assert_eq!(
            parse_select("SELECT * FROM users AS u").from_clause,
            From {
                table: TableRef {
                    name: "users".to_string(),
                    alias: Some("u".to_string()),
                },
                joins: vec![],
            }
        );
    }

    #[test]
    fn test_parse_inner_join() {
        assert_eq!(
            parse_select("SELECT * FROM a JOIN b ON x = y").from_clause,
            From {
                table: TableRef::new("a"),
                joins: vec![JoinClause {
                    kind: JoinKind::Inner,
                    table: TableRef::new("b"),
                    on: column_eq("x", "y"),
                }],
            }
        );
    }

    #[test]
    fn test_parse_inner_keyword_join() {
        assert_eq!(
            parse_select("SELECT * FROM a INNER JOIN b ON x = y").from_clause,
            parse_select("SELECT * FROM a JOIN b ON x = y").from_clause
        );
    }

    #[test]
    fn test_parse_left_and_left_outer_join() {
        let expected = vec![JoinClause {
            kind: JoinKind::Left,
            table: TableRef::new("b"),
            on: column_eq("x", "y"),
        }];
        assert_eq!(
            parse_select("SELECT * FROM a LEFT JOIN b ON x = y")
                .from_clause
                .joins,
            expected
        );
        assert_eq!(
            parse_select("SELECT * FROM a LEFT OUTER JOIN b ON x = y")
                .from_clause
                .joins,
            expected
        );
    }

    #[test]
    fn test_parse_join_chain_is_left_deep_in_order() {
        let select =
            parse_select("SELECT * FROM a JOIN b ON x = y LEFT JOIN c ON y = z WHERE z = 1");

        let joins = &select.from_clause.joins;
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].kind, JoinKind::Inner);
        assert_eq!(joins[0].table, TableRef::new("b"));
        assert_eq!(joins[1].kind, JoinKind::Left);
        assert_eq!(joins[1].table, TableRef::new("c"));
        assert_eq!(joins[1].on, column_eq("y", "z"));
        // ON stops at LEFT and WHERE is not swallowed by the last ON expression
        assert!(select.where_clause.is_some());
    }

    #[test]
    fn test_parse_join_with_aliases() {
        let from = parse_select("SELECT * FROM users u JOIN users AS v ON x = y").from_clause;
        assert_eq!(from.table.alias.as_deref(), Some("u"));
        assert_eq!(from.joins[0].table.alias.as_deref(), Some("v"));
    }

    #[test]
    fn test_parse_join_errors() {
        for query in [
            "SELECT * FROM a JOIN b",
            "SELECT * FROM a LEFT b ON x = y",
            "SELECT * FROM a RIGHT JOIN b ON x = y",
            "SELECT * FROM a FULL JOIN b ON x = y",
            "SELECT * FROM a u FULL JOIN b ON x = y",
            "SELECT * FROM a CROSS JOIN b",
        ] {
            assert!(
                SqlParser::new(query).parse().is_err(),
                "{query} should not parse"
            );
        }
    }

    #[test]
    fn test_parse_select_all() {
        match parse("SELECT * FROM users") {
            Statement::Select(SelectStatement {
                select_list,
                from_clause,
                where_clause,
                ..
            }) => {
                assert_eq!(select_list.0, vec![SelectTarget::Star]);
                assert_eq!(
                    from_clause,
                    From {
                        table: TableRef::new("users"),
                        joins: vec![],
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
                            expr: Expression::Identifier("id".to_string()),
                            alias: None,
                        },
                        SelectTarget::Expression {
                            expr: Expression::Identifier("name".to_string()),
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
                        expr: Expression::Identifier("id".to_string()),
                        alias: Some("user_id".to_string()),
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
                            expr: Expression::Identifier("id".to_string()),
                            alias: Some("Identity".to_string()),
                        },
                        SelectTarget::Expression {
                            expr: Expression::Identifier("name".to_string()),
                            alias: Some("firstName".to_string()),
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
                left: Box::new(Expression::Identifier("id".to_string())),
                op: Operator::Equal,
                right: Box::new(Expression::Literal(Value::Int64(1))),
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
            assert_eq!(*right, Expression::Literal(Value::Int64(42)));
        }

        // Float
        let expr = parse_where("SELECT * FROM t WHERE x = 3.15");
        if let Expression::BinaryOp { right, .. } = expr {
            assert_eq!(*right, Expression::Literal(Value::Float64(3.15)));
        }

        // Boolean
        let expr = parse_where("SELECT * FROM t WHERE active = TRUE");
        if let Expression::BinaryOp { right, .. } = expr {
            assert_eq!(*right, Expression::Literal(Value::Bool(true)));
        }
    }

    #[test]
    fn test_parse_create_table() {
        match parse(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, active BOOL UNIQUE)",
        ) {
            Statement::Create(CreateStatement {
                table_name,
                columns,
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(
                    columns,
                    vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Int64,
                            constraints: vec![ColumnConstraint::PrimaryKey],
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Text,
                            constraints: vec![ColumnConstraint::NotNull],
                        },
                        ColumnDef {
                            name: "active".to_string(),
                            data_type: DataType::Bool,
                            constraints: vec![ColumnConstraint::Unique],
                        },
                    ]
                );
            }
            _ => panic!("Expected CREATE statement"),
        }
    }

    #[test]
    fn test_parse_insert_single_row() {
        match parse("INSERT INTO users (id, name) VALUES (1, 'Alice')") {
            Statement::Insert(InsertStatement {
                table: table_name,
                columns,
                source,
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.unwrap(), vec!["id".to_string(), "name".to_string()]);
                assert_eq!(
                    source,
                    vec![vec![
                        Expression::Literal(Value::from(1)),
                        Expression::Literal(Value::from("Alice")),
                    ]]
                );
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_rows() {
        match parse("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')") {
            Statement::Insert(InsertStatement {
                table: table_name,
                columns,
                source,
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.unwrap(), vec!["id".to_string(), "name".to_string()]);
                assert_eq!(
                    source,
                    vec![
                        vec![
                            Expression::Literal(Value::from(1)),
                            Expression::Literal(Value::from("Alice")),
                        ],
                        vec![
                            Expression::Literal(Value::from(2)),
                            Expression::Literal(Value::from("Bob")),
                        ]
                    ]
                );
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_all_columns() {
        match parse("INSERT INTO users VALUES (1, 'Alice', true)") {
            Statement::Insert(InsertStatement {
                table: table_name,
                columns,
                source,
            }) => {
                assert_eq!(table_name, "users");
                // When columns are not specified, they should be empty and inferred during analysis
                assert!(columns.is_none());
                assert_eq!(
                    source,
                    vec![vec![
                        Expression::Literal(Value::from(1)),
                        Expression::Literal(Value::from("Alice")),
                        Expression::Literal(Value::from(true)),
                    ]]
                );
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_with_different_types() {
        match parse("INSERT INTO users (id, name, active) VALUES (42, 'Charlie', false)") {
            Statement::Insert(InsertStatement {
                table: table_name,
                columns,
                ..
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(
                    columns.unwrap(),
                    vec!["id".to_string(), "name".to_string(), "active".to_string()]
                );
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    fn parse_select(query: &str) -> SelectStatement {
        match parse(query) {
            Statement::Select(select) => select,
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_qualified_column() {
        let expr = parse_where("SELECT * FROM t WHERE u.id = 1");

        assert_eq!(
            expr,
            Expression::BinaryOp {
                left: Box::new(Expression::Qualified {
                    relation: "u".to_string(),
                    column: "id".to_string(),
                }),
                op: Operator::Equal,
                right: Box::new(Expression::Literal(Value::Int64(1))),
            }
        );
    }

    #[test]
    fn test_parse_qualified_in_select_group_order_and_aggregate() {
        let select = parse_select("SELECT u.id, COUNT(u.id) FROM t GROUP BY u.dept ORDER BY u.id");
        let qualified = |column: &str| Expression::Qualified {
            relation: "u".to_string(),
            column: column.to_string(),
        };

        match &select.select_list[0] {
            SelectTarget::Expression { expr, .. } => assert_eq!(*expr, qualified("id")),
            other => panic!("Expected expression target, got {other:?}"),
        }
        match &select.select_list[1] {
            SelectTarget::Expression {
                expr:
                    Expression::Function {
                        args: FunctionArgs::List(args),
                        ..
                    },
                ..
            } => assert_eq!(args, &vec![qualified("id")]),
            other => panic!("Expected COUNT call, got {other:?}"),
        }
        assert_eq!(select.group_by[0], qualified("dept"));
        assert_eq!(select.order_by[0].expr, qualified("id"));
    }

    #[test]
    fn test_parse_qualified_column_default_name() {
        let expr = Expression::Qualified {
            relation: "u".to_string(),
            column: "id".to_string(),
        };

        assert_eq!(expr.to_column_name(), "id");
    }

    #[test]
    fn test_parse_qualified_star() {
        let select = parse_select("SELECT u.*, o.total FROM t");

        assert_eq!(
            select.select_list.0,
            vec![
                SelectTarget::QualifiedStar("u".to_string()),
                SelectTarget::Expression {
                    expr: Expression::Qualified {
                        relation: "o".to_string(),
                        column: "total".to_string(),
                    },
                    alias: None,
                },
            ]
        );
    }

    #[test]
    fn test_parse_qualified_star_alone() {
        let select = parse_select("SELECT a.* FROM t");

        assert_eq!(
            select.select_list.0,
            vec![SelectTarget::QualifiedStar("a".to_string())]
        );
    }

    #[test]
    fn test_parse_multiplication_is_not_qualified_star() {
        let select = parse_select("SELECT 2 * 3 FROM t");

        assert!(matches!(
            select.select_list.0.as_slice(),
            [SelectTarget::Expression {
                expr: Expression::BinaryOp {
                    op: Operator::Multiply,
                    ..
                },
                ..
            }]
        ));
    }

    #[test]
    fn test_parse_invalid_column_reference() {
        for query in [
            "SELECT a. FROM t",
            "SELECT .a FROM t",
            "SELECT a.b.c FROM t",
        ] {
            assert!(
                SqlParser::new(query).parse().is_err(),
                "expected `{query}` to be rejected"
            );
        }
    }

    #[test]
    fn test_parse_qualified_join_on() {
        let select = parse_select("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

        assert_eq!(
            select.from_clause.joins[0].on,
            Expression::BinaryOp {
                left: Box::new(Expression::Qualified {
                    relation: "u".to_string(),
                    column: "id".to_string(),
                }),
                op: Operator::Equal,
                right: Box::new(Expression::Qualified {
                    relation: "o".to_string(),
                    column: "user_id".to_string(),
                }),
            }
        );
    }

    #[test]
    fn test_parse_order_by_limit_offset() {
        let select = parse_select("SELECT * FROM users ORDER BY age DESC, name LIMIT 10 OFFSET 5;");

        assert_eq!(
            select.order_by,
            vec![
                OrderByItem {
                    expr: Expression::Identifier("age".to_string()),
                    descending: true,
                },
                OrderByItem {
                    expr: Expression::Identifier("name".to_string()),
                    descending: false,
                },
            ]
        );
        assert_eq!(select.limit, Some(10));
        assert_eq!(select.offset, Some(5));
    }

    #[test]
    fn test_parse_where_then_order_by() {
        let select = parse_select("SELECT * FROM users WHERE age > 18 ORDER BY age ASC");

        assert!(select.where_clause.is_some());
        assert_eq!(select.order_by.len(), 1);
        assert!(!select.order_by[0].descending);
    }

    #[test]
    fn test_parse_count_star_with_group_by() {
        let select = parse_select("SELECT dept, COUNT(*) AS n FROM users GROUP BY dept");

        assert_eq!(
            select.select_list.0[1],
            SelectTarget::Expression {
                expr: Expression::Function {
                    name: "COUNT".to_string(),
                    args: FunctionArgs::Star,
                },
                alias: Some("n".to_string()),
            }
        );
        assert_eq!(
            select.group_by,
            vec![Expression::Identifier("dept".to_string())]
        );
    }

    #[test]
    fn test_parse_function_with_expression_arg() {
        let select = parse_select("SELECT SUM(price * 2) FROM items");

        assert!(matches!(
            &select.select_list.0[0],
            SelectTarget::Expression {
                expr: Expression::Function { args: FunctionArgs::List(args), .. },
                ..
            } if args.len() == 1
        ));
    }

    #[test]
    fn test_parse_star_followed_by_column() {
        let select = parse_select("SELECT *, id FROM users");
        assert_eq!(select.select_list.len(), 2);
    }

    #[test]
    fn test_parse_update() {
        match parse("UPDATE users SET name = 'Bob', age = age + 1 WHERE id = 1") {
            Statement::Update(UpdateStatement {
                table,
                assignments,
                where_clause,
            }) => {
                assert_eq!(table, "users");
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].0, "name");
                assert_eq!(assignments[1].0, "age");
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected Update statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        match parse("DELETE FROM users WHERE id = 1") {
            Statement::Delete(DeleteStatement {
                table,
                where_clause,
            }) => {
                assert_eq!(table, "users");
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected Delete statement"),
        }

        match parse("DELETE FROM users") {
            Statement::Delete(DeleteStatement { where_clause, .. }) => {
                assert!(where_clause.is_none());
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_parse_rejects_trailing_tokens() {
        for query in [
            "SELECT * FROM users HAVING x",
            "SELECT * FROM users u v",
            "SELECT * FROM users, orders",
            "SELECT * FROM users WHERE id = 1 extra",
            "SELECT * FROM users; SELECT * FROM users",
            "DELETE FROM users WHERE id = 1 LIMIT 1",
        ] {
            assert!(
                SqlParser::new(query).parse().is_err(),
                "{query} should not parse"
            );
        }
    }

    #[test]
    fn test_parse_rejects_negative_limit() {
        assert!(
            SqlParser::new("SELECT * FROM users LIMIT -1")
                .parse()
                .is_err()
        );
    }
}
