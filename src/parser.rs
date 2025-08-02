use miette::{Result, miette};

use std::iter::Peekable;

use crate::lexer::{Lexer, Token};

#[derive(Debug, PartialEq)]
pub enum LiteralValue {
    Number(f64),
    String(String),
}

#[derive(Debug, PartialEq)]
pub enum BinaryOperator {
    AND,
    OR,
    EQUAL,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    Column(String),
    Literal(LiteralValue),
}

#[derive(Debug)]
pub enum ColumnList {
    All,
    Columns(Vec<String>),
}

#[derive(Debug)]
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
                let _expression = self.parse_where_expression();

                None
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

    fn parse_where_expression(&mut self) -> Option<Expression> {
        let next = self.lexer.next()?;

        dbg!(next.unwrap());

        None
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
                        left: Box::new(Expression::Literal(LiteralValue::String("id".to_string()))),
                        op: BinaryOperator::EQUAL,
                        right: Box::new(Expression::Literal(LiteralValue::Number(1.))),
                    })
                );
            }
            _ => panic!("Expected Select statement"),
        }
    }
}
