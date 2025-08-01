use miette::{Result, miette};

use std::iter::Peekable;

use crate::lexer::{Lexer, Token};

#[derive(Debug)]
pub enum ColumnList {
    All,
    Columns(Vec<String>),
}

#[derive(Debug)]
pub enum Statement {
    Create,
    Select { columns: ColumnList, table: String },
    Update,
    Insert,
    Delete,
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
        let Some(Ok(token)) = self.lexer.next() else {
            return Err(miette!("Error occured while parsing"));
        };

        let statement = match token {
            Token::Keyword(keyword) => match keyword {
                "SELECT" => self.parse_select_statement()?,
                _ => return Err(miette!("Unsupported keyword: {:?}", keyword)),
            },
            _ => return Err(miette!("Unexpected token: {:?}", token)),
        };

        Ok(statement)
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        let mut columns: Option<ColumnList> = None;
        let mut table = "";

        while let Some(token) = self.lexer.peek() {
            let Ok(token) = token else { todo!() };

            match token {
                Token::Keyword(keyword) => {
                    if *keyword == "FROM" {
                        self.lexer.next();

                        if let Some(Ok(Token::Identifier(table_name))) = self.lexer.next() {
                            table = table_name;
                        } else {
                            return Err(miette!("Expected table name after FROM"));
                        }

                        break;
                    } else {
                        return Err(miette!("Unexpected keyword: {keyword}"));
                    }
                }
                Token::Identifier(_) | Token::Asterisk => {
                    if columns.is_none() {
                        columns = Some(self.parse_column_list().unwrap());
                    }
                }
                Token::Comma => {
                    self.lexer.next();
                }
                Token::SemiColon => {
                    self.lexer.next();
                    break;
                }
            }
        }

        Ok(Statement::Select {
            columns: columns.expect("Column list should be defined"),
            table: table.to_string(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let mut parser = SqlParser::new("SELECT * FROM users");
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select { columns, table } => {
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
            Statement::Select { columns, table } => {
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
}
