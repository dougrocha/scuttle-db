use std::str::FromStr;

use miette::{Result, miette};

use crate::keyword::Keyword;

/// A token in the SQL language.
#[derive(Debug, PartialEq)]
pub enum Token<'a> {
    /// SQL keyword (SELECT, FROM, WHERE, etc.) - always uppercase
    Keyword(Keyword),

    /// Table name, column name, or other identifier
    Identifier(&'a str),

    /// Numeric literal (integer or floating point)
    Number(f64),

    /// String literal (quotes not included)
    String(&'a str),

    /// Boolean
    Boolean(bool),

    /// Comma (,) - list separator
    Comma,

    /// Asterisk (*) - wildcard
    Asterisk,

    /// Semicolon (;) - statement terminator
    SemiColon,

    /// Equal (=) - comparison
    Equal,

    /// Not equal (!=) - comparison
    NotEqual,

    /// Greater than (>) - comparison
    GreaterThan,

    /// Less than (<) - comparison
    LessThan,

    /// Left parenthesis - not yet used
    LeftParen,

    /// Right parenthesis - not yet used
    RightParen,
}

/// SQL lexer that tokenizes a query string.
pub(crate) struct Lexer<'a> {
    /// The complete original query string
    pub whole: &'a str,

    /// The remaining portion to tokenize
    pub rest: &'a str,

    /// Current byte position
    pub position: usize,
}

impl<'a> Lexer<'a> {
    /// Creates a new lexer for the given SQL query string.
    pub fn new(input: &'a str) -> Self {
        Self {
            whole: input,
            rest: input,
            position: 0,
        }
    }

    /// Advances the lexer past any whitespace characters.
    fn skip_whitespace(&mut self) {
        let non_whitespace_pos = self
            .rest
            .char_indices()
            .find(|(_, ch)| !ch.is_whitespace())
            .map(|(pos, _)| pos)
            .unwrap_or(self.rest.len());

        self.position += non_whitespace_pos;
        self.rest = &self.rest[non_whitespace_pos..];
    }

    /// Consumes a word (identifier or keyword) from the input.
    ///
    /// Stops at whitespace, comma, or semicolon.
    fn consume_word(&mut self) -> Option<&'a str> {
        if self.rest.is_empty() {
            return None;
        }

        let word_index = self
            .rest
            .find(|c: char| c.is_whitespace() || c == ',' || c == ';')
            .unwrap_or(self.rest.len());

        let word = &self.rest[..word_index];
        self.position += word_index;
        self.rest = &self.rest[word_index..];

        if word.is_empty() { None } else { Some(word) }
    }

    /// Consumes a string literal from the input.
    ///
    /// Expects the opening quote to have already been consumed.
    /// Reads until the closing quote character.
    fn consume_string(&mut self, closing: char) -> &'a str {
        let mut end_index = 1;
        while end_index < self.rest.len() {
            if self.rest[end_index..].starts_with(closing) {
                break;
            }
            end_index += 1;
        }

        let string_value = &self.rest[1..end_index];
        self.position += end_index + 1;
        self.rest = &self.rest[end_index + 1..];

        string_value
    }

    /// Consumes a numeric literal from the input.
    ///
    /// Reads digits and optional decimal point.
    fn consume_number(&mut self) -> &'a str {
        let number_end = self
            .rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(self.rest.len());

        let number_str = &self.rest[..number_end];
        self.position += number_end;
        self.rest = &self.rest[number_end..];

        number_str
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.skip_whitespace();

        if self.rest.is_empty() {
            return None;
        }

        let char = self.rest.chars().next()?;

        let token = match char {
            ',' => {
                self.rest = &self.rest[1..];
                self.position += char.len_utf8();
                Ok(Token::Comma)
            }
            '*' => {
                self.rest = &self.rest[1..];
                self.position += char.len_utf8();
                Ok(Token::Asterisk)
            }
            ';' => {
                self.rest = &self.rest[1..];
                self.position += char.len_utf8();
                Ok(Token::SemiColon)
            }
            '=' => {
                self.rest = &self.rest[1..];
                self.position += char.len_utf8();
                Ok(Token::Equal)
            }
            '<' => {
                self.rest = &self.rest[1..];
                self.position += char.len_utf8();
                Ok(Token::LessThan)
            }
            '>' => {
                self.rest = &self.rest[1..];
                self.position += char.len_utf8();
                Ok(Token::GreaterThan)
            }
            '!' => {
                if self.rest.len() > 1 && self.rest.chars().nth(1) == Some('=') {
                    self.rest = &self.rest[2..];
                    self.position += 2;
                    Ok(Token::NotEqual)
                } else {
                    Err(miette!(
                        "Unexpected character '{}' at position {}",
                        char,
                        self.position
                    ))
                }
            }
            '\'' => {
                let string_value = self.consume_string('\'');
                Ok(Token::String(string_value))
            }
            _ if char.is_ascii_digit() => {
                let number_str = self.consume_number();
                match number_str.parse::<f64>() {
                    Ok(num) => Ok(Token::Number(num)),
                    Err(_) => Err(miette!("Invalid number format: {}", number_str)),
                }
            }
            _ if char.is_alphabetic() => {
                let word = self.consume_word()?;

                if let Ok(kw) = Keyword::from_str(word) {
                    Ok(Token::Keyword(kw))
                } else {
                    Ok(Token::Identifier(word))
                }
            }
            _ => Err(miette!(
                "Unexpected character '{}' at position {}",
                char,
                self.position
            )),
        };

        Some(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_token_eq(actual: Option<Result<Token>>, expected: Token) {
        match actual {
            Some(Ok(token)) => assert_eq!(token, expected),
            _ => panic!("Expected token {expected:?}, got {actual:?}"),
        }
    }

    #[test]
    fn test_lexer() {
        let mut lexer = Lexer::new("SELECT * FROM users");

        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Select));
        assert_token_eq(lexer.next(), Token::Asterisk);
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::From));
        assert_token_eq(lexer.next(), Token::Identifier("users"));
        assert!(lexer.next().is_none());
    }

    #[test]
    fn test_lexer_with_spaces() {
        let mut lexer = Lexer::new("  SELECT   *  FROM    users  ");

        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Select));
        assert_token_eq(lexer.next(), Token::Asterisk);
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::From));
        assert_token_eq(lexer.next(), Token::Identifier("users"));
        assert!(lexer.next().is_none());
    }

    #[test]
    fn test_lexer_with_columns() {
        let mut lexer = Lexer::new("SELECT id, name, email FROM users");

        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Select));
        assert_token_eq(lexer.next(), Token::Identifier("id"));
        assert_token_eq(lexer.next(), Token::Comma);
        assert_token_eq(lexer.next(), Token::Identifier("name"));
        assert_token_eq(lexer.next(), Token::Comma);
        assert_token_eq(lexer.next(), Token::Identifier("email"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::From));
        assert_token_eq(lexer.next(), Token::Identifier("users"));
        assert!(lexer.next().is_none());
    }

    #[test]
    fn test_lexer_with_where() {
        let mut lexer = Lexer::new("SELECT id FROM users WHERE name = 'Alice'");

        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Select));
        assert_token_eq(lexer.next(), Token::Identifier("id"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::From));
        assert_token_eq(lexer.next(), Token::Identifier("users"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Where));
        assert_token_eq(lexer.next(), Token::Identifier("name"));
        assert_token_eq(lexer.next(), Token::Equal);
        assert_token_eq(lexer.next(), Token::String("Alice"));
        assert!(lexer.next().is_none());
    }
}
