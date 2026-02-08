use std::str::FromStr;

use miette::{IntoDiagnostic, Result, miette};

use super::parser::Keyword;

/// A token in the SQL language.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Token<'a> {
    /// SQL keyword (SELECT, FROM, WHERE, etc.)
    Keyword(Keyword),

    /// Literals
    Identifier(&'a str),
    Integer(i64),
    Float(f64),
    String(&'a str),

    Comma,
    SemiColon,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanEqual,
    LessThanEqual,
    LeftParen,
    RightParen,
    Asterisk,
    Plus,
    Minus,
    Slash,
}

/// SQL lexer that tokenizes a query string.
#[derive(Clone, Copy)]
pub struct Lexer<'a> {
    /// The complete original query string
    #[allow(dead_code)]
    whole: &'a str,

    /// The remaining portion to tokenize
    rest: &'a str,

    /// Current byte position
    position: usize,
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
            .map_or(self.rest.len(), |(pos, _)| pos);

        self.position += non_whitespace_pos;
        self.rest = &self.rest[non_whitespace_pos..];
    }

    /// Consumes a symbol, assumes we have peeked ahead.
    ///
    /// Only consume one symbol
    fn consume_symbol(&mut self, token: Token<'a>) -> Token<'a> {
        let char_len = self.rest.chars().next().unwrap().len_utf8();
        self.rest = &self.rest[char_len..];
        self.position += char_len;

        token
    }

    /// Consumes a word (identifier or keyword) from the input.
    fn consume_word(&mut self) -> Option<&'a str> {
        if self.rest.is_empty() {
            return None;
        }

        let word_index = self
            .rest
            .find(|c: char| {
                c.is_whitespace()
                    || matches!(c, ',' | ';' | '=' | '*' | '(' | ')' | '<' | '>' | '!')
            })
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
    fn consume_number(&mut self) -> Result<Token<'a>> {
        let mut is_float = false;

        let number_end = self
            .rest
            .find(|c: char| {
                if c == '.' {
                    is_float = true;
                }

                !c.is_ascii_digit() && c != '.'
            })
            .unwrap_or(self.rest.len());

        let number_str = &self.rest[..number_end];
        self.position += number_end;
        self.rest = &self.rest[number_end..];

        let token = if is_float {
            let float = number_str.parse::<f64>().into_diagnostic()?;
            Token::Float(float)
        } else {
            let int = number_str.parse::<i64>().into_diagnostic()?;
            Token::Integer(int)
        };

        Ok(token)
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
            '(' => Ok(self.consume_symbol(Token::LeftParen)),
            ')' => Ok(self.consume_symbol(Token::RightParen)),
            ',' => Ok(self.consume_symbol(Token::Comma)),
            '+' => Ok(self.consume_symbol(Token::Plus)),
            '-' => Ok(self.consume_symbol(Token::Minus)),
            '*' => Ok(self.consume_symbol(Token::Asterisk)),
            '/' => Ok(self.consume_symbol(Token::Slash)),
            ';' => Ok(self.consume_symbol(Token::SemiColon)),
            '=' => Ok(self.consume_symbol(Token::Equal)),
            '<' => {
                if self.rest.len() > 1 && self.rest.chars().nth(1) == Some('=') {
                    self.rest = &self.rest[2..];
                    self.position += 2;
                    Ok(Token::LessThanEqual)
                } else {
                    Ok(self.consume_symbol(Token::LessThan))
                }
            }
            '>' => {
                if self.rest.len() > 1 && self.rest.chars().nth(1) == Some('=') {
                    self.rest = &self.rest[2..];
                    self.position += 2;
                    Ok(Token::GreaterThanEqual)
                } else {
                    Ok(self.consume_symbol(Token::GreaterThan))
                }
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
            '\"' => {
                let string_value = self.consume_string('\"');
                Ok(Token::Identifier(string_value))
            }
            _ if char.is_ascii_digit() => {
                let num_token = self.consume_number();
                match num_token {
                    Ok(num) => Ok(num),
                    Err(err) => Err(err),
                }
            }
            _ if char.is_alphabetic() => {
                let word = self.consume_word()?;

                if let Ok(kw) = Keyword::from_str(word) {
                    return Some(Ok(Token::Keyword(kw)));
                }

                let token = Token::Identifier(word);

                Ok(token)
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

    #[test]
    fn test_lexer_with_as() {
        let mut lexer = Lexer::new(
            "SELECT id as \"userID\", name AS firstName FROM users WHERE name = 'Alice'",
        );

        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Select));
        assert_token_eq(lexer.next(), Token::Identifier("id"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::As));
        assert_token_eq(lexer.next(), Token::Identifier("userID"));
        assert_token_eq(lexer.next(), Token::Comma);
        assert_token_eq(lexer.next(), Token::Identifier("name"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::As));
        assert_token_eq(lexer.next(), Token::Identifier("firstName"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::From));
        assert_token_eq(lexer.next(), Token::Identifier("users"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Where));
        assert_token_eq(lexer.next(), Token::Identifier("name"));
        assert_token_eq(lexer.next(), Token::Equal);
        assert_token_eq(lexer.next(), Token::String("Alice"));
        assert!(lexer.next().is_none());
    }

    #[test]
    fn test_lexer_with_is() {
        let mut lexer = Lexer::new("SELECT email IS NULL FROM users");

        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Select));
        assert_token_eq(lexer.next(), Token::Identifier("email"));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Is));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::Null));
        assert_token_eq(lexer.next(), Token::Keyword(Keyword::From));
        assert_token_eq(lexer.next(), Token::Identifier("users"));
        assert!(lexer.next().is_none());
    }

    #[test]
    fn test_number() {
        let mut lexer = Lexer::new("5.0 5");

        let mut peek = lexer.peekable();
        println!("Peek: {:?}", peek.peek());

        assert_token_eq(lexer.next(), Token::Float(5.0));
        assert_token_eq(lexer.next(), Token::Integer(5));

        assert!(lexer.next().is_none());
    }
}
