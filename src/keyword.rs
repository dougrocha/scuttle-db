use std::str::FromStr;

/// SQL keywords recognized by the parser.
///
/// These keywords are case-insensitive and reserved for SQL syntax.
#[derive(Debug, PartialEq, Eq)]
pub enum Keyword {
    Select,
    From,
    Where,
    Insert,
    Create,
    Table,
    As,
    And,
    Or,
    Is,
    Not,
}

impl FromStr for Keyword {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = match s.to_ascii_uppercase().as_str() {
            "SELECT" => Self::Select,
            "FROM" => Self::From,
            "WHERE" => Self::Where,
            "INSERT" => Self::Insert,
            "CREATE" => Self::Create,
            "TABLE" => Self::Table,

            "AS" => Self::As,

            "AND" => Self::And,
            "OR" => Self::Or,
            "IS" => Self::Is,
            "NOT" => Self::Not,
            _ => return Err(()),
        };

        Ok(s)
    }
}
