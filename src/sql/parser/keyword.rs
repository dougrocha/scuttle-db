use strum::EnumString;

/// SQL keywords recognized by the parser.
///
/// These keywords are case-insensitive and reserved for SQL syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString)]
#[strum(ascii_case_insensitive)]
pub enum Keyword {
    Create,
    Table,
    Drop,
    Alter,

    Select,
    Insert,
    Update,
    Delete,
    Where,
    From,
    Into,
    Values,

    Join,
    Inner,
    Left,
    Right,
    On,

    // Data Types
    Int,
    Integer,
    Varchar,
    Timestamp,
    Boolean,
    True,
    False,
    Null,

    As,
    And,
    Or,
    Is,
    Not,

    Distinct,
}

impl Keyword {
    pub fn is_bool(self) -> bool {
        matches!(self, Self::True | Self::False)
    }
}
