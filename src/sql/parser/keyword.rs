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
    Constraint,
    Primary,
    Key,
    References,
    Unique,
    Check,
    Default,

    Select,
    Insert,
    Update,
    Delete,
    Where,
    From,
    Into,
    Values,
    As,

    Join,
    Inner,
    Left,
    Right,
    On,

    // Data Types
    #[strum(serialize = "Int", serialize = "Integer")]
    Integer,
    Float,
    Varchar,
    #[strum(serialize = "Text", serialize = "String")]
    Text,
    Timestamp,
    Date,
    #[strum(serialize = "Bool", serialize = "Boolean")]
    Boolean,

    True,
    False,

    And,
    Or,
    Is,
    Not,
    In,
    Between,
    Like,
    Null,

    Distinct,
}

impl Keyword {
    pub fn is_bool_literal(self) -> bool {
        matches!(self, Self::True | Self::False)
    }

    pub fn is_type(self) -> bool {
        matches!(
            self,
            Self::Integer | Self::Float | Self::Text | Self::Timestamp | Self::Date | Self::Boolean
        )
    }
}
