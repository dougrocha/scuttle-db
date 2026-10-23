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
    Set,

    Group,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,

    Join,
    Inner,
    Left,
    Right,
    On,
    Outer,
    Full,
    Cross,

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

    Explain,
}

impl Keyword {
    pub fn is_bool_literal(self) -> bool {
        matches!(self, Self::True | Self::False)
    }
}
