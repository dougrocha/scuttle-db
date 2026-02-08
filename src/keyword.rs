use strum::EnumString;

/// SQL keywords recognized by the parser.
///
/// These keywords are case-insensitive and reserved for SQL syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString)]
#[strum(ascii_case_insensitive)]
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

    Distinct,
}
