use crate::sql::ast::keyword::Keyword;

/// Predicates to the 'IS' keyword.
#[derive(Debug, Clone, PartialEq)]
pub enum IsPredicate {
    True,
    False,
    Null,
}

impl std::fmt::Display for IsPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsPredicate::True => write!(f, "TRUE"),
            IsPredicate::False => write!(f, "FALSE"),
            IsPredicate::Null => write!(f, "NULL"),
        }
    }
}

impl TryFrom<Keyword> for IsPredicate {
    type Error = ();

    fn try_from(kw: Keyword) -> Result<Self, Self::Error> {
        match kw {
            Keyword::True => Ok(IsPredicate::True),
            Keyword::False => Ok(IsPredicate::False),
            Keyword::Null => Ok(IsPredicate::Null),
            _ => Err(()), // This keyword is not a predicate
        }
    }
}
