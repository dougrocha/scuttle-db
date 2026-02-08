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
