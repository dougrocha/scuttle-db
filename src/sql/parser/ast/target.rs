use super::expression::Expression;

/// Target list in a SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectTarget {
    /// SELECT * (all columns)
    Star,

    /// SELECT col1, col2, ... (specific columns)
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

/// Select List
pub type SelectList = Vec<SelectTarget>;
