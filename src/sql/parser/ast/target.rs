use std::borrow::Cow;

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

#[derive(Debug, Clone)]
pub struct SelectList(pub Vec<SelectTarget>);

impl<'src> std::ops::Deref for SelectList {
    type Target = Vec<SelectTarget>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for SelectList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
