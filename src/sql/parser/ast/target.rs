use std::borrow::Cow;

use super::expression::Expression;

/// Target list in a SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectTarget<'src> {
    /// SELECT * (all columns)
    Star,

    /// SELECT col1, col2, ... (specific columns)
    Expression {
        expr: Expression<'src>,
        alias: Option<Cow<'src, str>>,
    },
}

#[derive(Debug, Clone)]
pub struct SelectList<'src>(pub Vec<SelectTarget<'src>>);

impl<'src> std::ops::Deref for SelectList<'src> {
    type Target = Vec<SelectTarget<'src>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for SelectList<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
