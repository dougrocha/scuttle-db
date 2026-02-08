use super::{Expression, SelectList};

#[derive(Debug, Clone)]
pub struct SelectStatement<'src> {
    pub select_list: SelectList<'src>,
    pub from_clause: FromClause<'src>,
    pub where_clause: Option<Expression<'src>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause<'src> {
    pub table_name: &'src str,
}

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement<'src> {
    Create,
    Select(SelectStatement<'src>),
    Update,
    Insert,
    Delete,
}
