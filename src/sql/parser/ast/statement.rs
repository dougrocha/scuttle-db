use super::{Expression, SelectList};

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement<'src> {
    Create(CreateStatement<'src>),
    Select(SelectStatement<'src>),
    Update,
    Insert,
    Delete,
}

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

#[derive(Debug, Clone)]
pub struct CreateStatement<'src> {
    table_name: &'src str,
}
