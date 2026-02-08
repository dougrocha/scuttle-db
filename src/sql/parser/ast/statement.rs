use super::{Expression, SelectList};

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub select_list: SelectList,
    pub from_clause: FromClause,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub table_name: String,
}

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement {
    Create,
    Select(SelectStatement),
    Update,
    Insert,
    Delete,
}
