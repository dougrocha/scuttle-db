use super::{Expression, SelectList};

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement {
    Create,
    Select {
        select_list: SelectList,
        from_clause: String,
        where_clause: Option<Expression>,
    },
    Update {
        table: String,
        columns: Vec<String>,
        values: Vec<String>,
    },
    Insert,
    Delete,
}
