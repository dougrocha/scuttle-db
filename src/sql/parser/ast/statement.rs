use super::{Expression, TargetList};

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement {
    /// CREATE statement (not yet implemented)
    Create,

    /// SELECT statement
    Select {
        /// Columns to select (* or specific list)
        targets: TargetList,

        /// Table to select from
        table: String,

        /// Optional WHERE clause
        r#where: Option<Expression>,
    },

    /// UPDATE statement (not yet implemented)
    Update {
        /// Table to update
        table: String,

        /// Columns to update
        columns: Vec<String>,

        /// New values
        values: Vec<String>,
    },

    /// INSERT statement (not yet implemented)
    Insert,

    /// DELETE statement (not yet implemented)
    Delete,
}

impl Statement {
    /// Extracts the table name from this statement.
    pub fn table_name(&self) -> &str {
        match self {
            Statement::Select { table, .. } => table,
            Statement::Update { table, .. } => table,
            _ => panic!("NOT SUPPORTED YET"),
        }
    }
}
