use crate::DataType;

use super::{Expression, SelectList};

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement {
    Create(CreateStatement),
    Select(SelectStatement),
    Update,
    Insert,
    Delete,
}

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

#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table_name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    Nullable,
    PrimaryKey,
    Unique,
    Default(Expression),
}
