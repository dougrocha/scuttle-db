use crate::{
    ColumnDef, Value,
    sql::ast::{expression::Expression, target::SelectList},
};

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement {
    Create(CreateStatement),
    Select(SelectStatement),
    Update,
    Insert(InsertStatement),
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
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Vec<String>,
    pub source: InsertSource,
}

#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Expression>),
    Select(SelectStatement),
}
