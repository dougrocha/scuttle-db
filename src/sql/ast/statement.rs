use crate::{
    ColumnDef,
    sql::ast::{expression::Expression, target::SelectList},
};

/// A SQL statement (top-level AST node).
///
/// Currently only SELECT is fully implemented.
#[derive(Debug, Clone)]
pub enum Statement {
    Create(CreateStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub select_list: SelectList,
    pub from_clause: From,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct From {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub source: Vec<Vec<Expression>>,
}
