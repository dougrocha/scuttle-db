use crate::{
    ColumnDef,
    sql::ast::{expression::Expression, target::SelectList},
};

/// A SQL statement (top-level AST node).
#[derive(Debug, Clone)]
pub enum Statement {
    Create(CreateStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub select_list: SelectList,
    pub from_clause: From,
    pub where_clause: Option<Expression>,
    pub group_by: Vec<Expression>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// One sort key in an ORDER BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    /// An expression, an output column name/alias, or a 1-based output column position
    pub expr: Expression,
    pub descending: bool,
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

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,

    /// `SET column = expr` pairs, in the order they were written
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expression>,
}
