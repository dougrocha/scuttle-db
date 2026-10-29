use crate::{
    ColumnDef,
    sql::ast::{expression::Expression, target::SelectList},
};

/// A SQL statement (top-level AST node).
#[derive(Debug, Clone)]
pub enum Statement {
    Create(CreateStatement),

    /// `CREATE [UNIQUE] INDEX <name> ON <table> (<column>)`
    CreateIndex(CreateIndexStatement),

    /// `DROP TABLE <name>`
    DropTable(String),

    /// `DROP INDEX <name>`
    DropIndex(String),

    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),

    /// `EXPLAIN [ANALYZE] <statement>`: describe the plan instead of running it. With
    /// ANALYZE, run it too and report what each node produced.
    Explain {
        statement: Box<Statement>,
        analyze: bool,
    },
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

/// The FROM clause: one base relation followed by zero or more joins.
///
/// `joins` is an ordered list, which is *left-deep by construction*:
/// `FROM a JOIN b ON x LEFT JOIN c ON y` is `((a JOIN b) LEFT JOIN c)`.
/// Each ON clause may refer to any relation to its left plus its own table.
#[derive(Debug, Clone, PartialEq)]
pub struct From {
    pub table: TableRef,
    pub joins: Vec<JoinClause>,
}

/// `users`, `users u` or `users AS u`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    /// When present it REPLACES the table name as the only way to qualify columns.
    pub alias: Option<String>,
}

impl TableRef {
    /// Test/convenience constructor for an un-aliased table.
    #[cfg(test)]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            alias: None,
        }
    }

    /// The name columns of this relation are qualified with: alias if any, else table name.
    pub fn binding_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// One `JOIN ... ON ...` item in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: TableRef,
    /// Join predicate; evaluated while matching (not after the join).
    pub on: Expression,
}

/// Defined ONCE here and reused by `LogicalPlan::Join` (like `Operator`), so
/// there is no second enum to keep in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

impl std::fmt::Display for JoinKind {
    /// Lowercase ("inner" / "left"): used by EXPLAIN as `Join: left on ...`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inner => write!(f, "inner"),
            Self::Left => write!(f, "left"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table_name: String,
    pub column: String,
    pub unique: bool,
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
