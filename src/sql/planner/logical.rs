use crate::{
    ColumnDef,
    sql::{
        analyzer::{AggregateFunction, AnalyzedExpression},
        ast::statement::JoinKind,
    },
};

#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table_name: String,

        /// Table alias from the FROM clause, if any. Display-only: EXPLAIN prints
        /// `Scan: users u`. Physical planning ignores it.
        alias: Option<String>,
    },
    /// Nested-loop join. Output row is the left row followed by the right row.
    ///
    /// Children are `Scan` or `Join` (left-deep, one join per FROM clause entry).
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: JoinKind,

        /// Bound against `left ++ right`. Decides matching and, for LEFT, null-extension.
        /// Must stay on the join: a WHERE predicate is never merged into it.
        on: AnalyzedExpression,

        /// Number of columns the right child produces. This is the LEFT padding width.
        /// It cannot be read from rows, because the right table may be empty.
        right_width: usize,

        /// `"alias.column"` for every column of `left ++ right`. Display only: EXPLAIN
        /// reads it, execution ignores it.
        column_names: Vec<String>,
    },
    Filter {
        input: Box<LogicalPlan>,
        condition: AnalyzedExpression,
    },
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<AnalyzedExpression>,

        /// Output column names (aliases already applied)
        column_names: Vec<String>,
    },
    /// Collapses input rows into one row per group.
    ///
    /// Output rows are `[group_by values..., aggregate values...]`.
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<AnalyzedExpression>,
        aggregates: Vec<AggregateCall>,
    },
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<u64>,
        offset: u64,
    },
    Values {
        expressions: Vec<Vec<AnalyzedExpression>>,
    },
    Insert {
        table_name: String,
        column_names: Vec<String>,
        source: Box<LogicalPlan>,
    },
    Update {
        table_name: String,

        /// `(column index, new value)` pairs; expressions are evaluated against the old row
        assignments: Vec<(usize, AnalyzedExpression)>,
        filter: Option<AnalyzedExpression>,
    },
    Delete {
        table_name: String,
        filter: Option<AnalyzedExpression>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
    },
}

impl LogicalPlan {
    /// Names of the columns this plan produces, if it produces rows.
    pub fn column_names(&self) -> Vec<String> {
        match self {
            LogicalPlan::Projection { column_names, .. } => column_names.clone(),
            LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Filter { input, .. } => input.column_names(),
            _ => Vec::new(),
        }
    }
}

/// A single aggregate computed by [`LogicalPlan::Aggregate`].
#[derive(Debug)]
pub struct AggregateCall {
    pub function: AggregateFunction,

    /// `None` for `COUNT(*)`
    pub arg: Option<AnalyzedExpression>,
}

/// One ORDER BY key, evaluated against the sort's input row.
#[derive(Debug)]
pub struct SortKey {
    pub expr: AnalyzedExpression,
    pub descending: bool,
}
