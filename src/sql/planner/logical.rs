use crate::{
    ColumnDef,
    sql::analyzer::{AggregateFunction, AnalyzedExpression},
};

#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
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
