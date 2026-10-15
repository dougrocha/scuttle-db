use crate::{
    ColumnDef, DataType, Schema,
    sql::{
        analyzer::{AggregateFunction, AnalyzedExpression},
        ast::statement::JoinKind,
    },
};

use super::relation_schema::RelationSchema;

#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table_name: String,

        /// Table alias from the FROM clause, if any. When present it replaces `table_name`
        /// as the relation of every output column. EXPLAIN prints `Scan: users u`.
        alias: Option<String>,

        /// The table's schema as the analyzer saw it, so the node can state its output
        /// without a catalog. Physical planning reads rows by `table_name` and ignores it.
        schema: Schema,
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
    /// The columns of every row this node produces, in row order.
    ///
    /// Derived on every call, one arm per variant, NO `_` arm: a new node does not compile
    /// until it states its output. Each arm mirrors the node's executor:
    ///
    /// - Scan: table columns; relation = alias, else table name.
    /// - Join: left then right (`JoinExec` concatenates the same way).
    /// - Filter / Sort / Limit: input unchanged.
    /// - Aggregate: keys, then calls. Named AND labelled by text rendered against the
    ///   INPUT's labels, which is why a Sort over an Aggregate over a Join prints `u.name`.
    /// - Projection: one column per select-list output.
    ///
    /// Relies on: the binder rejects a bare `Literal(Null)` outside INSERT VALUES and
    /// UPDATE SET, so `get_type` never sees one in a SELECT plan (it panics on it).
    /// - Values, Insert, Update, Delete, CreateTable: empty. They return no rows to a parent.
    pub(crate) fn output_schema(&self) -> RelationSchema {
        match self {
            LogicalPlan::Scan {
                table_name,
                alias,
                schema,
            } => RelationSchema::table(alias.as_deref().unwrap_or(table_name), schema),
            LogicalPlan::Join { left, right, .. } => {
                RelationSchema::join(&left.output_schema(), &right.output_schema())
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => input.output_schema(),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let columns = input.output_schema();
                RelationSchema::computed(
                    group_by
                        .iter()
                        .map(|key| (columns.render(key), key.get_type()))
                        .chain(
                            aggregates
                                .iter()
                                .map(|call| (columns.render_aggregate(call), call.return_type)),
                        ),
                )
            }
            LogicalPlan::Projection {
                expressions,
                column_names,
                ..
            } => RelationSchema::computed(
                column_names
                    .iter()
                    .cloned()
                    .zip(expressions.iter().map(AnalyzedExpression::get_type)),
            ),
            LogicalPlan::Values { .. }
            | LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::CreateTable { .. } => RelationSchema::empty(),
        }
    }
}

/// A single aggregate computed by [`LogicalPlan::Aggregate`].
#[derive(Debug)]
pub struct AggregateCall {
    pub function: AggregateFunction,

    /// `None` for `COUNT(*)`
    pub arg: Option<AnalyzedExpression>,

    /// Type of the aggregate's result, as the analyzer resolved it.
    pub return_type: DataType,
}

/// One ORDER BY key, evaluated against the sort's input row.
#[derive(Debug)]
pub struct SortKey {
    pub expr: AnalyzedExpression,
    pub descending: bool,
}
