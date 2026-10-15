//! `RelationSchema`: the columns a plan node outputs, derived by `LogicalPlan::output_schema`.
//!
//! Position `i` of a node's schema describes `row.values[i]` of every row the node produces.
//! That is the only meaning a bound `ColumnRef { index }` has.
//!
//! Every column has two names, because two audiences read them:
//!
//! | Produced by | `name` (client header)       | `label` (EXPLAIN, for the node above) |
//! |-------------|------------------------------|---------------------------------------|
//! | Scan        | `name`                       | `name` (bare, even when aliased)      |
//! | Join        | `name`                       | `u.name` (qualified by the join)      |
//! | Aggregate   | `COUNT(o.id)`                | `COUNT(o.id)` (rendered text)         |
//! | Projection  | `n` / `count` / `?column?`   | same as `name`                        |

use crate::{
    Schema, Value,
    sql::{
        analyzer::{AggregateFunction, AnalyzedExpression, IsPredicateTarget},
        planner::logical::AggregateCall,
    },
};

/// One column of a node's output row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutputColumn {
    /// Binding name (alias, else table name) of the table the column is read from.
    /// `None` for computed columns (aggregate and projection outputs).
    pub(crate) relation: Option<String>,

    /// Name a client sees: the table column name, or the select-list output name.
    pub(crate) name: String,

    /// How EXPLAIN prints a reference to this column from the node above.
    pub(crate) label: String,
}

/// Ordered output columns of one plan node.
///
/// Built only by the constructors below, each of which mirrors how the matching executor
/// lays out its rows. Never stored on a plan node (derived on every call), so it cannot go
/// stale. `columns` is private so no caller can splice or reorder it.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct RelationSchema {
    columns: Vec<OutputColumn>,
}

impl RelationSchema {
    /// No columns: nodes that return no rows to a parent (DML, DDL, VALUES).
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// The columns of a table, read through `relation` (its alias or its name).
    /// Labels are BARE (`age`), even when `relation` is an alias.
    pub(crate) fn table(relation: &str, schema: &Schema) -> Self {
        Self {
            columns: schema
                .columns
                .iter()
                .map(|col| OutputColumn {
                    relation: Some(relation.to_string()),
                    name: col.name.clone(),
                    label: col.name.clone(),
                })
                .collect(),
        }
    }

    /// The row `JoinExec` emits: every `left` column, then every `right` column.
    ///
    /// Every column with a relation is relabelled `relation.name`. Re-qualifying a nested
    /// join's columns is a no-op (`u.id` stays `u.id`).
    pub(crate) fn join(left: &RelationSchema, right: &RelationSchema) -> Self {
        Self {
            columns: left
                .columns
                .iter()
                .chain(&right.columns)
                .map(|col| OutputColumn {
                    label: match &col.relation {
                        Some(relation) => format!("{relation}.{}", col.name),
                        None => col.label.clone(),
                    },
                    ..col.clone()
                })
                .collect(),
        }
    }

    /// Computed columns, in output order, with `relation = None` and `label == name`.
    pub(crate) fn computed(names: impl IntoIterator<Item = String>) -> Self {
        Self {
            columns: names
                .into_iter()
                .map(|name| OutputColumn {
                    relation: None,
                    label: name.clone(),
                    name,
                })
                .collect(),
        }
    }

    /// Client-facing names in row order; duplicates kept (`name`, `name`).
    pub(crate) fn names(&self) -> Vec<String> {
        self.columns.iter().map(|col| col.name.clone()).collect()
    }

    /// Number of columns in every row the node produces.
    pub(crate) fn len(&self) -> usize {
        self.columns.len()
    }

    /// EXPLAIN label of column `index`, or `#index` if out of range.
    pub(crate) fn label(&self, index: usize) -> String {
        self.columns
            .get(index)
            .map(|col| col.label.clone())
            .unwrap_or_else(|| format!("#{index}"))
    }

    /// SQL-like text of `expr`, whose column indexes point into this schema.
    pub(crate) fn render(&self, expr: &AnalyzedExpression) -> String {
        match expr {
            AnalyzedExpression::Literal(value) => literal(value),
            AnalyzedExpression::Column(column, _) => self.label(column.index),
            AnalyzedExpression::BinaryExpr {
                left, op, right, ..
            } => format!("{} {op} {}", self.operand(left), self.operand(right)),
            AnalyzedExpression::IsPredicate {
                expr,
                predicate,
                negated,
            } => {
                let target = match predicate {
                    IsPredicateTarget::True => "TRUE",
                    IsPredicateTarget::False => "FALSE",
                    IsPredicateTarget::Null => "NULL",
                };
                let not = if *negated { "NOT " } else { "" };
                format!("{} IS {not}{target}", self.operand(expr))
            }
        }
    }

    /// `COUNT(*)`, `SUM(o.total)`: an aggregate call rendered against this schema.
    pub(crate) fn render_aggregate(&self, call: &AggregateCall) -> String {
        let name = match call.function {
            AggregateFunction::Count => "COUNT",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
        };
        match &call.arg {
            Some(arg) => format!("{name}({})", self.render(arg)),
            None => format!("{name}(*)"),
        }
    }

    /// Like [`render`](Self::render), but parenthesizes nested binary expressions so
    /// precedence stays visible.
    fn operand(&self, expr: &AnalyzedExpression) -> String {
        match expr {
            AnalyzedExpression::BinaryExpr { .. } => format!("({})", self.render(expr)),
            _ => self.render(expr),
        }
    }
}

fn literal(value: &Value) -> String {
    match value {
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        other @ Value::Timestamp(_) => format!("TIMESTAMP '{other}'"),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDef, DataType, sql::planner::logical::LogicalPlan};

    /// `users(id, name)`
    fn users_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::Text),
        ])
    }

    /// `orders(id, total)`
    fn orders_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("total", DataType::Float64),
        ])
    }

    #[test]
    fn test_relation_schema_table_labels_are_bare_even_with_alias() {
        let plan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            alias: Some("u".to_string()),
            schema: users_schema(),
        };

        let schema = plan.output_schema();
        assert_eq!(schema.names(), vec!["id", "name"]);
        assert_eq!(schema.label(0), "id");
        assert_eq!(schema.label(1), "name");
        assert_eq!(schema.columns[0].relation.as_deref(), Some("u"));
    }

    #[test]
    fn test_relation_schema_join_is_left_then_right_and_qualified() {
        let joined = RelationSchema::join(
            &RelationSchema::table("u", &users_schema()),
            &RelationSchema::table("o", &orders_schema()),
        );

        let labels: Vec<String> = (0..4).map(|i| joined.label(i)).collect();
        assert_eq!(labels, vec!["u.id", "u.name", "o.id", "o.total"]);
        assert_eq!(joined.names(), vec!["id", "name", "id", "total"]);
    }

    #[test]
    fn test_relation_schema_label_out_of_range_is_hash_index() {
        let schema = RelationSchema::table("u", &users_schema());

        assert_eq!(schema.label(2), "#2");
        assert_eq!(RelationSchema::empty().label(0), "#0");
    }
}
