//! Renders a [`LogicalPlan`] as an indented tree, like Postgres' `EXPLAIN`.
//!
//! Expressions refer to columns by index, so each node is printed with the column
//! names of its input: a scan's table columns, a projection's output names, and so on.

use miette::Result;

use crate::Value;
use crate::sql::analyzer::{AggregateFunction, AnalyzedExpression, IsPredicateTarget};
use crate::sql::catalog_context::CatalogContext;
use crate::sql::planner::logical::{AggregateCall, LogicalPlan};

/// Formats `plan` as one line per node, children indented under their parent.
///
/// # Errors
///
/// Returns an error if a scanned table no longer exists in the catalog.
pub(crate) fn explain(plan: &LogicalPlan, catalog: &CatalogContext) -> Result<String> {
    let mut lines = Vec::new();
    write_node(plan, catalog, 0, &mut lines)?;
    Ok(lines.join("\n"))
}

fn write_node(
    plan: &LogicalPlan,
    catalog: &CatalogContext,
    depth: usize,
    lines: &mut Vec<String>,
) -> Result<()> {
    let indent = "  ".repeat(depth);

    match plan {
        LogicalPlan::Scan { table_name } => lines.push(format!("{indent}Scan: {table_name}")),
        LogicalPlan::Filter { input, condition } => {
            let columns = output_columns(input, catalog)?;
            lines.push(format!(
                "{indent}Filter: {}",
                expression(condition, &columns)
            ));
            write_node(input, catalog, depth + 1, lines)?;
        }
        LogicalPlan::Projection {
            input,
            column_names,
            ..
        } => {
            lines.push(format!("{indent}Projection: {}", column_names.join(", ")));
            write_node(input, catalog, depth + 1, lines)?;
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let columns = output_columns(input, catalog)?;
            let mut line = format!("{indent}Aggregate: ");
            if !group_by.is_empty() {
                let keys: Vec<String> = group_by.iter().map(|e| expression(e, &columns)).collect();
                line.push_str(&format!("group by {}; ", keys.join(", ")));
            }
            let calls: Vec<String> = aggregates.iter().map(|a| aggregate(a, &columns)).collect();
            line.push_str(&calls.join(", "));
            lines.push(line);
            write_node(input, catalog, depth + 1, lines)?;
        }
        LogicalPlan::Sort { input, keys } => {
            let columns = output_columns(input, catalog)?;
            let keys: Vec<String> = keys
                .iter()
                .map(|key| {
                    let expr = expression(&key.expr, &columns);
                    if key.descending {
                        format!("{expr} DESC")
                    } else {
                        expr
                    }
                })
                .collect();
            lines.push(format!("{indent}Sort: {}", keys.join(", ")));
            write_node(input, catalog, depth + 1, lines)?;
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let mut line = match limit {
                Some(limit) => format!("{indent}Limit: {limit}"),
                None => format!("{indent}Limit: all"),
            };
            if *offset > 0 {
                line.push_str(&format!(" offset {offset}"));
            }
            lines.push(line);
            write_node(input, catalog, depth + 1, lines)?;
        }
        LogicalPlan::Values { expressions } => {
            let rows = expressions.len();
            let noun = if rows == 1 { "row" } else { "rows" };
            lines.push(format!("{indent}Values: {rows} {noun}"));
        }
        LogicalPlan::Insert {
            table_name,
            column_names,
            source,
        } => {
            lines.push(format!(
                "{indent}Insert: {table_name} ({})",
                column_names.join(", ")
            ));
            write_node(source, catalog, depth + 1, lines)?;
        }
        LogicalPlan::Update {
            table_name,
            assignments,
            filter,
        } => {
            let columns = table_columns(table_name, catalog)?;
            let sets: Vec<String> = assignments
                .iter()
                .map(|(index, value)| {
                    format!(
                        "{} = {}",
                        column_name(*index, &columns),
                        expression(value, &columns)
                    )
                })
                .collect();
            lines.push(format!(
                "{indent}Update: {table_name} set {}",
                sets.join(", ")
            ));
            write_filtered_scan(table_name, filter.as_ref(), &columns, depth + 1, lines);
        }
        LogicalPlan::Delete { table_name, filter } => {
            let columns = table_columns(table_name, catalog)?;
            lines.push(format!("{indent}Delete: {table_name}"));
            write_filtered_scan(table_name, filter.as_ref(), &columns, depth + 1, lines);
        }
        LogicalPlan::CreateTable {
            table_name,
            columns,
        } => {
            let columns: Vec<String> = columns
                .iter()
                .map(|col| format!("{} {}", col.name, col.data_type))
                .collect();
            lines.push(format!(
                "{indent}Create table: {table_name} ({})",
                columns.join(", ")
            ));
        }
    }

    Ok(())
}

/// Writes the scan (and optional filter) that UPDATE and DELETE read their rows from.
fn write_filtered_scan(
    table_name: &str,
    filter: Option<&AnalyzedExpression>,
    columns: &[String],
    depth: usize,
    lines: &mut Vec<String>,
) {
    let mut depth = depth;
    if let Some(filter) = filter {
        let indent = "  ".repeat(depth);
        lines.push(format!("{indent}Filter: {}", expression(filter, columns)));
        depth += 1;
    }
    lines.push(format!("{}Scan: {table_name}", "  ".repeat(depth)));
}

/// Names of the columns `plan` produces, used to print expressions of the node above it.
fn output_columns(plan: &LogicalPlan, catalog: &CatalogContext) -> Result<Vec<String>> {
    Ok(match plan {
        LogicalPlan::Scan { table_name } => table_columns(table_name, catalog)?,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => output_columns(input, catalog)?,
        LogicalPlan::Projection { column_names, .. } => column_names.clone(),
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let columns = output_columns(input, catalog)?;
            group_by
                .iter()
                .map(|e| expression(e, &columns))
                .chain(aggregates.iter().map(|a| aggregate(a, &columns)))
                .collect()
        }
        _ => Vec::new(),
    })
}

fn table_columns(table_name: &str, catalog: &CatalogContext) -> Result<Vec<String>> {
    let schema = &catalog.get_table(table_name)?.schema;
    Ok(schema.columns.iter().map(|col| col.name.clone()).collect())
}

fn column_name(index: usize, columns: &[String]) -> String {
    columns
        .get(index)
        .cloned()
        .unwrap_or_else(|| format!("#{index}"))
}

fn expression(expr: &AnalyzedExpression, columns: &[String]) -> String {
    match expr {
        AnalyzedExpression::Literal(value) => literal(value),
        AnalyzedExpression::Column(column, _) => column_name(column.index, columns),
        AnalyzedExpression::BinaryExpr {
            left, op, right, ..
        } => format!(
            "{} {op} {}",
            operand(left, columns),
            operand(right, columns)
        ),
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
            format!("{} IS {not}{target}", operand(expr, columns))
        }
    }
}

/// Like [`expression`], but parenthesizes nested binary expressions so precedence stays visible.
fn operand(expr: &AnalyzedExpression, columns: &[String]) -> String {
    match expr {
        AnalyzedExpression::BinaryExpr { .. } => format!("({})", expression(expr, columns)),
        _ => expression(expr, columns),
    }
}

fn literal(value: &Value) -> String {
    match value {
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        other => other.to_string(),
    }
}

fn aggregate(call: &AggregateCall, columns: &[String]) -> String {
    let name = match call.function {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Avg => "AVG",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    };
    match &call.arg {
        Some(arg) => format!("{name}({})", expression(arg, columns)),
        None => format!("{name}(*)"),
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    fn users() -> Database {
        let mut db = Database::in_memory();
        db.execute_query("CREATE TABLE users (id INT, name TEXT, dept TEXT, age INT)")
            .unwrap();
        db.execute_query("INSERT INTO users VALUES (1, 'Alice', 'eng', 30), (2, 'Bob', 'ops', 25)")
            .unwrap();
        db
    }

    #[test]
    fn test_explain_select_with_filter() {
        let plan = users()
            .explain("SELECT name FROM users WHERE dept = 'eng' AND age > 20")
            .unwrap();
        assert_eq!(
            plan,
            "Projection: name\n  Filter: (dept = 'eng') AND (age > 20)\n    Scan: users"
        );
    }

    #[test]
    fn test_explain_select_star_without_filter() {
        let plan = users().explain("SELECT * FROM users").unwrap();
        assert_eq!(plan, "Projection: id, name, dept, age\n  Scan: users");
    }

    #[test]
    fn test_explain_order_by_and_limit() {
        let plan = users()
            .explain("SELECT name FROM users ORDER BY age DESC LIMIT 1")
            .unwrap();
        assert!(plan.starts_with("Limit: 1"), "{plan}");
        assert!(plan.contains("Sort: "), "{plan}");
        assert!(plan.contains("DESC"), "{plan}");
        assert!(plan.ends_with("Scan: users"), "{plan}");
    }

    #[test]
    fn test_explain_group_by_aggregate() {
        let plan = users()
            .explain("SELECT dept, COUNT(*) AS total FROM users GROUP BY dept")
            .unwrap();
        assert!(
            plan.contains("Aggregate: group by dept; COUNT(*)"),
            "{plan}"
        );
        assert!(plan.ends_with("Scan: users"), "{plan}");
    }

    #[test]
    fn test_explain_update_and_delete() {
        let mut db = users();
        assert_eq!(
            db.explain("UPDATE users SET age = age + 1 WHERE name = 'Bob'")
                .unwrap(),
            "Update: users set age = age + 1\n  Filter: name = 'Bob'\n    Scan: users"
        );
        assert_eq!(
            db.explain("DELETE FROM users").unwrap(),
            "Delete: users\n  Scan: users"
        );
    }

    #[test]
    fn test_explain_does_not_execute() {
        let mut db = users();
        db.explain("DELETE FROM users").unwrap();
        assert_eq!(
            db.execute_query("SELECT * FROM users").unwrap().rows.len(),
            2
        );
    }

    #[test]
    fn test_explain_missing_table_fails() {
        assert!(users().explain("SELECT * FROM nope").is_err());
    }
}
