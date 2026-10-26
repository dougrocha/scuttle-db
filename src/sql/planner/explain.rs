//! Renders a [`LogicalPlan`] as an indented tree, like Postgres' `EXPLAIN`.
//!
//! Expressions refer to columns by index, so each node is printed with the labels of the
//! schema it reads: a scan's table columns, a join's qualified columns, a projection's
//! output names, and so on.

use std::ops::Bound;

use crate::db::table::index::IndexRange;
use crate::sql::analyzer::AnalyzedExpression;
use crate::sql::planner::logical::{LogicalPlan, SortKey};
use crate::sql::planner::relation_schema::RelationSchema;

/// Formats `plan` as one line per node, children indented under their parent.
pub(crate) fn explain(plan: &LogicalPlan) -> String {
    let mut lines = Vec::new();
    write_node(plan, 0, &mut lines);
    lines.join("\n")
}

fn write_node(plan: &LogicalPlan, depth: usize, lines: &mut Vec<String>) {
    let indent = "  ".repeat(depth);

    match plan {
        LogicalPlan::Scan {
            table_name,
            alias,
            schema,
            index,
        } => {
            let mut line = match index {
                Some(_) => format!("{indent}IndexScan: {table_name}"),
                None => format!("{indent}Scan: {table_name}"),
            };
            if let Some(alias) = alias {
                line.push_str(&format!(" {alias}"));
            }
            if let Some(range) = index {
                let column = &schema.columns[range.column].name;
                line.push_str(&format!(" ({})", render_range(column, range)));
            }
            lines.push(line);
        }
        LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            ..
        } => {
            lines.push(format!(
                "{indent}Join: {kind} on {}",
                plan.output_schema().render(on)
            ));
            write_node(left, depth + 1, lines);
            write_node(right, depth + 1, lines);
        }
        LogicalPlan::Filter { input, condition } => {
            let columns = input.output_schema();
            lines.push(format!("{indent}Filter: {}", columns.render(condition)));
            write_node(input, depth + 1, lines);
        }
        LogicalPlan::Projection { input, .. } => {
            lines.push(format!(
                "{indent}Projection: {}",
                plan.output_schema().names().join(", ")
            ));
            write_node(input, depth + 1, lines);
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let columns = input.output_schema();
            let mut line = format!("{indent}Aggregate: ");
            if !group_by.is_empty() {
                let keys: Vec<String> = group_by.iter().map(|e| columns.render(e)).collect();
                line.push_str(&format!("group by {}; ", keys.join(", ")));
            }
            let calls: Vec<String> = aggregates
                .iter()
                .map(|a| columns.render_aggregate(a))
                .collect();
            line.push_str(&calls.join(", "));
            lines.push(line);
            write_node(input, depth + 1, lines);
        }
        LogicalPlan::Sort { input, keys } => {
            lines.push(format!("{indent}Sort: {}", render_keys(input, keys)));
            write_node(input, depth + 1, lines);
        }
        LogicalPlan::TopN { input, keys, k } => {
            lines.push(format!("{indent}TopN: {k} by {}", render_keys(input, keys)));
            write_node(input, depth + 1, lines);
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
            write_node(input, depth + 1, lines);
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
            write_node(source, depth + 1, lines);
        }
        LogicalPlan::Update {
            table_name,
            assignments,
            filter,
            schema,
        } => {
            let columns = RelationSchema::table(table_name, schema);
            let sets: Vec<String> = assignments
                .iter()
                .map(|(index, value)| {
                    format!("{} = {}", columns.label(*index), columns.render(value))
                })
                .collect();
            lines.push(format!(
                "{indent}Update: {table_name} set {}",
                sets.join(", ")
            ));
            write_filtered_scan(table_name, filter.as_ref(), &columns, depth + 1, lines);
        }
        LogicalPlan::Delete {
            table_name,
            filter,
            schema,
        } => {
            let columns = RelationSchema::table(table_name, schema);
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
}

/// Writes the scan (and optional filter) that UPDATE and DELETE read their rows from.
fn write_filtered_scan(
    table_name: &str,
    filter: Option<&AnalyzedExpression>,
    columns: &RelationSchema,
    depth: usize,
    lines: &mut Vec<String>,
) {
    let mut depth = depth;
    if let Some(filter) = filter {
        let indent = "  ".repeat(depth);
        lines.push(format!("{indent}Filter: {}", columns.render(filter)));
        depth += 1;
    }
    lines.push(format!("{}Scan: {table_name}", "  ".repeat(depth)));
}

/// An index range as the conditions it stands for: `id = 5`, or `id > 10 AND id <= 50`.
/// ORDER BY keys as written: `age DESC, name`.
fn render_keys(input: &LogicalPlan, keys: &[SortKey]) -> String {
    let columns = input.output_schema();
    let keys: Vec<String> = keys
        .iter()
        .map(|key| {
            let expr = columns.render(&key.expr);
            if key.descending {
                format!("{expr} DESC")
            } else {
                expr
            }
        })
        .collect();
    keys.join(", ")
}

fn render_range(column: &str, range: &IndexRange) -> String {
    if let (Bound::Included(lo), Bound::Included(hi)) = (&range.lower, &range.upper)
        && lo == hi
    {
        return format!("{column} = {lo}");
    }

    let lower = match &range.lower {
        Bound::Included(key) => Some(format!("{column} >= {key}")),
        Bound::Excluded(key) => Some(format!("{column} > {key}")),
        Bound::Unbounded => None,
    };
    let upper = match &range.upper {
        Bound::Included(key) => Some(format!("{column} <= {key}")),
        Bound::Excluded(key) => Some(format!("{column} < {key}")),
        Bound::Unbounded => None,
    };
    [lower, upper]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" AND ")
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
    fn test_explain_scan_shows_alias() {
        let plan = users().explain("SELECT u.name FROM users u").unwrap();
        assert_eq!(plan, "Projection: name\n  Scan: users u");
    }

    #[test]
    fn test_explain_order_by_and_limit() {
        let plan = |sql: &str| users().explain(sql).unwrap();

        // A LIMIT bounds the sort: keep limit + offset rows, then the Limit skips the offset
        assert_eq!(
            plan("SELECT name FROM users ORDER BY age DESC LIMIT 1 OFFSET 2"),
            "Limit: 1 offset 2\n  Projection: name\n    TopN: 3 by age DESC\n      Scan: users"
        );

        // Without a LIMIT every row is sorted
        assert!(plan("SELECT name FROM users ORDER BY age").contains("Sort: age"));
        assert!(plan("SELECT name FROM users ORDER BY age OFFSET 2").contains("Sort: age"));
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

    /// `users` plus `orders(id, user_id, total)` and `depts(code, title)`.
    fn shop() -> Database {
        let mut db = users();
        db.execute_query("CREATE TABLE orders (id INT, user_id INT, total INT)")
            .unwrap();
        db.execute_query("CREATE TABLE depts (code TEXT, title TEXT)")
            .unwrap();
        db
    }

    #[test]
    fn test_explain_inner_join() {
        let plan = shop()
            .explain("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
            .unwrap();
        assert_eq!(
            plan,
            "Projection: name, total\n  Join: inner on u.id = o.user_id\n    Scan: users u\n    Scan: orders o"
        );
    }

    #[test]
    fn test_explain_left_join_with_filter_uses_qualified_names() {
        let plan = shop()
            .explain(
                "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.total > 10",
            )
            .unwrap();
        assert_eq!(
            plan,
            "Projection: name, total\n  Filter: o.total > 10\n    Join: left on u.id = o.user_id\n      Scan: users u\n      Scan: orders o"
        );
    }

    #[test]
    fn test_explain_self_join_names_both_sides() {
        let plan = shop()
            .explain("SELECT a.name FROM users a JOIN users b ON a.id = b.id")
            .unwrap();
        assert!(plan.contains("Join: inner on a.id = b.id"), "{plan}");
    }

    #[test]
    fn test_explain_join_chain_is_left_deep() {
        let plan = shop()
            .explain(
                "SELECT u.name, o.total, d.title FROM users u JOIN orders o ON u.id = o.user_id LEFT JOIN depts d ON u.dept = d.code",
            )
            .unwrap();
        let (_, tree) = plan.split_once('\n').unwrap();
        assert_eq!(
            tree,
            "  Join: left on u.dept = d.code\n    Join: inner on u.id = o.user_id\n      Scan: users u\n      Scan: orders o\n    Scan: depts d"
        );
    }

    #[test]
    fn test_explain_aggregate_over_join() {
        let plan = shop()
            .explain(
                "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            )
            .unwrap();
        assert!(
            plan.contains("Aggregate: group by u.name; COUNT(o.id)"),
            "{plan}"
        );
    }

    #[test]
    fn test_explain_column_names_follow_input_shape() {
        let cases = [
            (
                "SELECT dept, COUNT(*) FROM users GROUP BY dept ORDER BY COUNT(*) DESC, dept",
                "Projection: dept, count\n  Sort: COUNT(*) DESC, dept\n    Aggregate: group by dept; COUNT(*)\n      Scan: users",
            ),
            (
                "SELECT age + 1, COUNT(*) FROM users GROUP BY age + 1 ORDER BY age + 1",
                "Projection: ?column?, count\n  Sort: age + 1\n    Aggregate: group by age + 1; COUNT(*)\n      Scan: users",
            ),
            (
                "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name, COUNT(o.id) DESC",
                "Projection: name, count\n  Sort: u.name, COUNT(o.id) DESC\n    Aggregate: group by u.name; COUNT(o.id)\n      Join: left on u.id = o.user_id\n        Scan: users u\n        Scan: orders o",
            ),
            (
                "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.total DESC LIMIT 2 OFFSET 1",
                "Limit: 2 offset 1\n  Projection: name, total\n    TopN: 3 by o.total DESC\n      Join: inner on u.id = o.user_id\n        Scan: users u\n        Scan: orders o",
            ),
            (
                "SELECT u.name FROM users u WHERE u.age > 20 ORDER BY u.age",
                "Projection: name\n  Sort: age\n    Filter: age > 20\n      Scan: users u",
            ),
            (
                "SELECT users.name FROM users users WHERE users.age > 20",
                "Projection: name\n  Filter: age > 20\n    Scan: users users",
            ),
            (
                "SELECT u.name, SUM(o.total) FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 5 GROUP BY u.name",
                "Projection: name, sum\n  Aggregate: group by u.name; SUM(o.total)\n    Filter: o.total > 5\n      Join: inner on u.id = o.user_id\n        Scan: users u\n        Scan: orders o",
            ),
            (
                "SELECT COUNT(*) FROM users",
                "Projection: count\n  Aggregate: COUNT(*)\n    Scan: users",
            ),
            (
                "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN depts d ON u.dept = d.code WHERE d.title = 'x' AND u.id IS NOT NULL",
                "Projection: id, name, dept, age, id, user_id, total, code, title\n  Filter: (d.title = 'x') AND u.id IS NOT NULL\n    Join: inner on u.dept = d.code\n      Join: inner on u.id = o.user_id\n        Scan: users u\n        Scan: orders o\n      Scan: depts d",
            ),
            (
                "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')",
                "Insert: users (id, name)\n  Values: 2 rows",
            ),
            (
                "UPDATE users SET age = age + 1, name = 'x' WHERE id = 1",
                "Update: users set age = age + 1, name = 'x'\n  Filter: id = 1\n    Scan: users",
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(shop().explain(sql).unwrap(), expected, "{sql}");
        }
    }

    #[test]
    fn test_explain_join_with_zero_column_table_is_still_qualified() {
        let mut db = shop();
        db.execute_query("CREATE TABLE z ()").unwrap();
        assert_eq!(
            db.explain("SELECT u.id FROM z JOIN users u ON u.id = 1 WHERE u.id > 1")
                .unwrap(),
            "Projection: id\n  Filter: u.id > 1\n    Join: inner on u.id = 1\n      Scan: z\n      Scan: users u"
        );
    }

    #[test]
    fn test_explain_join_errors() {
        let cases = [
            // Ambiguous unqualified column (`id` is in both tables).
            "SELECT id FROM users u JOIN orders o ON u.id = o.user_id",
            // Same unaliased table twice.
            "SELECT * FROM users JOIN users ON users.id = users.id",
            // Same alias twice.
            "SELECT * FROM users a JOIN orders a ON a.id = a.id",
            // Unknown alias in ON.
            "SELECT * FROM users u JOIN orders o ON u.id = x.user_id",
            // ON refers to a table that is joined later.
            "SELECT * FROM users u JOIN orders o ON u.id = d.code JOIN depts d ON u.dept = d.code",
            // ON is not boolean.
            "SELECT * FROM users u JOIN orders o ON u.id",
            // Table name is hidden once it has an alias.
            "SELECT users.name FROM users u JOIN orders o ON u.id = o.user_id",
        ];
        for sql in cases {
            assert!(shop().explain(sql).is_err(), "should fail: {sql}");
        }
    }
}
