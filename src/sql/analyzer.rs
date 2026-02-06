use miette::{Result, miette};

use crate::{
    Schema, Table,
    sql::{
        logical_planner::LogicalPlan,
        parser::{Expression, SelectList, SelectTarget, Statement},
        planner_context::PlannerContext,
    },
};

pub struct Analyzer<'a> {
    context: &'a PlannerContext<'a>,
}

impl<'a> Analyzer<'a> {
    pub fn new(context: &'a PlannerContext) -> Self {
        Self { context }
    }

    pub fn analyze_plan(&mut self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::Select {
                from_clause: table,
                select_list: targets,
                where_clause: r#where,
            } => {
                let resolved_table = self.context.get_table(&table)?;

                let (expanded_targets, names) = expand_targets(targets, resolved_table.schema())?;

                let mut plan = LogicalPlan::TableScan {
                    table: resolved_table.name().to_owned(),
                };

                if let Some(predicate) = r#where {
                    plan = LogicalPlan::Filter {
                        predicate,
                        input: Box::new(plan),
                    };
                }

                Ok(LogicalPlan::Projection {
                    expressions: expanded_targets,
                    column_names: names,
                    input: Box::new(plan),
                })
            }
            Statement::Create => Err(miette!("CREATE not implemented yet")),
            Statement::Update { .. } => Err(miette!("INSERT not implemented yet")),
            Statement::Insert => Err(miette!("INSERT not implemented yet")),
            Statement::Delete => Err(miette!("DELETE not implemented yet")),
        }
    }
}

/// Expands target list into expressions and column names.
///
/// This function handles:
/// - Star expansion: `SELECT *` → all columns from schema
/// - Aliased expressions: `SELECT id AS user_id` → Expression + alias
/// - Unaliased expressions: `SELECT id` → Expression + column name
///
/// # Arguments
///
/// * `targets` - The target list from the SELECT statement
/// * `schema` - The schema of the table being queried
///
/// # Returns
///
/// A tuple of (expressions, column_names) where:
/// - `expressions` are the actual expressions to evaluate
/// - `column_names` are the output column names (for schema generation)
pub(crate) fn expand_targets(
    targets: SelectList,
    schema: &Schema,
) -> Result<(Vec<Expression>, Vec<String>)> {
    let mut expanded = Vec::new();
    let mut names = Vec::new();

    for target in targets {
        match target {
            SelectTarget::Star => {
                // This is the "Star Expansion" logic
                for col in &schema.columns {
                    expanded.push(Expression::Column(col.name.clone()));
                    names.push(col.name.to_string());
                }
            }
            SelectTarget::Expression { expr, alias } => {
                expanded.push(expr.clone());
                names.push(alias.clone().unwrap_or_else(|| expr.to_column_name()));
            }
        }
    }

    Ok((expanded, names))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColumnDefinition, DataType,
        sql::parser::{LiteralValue, operators::Operator},
    };

    /// Creates a test schema with common columns
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            ColumnDefinition::new("id", DataType::Integer, false),
            ColumnDefinition::new("name", DataType::Text, false),
            ColumnDefinition::new("email", DataType::Text, true),
            ColumnDefinition::new("age", DataType::Integer, true),
        ])
    }

    #[test]
    fn test_expand_star() {
        let schema = create_test_schema();
        let targets = vec![SelectTarget::Star];

        let (expressions, names) = expand_targets(targets, &schema).expect("Failed to expand star");

        // Should expand to all 4 columns
        assert_eq!(expressions.len(), 4);
        assert_eq!(names.len(), 4);

        // Check that all columns are present
        assert_eq!(names, vec!["id", "name", "email", "age"]);

        // Check expressions are correct
        assert!(matches!(
            expressions[0],
            Expression::Column(ref name) if name == "id"
        ));
        assert!(matches!(
            expressions[1],
            Expression::Column(ref name) if name == "name"
        ));
        assert!(matches!(
            expressions[2],
            Expression::Column(ref name) if name == "email"
        ));
        assert!(matches!(
            expressions[3],
            Expression::Column(ref name) if name == "age"
        ));
    }

    #[test]
    fn test_expand_single_column_no_alias() {
        let schema = create_test_schema();
        let targets = vec![SelectTarget::Expression {
            expr: Expression::Column("name".to_string()),
            alias: None,
        }];

        let (expressions, names) =
            expand_targets(targets, &schema).expect("Failed to expand target");

        assert_eq!(expressions.len(), 1);
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "name");
        assert!(matches!(
            expressions[0],
            Expression::Column(ref n) if n == "name"
        ));
    }

    #[test]
    fn test_expand_column_with_alias() {
        let schema = create_test_schema();
        let targets = vec![SelectTarget::Expression {
            expr: Expression::Column("id".to_string()),
            alias: Some("user_id".to_string()),
        }];

        let (expressions, names) =
            expand_targets(targets, &schema).expect("Failed to expand target");

        assert_eq!(expressions.len(), 1);
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "user_id"); // Should use alias
        assert!(matches!(
            expressions[0],
            Expression::Column(ref n) if n == "id"
        ));
    }

    #[test]
    fn test_expand_multiple_columns_mixed_aliases() {
        let schema = create_test_schema();
        let targets = vec![
            SelectTarget::Expression {
                expr: Expression::Column("id".to_string()),
                alias: Some("user_id".to_string()),
            },
            SelectTarget::Expression {
                expr: Expression::Column("name".to_string()),
                alias: None,
            },
            SelectTarget::Expression {
                expr: Expression::Column("email".to_string()),
                alias: Some("contact_email".to_string()),
            },
        ];

        let (expressions, names) =
            expand_targets(targets, &schema).expect("Failed to expand targets");

        assert_eq!(expressions.len(), 3);
        assert_eq!(names, vec!["user_id", "name", "contact_email"]);
    }

    #[test]
    fn test_expand_literal_expression_no_alias() {
        let schema = create_test_schema();
        let targets = vec![SelectTarget::Expression {
            expr: Expression::Literal(LiteralValue::Integer(42)),
            alias: None,
        }];

        let (expressions, names) =
            expand_targets(targets, &schema).expect("Failed to expand target");

        assert_eq!(expressions.len(), 1);
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "?column?"); // Default for unnamed expression
        assert!(matches!(
            expressions[0],
            Expression::Literal(LiteralValue::Integer(42))
        ));
    }

    #[test]
    fn test_expand_literal_with_alias() {
        let schema = create_test_schema();
        let targets = vec![SelectTarget::Expression {
            expr: Expression::Literal(LiteralValue::String("Hello".to_string())),
            alias: Some("greeting".to_string()),
        }];

        let (expressions, names) =
            expand_targets(targets, &schema).expect("Failed to expand target");

        assert_eq!(expressions.len(), 1);
        assert_eq!(names[0], "greeting");
    }

    #[test]
    fn test_expand_binary_expression() {
        let schema = create_test_schema();

        // age + 10 AS next_decade_age
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: Operator::Add,
            right: Box::new(Expression::Literal(LiteralValue::Integer(10))),
        };

        let targets = vec![SelectTarget::Expression {
            expr,
            alias: Some("next_decade_age".to_string()),
        }];

        let (expressions, names) =
            expand_targets(targets, &schema).expect("Failed to expand target");

        assert_eq!(expressions.len(), 1);
        assert_eq!(names[0], "next_decade_age");
        assert!(matches!(expressions[0], Expression::BinaryOp { .. }));
    }

    #[test]
    fn test_expand_star_with_empty_schema() {
        let schema = Schema::new(vec![]); // Empty schema
        let targets = vec![SelectTarget::Star];

        let (expressions, names) = expand_targets(targets, &schema).expect("Failed to expand star");

        // Should return empty for empty schema
        assert_eq!(expressions.len(), 0);
        assert_eq!(names.len(), 0);
    }

    #[test]
    fn test_expand_mixed_star_and_expressions() {
        let schema = Schema::new(vec![
            ColumnDefinition::new("id", DataType::Integer, false),
            ColumnDefinition::new("name", DataType::Text, false),
        ]);

        // SELECT *, 'extra' AS bonus
        let targets = vec![
            SelectTarget::Star,
            SelectTarget::Expression {
                expr: Expression::Literal(LiteralValue::String("extra".to_string())),
                alias: Some("bonus".to_string()),
            },
        ];

        let (expressions, names) =
            expand_targets(targets, &schema).expect("Failed to expand targets");

        assert_eq!(expressions.len(), 3); // id, name, literal
        assert_eq!(names, vec!["id", "name", "bonus"]);
    }
}
