use miette::{Result, miette};

use crate::parser::{ColumnList, Expression, Statement};

#[derive(Debug)]
pub enum LogicalPlan {
    TableScan {
        table_name: String,
    },
    Filter {
        predicate: Expression,
        input: Box<LogicalPlan>,
    },
    Projection {
        columns: Vec<String>,
        input: Box<LogicalPlan>,
    },
}

impl LogicalPlan {
    pub fn from_statement(statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::Select {
                table,
                columns,
                r#where,
            } => Self::build_select_plan(table, columns, r#where),
            Statement::Create => Err(miette!("CREATE not implemented yet")),
            Statement::Update { .. } => Err(miette!("INSERT not implemented yet")),
            Statement::Insert => Err(miette!("INSERT not implemented yet")),
            Statement::Delete => Err(miette!("DELETE not implemented yet")),
        }
    }

    fn build_select_plan(
        table_name: String,
        projected_columns: ColumnList,
        where_clause: Option<Expression>,
    ) -> Result<LogicalPlan> {
        let mut plan = LogicalPlan::TableScan { table_name };

        if let Some(predicate) = where_clause {
            plan = LogicalPlan::Filter {
                predicate,
                input: Box::new(plan),
            };
        }

        if let ColumnList::Columns(cols) = projected_columns {
            plan = LogicalPlan::Projection {
                columns: cols,
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    pub fn extract_table_name(plan: &LogicalPlan) -> Result<&str> {
        match plan {
            LogicalPlan::TableScan { table_name } => Ok(table_name),
            LogicalPlan::Filter { input, .. } => Self::extract_table_name(input),
            LogicalPlan::Projection { input, .. } => Self::extract_table_name(input),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Expression, LiteralValue, Operator, SqlParser};

    #[test]
    fn test_table_scan_only() {
        let query = "SELECT * FROM users";
        let statement = SqlParser::new(query)
            .parse()
            .expect("Failed to parse query");
        let logical_plan =
            LogicalPlan::from_statement(statement).expect("Failed to create logical plan");

        match logical_plan {
            LogicalPlan::TableScan { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected a TableScan plan"),
        }
    }

    #[test]
    fn test_projection_without_filter() {
        let query = "SELECT id, name FROM users";
        let statement = SqlParser::new(query)
            .parse()
            .expect("Failed to parse query");
        let logical_plan =
            LogicalPlan::from_statement(statement).expect("Failed to create logical plan");

        match logical_plan {
            LogicalPlan::Projection { columns, input } => {
                assert_eq!(columns, vec!["id", "name"]);
                match *input {
                    LogicalPlan::TableScan { table_name } => {
                        assert_eq!(table_name, "users");
                    }
                    _ => panic!("Expected TableScan as input to Projection"),
                }
            }
            _ => panic!("Expected a Projection plan"),
        }
    }

    #[test]
    fn test_filter_without_projection() {
        let query = "SELECT * FROM products WHERE price = 100";
        let statement = SqlParser::new(query)
            .parse()
            .expect("Failed to parse query");
        let logical_plan =
            LogicalPlan::from_statement(statement).expect("Failed to create logical plan");

        match logical_plan {
            LogicalPlan::Filter { predicate, input } => {
                match predicate {
                    Expression::BinaryOp { left, op, right } => {
                        assert!(matches!(*left, Expression::Column(ref col) if col == "price"));
                        assert!(matches!(op, Operator::Equal));
                        assert!(
                            matches!(*right, Expression::Literal(LiteralValue::Number(n)) if n == 100.0)
                        );
                    }
                    _ => panic!("Expected BinaryOp predicate"),
                }
                match *input {
                    LogicalPlan::TableScan { table_name } => {
                        assert_eq!(table_name, "products");
                    }
                    _ => panic!("Expected TableScan as input to Filter"),
                }
            }
            _ => panic!("Expected a Filter plan"),
        }
    }

    #[test]
    fn test_complex_plan_with_projection_and_filter() {
        let query = "SELECT name, email FROM customers WHERE active = 1";
        let statement = SqlParser::new(query)
            .parse()
            .expect("Failed to parse query");
        let logical_plan =
            LogicalPlan::from_statement(statement).expect("Failed to create logical plan");

        match logical_plan {
            LogicalPlan::Projection { columns, input } => {
                assert_eq!(columns, vec!["name", "email"]);
                match *input {
                    LogicalPlan::Filter { predicate, input } => {
                        match predicate {
                            Expression::BinaryOp { left, op, right } => {
                                assert!(
                                    matches!(*left, Expression::Column(ref col) if col == "active")
                                );
                                assert!(matches!(op, Operator::Equal));
                                assert!(
                                    matches!(*right, Expression::Literal(LiteralValue::Number(n)) if n == 1.0)
                                );
                            }
                            _ => panic!("Expected BinaryOp predicate"),
                        }
                        match *input {
                            LogicalPlan::TableScan { table_name } => {
                                assert_eq!(table_name, "customers");
                            }
                            _ => panic!("Expected TableScan as input to Filter"),
                        }
                    }
                    _ => panic!("Expected Filter as input to Projection"),
                }
            }
            _ => panic!("Expected a Projection plan"),
        }
    }
}
