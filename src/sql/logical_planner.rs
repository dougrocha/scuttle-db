use miette::Result;

use super::parser::Expression;

#[derive(Debug)]
pub enum LogicalPlan {
    TableScan {
        table: String,
    },
    Filter {
        predicate: Expression,
        input: Box<LogicalPlan>,
    },
    Projection {
        expressions: Vec<Expression>,
        column_names: Vec<String>,
        input: Box<LogicalPlan>,
    },
}

impl LogicalPlan {
    pub fn extract_table_name(plan: &LogicalPlan) -> Result<&str> {
        match plan {
            LogicalPlan::TableScan { table: table_name } => Ok(table_name),
            LogicalPlan::Filter { input, .. } => Self::extract_table_name(input),
            LogicalPlan::Projection { input, .. } => Self::extract_table_name(input),
        }
    }
}
