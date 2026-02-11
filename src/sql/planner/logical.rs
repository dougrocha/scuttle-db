use crate::sql::analyzer::{AnalyzedExpression, schema::OutputSchema};

#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        schema: OutputSchema,
    },
    Filter {
        input: Box<LogicalPlan>,
        condition: AnalyzedExpression,
    },
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<AnalyzedExpression>,
        schema: OutputSchema,
    },
}

impl LogicalPlan {
    pub fn schema(&self) -> &OutputSchema {
        match self {
            LogicalPlan::Scan { schema, .. } | LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
        }
    }
}
