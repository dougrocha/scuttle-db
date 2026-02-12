use crate::{
    ColumnDef,
    sql::analyzer::{AnalyzedExpression, schema::OutputSchema},
};

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
    Values {
        expressions: Vec<Vec<AnalyzedExpression>>,
        schema: OutputSchema,
    },
    Insert {
        table_name: String,
        column_names: Vec<String>,
        source: Box<LogicalPlan>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
    },
}

impl LogicalPlan {
    pub fn output_schema(&self) -> &OutputSchema {
        match self {
            LogicalPlan::Scan { schema, .. } | LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.output_schema(),
            _ => panic!(),
        }
    }
}
