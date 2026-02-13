use crate::{ColumnDef, sql::analyzer::AnalyzedExpression};

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
    },
    Values {
        expressions: Vec<Vec<AnalyzedExpression>>,
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
