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
