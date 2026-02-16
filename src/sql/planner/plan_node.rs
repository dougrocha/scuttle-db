use crate::sql::analyzer::AnalyzedExpression;

#[derive(Debug)]
pub enum PlanNode {
    Scan {
        table: String,
    },
    Filter {
        input: Box<PlanNode>,
        condition: AnalyzedExpression,
    },
    Projection {
        input: Box<PlanNode>,
        expressions: Vec<AnalyzedExpression>,
    },
    Values {
        expressions: Vec<Vec<AnalyzedExpression>>,
    },
}
