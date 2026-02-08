use miette::{Result, miette};

use crate::{
    db::table::{Row, Value},
    sql::{
        analyzer::AnalyzedExpression,
        evaluator::{Evaluator, expression::ExpressionEvaluator},
    },
};

pub struct PredicateEvaluator;

impl Evaluator<bool> for PredicateEvaluator {
    fn evaluate(&self, analyzed_expr: &AnalyzedExpression, row: &Row) -> Result<bool> {
        let expr_evaluator = ExpressionEvaluator;
        let value = expr_evaluator.evaluate(analyzed_expr, row)?;

        match value {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Err(miette!(
                "WHERE clause must evaluate to a boolean, got {:?}",
                value
            )),
        }
    }
}
