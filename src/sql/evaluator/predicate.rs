use crate::{
    db::table::{Row, Schema, Value},
    sql::{
        evaluator::{Evaluator, expression::ExpressionEvaluator},
        parser::Expression,
    },
};
use miette::{Result, miette};

pub struct PredicateEvaluator;

impl Evaluator<bool> for PredicateEvaluator {
    fn evaluate(&self, expression: &Expression, row: &Row, schema: &Schema) -> Result<bool> {
        let expr_evaluator = ExpressionEvaluator;
        let value = expr_evaluator.evaluate(expression, row, schema)?;

        // Convert the result to a strict Boolean
        match value {
            Value::Boolean(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Err(miette!(
                "WHERE clause must evaluate to a boolean, got {:?}",
                value
            )),
        }
    }
}
