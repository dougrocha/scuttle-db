use miette::{Result, miette};

use crate::{
    Row, Value,
    sql::{
        analyzer::{AnalyzedExpression, IsPredicateTarget},
        evaluator::{
            Evaluator, values_add, values_divide, values_equal, values_greater_than,
            values_less_than, values_multiply, values_subtract,
        },
        parser::Operator,
    },
};

pub struct ExpressionEvaluator;

impl Evaluator<Value> for ExpressionEvaluator {
    fn evaluate(&self, analyzed_expr: &AnalyzedExpression, row: &Row) -> Result<Value> {
        match analyzed_expr {
            AnalyzedExpression::Literal(value) => Ok(value.clone()),
            AnalyzedExpression::Column(column_reference, _) => {
                let row_val = row
                    .get_value(column_reference.index)
                    .unwrap_or(&Value::Null);

                Ok(row_val.clone())
            }
            AnalyzedExpression::BinaryExpr {
                left, op, right, ..
            } => {
                match op {
                    Operator::And => return self.evaluate_and(left, right, row),
                    Operator::Or => return self.evaluate_or(left, right, row),
                    _ => {}
                }

                // For Math/Comparison, we must evaluate both sides first
                let left_val = self.evaluate(left, row)?;
                let right_val = self.evaluate(right, row)?;

                match op {
                    // Math
                    Operator::Add => values_add(&left_val, &right_val),
                    Operator::Subtract => values_subtract(&left_val, &right_val),
                    Operator::Multiply => values_multiply(&left_val, &right_val),
                    Operator::Divide => values_divide(&left_val, &right_val),

                    // Comparisons (These now return Value::Boolean)
                    Operator::Equal => Ok(values_equal(&left_val, &right_val)),
                    Operator::NotEqual => {
                        let eq = match values_equal(&left_val, &right_val) {
                            Value::Bool(b) => Value::Bool(!b),
                            Value::Null => Value::Null,
                            _ => unreachable!("values_equal should only return Boolean or Null"),
                        };

                        Ok(eq)
                    }
                    Operator::GreaterThan => values_greater_than(&left_val, &right_val),
                    Operator::LessThan => values_less_than(&left_val, &right_val),
                    Operator::GreaterThanEqual => {
                        let less = values_less_than(&left_val, &right_val)?;
                        match less {
                            Value::Bool(b) => Ok(Value::Bool(!b)),
                            Value::Null => Ok(Value::Null),
                            _ => unreachable!("Comparison should return Bool or Null"),
                        }
                    }
                    Operator::LessThanEqual => {
                        let less = values_greater_than(&left_val, &right_val)?;
                        match less {
                            Value::Bool(b) => Ok(Value::Bool(!b)),
                            Value::Null => Ok(Value::Null),
                            _ => unreachable!("Comparison should return Bool or Null"),
                        }
                    }

                    _ => Err(miette!(
                        "Operator {:?} not implemented in ExpressionEvaluator",
                        op
                    )),
                }
            }
            AnalyzedExpression::IsPredicate {
                expr,
                predicate,
                negated,
            } => {
                let value = self.evaluate(expr, row)?;

                let match_against = match predicate {
                    IsPredicateTarget::True => Value::Bool(true),
                    IsPredicateTarget::False => Value::Bool(false),
                    IsPredicateTarget::Null => Value::Null,
                };

                let bool = if *negated {
                    value != match_against
                } else {
                    value == match_against
                };

                Ok(Value::Bool(bool))
            }
        }
    }
}

impl ExpressionEvaluator {
    /// Helper to determine if a Value is "True" in SQL logic
    fn is_truthy(val: &Value) -> bool {
        match val {
            Value::Bool(b) => *b,
            _ => false, // Null, 0, strings are all "False" in strict boolean logic
        }
    }

    fn evaluate_and(
        &self,
        left: &AnalyzedExpression,
        right: &AnalyzedExpression,
        row: &Row,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, row)?;
        if !Self::is_truthy(&left_val) {
            return Ok(Value::Bool(false));
        }

        let right_val = self.evaluate(right, row)?;
        Ok(Value::Bool(Self::is_truthy(&right_val)))
    }

    fn evaluate_or(
        &self,
        left: &AnalyzedExpression,
        right: &AnalyzedExpression,
        row: &Row,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, row)?;
        if Self::is_truthy(&left_val) {
            return Ok(Value::Bool(true));
        }

        let right_val = self.evaluate(right, row)?;
        Ok(Value::Bool(Self::is_truthy(&right_val)))
    }
}
