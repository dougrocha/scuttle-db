use crate::{
    Row, Schema, Value,
    sql::{
        evaluator::{
            Evaluator, values_add, values_divide, values_equal, values_greater_than,
            values_less_than, values_multiply, values_subtract,
        },
        parser::{Expression, Operator},
    },
};
use miette::{Result, miette};

pub struct ExpressionEvaluator;

impl Evaluator<Value> for ExpressionEvaluator {
    fn evaluate(&self, expression: &Expression, row: &Row, schema: &Schema) -> Result<Value> {
        match expression {
            Expression::Literal(lit) => Ok(Value::from(lit.clone())),
            Expression::Column(name) => {
                let idx = schema
                    .get_column_index(name)
                    .ok_or(miette!("Column '{}' does not exist", name))?;

                Ok(row.get_value(idx).cloned().unwrap_or(Value::Null))
            }
            Expression::BinaryOp { left, op, right } => {
                // Special handling for Logic to support Short-Circuiting
                // (We don't want to evaluate the right side if left decides the result)
                match op {
                    Operator::And => return self.evaluate_and(left, right, row, schema),
                    Operator::Or => return self.evaluate_or(left, right, row, schema),
                    _ => {}
                }

                // For Math/Comparison, we must evaluate both sides first
                let left_val = self.evaluate(left, row, schema)?;
                let right_val = self.evaluate(right, row, schema)?;

                match op {
                    // Math
                    Operator::Add => values_add(&left_val, &right_val),
                    Operator::Subtract => values_subtract(&left_val, &right_val),
                    Operator::Multiply => values_multiply(&left_val, &right_val),
                    Operator::Divide => values_divide(&left_val, &right_val),

                    // Comparisons (These now return Value::Boolean)
                    Operator::Equal => values_equal(&left_val, &right_val),
                    Operator::NotEqual => {
                        let eq = match values_equal(&left_val, &right_val)? {
                            Value::Boolean(b) => Value::Boolean(!b),
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
                            Value::Boolean(b) => Ok(Value::Boolean(!b)),
                            Value::Null => Ok(Value::Null),
                            _ => unreachable!("Comparison should return Bool or Null"),
                        }
                    }
                    Operator::LessThanEqual => {
                        let less = values_greater_than(&left_val, &right_val)?;
                        match less {
                            Value::Boolean(b) => Ok(Value::Boolean(!b)),
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
        }
    }
}

impl ExpressionEvaluator {
    /// Helper to determine if a Value is "True" in SQL logic
    fn is_truthy(&self, val: &Value) -> bool {
        match val {
            Value::Boolean(b) => *b,
            _ => false, // Null, 0, strings are all "False" in strict boolean logic
        }
    }

    fn evaluate_and(
        &self,
        left: &Expression,
        right: &Expression,
        row: &Row,
        schema: &Schema,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, row, schema)?;
        if !self.is_truthy(&left_val) {
            return Ok(Value::Boolean(false));
        }

        let right_val = self.evaluate(right, row, schema)?;
        Ok(Value::Boolean(self.is_truthy(&right_val)))
    }

    fn evaluate_or(
        &self,
        left: &Expression,
        right: &Expression,
        row: &Row,
        schema: &Schema,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, row, schema)?;
        if self.is_truthy(&left_val) {
            return Ok(Value::Boolean(true));
        }

        let right_val = self.evaluate(right, row, schema)?;
        Ok(Value::Boolean(self.is_truthy(&right_val)))
    }
}
