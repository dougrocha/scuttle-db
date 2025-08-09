use miette::{Result, miette};

use crate::{
    parser::{Expression, LiteralValue, Operator},
    table::{Row, Schema, Value},
};

pub struct PredicateEvaluator;

impl PredicateEvaluator {
    pub fn evaluate(&self, expression: &Expression, row: &Row, schema: &Schema) -> Result<bool> {
        match expression {
            Expression::BinaryOp { left, op, right } => {
                self.evaluate_binary_op(left, op, right, row, schema)
            }
            Expression::Column(_) => {
                Err(miette!("Column expression cannot be evaluated as boolean"))
            }
            Expression::Literal(LiteralValue::Number(n)) => Ok(*n != 0.0),
            Expression::Literal(LiteralValue::String(s)) => Ok(!s.is_empty()),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Expression,
        op: &Operator,
        right: &Expression,
        row: &Row,
        schema: &Schema,
    ) -> Result<bool> {
        match op {
            Operator::Equal => {
                let left_val = self.evaluate_expression(left, row, schema)?;
                let right_val = self.evaluate_expression(right, row, schema)?;
                Ok(self.values_equal(&left_val, &right_val))
            }
            Operator::NotEqual => {
                let left_val = self.evaluate_expression(left, row, schema)?;
                let right_val = self.evaluate_expression(right, row, schema)?;
                Ok(!self.values_equal(&left_val, &right_val))
            }
            Operator::And => {
                let left_result = self.evaluate(left, row, schema)?;
                if !left_result {
                    return Ok(false);
                }
                self.evaluate(right, row, schema)
            }
            Operator::Or => {
                let left_result = self.evaluate(left, row, schema)?;
                if left_result {
                    return Ok(true);
                }
                self.evaluate(right, row, schema)
            }
        }
    }

    fn evaluate_expression(
        &self,
        expression: &Expression,
        row: &Row,
        schema: &Schema,
    ) -> Result<Value> {
        match expression {
            Expression::Column(column_name) => {
                let column_index = schema
                    .get_column_index(column_name)
                    .ok_or_else(|| miette!("Column '{}' not found", column_name))?;

                row.get_value(column_index)
                    .ok_or_else(|| miette!("Row doesn't have value at index {}", column_index))
                    .map(|v| v.clone())
            }
            Expression::Literal(literal) => Ok(self.literal_to_value(literal)),
            Expression::BinaryOp { .. } => {
                Err(miette!("Binary operations not supported as values"))
            }
        }
    }

    fn literal_to_value(&self, literal: &LiteralValue) -> Value {
        match literal {
            LiteralValue::Number(n) => Value::Float(*n),
            LiteralValue::String(s) => Value::Text(s.clone()),
        }
    }

    fn values_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Null, Value::Null) => true,

            // Type coercion
            (Value::Integer(a), Value::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (Value::Float(a), Value::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,

            _ => false,
        }
    }
}
