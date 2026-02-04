use miette::{Result, miette};

use crate::db::table::{Row, Schema, Value};

use super::parser::{Expression, LiteralValue, Operator};

/// Evaluates SQL predicate expressions against table rows.
///
/// The `PredicateEvaluator` takes parsed SQL expressions (typically from WHERE clauses)
/// and evaluates them against individual rows to determine if they match the criteria.
/// It supports:
/// - Comparison operators: `=`, `!=`, `>`, `<`
/// - Logical operators: `AND`, `OR`
/// - Type coercion between integers and floats
/// - Column references and literal values
pub struct PredicateEvaluator;

impl PredicateEvaluator {
    /// Evaluates an SQL expression against a row and returns whether it matches.
    ///
    /// This is the main entry point for predicate evaluation. It recursively evaluates
    /// the expression tree and returns a boolean result indicating whether the row
    /// satisfies the predicate.
    pub fn evaluate(&self, expression: &Expression, row: &Row, schema: &Schema) -> Result<bool> {
        match expression {
            Expression::BinaryOp { left, op, right } => {
                self.evaluate_binary_op(left, op, right, row, schema)
            }
            Expression::Column(_) => {
                Err(miette!("Column expression cannot be evaluated as boolean"))
            }
            Expression::Literal(LiteralValue::Float(n)) => Ok(*n != 0.0),
            Expression::Literal(LiteralValue::Integer(n)) => Ok(*n != 0),
            Expression::Literal(LiteralValue::String(s)) => Ok(!s.is_empty()),
        }
    }

    /// Evaluates a binary operation expression.
    ///
    /// Handles comparison operators (`=`, `!=`, `>`, `<`) and logical operators
    /// (`AND`, `OR`). For comparison operators, both sides are evaluated to values
    /// and compared. For logical operators, short-circuit evaluation is used.
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
            Operator::GreaterThan => {
                let left_result = self.evaluate_expression(left, row, schema)?;
                let right_result = self.evaluate_expression(right, row, schema)?;

                Ok(self.values_greater_than(&left_result, &right_result))
            }
            Operator::LessThan => {
                let left_result = self.evaluate_expression(left, row, schema)?;
                let right_result = self.evaluate_expression(right, row, schema)?;

                Ok(self.values_less_than(&left_result, &right_result))
            }
        }
    }

    /// Evaluates an expression to a concrete value.
    ///
    /// Converts an expression (column reference or literal) into an actual value
    /// that can be used in comparisons. For column references, retrieves the value
    /// from the row using the schema. For literals, converts them to the appropriate
    /// `Value` type.
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
                    .cloned()
            }
            Expression::Literal(literal) => Ok(self.literal_to_value(literal)),
            Expression::BinaryOp { .. } => {
                Err(miette!("Binary operations not supported as values"))
            }
        }
    }

    /// Converts a parsed literal value to a database `Value`.
    fn literal_to_value(&self, literal: &LiteralValue) -> Value {
        match literal {
            LiteralValue::Float(n) => Value::Float(*n),
            LiteralValue::Integer(n) => Value::Integer(*n),
            LiteralValue::String(s) => Value::Text(s.clone()),
        }
    }

    /// Compares two values for equality with type coercion.
    ///
    /// Supports comparing values of the same type, and automatically coerces
    /// between `Integer` and `Float` types. Floating-point comparisons use
    /// an epsilon tolerance to handle precision issues.
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

    /// Compares two values using the less-than operator with type coercion.
    ///
    /// Supports numeric comparisons between `Integer` and `Float` types,
    /// with automatic type coercion when needed.
    fn values_less_than(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => a < b,
            (Value::Float(a), Value::Float(b)) => a < b,
            (Value::Integer(a), Value::Float(b)) => (*a as f64) < *b,
            (Value::Float(a), Value::Integer(b)) => *a < (*b as f64),
            _ => {
                panic!("Unsupported comparison for types: {left:?} and {right:?}");
            }
        }
    }

    /// Compares two values using the greater-than operator with type coercion.
    ///
    /// Supports numeric comparisons between `Integer` and `Float` types,
    /// with automatic type coercion when needed.
    fn values_greater_than(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => a > b,
            (Value::Float(a), Value::Float(b)) => a > b,
            (Value::Integer(a), Value::Float(b)) => (*a as f64) > *b,
            (Value::Float(a), Value::Integer(b)) => *a > (*b as f64),
            _ => {
                panic!("Unsupported comparison for types: {left:?} and {right:?}");
            }
        }
    }
}
