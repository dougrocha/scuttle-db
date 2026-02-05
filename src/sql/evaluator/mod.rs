use crate::{Row, Schema, Value, sql::parser::Expression};
use miette::{Result, miette};

pub mod expression;
pub mod predicate;

/// The core trait that both evaluators must implement.
///
/// - `T = Value` for ExpressionEvaluator (math, strings)
/// - `T = bool` for PredicateEvaluator (WHERE clauses)
pub trait Evaluator<T> {
    fn evaluate(&self, expression: &Expression, row: &Row, schema: &Schema) -> Result<T>;
}

pub fn values_add(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + *b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(*a + *b as f64)),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot add {:?} and {:?}", left, right)),
    }
}

pub fn values_subtract(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a - b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - *b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(*a - *b as f64)),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot subtract {:?} and {:?}", left, right)),
    }
}

pub fn values_multiply(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a * b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * *b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(*a * *b as f64)),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot multiply {:?} and {:?}", left, right)),
    }
}

pub fn values_divide(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Integer(a / b))
        }
        (Value::Float(a), Value::Float(b)) => {
            if *b == 0.0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Float(a / b))
        }
        (Value::Integer(a), Value::Float(b)) => {
            if *b == 0.0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Float(*a as f64 / *b))
        }
        (Value::Float(a), Value::Integer(b)) => {
            if *b == 0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Float(*a / *b as f64))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot divide {:?} and {:?}", left, right)),
    }
}

pub fn values_equal(left: &Value, right: &Value) -> Result<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let result = match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Integer(a), Value::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        (Value::Float(a), Value::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Boolean(a), Value::Boolean(b)) => a == b,
        _ => false,
    };
    Ok(Value::Boolean(result))
}

pub fn values_greater_than(left: &Value, right: &Value) -> Result<Value> {
    let result = match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a > b,
        (Value::Float(a), Value::Float(b)) => a > b,
        (Value::Integer(a), Value::Float(b)) => (*a as f64) > *b,
        (Value::Float(a), Value::Integer(b)) => *a > (*b as f64),
        (Value::Null, _) | (_, Value::Null) => false,
        _ => {
            return Err(miette!(
                "Invalid comparison between {:?} and {:?}",
                left,
                right
            ));
        }
    };
    Ok(Value::Boolean(result))
}

pub fn values_less_than(left: &Value, right: &Value) -> Result<Value> {
    let result = match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a < b,
        (Value::Float(a), Value::Float(b)) => a < b,
        (Value::Integer(a), Value::Float(b)) => (*a as f64) < *b,
        (Value::Float(a), Value::Integer(b)) => *a < (*b as f64),
        (Value::Null, _) | (_, Value::Null) => false,
        _ => {
            return Err(miette!(
                "Invalid comparison between {:?} and {:?}",
                left,
                right
            ));
        }
    };
    Ok(Value::Boolean(result))
}
