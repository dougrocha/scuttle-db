use miette::{Result, miette};

use crate::{Row, Value, sql::analyzer::AnalyzedExpression};

pub mod expression;
pub mod predicate;

/// The core trait that both evaluators must implement.
///
/// - `T = Value` for `ExpressionEvaluator` (math, strings)
/// - `T = bool` for `PredicateEvaluator` (WHERE clauses)
pub trait Evaluator<T> {
    fn evaluate(&self, analyzed_expr: &AnalyzedExpression, row: &Row) -> Result<T>;
}

pub fn values_add(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a + b)),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a + b)),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(*a as f64 + *b)),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(*a + *b as f64)),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot add {:?} and {:?}", left, right)),
    }
}

pub fn values_subtract(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a - b)),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(*a as f64 - *b)),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(*a - *b as f64)),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot subtract {:?} and {:?}", left, right)),
    }
}

pub fn values_multiply(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a * b)),
        (Value::Float64(a), Value::Float64(b)) => Ok(Value::Float64(a * b)),
        (Value::Int64(a), Value::Float64(b)) => Ok(Value::Float64(*a as f64 * *b)),
        (Value::Float64(a), Value::Int64(b)) => Ok(Value::Float64(*a * *b as f64)),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot multiply {:?} and {:?}", left, right)),
    }
}

pub fn values_divide(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => {
            if *b == 0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Int64(a / b))
        }
        (Value::Float64(a), Value::Float64(b)) => {
            if *b == 0.0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Float64(a / b))
        }
        (Value::Int64(a), Value::Float64(b)) => {
            if *b == 0.0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Float64(*a as f64 / *b))
        }
        (Value::Float64(a), Value::Int64(b)) => {
            if *b == 0 {
                return Err(miette!("Division by zero"));
            }
            Ok(Value::Float64(*a / *b as f64))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(miette!("Cannot divide {:?} and {:?}", left, right)),
    }
}

pub fn values_equal(left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Value::Null;
    }

    let result = match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => a == b,
        (Value::Float64(a), Value::Float64(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Int64(a), Value::Float64(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        (Value::Float64(a), Value::Int64(b)) => (a - *b as f64).abs() < f64::EPSILON,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        _ => false,
    };
    Value::Bool(result)
}

pub fn values_greater_than(left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Value::Null;
    }

    let result = match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => a > b,
        (Value::Float64(a), Value::Float64(b)) => a > b,
        (Value::Int64(a), Value::Float64(b)) => (*a as f64) > *b,
        (Value::Float64(a), Value::Int64(b)) => *a > (*b as f64),
        _ => false,
    };
    Value::Bool(result)
}

pub fn values_less_than(left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Value::Null;
    }

    let result = match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => a < b,
        (Value::Float64(a), Value::Float64(b)) => a < b,
        (Value::Int64(a), Value::Float64(b)) => (*a as f64) < *b,
        (Value::Float64(a), Value::Int64(b)) => *a < (*b as f64),
        _ => false,
    };
    Value::Bool(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_add_integers() {
        let result = values_add(&Value::Int64(5), &Value::Int64(3));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int64(8));
    }

    #[test]
    fn test_values_add_floats() {
        let result = values_add(&Value::Float64(5.5), &Value::Float64(3.2));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float64(8.7));
    }

    #[test]
    fn test_values_add_mixed_types() {
        let result = values_add(&Value::Int64(5), &Value::Float64(3.5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float64(8.5));
    }

    #[test]
    fn test_values_add_with_null() {
        let result = values_add(&Value::Int64(5), &Value::Null);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_values_add_invalid_types() {
        let result = values_add(&Value::Text("hello".to_string()), &Value::Int64(5));
        assert!(result.is_err());
    }

    #[test]
    fn test_values_subtract_integers() {
        let result = values_subtract(&Value::Int64(10), &Value::Int64(3));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int64(7));
    }

    #[test]
    fn test_values_subtract_with_null() {
        let result = values_subtract(&Value::Float64(5.5), &Value::Null);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_values_multiply_integers() {
        let result = values_multiply(&Value::Int64(4), &Value::Int64(3));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int64(12));
    }

    #[test]
    fn test_values_multiply_mixed_types() {
        let result = values_multiply(&Value::Int64(4), &Value::Float64(2.5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float64(10.0));
    }

    #[test]
    fn test_values_divide_integers() {
        let result = values_divide(&Value::Int64(10), &Value::Int64(2));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int64(5));
    }

    #[test]
    fn test_values_divide_by_zero_integer() {
        let result = values_divide(&Value::Int64(10), &Value::Int64(0));
        assert!(result.is_err());
    }

    #[test]
    fn test_values_divide_by_zero_float() {
        let result = values_divide(&Value::Float64(10.0), &Value::Float64(0.0));
        assert!(result.is_err());
    }

    #[test]
    fn test_values_divide_with_null() {
        let result = values_divide(&Value::Int64(10), &Value::Null);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_values_equal_integers() {
        let result = values_equal(&Value::Int64(5), &Value::Int64(5));
        assert_eq!(result, Value::Bool(true));

        let result = values_equal(&Value::Int64(5), &Value::Int64(3));
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_values_equal_floats_with_epsilon() {
        let result = values_equal(&Value::Float64(5.0), &Value::Float64(5.0));
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_values_equal_mixed_types() {
        let result = values_equal(&Value::Int64(5), &Value::Float64(5.0));
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_values_equal_booleans() {
        let result = values_equal(&Value::Bool(true), &Value::Bool(true));
        assert_eq!(result, Value::Bool(true));

        let result = values_equal(&Value::Bool(true), &Value::Bool(false));
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_values_equal_with_null() {
        let result = values_equal(&Value::Int64(5), &Value::Null);
        assert_eq!(result, Value::Null);

        let result = values_equal(&Value::Null, &Value::Null);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_values_equal_text() {
        let result = values_equal(
            &Value::Text("hello".to_string()),
            &Value::Text("hello".to_string()),
        );
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_values_greater_than_integers() {
        let result = values_greater_than(&Value::Int64(10), &Value::Int64(5));
        assert_eq!(result, Value::Bool(true));

        let result = values_greater_than(&Value::Int64(3), &Value::Int64(5));
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_values_greater_than_floats() {
        let result = values_greater_than(&Value::Float64(10.5), &Value::Float64(5.2));
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_values_greater_than_mixed_types() {
        let result = values_greater_than(&Value::Int64(10), &Value::Float64(5.5));
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn test_values_greater_than_with_null() {
        let result = values_greater_than(&Value::Int64(10), &Value::Null);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_values_greater_than_invalid_types() {
        let result = values_greater_than(&Value::Text("hello".to_string()), &Value::Int64(5));
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_values_less_than_integers() {
        let result = values_less_than(&Value::Int64(3), &Value::Int64(10));
        assert_eq!(result, Value::Bool(true));

        let result = values_less_than(&Value::Int64(10), &Value::Int64(5));
        assert_eq!(result, Value::Bool(false));
    }

    #[test]
    fn test_values_less_than_with_null() {
        let result = values_less_than(&Value::Null, &Value::Int64(10));
        assert_eq!(result, Value::Null);
    }
}
