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

pub fn values_equal(left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Value::Null;
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
    Value::Boolean(result)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_add_integers() {
        let result = values_add(&Value::Integer(5), &Value::Integer(3));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(8));
    }

    #[test]
    fn test_values_add_floats() {
        let result = values_add(&Value::Float(5.5), &Value::Float(3.2));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float(8.7));
    }

    #[test]
    fn test_values_add_mixed_types() {
        let result = values_add(&Value::Integer(5), &Value::Float(3.5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float(8.5));
    }

    #[test]
    fn test_values_add_with_null() {
        let result = values_add(&Value::Integer(5), &Value::Null);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_values_add_invalid_types() {
        let result = values_add(&Value::Text("hello".to_string()), &Value::Integer(5));
        assert!(result.is_err());
    }

    #[test]
    fn test_values_subtract_integers() {
        let result = values_subtract(&Value::Integer(10), &Value::Integer(3));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(7));
    }

    #[test]
    fn test_values_subtract_with_null() {
        let result = values_subtract(&Value::Float(5.5), &Value::Null);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_values_multiply_integers() {
        let result = values_multiply(&Value::Integer(4), &Value::Integer(3));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(12));
    }

    #[test]
    fn test_values_multiply_mixed_types() {
        let result = values_multiply(&Value::Integer(4), &Value::Float(2.5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float(10.0));
    }

    #[test]
    fn test_values_divide_integers() {
        let result = values_divide(&Value::Integer(10), &Value::Integer(2));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(5));
    }

    #[test]
    fn test_values_divide_by_zero_integer() {
        let result = values_divide(&Value::Integer(10), &Value::Integer(0));
        assert!(result.is_err());
    }

    #[test]
    fn test_values_divide_by_zero_float() {
        let result = values_divide(&Value::Float(10.0), &Value::Float(0.0));
        assert!(result.is_err());
    }

    #[test]
    fn test_values_divide_with_null() {
        let result = values_divide(&Value::Integer(10), &Value::Null);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_values_equal_integers() {
        let result = values_equal(&Value::Integer(5), &Value::Integer(5));
        assert_eq!(result, Value::Boolean(true));

        let result = values_equal(&Value::Integer(5), &Value::Integer(3));
        assert_eq!(result, Value::Boolean(false));
    }

    #[test]
    fn test_values_equal_floats_with_epsilon() {
        let result = values_equal(&Value::Float(5.0), &Value::Float(5.0));
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_values_equal_mixed_types() {
        let result = values_equal(&Value::Integer(5), &Value::Float(5.0));
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_values_equal_booleans() {
        let result = values_equal(&Value::Boolean(true), &Value::Boolean(true));
        assert_eq!(result, Value::Boolean(true));

        let result = values_equal(&Value::Boolean(true), &Value::Boolean(false));
        assert_eq!(result, Value::Boolean(false));
    }

    #[test]
    fn test_values_equal_with_null() {
        let result = values_equal(&Value::Integer(5), &Value::Null);
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
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_values_greater_than_integers() {
        let result = values_greater_than(&Value::Integer(10), &Value::Integer(5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));

        let result = values_greater_than(&Value::Integer(3), &Value::Integer(5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_values_greater_than_floats() {
        let result = values_greater_than(&Value::Float(10.5), &Value::Float(5.2));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_values_greater_than_mixed_types() {
        let result = values_greater_than(&Value::Integer(10), &Value::Float(5.5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_values_greater_than_with_null() {
        let result = values_greater_than(&Value::Integer(10), &Value::Null);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_values_greater_than_invalid_types() {
        let result = values_greater_than(&Value::Text("hello".to_string()), &Value::Integer(5));
        assert!(result.is_err());
    }

    #[test]
    fn test_values_less_than_integers() {
        let result = values_less_than(&Value::Integer(3), &Value::Integer(10));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));

        let result = values_less_than(&Value::Integer(10), &Value::Integer(5));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_values_less_than_with_null() {
        let result = values_less_than(&Value::Null, &Value::Integer(10));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }
}
