use std::cmp::Ordering;

use miette::{Result, miette};

use crate::{Row, core::types::Value, sql::analyzer::AnalyzedExpression};

pub mod aggregate;
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
        (Value::Float64(a), Value::Float64(b)) => compare_floats(*a, *b).is_eq(),
        (Value::Int64(a), Value::Float64(b)) => compare_floats(*a as f64, *b).is_eq(),
        (Value::Float64(a), Value::Int64(b)) => compare_floats(*a, *b as f64).is_eq(),
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
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
        (Value::Float64(a), Value::Float64(b)) => compare_floats(*a, *b).is_gt(),
        (Value::Int64(a), Value::Float64(b)) => compare_floats(*a as f64, *b).is_gt(),
        (Value::Float64(a), Value::Int64(b)) => compare_floats(*a, *b as f64).is_gt(),
        (Value::Timestamp(a), Value::Timestamp(b)) => a > b,
        (Value::Text(a), Value::Text(b)) => a > b,
        (Value::Bool(a), Value::Bool(b)) => a > b,
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
        (Value::Float64(a), Value::Float64(b)) => compare_floats(*a, *b).is_lt(),
        (Value::Int64(a), Value::Float64(b)) => compare_floats(*a as f64, *b).is_lt(),
        (Value::Float64(a), Value::Int64(b)) => compare_floats(*a, *b as f64).is_lt(),
        (Value::Timestamp(a), Value::Timestamp(b)) => a < b,
        (Value::Text(a), Value::Text(b)) => a < b,
        (Value::Bool(a), Value::Bool(b)) => a < b,
        _ => false,
    };
    Value::Bool(result)
}

/// Total ordering used by ORDER BY, MIN and MAX.
///
/// NULL sorts after every other value, so it comes last in ascending order and
/// first in descending order (PostgreSQL's default). Values of unrelated types
/// compare as equal; the analyzer keeps them from meeting in practice.
pub fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => compare_floats(*a, *b),
        (Value::Int64(a), Value::Float64(b)) => compare_floats(*a as f64, *b),
        (Value::Float64(a), Value::Int64(b)) => compare_floats(*a, *b as f64),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// How every comparison orders floats, the same way an index sorts them (and as
/// PostgreSQL does): exactly, with -0.0 equal to 0.0, and NaN equal to itself and greater
/// than every other number.
pub fn compare_floats(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b)
        .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_and_bool_compare_in_order() {
        let (a, b) = (Value::from("a"), Value::from("b"));
        assert_eq!(values_less_than(&a, &b), Value::Bool(true));
        assert_eq!(values_less_than(&b, &a), Value::Bool(false));
        assert_eq!(values_greater_than(&b, &a), Value::Bool(true));
        assert_eq!(values_greater_than(&a, &a), Value::Bool(false));

        let (no, yes) = (Value::Bool(false), Value::Bool(true));
        assert_eq!(values_less_than(&no, &yes), Value::Bool(true));
        assert_eq!(values_greater_than(&no, &yes), Value::Bool(false));
    }

    #[test]
    fn test_compare_values_nulls_last() {
        assert_eq!(
            compare_values(&Value::Null, &Value::Int64(1)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Int64(1), &Value::Null),
            Ordering::Less
        );
        assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
    }

    #[test]
    fn test_compare_values_mixed_numbers() {
        assert_eq!(
            compare_values(&Value::Int64(2), &Value::Float64(1.5)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Text("a".into()), &Value::Text("b".into())),
            Ordering::Less
        );
    }

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

    #[test]
    fn test_values_compare_timestamps() {
        let earlier = Value::Timestamp(-1);
        let later = Value::Timestamp(1_772_366_400);

        assert_eq!(values_equal(&later, &later), Value::Bool(true));
        assert_eq!(values_equal(&earlier, &later), Value::Bool(false));
        assert_eq!(values_greater_than(&later, &earlier), Value::Bool(true));
        assert_eq!(values_greater_than(&earlier, &later), Value::Bool(false));
        assert_eq!(values_less_than(&earlier, &later), Value::Bool(true));
        assert_eq!(values_less_than(&later, &earlier), Value::Bool(false));
        assert_eq!(values_equal(&earlier, &Value::Null), Value::Null);
    }

    #[test]
    fn test_compare_values_timestamps_order_before_and_after_epoch() {
        let before_epoch = Value::Timestamp(-62_135_596_800);
        let epoch = Value::Timestamp(0);
        let after_epoch = Value::Timestamp(253_402_300_799);

        assert_eq!(compare_values(&before_epoch, &epoch), Ordering::Less);
        assert_eq!(compare_values(&epoch, &after_epoch), Ordering::Less);
        assert_eq!(
            compare_values(&after_epoch, &before_epoch),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&epoch, &Value::Timestamp(0)),
            Ordering::Equal
        );
        assert_eq!(compare_values(&epoch, &Value::Null), Ordering::Less);
    }
}
