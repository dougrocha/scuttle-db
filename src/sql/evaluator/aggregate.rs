//! Running state for aggregate functions (COUNT, SUM, AVG, MIN, MAX).

use std::cmp::Ordering;

use miette::Result;

use crate::{
    core::types::Value,
    sql::{
        analyzer::AggregateFunction,
        evaluator::{compare_values, values_add},
    },
};

/// Accumulates one aggregate over the rows of a single group.
///
/// Like SQL, every aggregate except `COUNT(*)` skips NULL inputs, and
/// SUM/AVG/MIN/MAX of no values is NULL.
#[derive(Debug)]
pub enum Accumulator {
    Count(i64),
    Sum(Value),
    Avg { sum: f64, count: i64 },
    Min(Value),
    Max(Value),
}

impl Accumulator {
    /// Creates an empty accumulator for the given function.
    pub fn new(function: AggregateFunction) -> Self {
        match function {
            AggregateFunction::Count => Self::Count(0),
            AggregateFunction::Sum => Self::Sum(Value::Null),
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => Self::Min(Value::Null),
            AggregateFunction::Max => Self::Max(Value::Null),
        }
    }

    /// Feeds one input row into the aggregate.
    ///
    /// `value` is `None` for `COUNT(*)`, which counts rows rather than values.
    pub fn update(&mut self, value: Option<Value>) -> Result<()> {
        let Some(value) = value else {
            if let Self::Count(count) = self {
                *count += 1;
            }
            return Ok(());
        };

        if value == Value::Null {
            return Ok(());
        }

        match self {
            Self::Count(count) => *count += 1,
            Self::Sum(sum) => {
                *sum = if *sum == Value::Null {
                    value
                } else {
                    values_add(sum, &value)?
                };
            }
            Self::Avg { sum, count } => {
                *sum += match value {
                    Value::Int64(n) => n as f64,
                    Value::Float64(n) => n,
                    _ => unreachable!("analyzer only allows AVG over numbers"),
                };
                *count += 1;
            }
            Self::Min(min) => {
                if compare_values(&value, min) == Ordering::Less {
                    *min = value;
                }
            }
            Self::Max(max) => {
                if *max == Value::Null || compare_values(&value, max) == Ordering::Greater {
                    *max = value;
                }
            }
        }

        Ok(())
    }

    /// Produces the final aggregate value.
    pub fn finish(self) -> Value {
        match self {
            Self::Count(count) => Value::Int64(count),
            Self::Sum(value) | Self::Min(value) | Self::Max(value) => value,
            Self::Avg { count: 0, .. } => Value::Null,
            Self::Avg { sum, count } => Value::Float64(sum / count as f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(function: AggregateFunction, values: Vec<Value>) -> Value {
        let mut acc = Accumulator::new(function);
        for value in values {
            acc.update(Some(value)).unwrap();
        }
        acc.finish()
    }

    #[test]
    fn test_aggregates_skip_nulls() {
        let values = vec![Value::Int64(4), Value::Null, Value::Int64(2)];

        assert_eq!(
            run(AggregateFunction::Count, values.clone()),
            Value::Int64(2)
        );
        assert_eq!(run(AggregateFunction::Sum, values.clone()), Value::Int64(6));
        assert_eq!(
            run(AggregateFunction::Avg, values.clone()),
            Value::Float64(3.0)
        );
        assert_eq!(run(AggregateFunction::Min, values.clone()), Value::Int64(2));
        assert_eq!(run(AggregateFunction::Max, values), Value::Int64(4));
    }

    #[test]
    fn test_aggregates_over_no_values() {
        assert_eq!(run(AggregateFunction::Count, vec![]), Value::Int64(0));
        assert_eq!(run(AggregateFunction::Sum, vec![Value::Null]), Value::Null);
        assert_eq!(run(AggregateFunction::Avg, vec![]), Value::Null);
        assert_eq!(run(AggregateFunction::Max, vec![]), Value::Null);
    }

    #[test]
    fn test_count_star_counts_null_rows() {
        let mut acc = Accumulator::new(AggregateFunction::Count);
        acc.update(None).unwrap();
        acc.update(None).unwrap();
        assert_eq!(acc.finish(), Value::Int64(2));
    }
}
