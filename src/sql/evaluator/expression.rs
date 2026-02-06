use miette::{Result, miette};

use crate::{
    Row, Schema, Value,
    sql::{
        evaluator::{
            Evaluator, values_add, values_divide, values_equal, values_greater_than,
            values_less_than, values_multiply, values_subtract,
        },
        parser::{Expression, IsPredicate, Operator},
    },
};

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
            Expression::Is {
                expr,
                predicate,
                is_negated,
            } => {
                let value = self.evaluate(expr, row, schema)?;

                let match_against = match predicate {
                    IsPredicate::True => Value::Boolean(true),
                    IsPredicate::False => Value::Boolean(false),
                    IsPredicate::Null => Value::Null,
                };

                let bool = if !is_negated {
                    value == match_against
                } else {
                    value != match_against
                };

                Ok(Value::Boolean(bool))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColumnDefinition, DataType,
        sql::parser::{LiteralValue, SqlParser},
    };

    /// Helper to parse an expression from a WHERE clause for testing
    ///
    /// Usage: parse_expr("id > 5") parses the WHERE clause expression
    fn parse_expr(expr_str: &str) -> Expression {
        // Parse a minimal SELECT statement with a WHERE clause
        let query = format!("SELECT * FROM dummy WHERE {}", expr_str);
        let mut parser = SqlParser::new(&query);
        match parser.parse() {
            Ok(crate::sql::parser::Statement::Select {
                r#where: Some(expr),
                ..
            }) => expr,
            Ok(_) => panic!("Query parsed but no WHERE clause found"),
            Err(e) => panic!("Failed to parse expression '{}': {:?}", expr_str, e),
        }
    }

    fn setup_test_data() -> (Schema, Row) {
        let schema = Schema {
            columns: vec![
                ColumnDefinition::new("id", DataType::Integer, false),
                ColumnDefinition::new("name", DataType::Text, false),
                ColumnDefinition::new("score", DataType::Float, true),
                ColumnDefinition::new("age", DataType::Integer, false),
                ColumnDefinition::new("is_active", DataType::Boolean, true),
            ],
        };
        let row = Row::new(vec![
            Value::Integer(10),
            Value::Text("Alice".to_string()),
            Value::Float(15.5),
            Value::Integer(30),
            Value::Boolean(true),
        ]);

        (schema, row)
    }

    #[test]
    fn test_literal_evaluation() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        let expr = Expression::Literal(LiteralValue::Integer(42));
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(42));

        let expr = Expression::Literal(LiteralValue::Float(std::f64::consts::PI));
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float(std::f64::consts::PI));

        let expr = Expression::Literal(LiteralValue::Boolean(true));
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_column_evaluation() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        let expr = Expression::Column("id".to_string());
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(10));

        let expr = Expression::Column("name".to_string());
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Text("Alice".to_string()));
    }

    #[test]
    fn test_column_not_found() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        let expr = Expression::Column("nonexistent".to_string());
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_arithmetic_add() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // id + 5 (10 + 5 = 15)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::Add,
            right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(15));
    }

    #[test]
    fn test_arithmetic_subtract() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // age - 5 (30 - 5 = 25)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: Operator::Subtract,
            right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(25));
    }

    #[test]
    fn test_arithmetic_multiply() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // id * 2 (10 * 2 = 20)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::Multiply,
            right: Box::new(Expression::Literal(LiteralValue::Integer(2))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(20));
    }

    #[test]
    fn test_arithmetic_divide() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // age / 3 (30 / 3 = 10)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: Operator::Divide,
            right: Box::new(Expression::Literal(LiteralValue::Integer(3))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(10));
    }

    #[test]
    fn test_arithmetic_mixed_types() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // id + 2.5 (10 + 2.5 = 12.5)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::Add,
            right: Box::new(Expression::Literal(LiteralValue::Float(2.5))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Float(12.5));
    }

    #[test]
    fn test_comparison_equal() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // id = 10 -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::Equal,
            right: Box::new(Expression::Literal(LiteralValue::Integer(10))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));

        // id = 5 -> false
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::Equal,
            right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_comparison_not_equal() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // id != 5 -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::NotEqual,
            right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_comparison_greater_than() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // id > 5 -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));

        // id > 20 -> false
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(LiteralValue::Integer(20))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_comparison_less_than() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // id < 20 -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::LessThan,
            right: Box::new(Expression::Literal(LiteralValue::Integer(20))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_logical_and_both_true() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (id > 5) AND (age > 20) -> true AND true -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("age".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(20))),
            }),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_logical_and_one_false() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (id > 5) AND (age > 50) -> true AND false -> false
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("age".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(50))),
            }),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_logical_or_one_true() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (id > 5) OR (age > 50) -> true OR false -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::Or,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("age".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(50))),
            }),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_logical_or_both_false() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (id < 5) OR (age > 50) -> false OR false -> false
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: Operator::LessThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::Or,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("age".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(50))),
            }),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_nested_arithmetic() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (id + 5) * 2 -> (10 + 5) * 2 = 30
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: Operator::Add,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::Multiply,
            right: Box::new(Expression::Literal(LiteralValue::Integer(2))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(30));
    }

    #[test]
    fn test_arithmetic_in_comparison() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (age + 5) > 30 -> (30 + 5) > 30 -> 35 > 30 -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("age".to_string())),
                op: Operator::Add,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(LiteralValue::Integer(30))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    // --- Tests using parse_expr helper (cleaner!) ---

    #[test]
    fn test_with_parser_simple_comparison() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // Much cleaner than manual Expression construction!
        let expr = parse_expr("id > 5");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_with_parser_arithmetic() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // age + 5 -> 30 + 5 = 35
        let expr = parse_expr("age + 5");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(35));
    }

    #[test]
    fn test_with_parser_complex_expression() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (age + 5) > 30 AND id < 20
        let expr = parse_expr("(age + 5) > 30 AND id < 20");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_with_parser_nested_arithmetic() {
        let evaluator = ExpressionEvaluator;
        let (schema, row) = setup_test_data();

        // (id + 5) * 2 -> (10 + 5) * 2 = 30
        let expr = parse_expr("(id + 5) * 2");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Integer(30));
    }
}
