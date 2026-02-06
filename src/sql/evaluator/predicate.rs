use crate::{
    db::table::{Row, Schema, Value},
    sql::{
        evaluator::{Evaluator, expression::ExpressionEvaluator},
        parser::Expression,
    },
};
use miette::{Result, miette};

pub struct PredicateEvaluator;

impl Evaluator<bool> for PredicateEvaluator {
    fn evaluate(&self, expression: &Expression, row: &Row, schema: &Schema) -> Result<bool> {
        let expr_evaluator = ExpressionEvaluator;
        let value = expr_evaluator.evaluate(expression, row, schema)?;

        match value {
            Value::Boolean(b) => Ok(b),
            Value::Null => Ok(false),
            _ => Err(miette!(
                "WHERE clause must evaluate to a boolean, got {:?}",
                value
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::{LiteralValue, Operator, SqlParser};
    use crate::{ColumnDefinition, DataType};

    /// Helper to parse an expression from a WHERE clause for testing
    fn parse_expr(expr_str: &str) -> Expression {
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
                ColumnDefinition::new("is_deleted", DataType::Boolean, false),
            ],
        };
        let row = Row::new(vec![
            Value::Integer(10),
            Value::Text("Alice".to_string()),
            Value::Float(15.5),
            Value::Integer(30),
            Value::Null, // is_active is NULL
            Value::Boolean(true),
        ]);

        (schema, row)
    }

    #[test]
    fn test_null_behavior() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // 1. Column = NULL (is_active = NULL) -> Null -> False (SQL Logic)
        let expr_eq_null = Expression::BinaryOp {
            left: Box::new(Expression::Column("is_active".to_string())),
            op: Operator::Equal,
            right: Box::new(Expression::Literal(LiteralValue::Null)),
        };
        let result = evaluator.evaluate(&expr_eq_null, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);

        // 2. NULL Comparison in Greater Than -> False
        let expr_gt_null = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(LiteralValue::Null)),
        };
        let result = evaluator.evaluate(&expr_gt_null, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_numeric_coercion_and_comparison() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // 1. Integer Column vs Float Literal: id > 9.9 (10 > 9.9) -> True
        let expr1 = Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(LiteralValue::Float(9.9))),
        };
        let result = evaluator.evaluate(&expr1, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        // 2. Float Column vs Integer Literal: score > 15 (15.5 > 15) -> True
        let expr2 = Expression::BinaryOp {
            left: Box::new(Expression::Column("score".to_string())),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(LiteralValue::Integer(15))),
        };
        let result = evaluator.evaluate(&expr2, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_logical_operators_with_null() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // (id > 5) AND (is_active)
        // id > 5 is True, but is_active is NULL (evaluates to false)
        // Result should be False
        let expr_and = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("id".to_string())),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::And,
            right: Box::new(Expression::Column("is_active".to_string())),
        };
        let result = evaluator.evaluate(&expr_and, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_boolean_equality() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // 1. is_deleted = true (row has is_deleted = true) -> True
        let expr_deleted_true = Expression::BinaryOp {
            left: Box::new(Expression::Column("is_deleted".to_string())),
            op: Operator::Equal,
            right: Box::new(Expression::Literal(LiteralValue::Boolean(true))),
        };
        let result = evaluator.evaluate(&expr_deleted_true, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        // 2. is_deleted = false (row has is_deleted = true) -> False
        let expr_deleted_false = Expression::BinaryOp {
            left: Box::new(Expression::Column("is_deleted".to_string())),
            op: Operator::Equal,
            right: Box::new(Expression::Literal(LiteralValue::Boolean(false))),
        };
        let result = evaluator.evaluate(&expr_deleted_false, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);

        // 3. is_deleted != false (row has is_deleted = true) -> True
        let expr_not_false = Expression::BinaryOp {
            left: Box::new(Expression::Column("is_deleted".to_string())),
            op: Operator::NotEqual,
            right: Box::new(Expression::Literal(LiteralValue::Boolean(false))),
        };
        let result = evaluator.evaluate(&expr_not_false, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        // 4. is_active = true (row has is_active = NULL) -> Null -> False
        let expr_active_true = Expression::BinaryOp {
            left: Box::new(Expression::Column("is_active".to_string())),
            op: Operator::Equal,
            right: Box::new(Expression::Literal(LiteralValue::Boolean(true))),
        };
        let result = evaluator.evaluate(&expr_active_true, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_type_mismatch_errors() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // Comparing Text to Integer: name > 10
        // This should trigger an error in values_greater_than
        let expr_mismatch = Expression::BinaryOp {
            left: Box::new(Expression::Column("name".to_string())),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(LiteralValue::Integer(10))),
        };

        let result = evaluator.evaluate(&expr_mismatch, &row, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_arithmetic_in_where_clause() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // WHERE age + 5 > 30 -> (30 + 5) > 30 -> 35 > 30 -> true
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
        assert_eq!(result.unwrap(), true);

        // WHERE age - 5 < 30 -> (30 - 5) < 30 -> 25 < 30 -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("age".to_string())),
                op: Operator::Subtract,
                right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
            }),
            op: Operator::LessThan,
            right: Box::new(Expression::Literal(LiteralValue::Integer(30))),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_non_boolean_expression_error() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // WHERE 42 (should error - not a boolean)
        let expr = Expression::Literal(LiteralValue::Integer(42));
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_err());

        // WHERE "hello" (should error - not a boolean)
        let expr = Expression::Literal(LiteralValue::String("hello".to_string()));
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_logical_expression() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // (id > 5 AND age > 20) OR (score < 10)
        // (10 > 5 AND 30 > 20) OR (15.5 < 10)
        // (true AND true) OR false
        // true OR false -> true
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
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
            }),
            op: Operator::Or,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column("score".to_string())),
                op: Operator::LessThan,
                right: Box::new(Expression::Literal(LiteralValue::Integer(10))),
            }),
        };
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_boolean_column_as_predicate() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // WHERE is_deleted (column is true)
        let expr = Expression::Column("is_deleted".to_string());
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);

        // WHERE is_active (column is NULL -> evaluates to false)
        let expr = Expression::Column("is_active".to_string());
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    // --- Tests using parse_expr helper (cleaner!) ---

    #[test]
    fn test_with_parser_simple_predicate() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // Much cleaner than manual construction!
        let expr = parse_expr("id > 5");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_with_parser_boolean_equality() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        let expr = parse_expr("is_deleted = TRUE");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_with_parser_complex_predicate() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // (id > 5 AND age > 20) OR score < 10
        let expr = parse_expr("(id > 5 AND age > 20) OR score < 10");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_with_parser_arithmetic_in_where() {
        let evaluator = PredicateEvaluator;
        let (schema, row) = setup_test_data();

        // age + 5 > 30 -> 35 > 30 -> true
        let expr = parse_expr("age + 5 > 30");
        let result = evaluator.evaluate(&expr, &row, &schema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }
}
