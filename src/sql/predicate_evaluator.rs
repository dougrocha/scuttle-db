// / Evaluates SQL predicate expressions against table rows.
// /
// / The `PredicateEvaluator` takes parsed SQL expressions (typically from WHERE clauses)
// / and evaluates them against individual rows to determine if they match the criteria.
// / It supports:
// / - Comparison operators: `=`, `!=`, `>`, `<`
// / - Logical operators: `AND`, `OR`
// / - Type coercion between integers and floats
// / - Column references and literal values
// pub struct PredicateEvaluator;
//
// impl PredicateEvaluator {
//     pub fn evaluate_predicate(
//         &self,
//         expr: &Expression,
//         row: &Row,
//         schema: &Schema,
//     ) -> Result<bool> {
//         let val = self.evaluate(expr, row, schema)?;
//         Ok(self.is_truthy(&val))
//     }
//
//     /// Evaluates an SQL expression against a row and returns whether it matches.
//     ///
//     /// This is the main entry point for predicate evaluation. It recursively evaluates
//     /// the expression tree and returns a boolean result indicating whether the row
//     /// satisfies the predicate.
//     fn evaluate(&self, expression: &Expression, row: &Row, schema: &Schema) -> Result<Value> {
//         match expression {
//             Expression::BinaryOp { left, op, right } => {
//                 self.evaluate_binary_op(left, op, right, row, schema)
//             }
//             Expression::Literal(literal_val) => Ok(self.is_truthy(literal_val)),
//             Expression::Column(col_name) => {
//                 let (col_idx, column_def) = schema
//                     .columns
//                     .iter()
//                     .enumerate()
//                     .find(|(_, col)| col.name == *col_name)
//                     .ok_or(miette!("Column {:?} does not exist in schema", col_name))?;
//
//                 if column_def.data_type != DataType::Boolean {
//                     return Err(miette!(
//                         "Cannot use column '{}' as a boolean filter because it is of type {:?}",
//                         col_name,
//                         column_def.data_type
//                     ));
//                 }
//
//                 let row_val = row
//                     .get_value(col_idx)
//                     .ok_or(miette!("Row idx {:?} does not exist in row", col_idx))?;
//
//                 match row_val {
//                     Value::Boolean(b) => Ok(*b),
//                     Value::Null => Ok(false),
//                     _ => Err(miette!(
//                         "Data corruption: Found non-boolean value in boolean column"
//                     )),
//                 }
//             }
//         }
//     }
//
//     /// Evaluates a binary operation expression.
//     ///
//     /// Handles comparison operators (`=`, `!=`, `>`, `<`) and logical operators
//     /// (`AND`, `OR`). For comparison operators, both sides are evaluated to values
//     /// and compared. For logical operators, short-circuit evaluation is used.
//     fn evaluate_binary_op(
//         &self,
//         left: &Expression,
//         op: &Operator,
//         right: &Expression,
//         row: &Row,
//         schema: &Schema,
//     ) -> Result<bool> {
//         match op {
//             Operator::Equal => {
//                 let left_val = self.evaluate_expression(left, row, schema)?;
//                 let right_val = self.evaluate_expression(right, row, schema)?;
//                 Ok(self.values_equal(&left_val, &right_val))
//             }
//             Operator::NotEqual => {
//                 let left_val = self.evaluate_expression(left, row, schema)?;
//                 let right_val = self.evaluate_expression(right, row, schema)?;
//                 Ok(!self.values_equal(&left_val, &right_val))
//             }
//             Operator::And => {
//                 let left_result = self.evaluate_predicate(left, row, schema)?;
//                 if !left_result {
//                     return Ok(false);
//                 }
//                 self.evaluate_predicate(right, row, schema)
//             }
//             Operator::Or => {
//                 let left_result = self.evaluate_predicate(left, row, schema)?;
//                 if left_result {
//                     return Ok(true);
//                 }
//                 self.evaluate_predicate(right, row, schema)
//             }
//             Operator::GreaterThan => {
//                 let left_result = self.evaluate_expression(left, row, schema)?;
//                 let right_result = self.evaluate_expression(right, row, schema)?;
//
//                 self.values_greater_than(&left_result, &right_result)
//             }
//             Operator::LessThan => {
//                 let left_result = self.evaluate_expression(left, row, schema)?;
//                 let right_result = self.evaluate_expression(right, row, schema)?;
//
//                 self.values_less_than(&left_result, &right_result)
//             }
//             Operator::Add => {
//                 let left_result = self.evaluate_expression(left, row, schema)?;
//                 let right_result = self.evaluate_expression(right, row, schema)?;
//
//                 self.values_less_than(&left_result, &right_result)
//             }
//             Operator::Multiply => {
//                 let left_result = self.evaluate_expression(left, row, schema)?;
//                 let right_result = self.evaluate_expression(right, row, schema)?;
//
//                 self.values_less_than(&left_result, &right_result)
//             }
//             Operator::Divide => {
//                 let left_result = self.evaluate_expression(left, row, schema)?;
//                 let right_result = self.evaluate_expression(right, row, schema)?;
//
//                 self.values_less_than(&left_result, &right_result)
//             }
//             Operator::Subtract => {
//                 let left_result = self.evaluate_expression(left, row, schema)?;
//                 let right_result = self.evaluate_expression(right, row, schema)?;
//
//                 self.values_less_than(&left_result, &right_result)
//             }
//         }
//     }
//
//     /// Evaluates an expression to a concrete value.
//     ///
//     /// Converts an expression (column reference or literal) into an actual value
//     /// that can be used in comparisons. For column references, retrieves the value
//     /// from the row using the schema. For literals, converts them to the appropriate
//     /// `Value` type.
//     fn evaluate_expression(
//         &self,
//         expression: &Expression,
//         row: &Row,
//         schema: &Schema,
//     ) -> Result<Value> {
//         match expression {
//             Expression::Column(name) => {
//                 let idx = schema
//                     .get_column_index(name)
//                     .ok_or(miette!("Column not found"))?;
//
//                 Ok(row.get_value(idx).cloned().unwrap_or(Value::Null))
//             }
//             Expression::Literal(literal) => Ok(Value::from(literal.clone())),
//             _ => Err(miette!("Cannot evaluate this expression to a value")),
//         }
//     }
//
//     /// Compares two values for equality with type coercion.
//     ///
//     /// Supports comparing values of the same type, and automatically coerces
//     /// between `Integer` and `Float` types. Floating-point comparisons use
//     /// an epsilon tolerance to handle precision issues.
//     fn values_equal(&self, left: &Value, right: &Value) -> bool {
//         match (left, right) {
//             (Value::Integer(a), Value::Integer(b)) => a == b,
//             (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
//             (Value::Integer(a), Value::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
//             (Value::Float(a), Value::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
//             (Value::Text(a), Value::Text(b)) => a == b,
//             (Value::Boolean(a), Value::Boolean(b)) => a == b,
//             // Nulls are never equal in SQL (returns False for simple equality)
//             (Value::Null, _) | (_, Value::Null) => false,
//             _ => false,
//         }
//     }
//
//     /// Compares two values using the less-than operator with type coercion.
//     ///
//     /// Supports numeric comparisons between `Integer` and `Float` types,
//     /// with automatic type coercion when needed.
//     fn values_less_than(&self, left: &Value, right: &Value) -> Result<bool> {
//         match (left, right) {
//             (Value::Integer(a), Value::Integer(b)) => Ok(a < b),
//             (Value::Float(a), Value::Float(b)) => Ok(a < b),
//             (Value::Integer(a), Value::Float(b)) => Ok((*a as f64) < *b),
//             (Value::Float(a), Value::Integer(b)) => Ok(*a < (*b as f64)),
//             (Value::Null, _) | (_, Value::Null) => Ok(false),
//             _ => Err(miette!("Invalid comparison")),
//         }
//     }
//
//     /// Compares two values using the greater-than operator with type coercion.
//     ///
//     /// Supports numeric comparisons between `Integer` and `Float` types,
//     /// with automatic type coercion when needed.
//     fn values_greater_than(&self, left: &Value, right: &Value) -> Result<bool> {
//         match (left, right) {
//             (Value::Integer(a), Value::Integer(b)) => Ok(a > b),
//             (Value::Float(a), Value::Float(b)) => Ok(a > b),
//             (Value::Integer(a), Value::Float(b)) => Ok((*a as f64) > *b),
//             (Value::Float(a), Value::Integer(b)) => Ok(*a > (*b as f64)),
//             (Value::Null, _) | (_, Value::Null) => Ok(false),
//             _ => Err(miette!("Invalid comparison")),
//         }
//     }
//
//     fn values_add(&self, left: &Value, right: &Value) -> Result<Value> {
//         let value = match (left, right) {
//             (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
//             (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
//             (Value::Integer(a), Value::Float(b)) => Value::Float((*a as f64) + *b),
//             (Value::Float(a), Value::Integer(b)) => Value::Float(*a + (*b as f64)),
//             (Value::Null, _) | (_, Value::Null) => Value::Null,
//             _ => return Err(miette!("Invalid comparison")),
//         };
//         Ok(value)
//     }
//
//     fn values_subtract(&self, left: &Value, right: &Value) -> Result<Value> {
//         let value = match (left, right) {
//             (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
//             (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
//             (Value::Integer(a), Value::Float(b)) => Value::Float((*a as f64) - *b),
//             (Value::Float(a), Value::Integer(b)) => Value::Float(*a - (*b as f64)),
//             (Value::Null, _) | (_, Value::Null) => Value::Null,
//             _ => return Err(miette!("Invalid comparison")),
//         };
//         Ok(value)
//     }
//
//     fn values_multiply(&self, left: &Value, right: &Value) -> Result<Value> {
//         let value = match (left, right) {
//             (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
//             (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
//             (Value::Integer(a), Value::Float(b)) => Value::Float((*a as f64) * *b),
//             (Value::Float(a), Value::Integer(b)) => Value::Float(*a * (*b as f64)),
//             (Value::Null, _) | (_, Value::Null) => Value::Null,
//             _ => return Err(miette!("Invalid comparison")),
//         };
//         Ok(value)
//     }
//
//     fn values_divide(&self, left: &Value, right: &Value) -> Result<Value> {
//         let value = match (left, right) {
//             (Value::Integer(a), Value::Integer(b)) => Value::Integer(a / b),
//             (Value::Float(a), Value::Float(b)) => Value::Float(a / b),
//             (Value::Integer(a), Value::Float(b)) => Value::Float((*a as f64) / *b),
//             (Value::Float(a), Value::Integer(b)) => Value::Float(*a / (*b as f64)),
//             (Value::Null, _) | (_, Value::Null) => Value::Null,
//             _ => return Err(miette!("Invalid comparison")),
//         };
//         Ok(value)
//     }
//
//     fn is_truthy(&self, lit: &LiteralValue) -> bool {
//         match lit {
//             LiteralValue::Boolean(b) => *b,
//             LiteralValue::Null => false,
//             _ => false, // Literals like 1 or "true" are not implicitly boolean in SQL
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::db::table::Schema;
//     use crate::{ColumnDefinition, DataType};
//
//     fn setup_test_data() -> (Schema, Row) {
//         let schema = Schema {
//             columns: vec![
//                 ColumnDefinition::new("id", DataType::Integer, false),
//                 ColumnDefinition::new("name", DataType::Text, false),
//                 ColumnDefinition::new("score", DataType::Float, true),
//                 ColumnDefinition::new("is_active", DataType::Boolean, true),
//                 ColumnDefinition::new("is_deleted", DataType::Boolean, true),
//             ],
//         };
//         let row = Row::new(vec![
//             Value::Integer(10),
//             Value::Text("Alice".to_string()),
//             Value::Float(15.5),
//             Value::Null, // is_active is NULL
//             Value::Boolean(true),
//         ]);
//
//         (schema, row)
//     }
//
//     #[test]
//     fn test_null_behavior() {
//         let evaluator = PredicateEvaluator;
//         let (schema, row) = setup_test_data();
//
//         // 1. Column = NULL (is_active = NULL) -> False (SQL Logic)
//         let expr_eq_null = Expression::BinaryOp {
//             left: Box::new(Expression::Column("is_active".to_string())),
//             op: Operator::Equal,
//             right: Box::new(Expression::Literal(LiteralValue::Null)),
//         };
//         let result = evaluator.evaluate_predicate(&expr_eq_null, &row, &schema);
//         assert!(result.is_ok());
//         assert!(!result.unwrap());
//
//         // 2. NULL Comparison in Greater Than -> False
//         let expr_gt_null = Expression::BinaryOp {
//             left: Box::new(Expression::Column("id".to_string())),
//             op: Operator::GreaterThan,
//             right: Box::new(Expression::Literal(LiteralValue::Null)),
//         };
//         let result = evaluator.evaluate_predicate(&expr_gt_null, &row, &schema);
//         assert!(result.is_ok());
//         assert!(!result.unwrap());
//     }
//
//     #[test]
//     fn test_numeric_coercion_and_comparison() {
//         let evaluator = PredicateEvaluator;
//         let (schema, row) = setup_test_data();
//
//         // 1. Integer Column vs Float Literal: id > 9.9 (10 > 9.9) -> True
//         let expr1 = Expression::BinaryOp {
//             left: Box::new(Expression::Column("id".to_string())),
//             op: Operator::GreaterThan,
//             right: Box::new(Expression::Literal(LiteralValue::Float(9.9))),
//         };
//         let result = evaluator.evaluate_predicate(&expr1, &row, &schema);
//         assert!(result.is_ok());
//         assert!(result.unwrap());
//
//         // 2. Float Column vs Integer Literal: score > 15 (15.5 > 15) -> True
//         let expr2 = Expression::BinaryOp {
//             left: Box::new(Expression::Column("score".to_string())),
//             op: Operator::GreaterThan,
//             right: Box::new(Expression::Literal(LiteralValue::Integer(15))),
//         };
//         let result = evaluator.evaluate_predicate(&expr2, &row, &schema);
//         assert!(result.is_ok());
//         assert!(result.unwrap());
//     }
//
//     #[test]
//     fn test_logical_operators_with_null() {
//         let evaluator = PredicateEvaluator;
//         let (schema, row) = setup_test_data();
//
//         // (id > 5) AND (is_active)
//         // id > 5 is True, but is_active is NULL (which your evaluator treats as false)
//         // Result should be False
//         let expr_and = Expression::BinaryOp {
//             left: Box::new(Expression::BinaryOp {
//                 left: Box::new(Expression::Column("id".to_string())),
//                 op: Operator::GreaterThan,
//                 right: Box::new(Expression::Literal(LiteralValue::Integer(5))),
//             }),
//             op: Operator::And,
//             right: Box::new(Expression::Column("is_active".to_string())),
//         };
//         let result = evaluator.evaluate_predicate(&expr_and, &row, &schema);
//         assert!(result.is_ok());
//         assert!(!result.unwrap());
//     }
//
//     #[test]
//     fn test_boolean_equality() {
//         let evaluator = PredicateEvaluator;
//         let (schema, row) = setup_test_data();
//
//         // 1. is_deleted = true (row has is_deleted = true) -> True
//         let expr_deleted_true = Expression::BinaryOp {
//             left: Box::new(Expression::Column("is_deleted".to_string())),
//             op: Operator::Equal,
//             right: Box::new(Expression::Literal(LiteralValue::Boolean(true))),
//         };
//         let result = evaluator.evaluate_predicate(&expr_deleted_true, &row, &schema);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), true);
//
//         // 2. is_deleted = false (row has is_deleted = true) -> False
//         let expr_deleted_false = Expression::BinaryOp {
//             left: Box::new(Expression::Column("is_deleted".to_string())),
//             op: Operator::Equal,
//             right: Box::new(Expression::Literal(LiteralValue::Boolean(false))),
//         };
//         let result = evaluator.evaluate_predicate(&expr_deleted_false, &row, &schema);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), false);
//
//         // 3. is_deleted != false (row has is_deleted = true) -> True
//         let expr_not_false = Expression::BinaryOp {
//             left: Box::new(Expression::Column("is_deleted".to_string())),
//             op: Operator::NotEqual,
//             right: Box::new(Expression::Literal(LiteralValue::Boolean(false))),
//         };
//         let result = evaluator.evaluate_predicate(&expr_not_false, &row, &schema);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), true);
//
//         // 4. is_active = true (row has is_active = NULL) -> False (NULL != true)
//         let expr_active_true = Expression::BinaryOp {
//             left: Box::new(Expression::Column("is_active".to_string())),
//             op: Operator::Equal,
//             right: Box::new(Expression::Literal(LiteralValue::Boolean(true))),
//         };
//         let result = evaluator.evaluate_predicate(&expr_active_true, &row, &schema);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), false);
//     }
//
//     #[test]
//     fn test_type_mismatch_errors() {
//         let evaluator = PredicateEvaluator;
//         let (schema, row) = setup_test_data();
//
//         // Comparing Text to Integer: name > 10
//         // This should trigger the Err(miette!("Invalid comparison")) in values_greater_than
//         let expr_mismatch = Expression::BinaryOp {
//             left: Box::new(Expression::Column("name".to_string())),
//             op: Operator::GreaterThan,
//             right: Box::new(Expression::Literal(LiteralValue::Integer(10))),
//         };
//
//         let result = evaluator.evaluate_predicate(&expr_mismatch, &row, &schema);
//         assert!(result.is_err());
//     }
// }
