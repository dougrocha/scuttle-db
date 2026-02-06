use crate::{
    DataType, Schema,
    sql::parser::{Expression, LiteralValue, operators::Operator},
};
use miette::{Result, miette};

pub(crate) struct InferredType {
    pub data_type: DataType,
    pub nullable: bool,
}

impl InferredType {
    fn new(data_type: DataType, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }

    fn not_null(data_type: DataType) -> Self {
        Self::new(data_type, false)
    }
}

pub(crate) fn infer_expression_type(expr: &Expression, schema: &Schema) -> Result<InferredType> {
    match expr {
        Expression::Column(col_name) => {
            let col = schema
                .columns
                .iter()
                .find(|col| col.name == *col_name)
                .ok_or_else(|| miette!("Column '{}' not found", col_name))?;

            Ok(InferredType::new(col.data_type, col.nullable))
        }
        Expression::Literal(literal_value) => match literal_value {
            LiteralValue::Integer(_) => Ok(InferredType::not_null(DataType::Integer)),
            LiteralValue::String(_) => Ok(InferredType::not_null(DataType::Text)),
            LiteralValue::Boolean(_) => Ok(InferredType::not_null(DataType::Boolean)),
            LiteralValue::Float(_) => Ok(InferredType::not_null(DataType::Float)),
            LiteralValue::Null => Err(miette!(
                "Cannot infer type from NULL literal without context"
            )),
        },
        Expression::BinaryOp { left, op, right } => {
            let left = infer_expression_type(left, schema)?;
            let right = infer_expression_type(right, schema)?;

            match op {
                Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                    let data_type = infer_math_result_type(*op, left.data_type, right.data_type)?;
                    Ok(InferredType::new(
                        data_type,
                        left.nullable || right.nullable,
                    ))
                }
                _ => Err(miette!(
                    "Type inference for operator {:?} not yet implemented",
                    op
                )),
            }
        }
        Expression::Is { .. } => Ok(InferredType::not_null(DataType::Boolean)),
    }
}

fn infer_math_result_type(op: Operator, left: DataType, right: DataType) -> Result<DataType> {
    match (left, right) {
        (DataType::Integer, DataType::Integer) => Ok(DataType::Integer),
        (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer) => {
            Ok(DataType::Float)
        }
        _ => Err(miette!(
            "Type inference for {} is not implemented between types {} and {}",
            op,
            left,
            right
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnDefinition;

    #[test]
    fn test_infer_type_nullable_column() {
        let schema = Schema::new(vec![ColumnDefinition::new("age", DataType::Integer, true)]);
        let expr = Expression::Column("age".to_string());
        let result = infer_expression_type(&expr, &schema).unwrap();

        assert_eq!(result.data_type, DataType::Integer);
        assert!(result.nullable);
    }

    #[test]
    fn test_infer_type_binary_op_propagates_nullability() {
        let schema = Schema::new(vec![ColumnDefinition::new("age", DataType::Integer, true)]);
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: Operator::Multiply,
            right: Box::new(Expression::Literal(LiteralValue::Integer(2))),
        };
        let result = infer_expression_type(&expr, &schema).unwrap();

        assert_eq!(result.data_type, DataType::Integer);
        assert!(result.nullable); // Should be nullable because 'age' is nullable
    }
}
