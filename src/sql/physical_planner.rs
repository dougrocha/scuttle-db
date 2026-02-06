use miette::{Result, miette};

use crate::{
    ColumnDefinition, DataType, Relation, Schema,
    sql::parser::{LiteralValue, Operator},
};

use super::{logical_planner::LogicalPlan, parser::Expression, planner_context::PlannerContext};

#[derive(Debug)]
pub enum PhysicalPlan {
    SeqScan {
        table: Relation,
    },
    Filter {
        predicate: Expression,
        input: Box<PhysicalPlan>,
    },
    Projection {
        expressions: Vec<Expression>,
        input: Box<PhysicalPlan>,
        schema: Schema,
    },
}

impl PhysicalPlan {
    pub fn schema(&self) -> Schema {
        match self {
            PhysicalPlan::SeqScan { table, .. } => table.schema.clone(),
            PhysicalPlan::Projection { schema, .. } => schema.clone(),
            PhysicalPlan::Filter { input, .. } => input.schema(),
        }
    }

    pub fn from_logical_plan(logical_plan: LogicalPlan, context: &PlannerContext) -> Result<Self> {
        let plan = match logical_plan {
            LogicalPlan::TableScan { table } => {
                let table = context.get_table(&table)?;

                Self::SeqScan {
                    table: table.clone(),
                }
            }
            LogicalPlan::Filter { predicate, input } => {
                let input = Self::from_logical_plan(*input, context)?;

                Self::Filter {
                    predicate,
                    input: Box::new(input),
                }
            }
            LogicalPlan::Projection {
                expressions,
                input,
                column_names,
            } => {
                let input_plan = Self::from_logical_plan(*input, context)?;
                let input_schema = input_plan.schema();

                let output_columns: Vec<ColumnDefinition> = expressions
                    .iter()
                    .zip(column_names.iter())
                    .map(|(expr, output_name)| {
                        match expr {
                            Expression::Column(col_name) => {
                                let input_col = input_schema
                                    .columns
                                    .iter()
                                    .find(|c| &c.name == col_name)
                                    .ok_or_else(|| {
                                        miette!("Column '{}' not found in input schema", col_name)
                                    })?;

                                Ok(ColumnDefinition {
                                    name: output_name.clone(),
                                    data_type: input_col.data_type,
                                    nullable: input_col.nullable,
                                })
                            }
                            Expression::Literal(lit_val) => {
                                let data_type = match lit_val {
                                    LiteralValue::Integer(_) => DataType::Integer,
                                    LiteralValue::Float(_) => DataType::Float,
                                    LiteralValue::String(_) => DataType::Text,
                                    LiteralValue::Boolean(_) => DataType::Boolean,
                                    LiteralValue::Null => {
                                        // Can't infer type from NULL alone
                                        // For now, default to Text or return error
                                        return Err(miette!("Cannot infer type from NULL literal"));
                                    }
                                };
                                Ok(ColumnDefinition {
                                    name: output_name.clone(),
                                    data_type,
                                    nullable: matches!(lit_val, LiteralValue::Null),
                                })
                            }
                            Expression::BinaryOp { .. } => {
                                let expr_type = infer_expression_type(expr, &input_plan.schema())?;

                                Ok(ColumnDefinition {
                                    name: output_name.clone(),
                                    data_type: expr_type,
                                    nullable: false,
                                })
                            }
                            Expression::Is { .. } => Ok(ColumnDefinition {
                                name: output_name.clone(),
                                data_type: DataType::Boolean,
                                nullable: false,
                            }),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let output_schema = Schema::new(output_columns);

                Self::Projection {
                    expressions,
                    input: Box::new(input_plan),
                    schema: output_schema,
                }
            }
        };

        Ok(plan)
    }

    pub fn extract_table_name(plan: &PhysicalPlan) -> Result<&str> {
        match plan {
            PhysicalPlan::SeqScan { table } => Ok(&table.name),
            PhysicalPlan::Filter { input, .. } => Self::extract_table_name(input),
            PhysicalPlan::Projection { input, .. } => Self::extract_table_name(input),
        }
    }
}

fn infer_expression_type(expr: &Expression, schema: &Schema) -> Result<DataType> {
    match expr {
        Expression::Column(col_name) => {
            let col = schema
                .columns
                .iter()
                .find(|col| col.name == *col_name)
                .ok_or_else(|| miette!("Column '{}' not found", col_name))?;

            Ok(col.data_type)
        }
        Expression::Literal(literal_value) => match literal_value {
            LiteralValue::Integer(_) => Ok(DataType::Integer),
            LiteralValue::String(_) => Ok(DataType::Text),
            LiteralValue::Boolean(_) => Ok(DataType::Boolean),
            LiteralValue::Float(_) => Ok(DataType::Float),
            LiteralValue::Null => Err(miette!(
                "Cannot infer type from NULL literal without context"
            )),
        },
        Expression::BinaryOp { left, op, right } => {
            let left = infer_expression_type(left, schema)?;
            let right = infer_expression_type(right, schema)?;

            match op {
                Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                    infer_math_result_type(*op, left, right)
                }
                _ => Err(miette!(
                    "Type inference for operator {:?} not yet implemented",
                    op
                )),
            }
        }
        Expression::Is { .. } => Ok(DataType::Boolean),
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
