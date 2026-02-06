use miette::{Result, miette};

use super::{logical_planner::LogicalPlan, parser::Expression, planner_context::PlannerContext};
use crate::{
    ColumnDefinition, DataType, Relation, Schema, Table,
    sql::{infer_type::infer_expression_type, parser::LiteralValue},
};

/// A physical plan is built from the [LogicalPlan]
///
/// This is a map from the logical (what we need to do) to a
/// physical representation of how we will grab the data.
///
/// Currently it is bare bones but the logical planner will say TableScan.
/// In the physical plan, we decide if a SeqScan or IndexScan will be better.
///
/// Here we will only devise a cost model (Row count, IO costs, CPU time) later on to help us determine the best
/// physical path.
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
            PhysicalPlan::SeqScan { table, .. } => table.schema().clone(),
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

                // TODO: Remove this from physical planner,
                // Type checking should go in [Analyzer] struct
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
                                let infer_type = infer_expression_type(expr, &input_plan.schema())?;

                                Ok(ColumnDefinition {
                                    name: output_name.clone(),
                                    data_type: infer_type.data_type,
                                    nullable: infer_type.nullable,
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
            LogicalPlan::Join {
                left: _,
                right: _,
                condition: _,
                join_type: _,
            } => todo!(),
            LogicalPlan::Limit { input: _, count: _ } => todo!(),
            LogicalPlan::Sort {
                input: _,
                order_by: _,
            } => todo!(),
        };

        Ok(plan)
    }
}
