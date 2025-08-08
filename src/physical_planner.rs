use miette::Result;

use crate::{logical_planner::LogicalPlan, parser::Expression, planner_context::PlannerContext};

#[derive(Debug)]
pub enum PhysicalPlan {
    SeqScan {
        table_name: String,
    },
    Filter {
        predicate: Expression,
        input: Box<PhysicalPlan>,
    },
    Projection {
        columns_indices: Vec<usize>,
        input: Box<PhysicalPlan>,
    },
}

impl PhysicalPlan {
    pub fn from_logical_plan(logical_plan: LogicalPlan, context: &PlannerContext) -> Result<Self> {
        let plan = match logical_plan {
            LogicalPlan::TableScan { table_name } => Self::SeqScan { table_name },
            LogicalPlan::Filter { predicate, input } => {
                let input = Self::from_logical_plan(*input, context)?;

                Self::Filter {
                    predicate,
                    input: Box::new(input),
                }
            }
            LogicalPlan::Projection { columns, input } => {
                let table_name = Self::extract_table_name(&input)?;
                let schema = context.get_schema(table_name)?;

                let columns_indices = columns
                    .iter()
                    .filter_map(|col| schema.get_column_index(col))
                    .collect::<Vec<usize>>();

                let input = Self::from_logical_plan(*input, context)?;

                Self::Projection {
                    columns_indices,
                    input: Box::new(input),
                }
            }
        };

        Ok(plan)
    }

    fn extract_table_name(plan: &LogicalPlan) -> Result<&str> {
        match plan {
            LogicalPlan::TableScan { table_name } => Ok(table_name),
            LogicalPlan::Filter { input, .. } => Self::extract_table_name(input),
            LogicalPlan::Projection { input, .. } => Self::extract_table_name(input),
        }
    }
}
