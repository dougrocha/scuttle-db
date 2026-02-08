use miette::Result;

use super::catalog_context::CatalogContext;
use crate::{
    Row, Value,
    sql::{
        analyzer::{AnalyzedExpression, LogicalPlan, OutputSchema},
        evaluator::{Evaluator, expression::ExpressionEvaluator, predicate::PredicateEvaluator},
    },
};

pub struct PhysicalPlanner<'a, 'db> {
    context: &'a mut CatalogContext<'db>,
}

impl<'a, 'db> PhysicalPlanner<'a, 'db> {
    pub(crate) fn new(context: &'a mut CatalogContext<'db>) -> Self {
        Self { context }
    }

    pub fn create_physical_plan(
        &mut self,
        analyzed_plan: LogicalPlan,
    ) -> Result<Box<dyn ExecutionNode>> {
        match analyzed_plan {
            LogicalPlan::Scan { table_name, schema } => {
                let data = self.context.database.get_rows(&table_name)?;

                Ok(Box::new(ScanExec { schema, data }))
            }
            LogicalPlan::Filter { input, condition } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(FilterExec {
                    child: child_node,
                    expr: condition,
                }))
            }
            LogicalPlan::Projection {
                input,
                expressions,
                schema,
            } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(ProjectionExec {
                    child: child_node,
                    exprs: expressions,
                    schema,
                }))
            }
        }
    }
}

#[derive(Debug)]
pub struct RecordBatch {
    pub rows: Vec<Row>,
}

pub trait ExecutionNode: std::fmt::Debug {
    fn schema(&self) -> &OutputSchema;

    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

#[derive(Debug)]
pub struct ScanExec {
    schema: OutputSchema,
    data: Vec<Row>,
}
impl ExecutionNode for ScanExec {
    fn schema(&self) -> &OutputSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let batch_size = 1024;

        if self.data.is_empty() {
            return Ok(None);
        }

        let end = batch_size.min(self.data.len());
        let chunk: Vec<Row> = self.data.drain(..end).collect();

        Ok(Some(RecordBatch { rows: chunk }))
    }
}
#[derive(Debug)]
pub struct ProjectionExec {
    child: Box<dyn ExecutionNode>,
    exprs: Vec<AnalyzedExpression>,
    schema: OutputSchema,
}
impl ExecutionNode for ProjectionExec {
    fn schema(&self) -> &OutputSchema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next()? else {
            return Ok(None);
        };

        let evaluator = ExpressionEvaluator;

        let projected_rows: Result<Vec<Row>> = batch
            .rows
            .into_iter()
            .map(|row| {
                let new_values: Result<Vec<Value>> = self
                    .exprs
                    .iter()
                    .map(|expr| evaluator.evaluate(expr, &row))
                    .collect();

                Ok(Row {
                    values: new_values?,
                })
            })
            .collect();

        Ok(Some(RecordBatch {
            rows: projected_rows?,
        }))
    }
}
#[derive(Debug)]
pub struct FilterExec {
    child: Box<dyn ExecutionNode>,
    expr: AnalyzedExpression,
}
impl ExecutionNode for FilterExec {
    fn schema(&self) -> &OutputSchema {
        self.child.schema()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let evaluator = PredicateEvaluator;

        while let Some(batch) = self.child.next()? {
            let mut filtered_rows = Vec::with_capacity(batch.rows.len());

            for row in batch.rows {
                if evaluator.evaluate(&self.expr, &row)? {
                    filtered_rows.push(row);
                }
            }

            // If this batch had 0 rows after filtering
            // we loop again to grab the next batch
            if !filtered_rows.is_empty() {
                return Ok(Some(RecordBatch {
                    rows: filtered_rows,
                }));
            }
        }

        Ok(None)
    }
}
