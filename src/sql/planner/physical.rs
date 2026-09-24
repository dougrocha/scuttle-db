use std::cmp::Ordering;

use miette::Result;

use crate::{
    Value,
    db::table::row::Row,
    sql::{
        analyzer::AnalyzedExpression,
        catalog_context::CatalogContext,
        evaluator::{
            Evaluator, aggregate::Accumulator, compare_values, expression::ExpressionEvaluator,
            predicate::PredicateEvaluator,
        },
        planner::logical::{AggregateCall, LogicalPlan, SortKey},
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
            LogicalPlan::Scan { table_name } => {
                let data = self.context.database.get_rows(&table_name)?;

                Ok(Box::new(ScanExec { data }))
            }
            LogicalPlan::Filter { input, condition } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(FilterExec {
                    child: child_node,
                    expr: condition,
                }))
            }
            LogicalPlan::Projection {
                input, expressions, ..
            } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(ProjectionExec {
                    child: child_node,
                    exprs: expressions,
                }))
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(AggregateExec {
                    child: child_node,
                    group_by,
                    aggregates,
                    done: false,
                }))
            }
            LogicalPlan::Sort { input, keys } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(SortExec {
                    child: child_node,
                    keys,
                    done: false,
                }))
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(LimitExec {
                    child: child_node,
                    remaining: limit,
                    to_skip: offset,
                }))
            }
            _ => todo!(),
        }
    }
}

#[derive(Debug)]
pub struct RecordBatch {
    pub rows: Vec<Row>,
}

pub trait ExecutionNode: std::fmt::Debug {
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

#[derive(Debug)]
pub struct ScanExec {
    data: Vec<Row>,
}
impl ExecutionNode for ScanExec {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let batch_size = 1024;

        if self.data.is_empty() {
            return Ok(None);
        }

        let end = batch_size.min(self.data.len());
        let chunk = self.data.drain(..end).collect();

        Ok(Some(RecordBatch { rows: chunk }))
    }
}
#[derive(Debug)]
pub struct ProjectionExec {
    child: Box<dyn ExecutionNode>,
    exprs: Vec<AnalyzedExpression>,
}
impl ExecutionNode for ProjectionExec {
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

/// Groups all input rows and emits one row per group.
///
/// Blocking: it has to see every input row before emitting anything.
#[derive(Debug)]
pub struct AggregateExec {
    child: Box<dyn ExecutionNode>,
    group_by: Vec<AnalyzedExpression>,
    aggregates: Vec<AggregateCall>,
    done: bool,
}
impl AggregateExec {
    fn new_accumulators(&self) -> Vec<Accumulator> {
        self.aggregates
            .iter()
            .map(|call| Accumulator::new(call.function))
            .collect()
    }
}
impl ExecutionNode for AggregateExec {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let evaluator = ExpressionEvaluator;

        // Groups in first-seen order. `Value` can't be hashed (f64), so groups are
        // found with a linear search, which is fine at this scale.
        let mut groups: Vec<(Vec<Value>, Vec<Accumulator>)> = Vec::new();

        while let Some(batch) = self.child.next()? {
            for row in batch.rows {
                let key = self
                    .group_by
                    .iter()
                    .map(|expr| evaluator.evaluate(expr, &row))
                    .collect::<Result<Vec<_>>>()?;

                let index = match groups.iter().position(|(k, _)| *k == key) {
                    Some(index) => index,
                    None => {
                        groups.push((key, self.new_accumulators()));
                        groups.len() - 1
                    }
                };

                for (acc, call) in groups[index].1.iter_mut().zip(&self.aggregates) {
                    let value = call
                        .arg
                        .as_ref()
                        .map(|arg| evaluator.evaluate(arg, &row))
                        .transpose()?;
                    acc.update(value)?;
                }
            }
        }

        // Without GROUP BY there is always exactly one group, even over zero rows
        // (so `SELECT COUNT(*)` on an empty table returns 0, not nothing).
        if groups.is_empty() && self.group_by.is_empty() {
            groups.push((Vec::new(), self.new_accumulators()));
        }

        let rows = groups
            .into_iter()
            .map(|(mut values, accumulators)| {
                values.extend(accumulators.into_iter().map(Accumulator::finish));
                Row::new(values)
            })
            .collect();

        Ok(Some(RecordBatch { rows }))
    }
}

/// Sorts all input rows by the ORDER BY keys.
///
/// Blocking, and stable: rows with equal keys keep their input order.
#[derive(Debug)]
pub struct SortExec {
    child: Box<dyn ExecutionNode>,
    keys: Vec<SortKey>,
    done: bool,
}
impl ExecutionNode for SortExec {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let evaluator = ExpressionEvaluator;

        let mut keyed_rows = Vec::new();
        while let Some(batch) = self.child.next()? {
            for row in batch.rows {
                let key = self
                    .keys
                    .iter()
                    .map(|key| evaluator.evaluate(&key.expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                keyed_rows.push((key, row));
            }
        }

        keyed_rows.sort_by(|(a, _), (b, _)| {
            for ((a, b), key) in a.iter().zip(b).zip(&self.keys) {
                let ordering = compare_values(a, b);
                let ordering = if key.descending {
                    ordering.reverse()
                } else {
                    ordering
                };

                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });

        Ok(Some(RecordBatch {
            rows: keyed_rows.into_iter().map(|(_, row)| row).collect(),
        }))
    }
}

/// Skips the first `to_skip` rows, then passes through at most `remaining` rows.
#[derive(Debug)]
pub struct LimitExec {
    child: Box<dyn ExecutionNode>,

    /// Rows still allowed through; `None` means no LIMIT
    remaining: Option<u64>,

    /// OFFSET rows not skipped yet
    to_skip: u64,
}
impl ExecutionNode for LimitExec {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.remaining == Some(0) {
                return Ok(None);
            }

            let Some(mut batch) = self.child.next()? else {
                return Ok(None);
            };

            let skip = self.to_skip.min(batch.rows.len() as u64);
            batch.rows.drain(..skip as usize);
            self.to_skip -= skip;

            if let Some(remaining) = self.remaining {
                batch
                    .rows
                    .truncate(remaining.min(batch.rows.len() as u64) as usize);
                self.remaining = Some(remaining - batch.rows.len() as u64);
            }

            if !batch.rows.is_empty() {
                return Ok(Some(batch));
            }
        }
    }
}
