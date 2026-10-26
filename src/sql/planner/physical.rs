use std::{cell::Cell, cmp::Ordering, collections::BinaryHeap, rc::Rc, time::Duration};

use miette::{Result, miette};
use web_time::Instant;

use crate::{
    Schema, Value,
    db::table::{
        cursor::{TableCursor, fetch_row},
        index::Ctid,
        row::Row,
    },
    sql::{
        analyzer::AnalyzedExpression,
        ast::statement::JoinKind,
        catalog_context::CatalogContext,
        evaluator::{
            Evaluator, aggregate::Accumulator, compare_values, expression::ExpressionEvaluator,
            predicate::PredicateEvaluator,
        },
        planner::logical::{AggregateCall, LogicalPlan, SortKey},
    },
    storage::buffer_pool::BufferPool,
};

pub struct PhysicalPlanner<'a, 'db> {
    context: &'a mut CatalogContext<'db>,

    /// Set by [`PhysicalPlanner::profiled`]: one entry per node, in the order EXPLAIN
    /// prints them.
    profile: Option<Vec<Rc<NodeStats>>>,
}

impl<'a, 'db> PhysicalPlanner<'a, 'db> {
    pub(crate) fn new(context: &'a mut CatalogContext<'db>) -> Self {
        Self {
            context,
            profile: None,
        }
    }

    /// A planner whose nodes count their rows and time, for EXPLAIN ANALYZE. Read the
    /// counts with [`PhysicalPlanner::into_profile`] after the plan has run.
    pub(crate) fn profiled(context: &'a mut CatalogContext<'db>) -> Self {
        Self {
            context,
            profile: Some(Vec::new()),
        }
    }

    /// The stats of every node built so far, parent before children (the order
    /// EXPLAIN prints them). Empty unless the planner is [`PhysicalPlanner::profiled`].
    pub(crate) fn into_profile(self) -> Vec<Rc<NodeStats>> {
        self.profile.unwrap_or_default()
    }

    pub fn create_physical_plan(
        &mut self,
        analyzed_plan: LogicalPlan,
    ) -> Result<Box<dyn ExecutionNode>> {
        // The slot is taken before the children are built, so parents come first
        let Some(profile) = &mut self.profile else {
            return self.build_node(analyzed_plan);
        };
        let stats = Rc::new(NodeStats::default());
        profile.push(Rc::clone(&stats));

        let child = self.build_node(analyzed_plan)?;
        Ok(Box::new(ProfiledExec { child, stats }))
    }

    fn build_node(&mut self, analyzed_plan: LogicalPlan) -> Result<Box<dyn ExecutionNode>> {
        match analyzed_plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                index: Some(range),
                ..
            } => {
                let table = self.context.get_table(&table_name)?;
                let index = table
                    .indexes
                    .iter()
                    .find(|index| index.column == range.column)
                    .ok_or_else(|| {
                        miette!("No index on column {} of {table_name}", range.column)
                    })?;

                Ok(Box::new(IndexScanExec {
                    ctids: index.range(&range).into_iter(),
                    table_name,
                    schema,
                }))
            }
            LogicalPlan::Scan {
                table_name, schema, ..
            } => {
                let pool = &mut self.context.database.buffer_manager;

                Ok(Box::new(ScanExec {
                    cursor: TableCursor::open(pool, table_name, schema),
                }))
            }
            LogicalPlan::Filter { input, condition } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(FilterExec {
                    child: child_node,
                    expr: condition,
                }))
            }
            LogicalPlan::Projection { input, columns } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(ProjectionExec {
                    child: child_node,
                    exprs: columns.into_iter().map(|column| column.expr).collect(),
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
            LogicalPlan::TopN { input, keys, k } => {
                let child_node = self.create_physical_plan(*input)?;

                Ok(Box::new(TopNExec {
                    child: child_node,
                    keys,
                    k: usize::try_from(k).unwrap_or(usize::MAX),
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
            LogicalPlan::Join {
                left,
                right,
                kind,
                on,
            } => {
                // Read before `right` is moved. The width comes from the schema, not from rows,
                // because the right table may be empty.
                let right_width = right.output_schema().len();
                let left = self.create_physical_plan(*left)?;
                let right = self.create_physical_plan(*right)?;

                Ok(Box::new(JoinExec {
                    left,
                    right,
                    kind,
                    on,
                    right_width,
                    right_rows: None,
                }))
            }
            LogicalPlan::Values { .. }
            | LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::CreateTable { .. } => Err(miette!(
                "VALUES and statement plans are executed by Database, not the physical planner"
            )),
        }
    }
}

#[derive(Debug)]
pub struct RecordBatch {
    pub rows: Vec<Row>,
}

/// What a node may use while it runs. Only the pages for now. Phase 2 of
/// `docs/roadmap.md` adds the tables, so an index can be read while the plan runs.
///
/// The caller lends it for one `next` call only. Nodes are `'static`, so they can't keep
/// it, and that lets two scans of the same table share the pool (a self-join).
pub struct ExecCtx<'a> {
    pub(crate) pool: &'a mut BufferPool,
}

/// A node of the physical plan, pulled one batch at a time.
pub trait ExecutionNode: std::fmt::Debug {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>>;
}

/// What one node of a profiled plan produced.
#[derive(Debug, Default)]
pub(crate) struct NodeStats {
    pub(crate) rows: Cell<usize>,

    /// Time spent in the node's `next`, including the children it pulled from.
    pub(crate) time: Cell<Duration>,
}

/// Wraps a node and records its rows and time in `stats`.
#[derive(Debug)]
struct ProfiledExec {
    child: Box<dyn ExecutionNode>,
    stats: Rc<NodeStats>,
}
impl ExecutionNode for ProfiledExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        let start = Instant::now();
        let batch = self.child.next(ctx)?;
        self.stats.time.set(self.stats.time.get() + start.elapsed());

        if let Some(batch) = &batch {
            self.stats
                .rows
                .set(self.stats.rows.get() + batch.rows.len());
        }
        Ok(batch)
    }
}

/// Full table scan. It reads and decodes pages when a batch is asked for, not up front.
#[derive(Debug)]
pub struct ScanExec {
    cursor: TableCursor,
}
impl ExecutionNode for ScanExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        let batch_size = 1024;

        // Whole pages only, so a batch is at least `batch_size` rows unless the table ran out
        let mut rows = Vec::with_capacity(batch_size);
        while rows.len() < batch_size
            && self.cursor.read_next_page(ctx.pool, |_, _, row| {
                rows.push(row);
                Ok(())
            })?
        {}

        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(RecordBatch { rows }))
    }
}

/// Reads only the rows an index found, in key order.
///
/// The index lookup happens when the plan is built, which costs little: it is a list of
/// row locations, not rows. Rows are fetched from the heap as batches are asked for.
#[derive(Debug)]
pub struct IndexScanExec {
    table_name: String,
    schema: Schema,
    ctids: std::vec::IntoIter<Ctid>,
}
impl ExecutionNode for IndexScanExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        let batch_size = 1024;

        let mut rows = Vec::with_capacity(batch_size.min(self.ctids.len()));
        while rows.len() < batch_size
            && let Some(ctid) = self.ctids.next()
        {
            // The index drops a row when it is deleted, so this only skips rows deleted
            // after the lookup
            if let Some(row) = fetch_row(ctx.pool, &self.table_name, &self.schema, ctid)? {
                rows.push(row);
            }
        }

        if rows.is_empty() {
            return Ok(None);
        }

        Ok(Some(RecordBatch { rows }))
    }
}
#[derive(Debug)]
pub struct ProjectionExec {
    child: Box<dyn ExecutionNode>,
    exprs: Vec<AnalyzedExpression>,
}
impl ExecutionNode for ProjectionExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next(ctx)? else {
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
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        let evaluator = PredicateEvaluator;

        while let Some(batch) = self.child.next(ctx)? {
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

/// Nested-loop join. The right side is drained ONCE (first call to `next`) and kept
/// in memory; the left side streams batch by batch (same precedent as
/// AggregateExec/SortExec draining their child).
#[derive(Debug)]
pub struct JoinExec {
    left: Box<dyn ExecutionNode>,
    right: Box<dyn ExecutionNode>,
    kind: JoinKind,
    on: AnalyzedExpression,
    right_width: usize,
    /// `None` until the right child has been drained.
    right_rows: Option<Vec<Row>>,
}
impl ExecutionNode for JoinExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        // 1. Drain the right side exactly once.
        if self.right_rows.is_none() {
            let mut rows = Vec::new();
            while let Some(batch) = self.right.next(ctx)? {
                rows.extend(batch.rows);
            }
            self.right_rows = Some(rows);
        }
        let right_rows = self.right_rows.as_ref().expect("drained above");
        let evaluator = PredicateEvaluator; // Null -> false: right for ON

        loop {
            let Some(batch) = self.left.next(ctx)? else {
                return Ok(None);
            };
            let mut out = Vec::new();
            for left_row in batch.rows {
                let mut matched = false;
                for right_row in right_rows {
                    let joined = Row::new(
                        left_row
                            .values
                            .iter()
                            .chain(&right_row.values)
                            .cloned()
                            .collect(),
                    );
                    if evaluator.evaluate(&self.on, &joined)? {
                        out.push(joined);
                        matched = true;
                    }
                }
                if !matched && self.kind == JoinKind::Left {
                    // Pad with `right_width` NULLs, NOT right_rows[0].len():
                    // the right table may be empty.
                    let mut values = left_row.values;
                    values.extend(std::iter::repeat_n(Value::Null, self.right_width));
                    out.push(Row::new(values));
                }
            }
            // Like FilterExec: a left batch that produced nothing must not end the stream.
            if !out.is_empty() {
                return Ok(Some(RecordBatch { rows: out }));
            }
        }
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
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let evaluator = ExpressionEvaluator;

        // Groups in first-seen order. `Value` can't be hashed (f64), so groups are
        // found with a linear search, which is fine at this scale.
        let mut groups: Vec<(Vec<Value>, Vec<Accumulator>)> = Vec::new();

        while let Some(batch) = self.child.next(ctx)? {
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
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let evaluator = ExpressionEvaluator;

        let mut keyed_rows = Vec::new();
        while let Some(batch) = self.child.next(ctx)? {
            for row in batch.rows {
                let key = self
                    .keys
                    .iter()
                    .map(|key| evaluator.evaluate(&key.expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                keyed_rows.push((key, row));
            }
        }

        keyed_rows.sort_by(|(a, _), (b, _)| compare_keys(a, b, &self.keys));

        Ok(Some(RecordBatch {
            rows: keyed_rows.into_iter().map(|(_, row)| row).collect(),
        }))
    }
}

/// Orders two rows by their ORDER BY values, key by key, honoring DESC.
fn compare_keys(a: &[Value], b: &[Value], keys: &[SortKey]) -> Ordering {
    for ((a, b), key) in a.iter().zip(b).zip(keys) {
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
}

/// Returns the first `k` rows in sort order without sorting the rest, like Postgres'
/// "top-N heapsort".
///
/// A max-heap holds the best `k` rows seen so far, with the worst of them on top. A new
/// row only gets in by beating that one, so each row costs log k instead of log n, and
/// only `k` rows are kept in memory.
#[derive(Debug)]
pub struct TopNExec {
    child: Box<dyn ExecutionNode>,
    keys: Vec<SortKey>,
    k: usize,
    done: bool,
}
impl ExecutionNode for TopNExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let Self { child, keys, k, .. } = self;
        let evaluator = ExpressionEvaluator;

        let mut heap = BinaryHeap::new();
        let mut seq = 0;
        while let Some(batch) = child.next(ctx)? {
            for row in batch.rows {
                let key = keys
                    .iter()
                    .map(|key| evaluator.evaluate(&key.expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                let ranked = Ranked {
                    key,
                    seq,
                    row,
                    keys,
                };
                seq += 1;

                if heap.len() < *k {
                    heap.push(ranked);
                } else if let Some(mut worst) = heap.peek_mut()
                    && ranked < *worst
                {
                    *worst = ranked;
                }
            }
        }

        Ok(Some(RecordBatch {
            rows: heap
                .into_sorted_vec()
                .into_iter()
                .map(|ranked| ranked.row)
                .collect(),
        }))
    }
}

/// A row in [`TopNExec`]'s heap, ordered the way the Sort would order it.
///
/// `seq` is the row's position in the input. It breaks ties, so rows with equal keys come
/// out in input order, as they do from the (stable) Sort.
struct Ranked<'k> {
    key: Vec<Value>,
    seq: usize,
    row: Row,
    keys: &'k [SortKey],
}
impl Ord for Ranked<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_keys(&self.key, &other.key, self.keys).then(self.seq.cmp(&other.seq))
    }
}
impl PartialOrd for Ranked<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Ranked<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Ranked<'_> {}

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
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        loop {
            if self.remaining == Some(0) {
                return Ok(None);
            }

            let Some(mut batch) = self.child.next(ctx)? else {
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
