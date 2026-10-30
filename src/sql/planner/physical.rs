use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    rc::Rc,
    time::Duration,
};

use miette::{Result, miette};
use web_time::Instant;

use crate::{
    Schema, Value,
    db::table::{
        cursor::{TableCursor, fetch_row},
        index::IndexCursor,
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
    storage::{
        buffer_pool::BufferPool,
        external_sort::{ExternalSort, Record, Sorted, Spill},
        spill::{decode_values, encode_values},
    },
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
                let db = &mut *self.context.database;
                let index = db
                    .tables
                    .get(&table_name)
                    .and_then(|table| {
                        table
                            .indexes
                            .iter()
                            .find(|index| index.column == range.column)
                    })
                    .ok_or_else(|| {
                        miette!("No index on column {} of {table_name}", range.column)
                    })?;

                Ok(Box::new(IndexScanExec {
                    ctids: index.scan(&range),
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
                    keys: keys.into(),
                    spill: self.context.database.sort_spill(),
                    state: SortState::Start,
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

/// What a node may use while it runs. Only the pages for now. Phase 3 of
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

    /// Anything else EXPLAIN ANALYZE should show about how the node ran, such as how a
    /// sort was done.
    fn detail(&self) -> Option<String> {
        None
    }
}

/// What one node of a profiled plan produced.
#[derive(Debug, Default)]
pub(crate) struct NodeStats {
    pub(crate) rows: Cell<usize>,

    /// Time spent in the node's `next`, including the children it pulled from.
    pub(crate) time: Cell<Duration>,

    /// Page requests the cache answered from memory, and pages it read from disk, during
    /// the node's `next`. Like `time`, these include the children.
    pub(crate) hits: Cell<u64>,
    pub(crate) reads: Cell<u64>,

    /// The node's [`ExecutionNode::detail`] after its last `next`.
    pub(crate) detail: RefCell<Option<String>>,
}

/// Wraps a node and records its rows and time in `stats`.
#[derive(Debug)]
struct ProfiledExec {
    child: Box<dyn ExecutionNode>,
    stats: Rc<NodeStats>,
}
impl ExecutionNode for ProfiledExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        let (start, cache) = (Instant::now(), ctx.pool.stats());
        let batch = self.child.next(ctx)?;
        self.stats.time.set(self.stats.time.get() + start.elapsed());

        let after = ctx.pool.stats();
        let stats = &self.stats;
        stats.hits.set(stats.hits.get() + after.hits - cache.hits);
        stats
            .reads
            .set(stats.reads.get() + after.reads - cache.reads);

        if let Some(batch) = &batch {
            self.stats
                .rows
                .set(self.stats.rows.get() + batch.rows.len());
        }
        *self.stats.detail.borrow_mut() = self.child.detail();
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

/// Reads only the rows an index finds, in key order.
///
/// The leaves are read and the rows fetched from the heap only as batches are asked for,
/// so a scan that stops early (under a LIMIT) reads no further.
#[derive(Debug)]
pub struct IndexScanExec {
    table_name: String,
    schema: Schema,
    ctids: IndexCursor,
}
impl ExecutionNode for IndexScanExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        let batch_size = 1024;

        let mut rows = Vec::new();
        while rows.len() < batch_size
            && let Some(ctid) = self.ctids.next(ctx.pool)?
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

                // Keys match as ORDER BY sees them, so all NULLs form one group, and so do
                // all NaNs, and -0.0 and 0.0
                let same = |k: &[Value]| {
                    k.iter()
                        .zip(&key)
                        .all(|(a, b)| compare_values(a, b).is_eq())
                };
                let index = match groups.iter().position(|(k, _)| same(k)) {
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

/// How many bytes of rows a sort holds in memory before it writes them to disk, unless
/// changed with [`Database::set_sort_mem`](crate::Database::set_sort_mem). 64 MB.
pub const DEFAULT_SORT_MEM: usize = 64 * 1024 * 1024;

/// Sorts all input rows by the ORDER BY keys.
///
/// Blocking, and stable: rows with equal keys keep their input order.
///
/// Rows that fit in the memory budget are sorted in memory. Past the budget it becomes an
/// external merge sort (see [`ExternalSort`]): sorted runs are written to disk and merged,
/// and the output streams out a batch at a time.
#[derive(Debug)]
pub struct SortExec {
    child: Box<dyn ExecutionNode>,
    keys: Rc<[SortKey]>,

    /// `None` sorts in memory however many rows come in (an in-memory database).
    spill: Option<Spill>,
    state: SortState,
}

#[derive(Debug)]
enum SortState {
    /// Nothing has been read yet.
    Start,

    /// Every row has been read and sorted, and is being returned.
    Sorted(Sorted<KeyedRow>),
}

impl SortExec {
    /// Reads the whole input into a sort, writing runs to disk past the budget.
    fn read_input(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Sorted<KeyedRow>> {
        let evaluator = ExpressionEvaluator;
        let keys = Rc::clone(&self.keys);
        let mut sort = ExternalSort::new(
            move |a: &KeyedRow, b: &KeyedRow| compare_keys(&a.key, &b.key, &keys),
            self.spill.clone(),
        );

        while let Some(batch) = self.child.next(ctx)? {
            for row in batch.rows {
                let key = self
                    .keys
                    .iter()
                    .map(|key| evaluator.evaluate(&key.expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                sort.push(KeyedRow { key, row })?;
            }
        }
        sort.finish()
    }
}

impl ExecutionNode for SortExec {
    fn next(&mut self, ctx: &mut ExecCtx<'_>) -> Result<Option<RecordBatch>> {
        if let SortState::Start = self.state {
            self.state = SortState::Sorted(self.read_input(ctx)?);
        }
        let SortState::Sorted(sorted) = &mut self.state else {
            unreachable!("set above")
        };

        let mut rows = Vec::with_capacity(1024);
        while rows.len() < 1024
            && let Some(keyed) = sorted.next()?
        {
            rows.push(keyed.row);
        }
        Ok((!rows.is_empty()).then_some(RecordBatch { rows }))
    }

    fn detail(&self) -> Option<String> {
        let SortState::Sorted(sorted) = &self.state else {
            return None;
        };
        Some(match (sorted.runs, sorted.passes) {
            (0, _) => "sort: in memory".to_string(),
            (runs, 1) => format!("sort: external merge, {runs} runs"),
            (runs, passes) => format!("sort: external merge, {runs} runs, {passes} passes"),
        })
    }
}

/// A row and its ORDER BY values, as a sort holds it.
struct KeyedRow {
    key: Vec<Value>,
    row: Row,
}

impl Record for KeyedRow {
    fn encode(&self, out: &mut Vec<u8>) {
        encode_values(&self.key, out);
        encode_values(&self.row.values, out);
    }

    fn decode(mut record: &[u8]) -> Result<Self> {
        Ok(KeyedRow {
            key: decode_values(&mut record)?,
            row: Row::new(decode_values(&mut record)?),
        })
    }

    fn heap_size(&self) -> usize {
        size_in_memory(&self.key) + size_in_memory(&self.row.values)
    }
}

/// About how many bytes `values` take on the heap: the values themselves, plus the text
/// they point to.
fn size_in_memory(values: &[Value]) -> usize {
    let text: usize = values
        .iter()
        .map(|value| match value {
            Value::Text(text) => text.capacity(),
            _ => 0,
        })
        .sum();
    std::mem::size_of_val(values) + text
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

/// Returns the first `k` rows in sort order without sorting the rest.
///
/// Rows collect in a buffer. Each time it holds `2k` rows (at least 1,024), a
/// `select_nth` keeps the best `k` and drops the rest, so at most `2k` rows are in memory
/// and each row costs constant time on average. The worst row kept becomes a cutoff: a
/// row that doesn't sort before it can never make the top `k`, so it is dropped without
/// being stored. When `k` is close to the row count the buffer is never cut and this is
/// a plain sort, where a heap would be slower than sorting.
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
        let k = *k;
        if k == 0 {
            return Ok(Some(RecordBatch { rows: Vec::new() }));
        }
        let evaluator = ExpressionEvaluator;
        let capacity = k.saturating_mul(2).max(1024);

        let mut kept: Vec<Ranked> = Vec::new();
        let mut cutoff: Option<Vec<Value>> = None;
        let mut seq = 0;
        while let Some(batch) = child.next(ctx)? {
            for row in batch.rows {
                let key = keys
                    .iter()
                    .map(|key| evaluator.evaluate(&key.expr, &row))
                    .collect::<Result<Vec<_>>>()?;
                seq += 1;

                // A tie loses too: it came later in the input than the row it ties with
                if let Some(cutoff) = &cutoff
                    && compare_keys(&key, cutoff, keys).is_ge()
                {
                    continue;
                }

                kept.push(Ranked {
                    key,
                    seq,
                    row,
                    keys,
                });
                if kept.len() >= capacity {
                    keep_best(&mut kept, k);
                    cutoff = Some(kept[k - 1].key.clone());
                }
            }
        }

        keep_best(&mut kept, k);
        // `Ranked` never ties (`seq` breaks them), so an unstable sort gives the same order
        kept.sort_unstable();
        Ok(Some(RecordBatch {
            rows: kept.into_iter().map(|ranked| ranked.row).collect(),
        }))
    }
}

/// Drops all but the `k` best rows, leaving the worst of them last.
fn keep_best(rows: &mut Vec<Ranked>, k: usize) {
    if rows.len() > k {
        rows.select_nth_unstable(k - 1);
        rows.truncate(k);
    }
}

/// A row kept by [`TopNExec`], ordered the way the Sort would order it.
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
