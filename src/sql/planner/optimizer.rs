//! Rewrites a logical plan before it runs. Three rules, in order:
//!
//! - Read a filtered table through an index when the WHERE clause narrows an indexed
//!   column.
//! - Read a table in an index's key order instead of sorting it, for an ORDER BY on an
//!   indexed column.
//! - Keep only the rows a LIMIT needs instead of sorting all of them.

use std::{collections::BTreeMap, ops::Bound};

use miette::Result;

use crate::{
    Value,
    db::table::{
        index::{IndexKey, IndexRange, KeyRange, TableIndex},
        table_def::TableDef,
    },
    sql::{
        analyzer::AnalyzedExpression,
        ast::operator::Operator,
        planner::logical::{LogicalPlan, SortKey},
    },
    storage::buffer_pool::BufferPool,
};

/// Above this share of an index's rows, fetching rows one at a time through the index
/// costs more than reading every page in order. Postgres' crossover is usually 5 to 10%.
/// With pages cached in memory it was nearer 50% here, so 10% leaves room to spare.
const MAX_INDEX_FRACTION: f64 = 0.1;

/// Runs every rewrite on a plan before it is executed or explained.
///
/// Reads index pages through `pool`, to count how many rows an index would find.
pub(crate) fn optimize(
    plan: &mut LogicalPlan,
    tables: &BTreeMap<String, TableDef>,
    pool: &mut BufferPool,
) -> Result<()> {
    use_indexes(plan, tables, pool)?;
    use_index_order(plan, tables, pool);
    use_top_n(plan);
    Ok(())
}

/// Removes the Sort when its one key is an indexed column of the table it sorts. The Scan
/// reads that column's index in key order instead. Under a LIMIT, the LIMIT stops it
/// after the first batch.
///
/// For ASC, rows with equal keys come out in the order they are stored, as the Sort's
/// stable sort would put them, because an index entry ends with the row's location. DESC
/// reads the index backward, which returns them in reverse storage order. SQL leaves the
/// order of ties undefined, and Postgres's backward index scans do the same.
///
/// Without a LIMIT, every row is fetched through the index, in an order that may jump
/// around the table file. That only pays while the table and index fit in the cache. On
/// 1M rows, ordering by a column whose order has nothing to do with where rows are
/// stored took 293 ms through the index against 560 ms for Scan + Sort when they fit.
/// With a 1,000-page cache it took 992 ms against 530 ms, because nearly every row read
/// a page. Postgres instead estimates how closely the index order follows the table
/// order (the `correlation` statistic), which needs statistics we don't keep yet.
pub(crate) fn use_index_order(
    plan: &mut LogicalPlan,
    tables: &BTreeMap<String, TableDef>,
    pool: &mut BufferPool,
) {
    match plan {
        LogicalPlan::Limit {
            input,
            limit: Some(_),
            ..
        } => remove_sort(input, tables, None),
        LogicalPlan::Limit { input, .. } => remove_sort(input, tables, Some(pool)),
        _ => remove_sort(plan, tables, Some(pool)),
    }
}

/// `unlimited` holds the pool when every row will be read, so the rule checks the
/// table fits in it first.
fn remove_sort(
    plan: &mut LogicalPlan,
    tables: &BTreeMap<String, TableDef>,
    unlimited: Option<&mut BufferPool>,
) {
    match plan {
        LogicalPlan::Projection { input, .. } => remove_sort(input, tables, unlimited),
        LogicalPlan::Sort { input, keys } => {
            let [
                SortKey {
                    expr: AnalyzedExpression::Column(column, _),
                    descending,
                },
            ] = keys.as_slice()
            else {
                return;
            };
            if scan_in_key_order(input, column.index, *descending, tables, unlimited, None) {
                let input = std::mem::replace(
                    input.as_mut(),
                    LogicalPlan::Values {
                        expressions: vec![],
                    },
                );
                *plan = input;
            }
        }
        _ => {}
    }
}

/// Makes the Scan under `plan`, looking through a Filter, read the index on `column` in
/// key order, or largest key first if `descending`. Returns false, changing nothing, if
/// it can't, or if `unlimited` holds a pool that the table and index don't fit in.
///
/// The scan reads only the part of the index that the Filter's `condition` allows, so
/// `WHERE id > 500000 ORDER BY id` starts at 500000 instead of filtering out every row
/// before it.
fn scan_in_key_order(
    plan: &mut LogicalPlan,
    column: usize,
    descending: bool,
    tables: &BTreeMap<String, TableDef>,
    unlimited: Option<&mut BufferPool>,
    condition: Option<&AnalyzedExpression>,
) -> bool {
    match plan {
        LogicalPlan::Filter { input, condition } => scan_in_key_order(
            input,
            column,
            descending,
            tables,
            unlimited,
            Some(condition),
        ),
        LogicalPlan::Scan {
            table_name, index, ..
        } => {
            let Some(by_column) = tables
                .get(table_name.as_str())
                .and_then(|table| table.indexes.iter().find(|i| i.column == column))
            else {
                return false;
            };
            if let Some(pool) = unlimited {
                let pages = pool.page_count(table_name) + pool.page_count(by_column.file_name());
                if !pool.fits(pages as usize) {
                    return false;
                }
            }
            match index {
                // The WHERE clause already reads this index, and a range comes out in order
                Some(range) if range.column == column => {
                    range.descending = descending;
                    true
                }
                Some(_) => false,
                None => {
                    let mut conjuncts = Vec::new();
                    if let Some(condition) = condition {
                        split_and(condition, &mut conjuncts);
                    }
                    let mut range =
                        narrow(by_column, &conjuncts).unwrap_or_else(|| IndexRange::all(column));
                    range.descending = descending;
                    *index = Some(range);
                    true
                }
            }
        }
        _ => false,
    }
}

/// Turns the Sort under a `LIMIT n OFFSET m` into a TopN that keeps only `n + m` rows.
///
/// The Limit stays on top to skip the offset. A Projection between them keeps the row
/// count, so the rule looks through it.
pub(crate) fn use_top_n(plan: &mut LogicalPlan) {
    if let LogicalPlan::Limit {
        input,
        limit: Some(limit),
        offset,
    } = plan
    {
        bound_sort(input, limit.saturating_add(*offset));
    }
}

fn bound_sort(plan: &mut LogicalPlan, k: u64) {
    match plan {
        LogicalPlan::Projection { input, .. } => bound_sort(input, k),
        LogicalPlan::Sort { input, keys } => {
            let input = std::mem::replace(
                input,
                Box::new(LogicalPlan::Values {
                    expressions: vec![],
                }),
            );
            let keys = std::mem::take(keys);
            *plan = LogicalPlan::TopN { input, keys, k };
        }
        _ => {}
    }
}

/// Sets `index` on every Scan whose Filter compares an indexed column with a literal,
/// when the index finds few enough rows to beat reading the whole table.
///
/// The Filter stays in place and still checks each row, so the index only has to find
/// a superset of the matching rows.
pub(crate) fn use_indexes(
    plan: &mut LogicalPlan,
    tables: &BTreeMap<String, TableDef>,
    pool: &mut BufferPool,
) -> Result<()> {
    match plan {
        LogicalPlan::Filter { input, condition } => {
            if let LogicalPlan::Scan {
                table_name, index, ..
            } = input.as_mut()
            {
                if let Some(table) = tables.get(table_name.as_str()) {
                    *index = pick_index(condition, &table.indexes, pool)?;
                }
                Ok(())
            } else {
                use_indexes(input, tables, pool)
            }
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::TopN { input, .. }
        | LogicalPlan::Limit { input, .. } => use_indexes(input, tables, pool),
        LogicalPlan::Join { left, right, .. } => {
            use_indexes(left, tables, pool)?;
            use_indexes(right, tables, pool)
        }
        _ => Ok(()),
    }
}

/// The range of the index that finds the fewest rows, if any finds few enough.
fn pick_index(
    condition: &AnalyzedExpression,
    indexes: &[TableIndex],
    pool: &mut BufferPool,
) -> Result<Option<IndexRange>> {
    let mut conjuncts = Vec::new();
    split_and(condition, &mut conjuncts);

    let mut best: Option<(usize, IndexRange)> = None;
    for index in indexes {
        let Some(range) = narrow(index, &conjuncts) else {
            continue;
        };
        let limit = (index.len(pool)? as f64 * MAX_INDEX_FRACTION) as usize + 1;
        let rows = index.count_up_to(pool, &range, limit)?;
        if rows < limit && best.as_ref().is_none_or(|(fewest, _)| rows < *fewest) {
            best = Some((rows, range));
        }
    }
    Ok(best.map(|(_, range)| range))
}

/// The keys of `index` that the conditions allow, or `None` if none of them is about
/// the index's column.
fn narrow(index: &TableIndex, conjuncts: &[&AnalyzedExpression]) -> Option<IndexRange> {
    // No comparison is true for NULL, so the ranges stop before the NULLs at the end
    let mut ranges = vec![KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Excluded(IndexKey::Null),
    }];
    let mut narrowed = false;

    for conjunct in conjuncts {
        let Some(allowed) = key_ranges(index, conjunct) else {
            continue;
        };
        // Both lists are sorted and don't overlap, so the pairs that meet come out in
        // order and don't overlap either
        ranges = ranges
            .iter()
            .flat_map(|a| allowed.iter().filter_map(|b| a.intersect(b)))
            .collect();
        narrowed = true;
    }

    narrowed.then_some(IndexRange {
        column: index.column,
        ranges,
        descending: false,
    })
}

/// The keys of `index` that `conjunct` allows, sorted and not overlapping, or `None` if
/// it isn't a condition on the index's column that an index can answer.
fn key_ranges(index: &TableIndex, conjunct: &AnalyzedExpression) -> Option<Vec<KeyRange>> {
    if let AnalyzedExpression::InList {
        expr,
        list,
        negated: false,
    } = conjunct
        && let AnalyzedExpression::Column(column, _) = expr.as_ref()
        && column.index == index.column
    {
        let mut keys = list
            .iter()
            .map(|item| match item {
                AnalyzedExpression::Literal(value) => index.key_for(value),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        keys.sort();
        keys.dedup();
        return Some(keys.into_iter().map(KeyRange::point).collect());
    }

    let (column, op, value) = column_vs_literal(conjunct)?;
    let key = index.key_for(value).filter(|_| column == index.column)?;
    let (lower, upper) = match op {
        Operator::Equal => return Some(vec![KeyRange::point(key)]),
        Operator::GreaterThan => (Bound::Excluded(key), Bound::Unbounded),
        Operator::GreaterThanEqual => (Bound::Included(key), Bound::Unbounded),
        Operator::LessThan => (Bound::Unbounded, Bound::Excluded(key)),
        Operator::LessThanEqual => (Bound::Unbounded, Bound::Included(key)),
        _ => return None,
    };
    Some(vec![KeyRange { lower, upper }])
}

/// The parts of `a AND b AND c`, or the expression itself when it isn't an AND.
fn split_and<'a>(expr: &'a AnalyzedExpression, out: &mut Vec<&'a AnalyzedExpression>) {
    match expr {
        AnalyzedExpression::BinaryExpr {
            left,
            op: Operator::And,
            right,
            ..
        } => {
            split_and(left, out);
            split_and(right, out);
        }
        _ => out.push(expr),
    }
}

/// `column op literal` as (column, op, value). `literal op column` is flipped, so
/// `5 < id` comes back as `id > 5`.
fn column_vs_literal(expr: &AnalyzedExpression) -> Option<(usize, Operator, &Value)> {
    let AnalyzedExpression::BinaryExpr {
        left, op, right, ..
    } = expr
    else {
        return None;
    };

    match (left.as_ref(), right.as_ref()) {
        (AnalyzedExpression::Column(column, _), AnalyzedExpression::Literal(value)) => {
            Some((column.index, *op, value))
        }
        (AnalyzedExpression::Literal(value), AnalyzedExpression::Column(column, _)) => {
            let flipped = match op {
                Operator::GreaterThan => Operator::LessThan,
                Operator::GreaterThanEqual => Operator::LessThanEqual,
                Operator::LessThan => Operator::GreaterThan,
                Operator::LessThanEqual => Operator::GreaterThanEqual,
                other => *other,
            };
            Some((column.index, flipped, value))
        }
        _ => None,
    }
}
