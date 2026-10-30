//! Rewrites a logical plan before it runs. Three rules, in order:
//!
//! - Read a filtered table through an index when the WHERE clause narrows an indexed
//!   column.
//! - Read a table in an index's key order instead of sorting it, for an ORDER BY with a
//!   LIMIT.
//! - Keep only the rows a LIMIT needs instead of sorting all of them.

use std::{collections::BTreeMap, ops::Bound};

use miette::Result;

use crate::{
    Value,
    db::table::{
        index::{IndexKey, IndexRange, TableIndex},
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
    use_index_order(plan, tables);
    use_top_n(plan);
    Ok(())
}

/// Removes the Sort under a LIMIT when its one key is an indexed column of the table it
/// sorts. The Scan reads that column's index in key order instead, so the LIMIT stops
/// it after the first batch.
///
/// For ASC, rows with equal keys come out in the order they are stored, as the Sort's
/// stable sort would put them, because an index entry ends with the row's location. DESC
/// reads the index backward, which returns them in reverse storage order. SQL leaves the
/// order of ties undefined, and Postgres's backward index scans do the same.
///
/// Without a LIMIT the Sort stays: reading every row through the index jumps around the
/// table file, which may cost more than reading it in order and sorting.
pub(crate) fn use_index_order(plan: &mut LogicalPlan, tables: &BTreeMap<String, TableDef>) {
    if let LogicalPlan::Limit {
        input,
        limit: Some(_),
        ..
    } = plan
    {
        remove_sort(input, tables);
    }
}

fn remove_sort(plan: &mut LogicalPlan, tables: &BTreeMap<String, TableDef>) {
    match plan {
        LogicalPlan::Projection { input, .. } => remove_sort(input, tables),
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
            if scan_in_key_order(input, column.index, *descending, tables, None) {
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
/// it can't.
///
/// The scan reads only the part of the index that the Filter's `condition` allows, so
/// `WHERE id > 500000 ORDER BY id` starts at 500000 instead of filtering out every row
/// before it.
fn scan_in_key_order(
    plan: &mut LogicalPlan,
    column: usize,
    descending: bool,
    tables: &BTreeMap<String, TableDef>,
    condition: Option<&AnalyzedExpression>,
) -> bool {
    match plan {
        LogicalPlan::Filter { input, condition } => {
            scan_in_key_order(input, column, descending, tables, Some(condition))
        }
        LogicalPlan::Scan {
            table_name,
            schema,
            index,
            ..
        } => {
            // An index leaves out NULLs, so a nullable column would lose its NULL rows
            if schema.columns[column].is_nullable() {
                return false;
            }
            let Some(by_column) = tables
                .get(table_name.as_str())
                .and_then(|table| table.indexes.iter().find(|i| i.column == column))
            else {
                return false;
            };
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
                    let mut range = narrow(by_column, &conjuncts).unwrap_or(IndexRange {
                        column,
                        lower: Bound::Unbounded,
                        upper: Bound::Unbounded,
                        descending: false,
                    });
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
    let mut range = IndexRange {
        column: index.column,
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
        descending: false,
    };
    let mut narrowed = false;

    for conjunct in conjuncts {
        let Some((column, op, value)) = column_vs_literal(conjunct) else {
            continue;
        };
        let Some(key) = index.key_for(value).filter(|_| column == index.column) else {
            continue;
        };

        match op {
            Operator::Equal => {
                tighten_lower(&mut range.lower, Bound::Included(key.clone()));
                tighten_upper(&mut range.upper, Bound::Included(key));
            }
            Operator::GreaterThan => tighten_lower(&mut range.lower, Bound::Excluded(key)),
            Operator::GreaterThanEqual => tighten_lower(&mut range.lower, Bound::Included(key)),
            Operator::LessThan => tighten_upper(&mut range.upper, Bound::Excluded(key)),
            Operator::LessThanEqual => tighten_upper(&mut range.upper, Bound::Included(key)),
            _ => continue,
        }
        narrowed = true;
    }

    narrowed.then_some(range)
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

/// Keeps whichever lower bound lets fewer keys through.
fn tighten_lower(current: &mut Bound<IndexKey>, new: Bound<IndexKey>) {
    let tighter = match (&*current, &new) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b))
            if a != b =>
        {
            b > a
        }
        // Same key: excluding it is tighter than including it
        (_, Bound::Excluded(_)) => true,
        _ => false,
    };
    if tighter {
        *current = new;
    }
}

/// Keeps whichever upper bound lets fewer keys through.
fn tighten_upper(current: &mut Bound<IndexKey>, new: Bound<IndexKey>) {
    let tighter = match (&*current, &new) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b))
            if a != b =>
        {
            b < a
        }
        (_, Bound::Excluded(_)) => true,
        _ => false,
    };
    if tighter {
        *current = new;
    }
}
