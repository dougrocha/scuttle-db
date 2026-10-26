//! Rewrites a logical plan before it runs. Two rules: read a filtered table through an
//! index when the WHERE clause narrows an indexed column, and keep only the rows a LIMIT
//! needs instead of sorting all of them.

use std::{collections::BTreeMap, ops::Bound};

use crate::{
    Value,
    db::table::{
        index::{IndexKey, IndexRange, TableIndex},
        table_def::TableDef,
    },
    sql::{analyzer::AnalyzedExpression, ast::operator::Operator, planner::logical::LogicalPlan},
};

/// Above this share of an index's rows, fetching rows one at a time through the index
/// costs more than reading every page in order. Postgres' crossover is usually 5 to 10%.
/// With pages cached in memory it was nearer 50% here, so 10% leaves room to spare.
const MAX_INDEX_FRACTION: f64 = 0.1;

/// Runs every rewrite on a plan before it is executed or explained.
pub(crate) fn optimize(plan: &mut LogicalPlan, tables: &BTreeMap<String, TableDef>) {
    use_indexes(plan, tables);
    use_top_n(plan);
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
pub(crate) fn use_indexes(plan: &mut LogicalPlan, tables: &BTreeMap<String, TableDef>) {
    match plan {
        LogicalPlan::Filter { input, condition } => {
            if let LogicalPlan::Scan {
                table_name, index, ..
            } = input.as_mut()
            {
                if let Some(table) = tables.get(table_name.as_str()) {
                    *index = pick_index(condition, &table.indexes);
                }
            } else {
                use_indexes(input, tables);
            }
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::TopN { input, .. }
        | LogicalPlan::Limit { input, .. } => use_indexes(input, tables),
        LogicalPlan::Join { left, right, .. } => {
            use_indexes(left, tables);
            use_indexes(right, tables);
        }
        _ => {}
    }
}

/// The range of the index that finds the fewest rows, if any finds few enough.
fn pick_index(condition: &AnalyzedExpression, indexes: &[TableIndex]) -> Option<IndexRange> {
    let mut conjuncts = Vec::new();
    split_and(condition, &mut conjuncts);

    indexes
        .iter()
        .filter_map(|index| {
            let range = narrow(index, &conjuncts)?;
            let limit = (index.len as f64 * MAX_INDEX_FRACTION) as usize + 1;
            let rows = index.count_up_to(&range, limit);
            (rows < limit).then_some((rows, range))
        })
        .min_by_key(|(rows, _)| *rows)
        .map(|(_, range)| range)
}

/// The keys of `index` that the conditions allow, or `None` if none of them is about
/// the index's column.
fn narrow(index: &TableIndex, conjuncts: &[&AnalyzedExpression]) -> Option<IndexRange> {
    let mut range = IndexRange {
        column: index.column,
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
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
