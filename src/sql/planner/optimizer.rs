//! Rewrites a logical plan before it runs. For now only one rule: read a filtered table
//! through an index when the WHERE clause narrows an indexed column.

use std::{collections::BTreeMap, ops::Bound};

use crate::{
    Value,
    db::table::{
        index::{IndexKey, IndexRange, TableIndex},
        table_def::TableDef,
    },
    sql::{analyzer::AnalyzedExpression, ast::operator::Operator, planner::logical::LogicalPlan},
};

/// Sets `index` on every Scan whose Filter compares an indexed column with a literal.
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
        | LogicalPlan::Limit { input, .. } => use_indexes(input, tables),
        LogicalPlan::Join { left, right, .. } => {
            use_indexes(left, tables);
            use_indexes(right, tables);
        }
        _ => {}
    }
}

/// The range of the first index that some part of the condition narrows.
fn pick_index(condition: &AnalyzedExpression, indexes: &[TableIndex]) -> Option<IndexRange> {
    let mut conjuncts = Vec::new();
    split_and(condition, &mut conjuncts);

    indexes.iter().find_map(|index| {
        let mut range = IndexRange {
            column: index.column,
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let mut narrowed = false;

        for conjunct in &conjuncts {
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
    })
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
