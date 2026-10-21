use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

use miette::Result;

use crate::{
    DataType, DatabaseError, Value,
    db::table::{column_def::ColumnConstraint, row::Row, schema::Schema},
    storage::page::{ItemId, PageId},
};

/// Where a row is stored: its heap page and item (PostgreSQL's `ctid`).
pub(crate) type Ctid = (PageId, ItemId);

/// A value as an index orders it.
///
/// `Value` can't be ordered as a whole (`Float64` has NaN), so only the types an index
/// supports get a key. Every key in one index has the same variant, because the column
/// has one type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum IndexKey {
    Int(i64),
    Text(String),
    Timestamp(i64),
}

impl IndexKey {
    /// The key for `value` in a column of type `data_type`, or `None` if the index can't
    /// use it: NULL, or a value of another type (such as `id = 1.5` on an integer column).
    pub(crate) fn for_column(data_type: &DataType, value: &Value) -> Option<Self> {
        match (data_type, value) {
            (DataType::Int64, Value::Int64(n)) => Some(Self::Int(*n)),
            (DataType::Text | DataType::VarChar(_), Value::Text(s)) => Some(Self::Text(s.clone())),
            (DataType::Timestamp, Value::Timestamp(t)) => Some(Self::Timestamp(*t)),
            _ => None,
        }
    }
}

/// An in-memory index over one column: every key, in order, with the rows that hold it.
///
/// Only the index's existence is stored, as the column's PRIMARY KEY or UNIQUE constraint
/// in the schema. The entries are rebuilt from the table when the database opens.
///
/// NULLs are not indexed, so a UNIQUE column may hold any number of them, as in SQL.
#[derive(Debug, Clone)]
pub(crate) struct TableIndex {
    pub(crate) column: usize,
    data_type: DataType,
    entries: BTreeMap<IndexKey, Vec<Ctid>>,

    /// Rows in the index. NULLs aren't indexed, so this can be less than the table's rows.
    pub(crate) len: usize,
}

impl TableIndex {
    /// One index per PRIMARY KEY or UNIQUE column whose type can be indexed, all empty.
    pub(crate) fn for_schema(schema: &Schema) -> Vec<Self> {
        schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                column
                    .constraints
                    .iter()
                    .any(|c| matches!(c, ColumnConstraint::PrimaryKey | ColumnConstraint::Unique))
            })
            .filter(|(_, column)| {
                matches!(
                    column.data_type,
                    DataType::Int64 | DataType::Text | DataType::VarChar(_) | DataType::Timestamp
                )
            })
            .map(|(column, def)| Self {
                column,
                data_type: def.data_type,
                entries: BTreeMap::new(),
                len: 0,
            })
            .collect()
    }

    fn key(&self, row: &Row) -> Option<IndexKey> {
        self.key_for(&row.values[self.column])
    }

    /// The key this index would store `value` under, if it can hold it at all.
    pub(crate) fn key_for(&self, value: &Value) -> Option<IndexKey> {
        IndexKey::for_column(&self.data_type, value)
    }

    /// The rows whose key is within `range`, in key order.
    pub(crate) fn range(&self, range: &IndexRange) -> Vec<Ctid> {
        self.ctids_in(range).collect()
    }

    /// How many rows fall in `range`, counting no further than `limit`.
    ///
    /// A `BTreeMap` can't count a range without walking it, so the limit keeps the cost
    /// of asking "is this range small?" small even when the answer is no.
    pub(crate) fn count_up_to(&self, range: &IndexRange, limit: usize) -> usize {
        self.ctids_in(range).take(limit).count()
    }

    fn ctids_in<'a>(&'a self, range: &'a IndexRange) -> impl Iterator<Item = Ctid> + 'a {
        // `BTreeMap::range` panics on bounds that cross, such as `id > 5 AND id < 3`.
        // No key can match those, so there is nothing to read.
        let crossed = match (&range.lower, &range.upper) {
            (Bound::Included(lo), Bound::Included(hi)) => lo > hi,
            (
                Bound::Included(lo) | Bound::Excluded(lo),
                Bound::Included(hi) | Bound::Excluded(hi),
            ) => lo >= hi,
            _ => false,
        };

        (!crossed)
            .then(|| {
                self.entries
                    .range::<IndexKey, _>((range.lower.as_ref(), range.upper.as_ref()))
            })
            .into_iter()
            .flatten()
            .flat_map(|(_, ctids)| ctids.iter().copied())
    }

    pub(crate) fn insert(&mut self, row: &Row, ctid: Ctid) {
        if let Some(key) = self.key(row) {
            self.entries.entry(key).or_default().push(ctid);
            self.len += 1;
        }
    }

    pub(crate) fn remove(&mut self, row: &Row, ctid: Ctid) {
        let Some(key) = self.key(row) else { return };
        if let Some(ctids) = self.entries.get_mut(&key) {
            let before = ctids.len();
            ctids.retain(|&c| c != ctid);
            self.len -= before - ctids.len();
            if ctids.is_empty() {
                self.entries.remove(&key);
            }
        }
    }

    /// Fails if writing `new_rows` would give two live rows the same key.
    ///
    /// `replacing` holds the rows the statement removes before writing (an UPDATE's old
    /// versions), so a row may keep its own key.
    pub(crate) fn check_unique(
        &self,
        column_name: &str,
        new_rows: &[Row],
        replacing: &BTreeSet<Ctid>,
    ) -> Result<()> {
        let mut seen = BTreeSet::new();

        for key in new_rows.iter().filter_map(|row| self.key(row)) {
            let taken = self
                .entries
                .get(&key)
                .is_some_and(|ctids| ctids.iter().any(|ctid| !replacing.contains(ctid)));

            if taken || !seen.insert(key.clone()) {
                return Err(DatabaseError::ConstraintViolation(format!(
                    "duplicate key {key} in unique column {column_name}"
                ))
                .into());
            }
        }

        Ok(())
    }
}

/// The part of one index a scan reads: the keys of `column` between two bounds.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexRange {
    pub(crate) column: usize,
    pub(crate) lower: Bound<IndexKey>,
    pub(crate) upper: Bound<IndexKey>,
}

impl std::fmt::Display for IndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexKey::Int(n) => write!(f, "{n}"),
            IndexKey::Text(s) => write!(f, "'{s}'"),
            IndexKey::Timestamp(t) => write!(f, "TIMESTAMP '{}'", Value::Timestamp(*t)),
        }
    }
}
