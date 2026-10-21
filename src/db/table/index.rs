use std::collections::{BTreeMap, BTreeSet};

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
            })
            .collect()
    }

    fn key(&self, row: &Row) -> Option<IndexKey> {
        IndexKey::for_column(&self.data_type, &row.values[self.column])
    }

    pub(crate) fn insert(&mut self, row: &Row, ctid: Ctid) {
        if let Some(key) = self.key(row) {
            self.entries.entry(key).or_default().push(ctid);
        }
    }

    pub(crate) fn remove(&mut self, row: &Row, ctid: Ctid) {
        let Some(key) = self.key(row) else { return };
        if let Some(ctids) = self.entries.get_mut(&key) {
            ctids.retain(|&c| c != ctid);
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
                    "duplicate key {key:?} in unique column {column_name}"
                ))
                .into());
            }
        }

        Ok(())
    }
}
