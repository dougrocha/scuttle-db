use std::{collections::BTreeSet, ops::Bound};

use miette::Result;

use crate::{
    DataType, DatabaseError, Value,
    db::table::{column_def::ColumnConstraint, row::Row, schema::Schema},
    storage::{
        btree::{BTree, Cursor, MAX_ENTRY},
        buffer_pool::BufferPool,
        external_sort::Sorted,
        page::{ItemId, PageId},
    },
};

/// Where a row is stored: its heap page and item (PostgreSQL's `ctid`).
pub(crate) type Ctid = (PageId, ItemId);

/// A value as an index orders it.
///
/// `Value` can't be ordered as a whole (`Float64` has NaN), so a key stores a float as
/// bits that can be. Every key in one index has the same variant, because the column has
/// one type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum IndexKey {
    Int(i64),
    Text(String),
    Timestamp(i64),
    Bool(bool),

    /// A float as bits that sort in number order, see [`IndexKey::float`].
    Float(u64),
}

impl IndexKey {
    /// The key for `value` in a column of type `data_type`, or `None` if the index can't
    /// use it: NULL, or a value of another type (such as `id = 1.5` on an integer column).
    pub(crate) fn for_column(data_type: &DataType, value: &Value) -> Option<Self> {
        match (data_type, value) {
            (DataType::Int64, Value::Int64(n)) => Some(Self::Int(*n)),
            (DataType::Text | DataType::VarChar(_), Value::Text(s)) => Some(Self::Text(s.clone())),
            (DataType::Timestamp, Value::Timestamp(t)) => Some(Self::Timestamp(*t)),
            (DataType::Bool, Value::Bool(b)) => Some(Self::Bool(*b)),
            (DataType::Float64, Value::Float64(f)) => Some(Self::float(*f)),
            _ => None,
        }
    }

    /// The key for a float. Its bits sort as the numbers do once the sign bit is set on a
    /// positive number and every bit is flipped on a negative one (which makes the more
    /// negative one smaller). `-0.0` becomes `0.0`, since they are equal, and every NaN
    /// becomes one NaN that sorts after all numbers, as in Postgres.
    fn float(f: f64) -> Self {
        let f = if f == 0.0 {
            0.0
        } else if f.is_nan() {
            f64::NAN
        } else {
            f
        };
        let bits = f.to_bits();
        Self::Float(if bits >> 63 == 1 {
            !bits
        } else {
            bits | 1 << 63
        })
    }

    /// The float a [`IndexKey::Float`] holds.
    fn float_value(key: u64) -> f64 {
        f64::from_bits(if key >> 63 == 1 {
            key & !(1 << 63)
        } else {
            !key
        })
    }
}

impl IndexKey {
    /// Writes the key as bytes that sort in the same order as the keys.
    ///
    /// - A number flips its sign bit and is written big-endian, so negative numbers sort
    ///   first and comparing bytes is comparing numbers.
    /// - Text ends with `00 00`, and a `00` inside it is written `00 FF`. That way text
    ///   that is a prefix of other text ("ab" before "abc") still sorts first.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        match self {
            IndexKey::Int(n) | IndexKey::Timestamp(n) => {
                out.extend_from_slice(&((*n as u64) ^ (1 << 63)).to_be_bytes());
            }
            IndexKey::Float(bits) => out.extend_from_slice(&bits.to_be_bytes()),
            IndexKey::Bool(b) => out.push(u8::from(*b)),
            IndexKey::Text(text) => {
                for &byte in text.as_bytes() {
                    out.push(byte);
                    if byte == 0 {
                        out.push(0xFF);
                    }
                }
                out.extend_from_slice(&[0, 0]);
            }
        }
    }
}

/// Bytes a ctid adds to the end of an index entry.
pub(crate) const CTID_LEN: usize = 6;

/// An index entry as the B-tree stores it: the key, then the ctid. Two rows with the same
/// key differ in their ctids, so every entry is unique and a delete finds exactly its own.
pub(crate) fn encode_entry(key: &IndexKey, (page, item): Ctid) -> Vec<u8> {
    let mut out = Vec::new();
    key.encode(&mut out);
    out.extend_from_slice(&page.to_be_bytes());
    out.extend_from_slice(&item.to_be_bytes());
    out
}

/// The ctid at the end of an entry made by [`encode_entry`].
pub(crate) fn entry_ctid(entry: &[u8]) -> Ctid {
    let ctid = &entry[entry.len() - CTID_LEN..];
    (
        PageId::from_be_bytes(ctid[..4].try_into().unwrap()),
        ItemId::from_be_bytes(ctid[4..].try_into().unwrap()),
    )
}

/// An index over one column, stored as a B+tree in its own file, `<table>.<column>.index`.
///
/// Each entry is the column's key followed by the row's ctid (see [`encode_entry`]), so
/// rows sharing a key are separate entries and a delete finds exactly its own.
///
/// NULLs are not indexed, so a UNIQUE column may hold any number of them, as in SQL.
#[derive(Debug, Clone)]
pub(crate) struct TableIndex {
    pub(crate) column: usize,
    data_type: DataType,
    tree: BTree,
}

impl TableIndex {
    /// One index per PRIMARY KEY or UNIQUE column. Their files
    /// aren't touched: see [`TableIndex::exists`] and [`TableIndex::create`].
    pub(crate) fn for_schema(table_name: &str, schema: &Schema) -> Vec<Self> {
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
            .map(|(column, def)| Self {
                column,
                data_type: def.data_type,
                tree: BTree::open(&format!("{table_name}.{}.index", def.name)),
            })
            .collect()
    }

    /// The name of the index's file, which is also its name in the buffer pool.
    pub(crate) fn file_name(&self) -> &str {
        self.tree.name()
    }

    /// Whether the index's file has been created.
    pub(crate) fn exists(&self, pool: &mut BufferPool) -> bool {
        pool.page_count(self.tree.name()) > 0
    }

    /// Creates the index's file, holding no entries.
    pub(crate) fn create(&self, pool: &mut BufferPool) -> Result<()> {
        BTree::create(pool, self.tree.name()).map(|_| ())
    }

    /// The entry this index stores for `row`, or `None` if it doesn't index the row (a NULL).
    pub(crate) fn entry(&self, row: &Row, ctid: Ctid) -> Option<Vec<u8>> {
        Some(encode_entry(&self.key(row)?, ctid))
    }

    /// Creates the index's file holding `entries`, building the tree from the bottom up
    /// (see [`BTree::build`]). The entries come sorted, from an external sort of
    /// [`TableIndex::entry`] for each row.
    pub(crate) fn build(&self, pool: &mut BufferPool, mut entries: Sorted<Vec<u8>>) -> Result<()> {
        let entries = std::iter::from_fn(|| entries.next().transpose());
        BTree::build(pool, self.tree.name(), entries).map(|_| ())
    }

    /// Rows in the index. NULLs aren't indexed, so this can be less than the table's rows.
    pub(crate) fn len(&self, pool: &mut BufferPool) -> Result<u64> {
        self.tree.len(pool)
    }

    fn key(&self, row: &Row) -> Option<IndexKey> {
        self.key_for(&row.values[self.column])
    }

    /// The key this index would store `value` under, if it can hold it at all.
    pub(crate) fn key_for(&self, value: &Value) -> Option<IndexKey> {
        IndexKey::for_column(&self.data_type, value)
    }

    /// The rows whose key is within `range`, in key order, read as they are asked for.
    pub(crate) fn scan(&self, pool: &mut BufferPool, range: &IndexRange) -> Result<IndexCursor> {
        let encode = |key: &IndexKey| {
            let mut bytes = Vec::new();
            key.encode(&mut bytes);
            bytes
        };
        let lower = match &range.lower {
            Bound::Included(key) => Bound::Included(encode(key)),
            Bound::Excluded(key) => Bound::Excluded(encode(key)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match &range.upper {
            Bound::Included(key) => Bound::Included(encode(key)),
            Bound::Excluded(key) => Bound::Excluded(encode(key)),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Every entry of a key starts with the key's bytes, so they all sort at or after it
        let start = match &lower {
            Bound::Included(key) | Bound::Excluded(key) => key.as_slice(),
            Bound::Unbounded => &[],
        };
        Ok(IndexCursor {
            cursor: self.tree.seek(pool, start)?,
            lower,
            upper,
        })
    }

    /// How many rows fall in `range`, counting no further than `limit`.
    ///
    /// The tree doesn't know how many entries a range holds without walking it, so the
    /// limit keeps the cost of asking "is this range small?" small even when the answer
    /// is no.
    pub(crate) fn count_up_to(
        &self,
        pool: &mut BufferPool,
        range: &IndexRange,
        limit: usize,
    ) -> Result<usize> {
        let mut cursor = self.scan(pool, range)?;
        let mut count = 0;
        while count < limit && cursor.next(pool)?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    pub(crate) fn insert(&self, pool: &mut BufferPool, row: &Row, ctid: Ctid) -> Result<()> {
        match self.key(row) {
            Some(key) => self.tree.insert(pool, &encode_entry(&key, ctid)),
            None => Ok(()),
        }
    }

    pub(crate) fn remove(&self, pool: &mut BufferPool, row: &Row, ctid: Ctid) -> Result<()> {
        if let Some(key) = self.key(row) {
            self.tree.remove(pool, &encode_entry(&key, ctid))?;
        }
        Ok(())
    }

    /// Fails if writing `new_rows` would give two live rows the same key, or a key is too
    /// long for the index. Called before anything is written, so a failing statement
    /// changes nothing.
    ///
    /// `replacing` holds the rows the statement removes before writing (an UPDATE's old
    /// versions), so a row may keep its own key.
    pub(crate) fn check_unique(
        &self,
        pool: &mut BufferPool,
        column_name: &str,
        new_rows: &[Row],
        replacing: &BTreeSet<Ctid>,
    ) -> Result<()> {
        let mut seen = BTreeSet::new();

        for key in new_rows.iter().filter_map(|row| self.key(row)) {
            // Any ctid makes an entry of the same length
            let len = encode_entry(&key, (0, 0)).len();
            if len > MAX_ENTRY {
                return Err(DatabaseError::ConstraintViolation(format!(
                    "a value in indexed column {column_name} takes {len} bytes in the index, \
                     more than the {MAX_ENTRY} it allows"
                ))
                .into());
            }

            let range = IndexRange {
                column: self.column,
                lower: Bound::Included(key.clone()),
                upper: Bound::Included(key.clone()),
            };
            let mut taken = false;
            let mut rows = self.scan(pool, &range)?;
            while let Some(ctid) = rows.next(pool)? {
                if !replacing.contains(&ctid) {
                    taken = true;
                    break;
                }
            }

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

/// The rows of one index range, read from the tree a row at a time.
#[derive(Debug)]
pub(crate) struct IndexCursor {
    cursor: Cursor,

    /// The range's bounds, as encoded keys. The cursor starts at the lower bound, but
    /// still has to skip a key the lower bound excludes.
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
}

impl IndexCursor {
    /// The next row in the range, or `None` past its end.
    pub(crate) fn next(&mut self, pool: &mut BufferPool) -> Result<Option<Ctid>> {
        while let Some(entry) = self.cursor.next(pool)? {
            let key = &entry[..entry.len() - CTID_LEN];
            if let Bound::Excluded(lower) = &self.lower
                && key == lower.as_slice()
            {
                continue;
            }

            let past_end = match &self.upper {
                Bound::Included(upper) => key > upper.as_slice(),
                Bound::Excluded(upper) => key >= upper.as_slice(),
                Bound::Unbounded => false,
            };
            if past_end {
                return Ok(None);
            }
            return Ok(Some(entry_ctid(&entry)));
        }
        Ok(None)
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
            IndexKey::Bool(b) => write!(f, "{b}"),
            IndexKey::Float(bits) => write!(f, "{}", Self::float_value(*bits)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed-seed generator, so a failure repeats.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    #[test]
    fn test_float_and_bool_keys_sort_like_their_values() {
        let floats = [
            f64::NEG_INFINITY,
            -1e300,
            -2.5,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            1.5,
            1e300,
            f64::INFINITY,
            f64::NAN,
        ];
        let bytes = |key: IndexKey| {
            let mut out = Vec::new();
            key.encode(&mut out);
            out
        };
        let encoded: Vec<Vec<u8>> = floats.iter().map(|&f| bytes(IndexKey::float(f))).collect();
        assert!(encoded.is_sorted_by(|a, b| a < b), "floats out of order");
        assert_eq!(IndexKey::float(-0.0), IndexKey::float(0.0));
        assert_eq!(IndexKey::float(-f64::NAN), IndexKey::float(f64::NAN));
        for f in floats.into_iter().filter(|f| !f.is_nan()) {
            assert_eq!(
                IndexKey::float_value(match IndexKey::float(f) {
                    IndexKey::Float(bits) => bits,
                    _ => unreachable!(),
                }),
                f
            );
        }

        assert!(bytes(IndexKey::Bool(false)) < bytes(IndexKey::Bool(true)));
    }

    #[test]
    fn test_entry_bytes_sort_like_key_then_ctid() {
        let mut rng = Rng(0x5eed);
        let ints = [i64::MIN, -300, -1, 0, 1, 255, 256, i64::MAX];
        let texts = ["", "\0", "\0\0", "a", "a\0", "a\0b", "ab", "abc", "b", "é"];
        let ctids = [
            (0, 0),
            (0, 1),
            (1, 0),
            (256, 3),
            (PageId::MAX - 1, ItemId::MAX),
        ];

        let mut entries: Vec<(IndexKey, Ctid)> = Vec::new();
        for &n in &ints {
            entries.extend(ctids.iter().map(|&ctid| (IndexKey::Int(n), ctid)));
        }
        for text in texts {
            entries.extend(
                ctids
                    .iter()
                    .map(|&ctid| (IndexKey::Text(text.to_string()), ctid)),
            );
        }
        for _ in 0..500 {
            let ctid = (rng.next() as PageId % 5, rng.next() as ItemId % 3);
            entries.push((IndexKey::Int(rng.next() as i64), ctid));
            let len = rng.next() % 4;
            let text: String = (0..len)
                .map(|_| ['\0', 'a', 'b'][rng.next() as usize % 3])
                .collect();
            entries.push((IndexKey::Text(text), ctid));
        }

        for a in &entries {
            for b in &entries {
                // An index holds one type, so only keys of the same type are compared
                if std::mem::discriminant(&a.0) != std::mem::discriminant(&b.0) {
                    continue;
                }
                let (ea, eb) = (encode_entry(&a.0, a.1), encode_entry(&b.0, b.1));
                assert_eq!(ea.cmp(&eb), a.cmp(b), "{a:?} vs {b:?}");
                assert_eq!(entry_ctid(&ea), a.1);
            }
        }
    }
}
