use std::{
    cmp::Ordering,
    collections::{BTreeSet, VecDeque},
    ops::Bound,
};

use miette::Result;

use crate::{
    ColumnDef, DataType, DatabaseError, Value,
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
/// bits that can be. The keys in one index all have the same variant, because the column
/// has one type, except for `Null`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum IndexKey {
    Int(i64),
    Text(String),
    Timestamp(i64),
    Bool(bool),

    /// A float as bits that sort in number order, see [`IndexKey::float`].
    Float(u64),

    /// A NULL, which sorts after every value, as `ORDER BY` puts it and as in Postgres.
    /// It is last here so the derived order agrees.
    Null,
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
    /// - A value starts with `00` and a NULL is just `01`, so NULL sorts after every value.
    /// - A number flips its sign bit and is written big-endian, so negative numbers sort
    ///   first and comparing bytes is comparing numbers.
    /// - Text ends with `00 00`, and a `00` inside it is written `00 FF`. That way text
    ///   that is a prefix of other text ("ab" before "abc") still sorts first.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.push(u8::from(*self == IndexKey::Null));
        match self {
            IndexKey::Null => {}
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

/// Whether an encoded key is NULL's.
fn is_null(key: &[u8]) -> bool {
    key.first() == Some(&1)
}

/// The ctid at the end of an entry made by [`encode_entry`].
pub(crate) fn entry_ctid(entry: &[u8]) -> Ctid {
    let ctid = &entry[entry.len() - CTID_LEN..];
    (
        PageId::from_be_bytes(ctid[..4].try_into().unwrap()),
        ItemId::from_be_bytes(ctid[4..].try_into().unwrap()),
    )
}

/// An index over one column, stored as a B+tree in its own file, `<name>.index`.
///
/// Each entry is the column's key followed by the row's ctid (see [`encode_entry`]), so
/// rows sharing a key are separate entries and a delete finds exactly its own.
///
/// NULLs are indexed, after every value, so `ORDER BY` can read a nullable column through
/// its index. A unique index lets any number of rows be NULL, as SQL does.
#[derive(Debug, Clone)]
pub(crate) struct TableIndex {
    pub(crate) name: String,
    pub(crate) column: usize,

    /// Whether two rows may share a key.
    pub(crate) unique: bool,

    /// The column constraint the index enforces, if CREATE TABLE made it for one. DROP
    /// INDEX refuses such an index. It goes away when its table is dropped.
    pub(crate) constraint: Option<IndexConstraint>,

    data_type: DataType,
    tree: BTree,
}

/// A column constraint that an index enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexConstraint {
    PrimaryKey,
    Unique,
}

impl IndexConstraint {
    /// The constraint as SQL spells it, which is also how the catalog stores it.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PrimaryKey => "PRIMARY KEY",
            Self::Unique => "UNIQUE",
        }
    }

    pub(crate) fn parse(text: &str) -> Option<Self> {
        match text {
            "PRIMARY KEY" => Some(Self::PrimaryKey),
            "UNIQUE" => Some(Self::Unique),
            _ => None,
        }
    }

    /// The constraint `column` declares, if any. If it declares both, this returns
    /// PRIMARY KEY.
    pub(crate) fn of(column: &ColumnDef) -> Option<Self> {
        let has = |wanted: &ColumnConstraint| column.constraints.contains(wanted);
        if has(&ColumnConstraint::PrimaryKey) {
            Some(Self::PrimaryKey)
        } else if has(&ColumnConstraint::Unique) {
            Some(Self::Unique)
        } else {
            None
        }
    }
}

impl TableIndex {
    /// The index called `name` on `schema`'s column number `column`. This doesn't touch
    /// the index's file. See [`TableIndex::exists`] and [`TableIndex::create`].
    pub(crate) fn new(
        name: String,
        schema: &Schema,
        column: usize,
        unique: bool,
        constraint: Option<IndexConstraint>,
    ) -> Self {
        let tree = BTree::open(&format!("{name}.index"));
        Self {
            name,
            column,
            unique,
            constraint,
            data_type: schema.columns[column].data_type,
            tree,
        }
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

    /// The entry this index stores for `row`.
    pub(crate) fn entry(&self, row: &Row, ctid: Ctid) -> Vec<u8> {
        encode_entry(&self.key(row), ctid)
    }

    /// Creates the index's file holding `entries`, building the tree from the bottom up
    /// (see [`BTree::build`]). The entries come sorted, from an external sort of
    /// [`TableIndex::entry`] for each row.
    ///
    /// A unique index fails if two entries have the same key, other than NULL. Sorted,
    /// they are next to each other.
    pub(crate) fn build(&self, pool: &mut BufferPool, mut entries: Sorted<Vec<u8>>) -> Result<()> {
        let mut last: Option<Vec<u8>> = None;
        let entries = std::iter::from_fn(|| {
            let entry = match entries.next().transpose()? {
                Ok(entry) => entry,
                Err(error) => return Some(Err(error)),
            };
            if self.unique {
                let key = &entry[..entry.len() - CTID_LEN];
                if last.as_deref() == Some(key) && !is_null(key) {
                    return Some(Err(DatabaseError::ConstraintViolation(format!(
                        "two rows have the same key, so unique index {} can't be made",
                        self.name
                    ))
                    .into()));
                }
                last = Some(key.to_vec());
            }
            Some(Ok(entry))
        });
        BTree::build(pool, self.tree.name(), entries).map(|_| ())
    }

    /// Rows in the index, which is every row of the table.
    pub(crate) fn len(&self, pool: &mut BufferPool) -> Result<u64> {
        self.tree.len(pool)
    }

    fn key(&self, row: &Row) -> IndexKey {
        match &row.values[self.column] {
            Value::Null => IndexKey::Null,
            value => self
                .key_for(value)
                .expect("a row's values have its columns' types"),
        }
    }

    /// The key a WHERE clause comparing the column with `value` looks for, or `None` if
    /// the index can't find it: NULL, which no comparison matches, or a value of another
    /// type.
    pub(crate) fn key_for(&self, value: &Value) -> Option<IndexKey> {
        IndexKey::for_column(&self.data_type, value)
    }

    /// The rows whose key is within `range`, in key order, or largest key first if the
    /// range is `descending`. They are read as they are asked for.
    pub(crate) fn scan(&self, range: &IndexRange) -> IndexCursor {
        let encode = |bound: &Bound<IndexKey>| {
            bound.as_ref().map(|key| {
                let mut bytes = Vec::new();
                key.encode(&mut bytes);
                bytes
            })
        };
        let mut ranges: VecDeque<_> = range
            .ranges
            .iter()
            .map(|range| (encode(&range.lower), encode(&range.upper)))
            .collect();
        if range.descending {
            ranges.make_contiguous().reverse();
        }
        IndexCursor {
            tree: self.tree.clone(),
            ranges,
            current: None,
            descending: range.descending,
        }
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
        let mut cursor = self.scan(range);
        let mut count = 0;
        while count < limit && cursor.next(pool)?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    pub(crate) fn insert(&self, pool: &mut BufferPool, row: &Row, ctid: Ctid) -> Result<()> {
        self.tree.insert(pool, &self.entry(row, ctid))
    }

    pub(crate) fn remove(&self, pool: &mut BufferPool, row: &Row, ctid: Ctid) -> Result<()> {
        self.tree.remove(pool, &self.entry(row, ctid)).map(|_| ())
    }

    /// Fails if a key of `new_rows` is too long for the index. A unique index also fails if
    /// writing them would give two live rows the same key. Statements call this before
    /// writing anything, so a failing statement changes nothing.
    ///
    /// `replacing` holds the rows the statement removes before writing (an UPDATE's old
    /// versions), so a row may keep its own key.
    pub(crate) fn check_rows(
        &self,
        pool: &mut BufferPool,
        column_name: &str,
        new_rows: &[Row],
        replacing: &BTreeSet<Ctid>,
    ) -> Result<()> {
        let mut seen = BTreeSet::new();

        for key in new_rows.iter().map(|row| self.key(row)) {
            // Any ctid makes an entry of the same length
            let len = encode_entry(&key, (0, 0)).len();
            if len > MAX_ENTRY {
                return Err(DatabaseError::ConstraintViolation(format!(
                    "a value in indexed column {column_name} takes {len} bytes in the index, \
                     more than the {MAX_ENTRY} it allows"
                ))
                .into());
            }

            if !self.unique || key == IndexKey::Null {
                continue;
            }
            let range = IndexRange {
                column: self.column,
                ranges: vec![KeyRange::point(key.clone())],
                descending: false,
            };
            let mut taken = false;
            let mut rows = self.scan(&range);
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

/// A range bound as the bytes of its key.
type EncodedBound = Bound<Vec<u8>>;

/// The rows of one index's ranges, read from the tree a row at a time.
#[derive(Debug)]
pub(crate) struct IndexCursor {
    tree: BTree,

    /// The ranges not yet finished, in the order they are read, as encoded keys. The
    /// first one is being read.
    ranges: VecDeque<(EncodedBound, EncodedBound)>,

    /// Where the first range is being read from, once it has been opened.
    current: Option<Cursor>,
    descending: bool,
}

impl IndexCursor {
    /// The next row in the ranges, or `None` past the end of the last one.
    pub(crate) fn next(&mut self, pool: &mut BufferPool) -> Result<Option<Ctid>> {
        while let Some((lower, upper)) = self.ranges.front() {
            let cursor = match &mut self.current {
                Some(cursor) => cursor,
                None => self
                    .current
                    .insert(open(&self.tree, pool, lower, upper, self.descending)?),
            };
            let ctid = if self.descending {
                next_down(cursor, pool, lower)?
            } else {
                next_up(cursor, pool, lower, upper)?
            };
            if ctid.is_some() {
                return Ok(ctid);
            }
            self.ranges.pop_front();
            self.current = None;
        }
        Ok(None)
    }
}

/// A cursor placed at the start of the range between `lower` and `upper`: before its
/// first entry going up, or after its last going down.
fn open(
    tree: &BTree,
    pool: &mut BufferPool,
    lower: &EncodedBound,
    upper: &EncodedBound,
    descending: bool,
) -> Result<Cursor> {
    if descending {
        // Starts before the first entry past the upper bound. An entry is its key's
        // bytes and then a ctid, so every entry of `key` sorts after `key` itself and
        // before `key` followed by more 0xFF bytes than a ctid has. No key's bytes
        // start with another key's bytes, so no other key sorts in between.
        let start = match upper {
            Bound::Included(key) => {
                let mut past = key.clone();
                past.extend_from_slice(&[0xFF; CTID_LEN + 1]);
                Some(past)
            }
            Bound::Excluded(key) => Some(key.clone()),
            Bound::Unbounded => None,
        };
        tree.seek_back(pool, start.as_deref())
    } else {
        // Every entry of a key starts with the key's bytes, so they all sort at or after it
        let start = match lower {
            Bound::Included(key) | Bound::Excluded(key) => key.as_slice(),
            Bound::Unbounded => &[],
        };
        tree.seek(pool, start)
    }
}

/// The next row going up, or `None` past `upper`. The cursor starts at the lower bound,
/// but still has to skip a key the lower bound excludes.
fn next_up(
    cursor: &mut Cursor,
    pool: &mut BufferPool,
    lower: &EncodedBound,
    upper: &EncodedBound,
) -> Result<Option<Ctid>> {
    while let Some(entry) = cursor.next(pool)? {
        let key = &entry[..entry.len() - CTID_LEN];
        if let Bound::Excluded(lower) = lower
            && key == lower.as_slice()
        {
            continue;
        }

        let past_end = match upper {
            Bound::Included(upper) => key > upper.as_slice(),
            Bound::Excluded(upper) => key >= upper.as_slice(),
            Bound::Unbounded => false,
        };
        return Ok((!past_end).then(|| entry_ctid(&entry)));
    }
    Ok(None)
}

/// The next row going down, or `None` past `lower`. The cursor starts below the upper
/// bound, past any key it excludes.
fn next_down(
    cursor: &mut Cursor,
    pool: &mut BufferPool,
    lower: &EncodedBound,
) -> Result<Option<Ctid>> {
    let Some(entry) = cursor.prev(pool)? else {
        return Ok(None);
    };
    let key = &entry[..entry.len() - CTID_LEN];
    let past_end = match lower {
        Bound::Included(lower) => key < lower.as_slice(),
        Bound::Excluded(lower) => key <= lower.as_slice(),
        Bound::Unbounded => false,
    };
    Ok((!past_end).then(|| entry_ctid(&entry)))
}

/// The part of one index a scan reads: keys of `column` in one or more ranges.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexRange {
    pub(crate) column: usize,

    /// Sorted and not overlapping, so reading them in turn returns rows in key order,
    /// each once. `id IN (1, 5)` is two ranges of one key each.
    pub(crate) ranges: Vec<KeyRange>,

    /// Read the ranges largest key first, for an `ORDER BY ... DESC`.
    pub(crate) descending: bool,
}

impl IndexRange {
    /// Every key of `column`, NULLs included.
    pub(crate) fn all(column: usize) -> Self {
        Self {
            column,
            ranges: vec![KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            }],
            descending: false,
        }
    }
}

/// The keys between two bounds.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KeyRange {
    pub(crate) lower: Bound<IndexKey>,
    pub(crate) upper: Bound<IndexKey>,
}

impl KeyRange {
    /// The keys equal to `key`.
    pub(crate) fn point(key: IndexKey) -> Self {
        Self {
            lower: Bound::Included(key.clone()),
            upper: Bound::Included(key),
        }
    }

    /// The keys in both `self` and `other`, or `None` if there are none.
    pub(crate) fn intersect(&self, other: &KeyRange) -> Option<KeyRange> {
        let lower = tighter(&self.lower, &other.lower, Ordering::Greater);
        let upper = tighter(&self.upper, &other.upper, Ordering::Less);
        let empty = match (&lower, &upper) {
            (Bound::Included(lo), Bound::Included(hi)) => lo > hi,
            (
                Bound::Included(lo) | Bound::Excluded(lo),
                Bound::Included(hi) | Bound::Excluded(hi),
            ) => lo >= hi,
            _ => false,
        };
        (!empty).then_some(KeyRange { lower, upper })
    }
}

/// Whichever of two bounds lets fewer keys through. `keep` is how the tighter key
/// compares with the other: `Greater` for lower bounds, `Less` for upper bounds.
fn tighter(a: &Bound<IndexKey>, b: &Bound<IndexKey>, keep: Ordering) -> Bound<IndexKey> {
    match (a, b) {
        (Bound::Unbounded, other) | (other, Bound::Unbounded) => other.clone(),
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y))
            if x != y =>
        {
            if y.cmp(x) == keep {
                b.clone()
            } else {
                a.clone()
            }
        }
        // Same key: excluding it is tighter than including it
        (Bound::Excluded(_), _) => a.clone(),
        _ => b.clone(),
    }
}

impl std::fmt::Display for IndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexKey::Int(n) => write!(f, "{n}"),
            IndexKey::Text(s) => write!(f, "'{s}'"),
            IndexKey::Timestamp(t) => write!(f, "TIMESTAMP '{}'", Value::Timestamp(*t)),
            IndexKey::Bool(b) => write!(f, "{b}"),
            IndexKey::Float(bits) => write!(f, "{}", Self::float_value(*bits)),
            IndexKey::Null => write!(f, "NULL"),
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
    fn test_a_backward_scan_returns_the_forward_scan_reversed_for_every_key_type() {
        let text = |s: &str| Value::Text(s.to_string());
        let columns: [(DataType, Vec<Value>); 5] = [
            (
                DataType::Int64,
                [i64::MIN, -5, -1, 0, 1, 7, i64::MAX]
                    .map(Value::Int64)
                    .to_vec(),
            ),
            (
                // Prefixes of each other, NUL bytes, and the empty string
                DataType::Text,
                [
                    "", "\0", "\0\0", "\0a", "a", "a\0", "a\0b", "ab", "abc", "b", "\u{ff}",
                ]
                .map(text)
                .to_vec(),
            ),
            (
                DataType::Float64,
                [
                    f64::NEG_INFINITY,
                    -2.5,
                    -0.0,
                    1e-300,
                    1.5,
                    f64::INFINITY,
                    f64::NAN,
                ]
                .map(Value::Float64)
                .to_vec(),
            ),
            (DataType::Bool, vec![Value::Bool(false), Value::Bool(true)]),
            (
                DataType::Timestamp,
                [-86_400, 0, 1_767_225_600].map(Value::Timestamp).to_vec(),
            ),
        ];

        for (data_type, values) in columns {
            let schema = Schema::new(vec![ColumnDef::new("c", data_type)]);
            let index = TableIndex::new("t_c".to_string(), &schema, 0, false, None);
            let mut pool = BufferPool::in_memory();
            index.create(&mut pool).unwrap();
            // Each value three times, so every key has entries on both sides of a ctid
            let mut item = 0;
            for _ in 0..3 {
                for value in &values {
                    let row = Row::new(vec![value.clone()]);
                    index
                        .insert(&mut pool, &row, (item / 7, (item % 7) as u16))
                        .unwrap();
                    item += 1;
                }
            }

            let read = |pool: &mut BufferPool, range: &IndexRange| {
                let mut cursor = index.scan(range);
                std::iter::from_fn(|| cursor.next(pool).unwrap()).collect::<Vec<_>>()
            };
            let keys: Vec<IndexKey> = values.iter().filter_map(|v| index.key_for(v)).collect();
            let mut bounds = vec![Bound::Unbounded];
            for key in &keys {
                bounds.push(Bound::Included(key.clone()));
                bounds.push(Bound::Excluded(key.clone()));
            }
            for lower in &bounds {
                for upper in &bounds {
                    let mut range = IndexRange {
                        column: 0,
                        ranges: vec![KeyRange {
                            lower: lower.clone(),
                            upper: upper.clone(),
                        }],
                        descending: false,
                    };
                    let mut forward = read(&mut pool, &range);
                    range.descending = true;
                    forward.reverse();
                    assert_eq!(
                        read(&mut pool, &range),
                        forward,
                        "{data_type:?} from {lower:?} to {upper:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_null_sorts_after_the_largest_value_of_every_type() {
        let bytes = |key: &IndexKey| {
            let mut out = Vec::new();
            key.encode(&mut out);
            out
        };
        let null = bytes(&IndexKey::Null);
        for largest in [
            IndexKey::Int(i64::MAX),
            IndexKey::Timestamp(i64::MAX),
            IndexKey::Bool(true),
            IndexKey::float(f64::NAN),
            IndexKey::Text("\u{10FFFF}".repeat(4)),
            IndexKey::Text("\u{FF}\0".into()),
        ] {
            assert!(bytes(&largest) < null, "{largest}");
            assert!(largest < IndexKey::Null, "{largest}");
            // Neither starts with the other, which backward scans rely on
            assert!(!null.starts_with(&bytes(&largest)) && !bytes(&largest).starts_with(&null));
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
        // SQL compares floats in the same order, or a filter that uses the index would
        // find different rows than one that doesn't
        let more = [-0.0, -f64::NAN];
        for a in floats.iter().chain(&more) {
            for b in floats.iter().chain(&more) {
                assert_eq!(
                    bytes(IndexKey::float(*a)).cmp(&bytes(IndexKey::float(*b))),
                    crate::sql::evaluator::compare_floats(*a, *b),
                    "{a} vs {b}"
                );
            }
        }
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
