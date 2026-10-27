//! Temporary files that hold records a query can't keep in memory, such as the sorted
//! runs of an external sort.
//!
//! A record is a byte string. Records are written one after another, each after its
//! length, and read back in the same order. [`encode_values`] turns a row into a record
//! without a schema, because the rows a query spills may not match any table (a join's
//! rows, say). Each value carries a tag byte for its type.

use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use miette::{IntoDiagnostic, Result, miette};

use crate::Value;

/// Gives each spill file of this process its own name.
static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

const TAG_INT: u8 = 0;
const TAG_FLOAT: u8 = 1;
const TAG_TEXT: u8 = 2;
const TAG_BOOL: u8 = 3;
const TAG_TIMESTAMP: u8 = 4;
const TAG_NULL: u8 = 5;

/// A spill file being written. [`SpillWriter::finish`] closes it.
pub(crate) struct SpillWriter {
    file: BufWriter<File>,
    path: TempPath,
}

impl SpillWriter {
    /// Creates a new, empty spill file in `dir`, creating `dir` if needed.
    pub(crate) fn create(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).into_diagnostic()?;
        let n = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = TempPath(dir.join(format!("spill-{}-{n}", std::process::id())));
        let file = File::create(&path.0).into_diagnostic()?;

        Ok(Self {
            file: BufWriter::new(file),
            path,
        })
    }

    /// Appends one record.
    pub(crate) fn write(&mut self, record: &[u8]) -> Result<()> {
        self.file
            .write_all(&(record.len() as u32).to_le_bytes())
            .into_diagnostic()?;
        self.file.write_all(record).into_diagnostic()
    }

    /// Writes out what is buffered and closes the file. It isn't opened again until it is
    /// read, so a sort with many runs doesn't hold a file open for each.
    pub(crate) fn finish(self) -> Result<SpillFile> {
        let Self { file, path } = self;
        file.into_inner()
            .map_err(|error| miette!("Could not write spill file: {error}"))?;
        Ok(SpillFile { path })
    }
}

/// A finished spill file, closed. It is deleted when dropped, or once read.
#[derive(Debug)]
pub(crate) struct SpillFile {
    path: TempPath,
}

impl SpillFile {
    /// Opens the file to read its records from the start.
    pub(crate) fn open(self) -> Result<SpillReader> {
        let file = File::open(&self.path.0).into_diagnostic()?;
        Ok(SpillReader {
            file: BufReader::new(file),
            _path: self.path,
        })
    }
}

/// A spill file read back record by record. The file is deleted when this is dropped.
pub(crate) struct SpillReader {
    file: BufReader<File>,
    _path: TempPath,
}

impl SpillReader {
    /// The next record, or `None` at the end of the file.
    pub(crate) fn read(&mut self) -> Result<Option<Vec<u8>>> {
        let mut len = [0; 4];
        match self.file.read_exact(&mut len) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error).into_diagnostic(),
        }

        let mut record = vec![0; u32::from_le_bytes(len) as usize];
        self.file.read_exact(&mut record).into_diagnostic()?;
        Ok(Some(record))
    }
}

/// Deletes the file at this path when dropped, so a query that ends early or fails leaves
/// nothing behind.
#[derive(Debug)]
struct TempPath(PathBuf);

impl Drop for TempPath {
    fn drop(&mut self) {
        fs::remove_file(&self.0).ok();
    }
}

/// Appends `values` to `out` as a record that [`decode_values`] reads back.
pub(crate) fn encode_values(values: &[Value], out: &mut Vec<u8>) {
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        encode_value(value, out);
    }
}

/// Reads the values [`encode_values`] wrote from the start of `record`, leaving `record`
/// at whatever follows them.
pub(crate) fn decode_values(record: &mut &[u8]) -> Result<Vec<Value>> {
    let len = u32::from_le_bytes(read_bytes(record)?);
    (0..len).map(|_| decode_value(record)).collect()
}

fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int64(n) => {
            out.push(TAG_INT);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            out.push(TAG_FLOAT);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(text) => {
            out.push(TAG_TEXT);
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
        Value::Bool(b) => out.extend_from_slice(&[TAG_BOOL, u8::from(*b)]),
        Value::Timestamp(seconds) => {
            out.push(TAG_TIMESTAMP);
            out.extend_from_slice(&seconds.to_le_bytes());
        }
        Value::Null => out.push(TAG_NULL),
    }
}

fn decode_value(file: &mut impl Read) -> Result<Value> {
    let [tag] = read_bytes(file)?;
    Ok(match tag {
        TAG_INT => Value::Int64(i64::from_le_bytes(read_bytes(file)?)),
        TAG_FLOAT => Value::Float64(f64::from_le_bytes(read_bytes(file)?)),
        TAG_TEXT => {
            let len = u32::from_le_bytes(read_bytes(file)?) as usize;
            let mut text = vec![0; len];
            file.read_exact(&mut text).into_diagnostic()?;
            Value::Text(String::from_utf8(text).into_diagnostic()?)
        }
        TAG_BOOL => {
            let [b] = read_bytes(file)?;
            Value::Bool(b != 0)
        }
        TAG_TIMESTAMP => Value::Timestamp(i64::from_le_bytes(read_bytes(file)?)),
        TAG_NULL => Value::Null,
        _ => return Err(miette!("Corrupt spill file: unknown value tag {tag}")),
    })
}

fn read_bytes<const N: usize>(file: &mut impl Read) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    file.read_exact(&mut bytes).into_diagnostic()?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("scuttle-spill-{name}-{}", std::process::id()))
    }

    #[test]
    fn test_records_read_back_in_the_order_they_were_written() {
        let records: Vec<Vec<u8>> = vec![b"first".to_vec(), vec![], vec![0, 255, 7]];

        let mut writer = SpillWriter::create(&temp_dir("roundtrip")).unwrap();
        for record in &records {
            writer.write(record).unwrap();
        }
        let mut reader = writer.finish().unwrap().open().unwrap();

        let mut read = Vec::new();
        while let Some(record) = reader.read().unwrap() {
            read.push(record);
        }
        assert_eq!(read, records);
    }

    #[test]
    fn test_values_survive_encoding() {
        let rows = [
            vec![
                Value::Int64(-7),
                Value::Float64(2.5),
                Value::Text("héllo".to_string()),
                Value::Bool(true),
                Value::Timestamp(1_700_000_000),
                Value::Null,
            ],
            vec![],
            vec![Value::Text(String::new()), Value::Bool(false)],
        ];
        for row in rows {
            let mut record = Vec::new();
            encode_values(&row, &mut record);
            assert_eq!(decode_values(&mut record.as_slice()).unwrap(), row);
        }
    }

    #[test]
    fn test_the_file_is_deleted_once_read_or_dropped() {
        let dir = temp_dir("cleanup");
        let files = || fs::read_dir(&dir).unwrap().count();

        let mut writer = SpillWriter::create(&dir).unwrap();
        writer.write(b"x").unwrap();
        let file = writer.finish().unwrap();
        assert_eq!(files(), 1);
        drop(file.open().unwrap());
        assert_eq!(files(), 0);

        let file = SpillWriter::create(&dir).unwrap().finish().unwrap();
        assert_eq!(files(), 1);
        drop(file);
        assert_eq!(files(), 0);
    }
}
