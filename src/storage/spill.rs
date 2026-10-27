//! Temporary files that hold rows a query can't keep in memory, such as the sorted runs of
//! an external sort.
//!
//! Rows are written one after another and read back in the same order. They are encoded
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

/// A spill file being written. Call [`SpillWriter::finish`] to read it back.
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

    /// Appends one record: a list of values.
    pub(crate) fn write(&mut self, values: &[Value]) -> Result<()> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for value in values {
            encode_value(value, &mut bytes);
        }
        self.file.write_all(&bytes).into_diagnostic()
    }

    /// Flushes what was written and opens the file for reading from the start.
    pub(crate) fn finish(self) -> Result<SpillReader> {
        let Self { file, path } = self;
        file.into_inner()
            .map_err(|error| miette!("Could not write spill file: {error}"))?;
        let file = File::open(&path.0).into_diagnostic()?;

        Ok(SpillReader {
            file: BufReader::new(file),
            _path: path,
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
    pub(crate) fn read(&mut self) -> Result<Option<Vec<Value>>> {
        let mut len = [0; 4];
        match self.file.read_exact(&mut len) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error).into_diagnostic(),
        }

        (0..u32::from_le_bytes(len))
            .map(|_| decode_value(&mut self.file))
            .collect::<Result<_>>()
            .map(Some)
    }
}

/// Deletes the file at this path when dropped, so a query that ends early or fails leaves
/// nothing behind.
struct TempPath(PathBuf);

impl Drop for TempPath {
    fn drop(&mut self) {
        fs::remove_file(&self.0).ok();
    }
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
        let records = vec![
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

        let mut writer = SpillWriter::create(&temp_dir("roundtrip")).unwrap();
        for record in &records {
            writer.write(record).unwrap();
        }
        let mut reader = writer.finish().unwrap();

        let mut read = Vec::new();
        while let Some(record) = reader.read().unwrap() {
            read.push(record);
        }
        assert_eq!(read, records);
    }

    #[test]
    fn test_the_file_is_deleted_once_read() {
        let dir = temp_dir("cleanup");
        let mut writer = SpillWriter::create(&dir).unwrap();
        writer.write(&[Value::Int64(1)]).unwrap();
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);

        let reader = writer.finish().unwrap();
        drop(reader);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
    }
}
