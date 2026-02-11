use miette::{Result, miette};

use super::{column_def::ColumnDef, row::Row};
use crate::{Value, core::types::DataType, db::null_bitmap::NullBitmap};

/// A table schema defining the structure of rows.
///
/// A schema is an ordered list of column definitions. All rows in a table
/// must conform to the table's schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    /// The ordered list of column definitions.
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    /// Creates a new schema from a vector of column definitions.
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    /// Finds the index of a column by name.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    /// Encodes a row to bytes for storage.
    ///
    /// Internal method used by the storage layer to serialize rows into pages.
    /// Each value is encoded according to its type:
    /// - Integer/Float: 8 bytes (little-endian i64)
    /// - Text/VarChar: 4-byte length + UTF-8 bytes
    /// - Boolean: 1 Bit
    pub(crate) fn encode_row(&self, row: &Row) -> Vec<u8> {
        let mut bytes = vec![];

        let mut bitmap = NullBitmap::new(self.columns.len());
        for (i, value) in row.values.iter().enumerate() {
            if matches!(value, Value::Null) {
                bitmap.set_null(i);
            }
        }

        bytes.extend_from_slice(&bitmap.bytes);

        for (value, column) in row.values.iter().zip(self.columns.iter()) {
            if let Value::Null = value {
                continue;
            }

            match (column.data_type, value) {
                (DataType::Int64, Value::Int64(number)) => {
                    bytes.extend_from_slice(&number.to_le_bytes());
                }
                (DataType::Float64, Value::Float64(number)) => {
                    bytes.extend_from_slice(&number.to_le_bytes());
                }
                (DataType::Text, Value::Text(text)) => {
                    let text_bytes = text.as_bytes();
                    let length = text_bytes.len() as u32;
                    bytes.extend_from_slice(&length.to_le_bytes());
                    bytes.extend_from_slice(text_bytes);
                }
                (DataType::VarChar(max_length), Value::Text(text)) => {
                    let text_bytes = text.as_bytes();
                    let length = text_bytes.len() as u32;
                    assert!(
                        length as usize <= max_length,
                        "Text exceeds VARCHAR({max_length}) limit"
                    );
                    bytes.extend_from_slice(&length.to_le_bytes());
                    bytes.extend_from_slice(text_bytes);
                }
                (DataType::Bool, Value::Bool(b)) => {
                    bytes.push(if *b { 1 } else { 0 });
                }
                _ => panic!(
                    "Column type ({:?}) and value ({:?}) combination not implemented",
                    column.data_type, value
                ),
            }
        }

        bytes
    }

    /// Decodes a row from bytes read from storage.
    ///
    /// Internal method used by the storage layer to deserialize rows from pages.
    /// Decodes values according to the schema's column types.
    pub(crate) fn decode_row(&self, bytes: &[u8]) -> Result<Row> {
        let mut values = Vec::new();
        let mut offset = 0;

        let bitmap = NullBitmap::from_bytes(bytes, self.columns.len())?;
        offset += bitmap.bytes.len();

        for (idx, column) in self.columns.iter().enumerate() {
            if bitmap.is_null(idx) {
                values.push(Value::Null);
                continue;
            }

            match column.data_type {
                DataType::Int64 => {
                    if offset + 8 > bytes.len() {
                        return Err(miette!("Not enough bytes for integer value"));
                    }
                    let mut num_bytes = [0u8; 8];
                    num_bytes.copy_from_slice(&bytes[offset..offset + 8]);
                    let value = i64::from_le_bytes(num_bytes);
                    values.push(Value::Int64(value));
                    offset += 8;
                }
                DataType::Float64 => {
                    if offset + 8 > bytes.len() {
                        return Err(miette!("Not enough bytes for float value"));
                    }
                    let mut num_bytes = [0u8; 8];
                    num_bytes.copy_from_slice(&bytes[offset..offset + 8]);
                    let value = f64::from_le_bytes(num_bytes);
                    values.push(Value::Float64(value));
                    offset += 8;
                }
                DataType::Bool => {
                    if offset + 1 > bytes.len() {
                        return Err(miette!("Not enough bytes for boolean value"));
                    }
                    let raw_byte = bytes[offset];
                    let bool_val = raw_byte != 0;
                    values.push(Value::Bool(bool_val));
                    offset += 1;
                }
                DataType::Text | DataType::VarChar(_) => {
                    if offset + 4 > bytes.len() {
                        return Err(miette!("Not enough bytes for string length"));
                    }
                    let mut len_bytes = [0u8; 4];
                    len_bytes.copy_from_slice(&bytes[offset..offset + 4]);
                    let length = u32::from_le_bytes(len_bytes) as usize;
                    offset += 4;

                    if offset + length > bytes.len() {
                        return Err(miette!("Not enough bytes for string content"));
                    }
                    let text_bytes = &bytes[offset..offset + length];
                    match std::str::from_utf8(text_bytes) {
                        Ok(text) => {
                            values.push(Value::Text(text.to_owned()));
                            offset += length;
                        }
                        Err(_) => {
                            return Err(miette!("Invalid UTF-8 sequence"));
                        }
                    }
                }
                DataType::Timestamp => todo!(),
            }
        }

        Ok(Row::new(values))
    }
}
