use std::fmt::Display;

use miette::{Result, miette};

use crate::{DatabaseError, db::null_bitmap::NullBitmap, sql::parser::literal_value::ScalarValue};

/// Trait for table-like structures.
///
/// Defines the common interface for tables and relations.
/// Currently only implemented by [`Relation`].
pub trait Table {
    /// Returns the table name.
    fn name(&self) -> &str;

    /// Returns the table's schema.
    fn schema(&self) -> &Schema;

    /// Inserts a row into the table (validates against schema).
    fn insert_row(&mut self, row: Row) -> Result<(), DatabaseError>;

    /// Gets rows matching a specific column value.
    fn get_rows(&self, column: &str, value: Value) -> Result<Vec<Row>, DatabaseError>;
}

/// SQL data types supported by Scuttle DB.
///
/// These types define the kind of data a column can hold and how
/// it's encoded/decoded in storage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PhysicalType {
    /// 64-bit signed integer.
    ///
    /// Stored as 8 bytes in little-endian format.
    Int64,

    /// Variable-length text with no size limit.
    ///
    /// Stored as 4-byte length prefix + UTF-8 bytes.
    Text,

    /// Variable-length text with maximum length.
    ///
    /// Stored as 4-byte length prefix + UTF-8 bytes.
    /// The usize parameter specifies the maximum number of bytes allowed.
    VarChar(usize),

    /// Boolean true/false value.
    Bool,

    /// 64-bit floating point number.
    Float64,
}

impl Display for PhysicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PhysicalType::Int64 => write!(f, "Integer"),
            PhysicalType::Text | PhysicalType::VarChar(_) => write!(f, "String"),
            PhysicalType::Bool => write!(f, "Boolean"),
            PhysicalType::Float64 => write!(f, "Float"),
        }
    }
}

/// A value that can be stored in a database column.
///
/// Values are strongly typed and correspond to [`DataType`] definitions.
/// Each variant can be compared, ordered, and checked for type compatibility.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    /// A 64-bit signed integer value.
    Integer(i64),

    /// A UTF-8 text string.
    Text(String),

    /// A boolean value (true/false).
    Boolean(bool),

    /// A 64-bit floating point number.
    Float(f64),

    /// Represents a NULL value (absence of data).
    ///
    /// Only allowed in nullable columns.
    Null,
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Text(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl From<ScalarValue> for Value {
    fn from(literal: ScalarValue) -> Self {
        match literal {
            ScalarValue::Int64(i) => Value::Integer(i),
            ScalarValue::Float64(f) => Value::Float(f),
            ScalarValue::Text(s) => Value::Text(s),
            ScalarValue::Bool(b) => Value::Boolean(b),
            ScalarValue::Null => Value::Null,
        }
    }
}

impl Value {
    /// Returns the data type of this value.
    pub fn data_type(&self) -> Option<PhysicalType> {
        match self {
            Value::Integer(_) => Some(PhysicalType::Int64),
            Value::Text(_) => Some(PhysicalType::Text),
            Value::Boolean(_) => Some(PhysicalType::Bool),
            Value::Float(_) => Some(PhysicalType::Float64),
            Value::Null => None,
        }
    }

    /// Checks if this value can be stored in a column of the given type.
    ///
    /// Performs type checking and, for VARCHAR, length validation.
    pub fn is_compatible_with(&self, data_type: &PhysicalType) -> Result<(), String> {
        match (self, data_type) {
            (Value::Text(s), PhysicalType::VarChar(max_len)) => {
                if s.len() <= *max_len {
                    Ok(())
                } else {
                    Err(format!(
                        "Text length {} exceeds VARCHAR({}) limit",
                        s.len(),
                        max_len
                    ))
                }
            }
            (Value::Integer(_), PhysicalType::Int64)
            | (Value::Boolean(_), PhysicalType::Bool)
            | (Value::Float(_), PhysicalType::Float64)
            | (Value::Null, _)
            | (Value::Text(_), PhysicalType::Text) => Ok(()),

            _ => Err(format!(
                "Type mismatch: {self:?} cannot be stored as {data_type:?}"
            )),
        }
    }
}

/// Definition of a single column in a table schema.
///
/// Specifies the column name, data type, and whether NULL values are allowed.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    /// The column name.
    pub name: String,

    /// The data type for values in this column.
    pub data_type: PhysicalType,

    /// Whether this column can contain NULL values.
    pub nullable: bool,
}

impl ColumnDefinition {
    /// Creates a new column definition.
    pub fn new(name: &str, data_type: PhysicalType, nullable: bool) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
            nullable,
        }
    }
}

/// A table schema defining the structure of rows.
///
/// A schema is an ordered list of column definitions. All rows in a table
/// must conform to the table's schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    /// The ordered list of column definitions.
    pub columns: Vec<ColumnDefinition>,
}

impl Schema {
    /// Creates a new schema from a vector of column definitions.
    pub fn new(columns: Vec<ColumnDefinition>) -> Self {
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
                (PhysicalType::Int64, Value::Integer(number)) => {
                    bytes.extend_from_slice(&number.to_le_bytes());
                }
                (PhysicalType::Float64, Value::Float(number)) => {
                    bytes.extend_from_slice(&number.to_le_bytes());
                }
                (PhysicalType::Text, Value::Text(text)) => {
                    let text_bytes = text.as_bytes();
                    let length = text_bytes.len() as u32;
                    bytes.extend_from_slice(&length.to_le_bytes());
                    bytes.extend_from_slice(text_bytes);
                }
                (PhysicalType::VarChar(max_length), Value::Text(text)) => {
                    let text_bytes = text.as_bytes();
                    let length = text_bytes.len() as u32;
                    assert!(
                        length as usize <= max_length,
                        "Text exceeds VARCHAR({max_length}) limit"
                    );
                    bytes.extend_from_slice(&length.to_le_bytes());
                    bytes.extend_from_slice(text_bytes);
                }
                (PhysicalType::Bool, Value::Boolean(b)) => {
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
                PhysicalType::Int64 => {
                    if offset + 8 > bytes.len() {
                        return Err(miette!("Not enough bytes for integer value"));
                    }
                    let mut num_bytes = [0u8; 8];
                    num_bytes.copy_from_slice(&bytes[offset..offset + 8]);
                    let value = i64::from_le_bytes(num_bytes);
                    values.push(Value::Integer(value));
                    offset += 8;
                }
                PhysicalType::Float64 => {
                    if offset + 8 > bytes.len() {
                        return Err(miette!("Not enough bytes for float value"));
                    }
                    let mut num_bytes = [0u8; 8];
                    num_bytes.copy_from_slice(&bytes[offset..offset + 8]);
                    let value = f64::from_le_bytes(num_bytes);
                    values.push(Value::Float(value));
                    offset += 8;
                }
                PhysicalType::Bool => {
                    if offset + 1 > bytes.len() {
                        return Err(miette!("Not enough bytes for boolean value"));
                    }
                    let raw_byte = bytes[offset];
                    let bool_val = raw_byte != 0;
                    values.push(Value::Boolean(bool_val));
                    offset += 1;
                }
                PhysicalType::Text | PhysicalType::VarChar(_) => {
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
            }
        }

        Ok(Row::new(values))
    }
}

/// A row of data containing values for each column.
///
/// Rows are ordered collections of values that correspond to a schema's columns.
/// The number and types of values must match the schema.
#[derive(Debug, Clone)]
pub struct Row {
    /// The ordered values in this row.
    pub values: Vec<Value>,
}

impl Row {
    /// Creates a new row from a vector of values.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Gets a reference to the value at the given column index.
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

/// A table (relation) with a name and schema.
///
/// Represents a table in the database with its structure definition.
/// Currently, the actual row data is stored separately in pages managed
/// by the buffer pool.
#[derive(Debug, Clone)]
pub struct Relation {
    /// The table name.
    name: String,

    /// The table's schema defining its columns.
    schema: Schema,
}

impl Relation {
    /// Creates a new relation (table) with the given name and schema.
    pub fn new(name: String, schema: Schema) -> Self {
        Self { name, schema }
    }
}

impl Table for Relation {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn insert_row(&mut self, row: Row) -> Result<(), DatabaseError> {
        if row.values.len() != self.schema.columns.len() {
            return Err(DatabaseError::TypeMismatch(
                "Row length doesn't match schema".to_string(),
            ));
        }

        for (i, value) in row.values.iter().enumerate() {
            let column = &self.schema.columns[i];

            match value {
                Value::Null => {
                    if !column.nullable {
                        return Err(DatabaseError::TypeMismatch(format!(
                            "Column {} cannot be null",
                            column.name
                        )));
                    }
                }
                _ => {
                    if let Err(msg) = value.is_compatible_with(&column.data_type) {
                        return Err(DatabaseError::TypeMismatch(msg));
                    }
                }
            }
        }

        Ok(())
    }

    fn get_rows(&self, _column: &str, _value: Value) -> Result<Vec<Row>, DatabaseError> {
        // Implementation for retrieving rows from the relation
        todo!()
    }
}
