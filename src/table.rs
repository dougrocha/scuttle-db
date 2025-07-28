use miette::{Result, miette};

use crate::DatabaseError;

pub trait Table {
    fn name(&self) -> &str;
    fn schema(&self) -> &Schema;
    fn insert_row(&mut self, row: Row) -> Result<(), DatabaseError>;
    fn get_rows(&self, column: &str, value: Value) -> Result<Vec<Row>, DatabaseError>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    Integer,
    Text,
    VarChar(usize),
    Boolean,
    Float,
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Float(f64),
    Null,
}

impl Value {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Integer(_) => Some(DataType::Integer),
            Value::Text(_) => Some(DataType::Text),
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Float(_) => Some(DataType::Float),
            Value::Null => None,
        }
    }

    pub fn is_compatible_with(&self, data_type: &DataType) -> Result<(), String> {
        match (self, data_type) {
            (Value::Integer(_), DataType::Integer) => Ok(()),
            (Value::Boolean(_), DataType::Boolean) => Ok(()),
            (Value::Float(_), DataType::Float) => Ok(()),

            // Handle both Text and VarChar for string values
            (Value::Text(_), DataType::Text) => Ok(()),
            (Value::Text(s), DataType::VarChar(max_len)) => {
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

            (Value::Null, _) => Ok(()), // Null handled separately by nullable check

            _ => Err(format!(
                "Type mismatch: {self:?} cannot be stored as {data_type:?}"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl ColumnDefinition {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
            nullable,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub columns: Vec<ColumnDefinition>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDefinition>) -> Self {
        Self { columns }
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == name)
    }

    pub(crate) fn encode_row(&self, row: Row) -> Vec<u8> {
        let mut bytes = vec![];

        for (value, column) in row.values.iter().zip(self.columns.iter()) {
            match (column.data_type, value) {
                (DataType::Integer, Value::Integer(number)) => {
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
                // (DataType::Boolean, Value::Boolean(_)) => todo!(),
                // (DataType::Float, Value::Float(_)) => todo!(),
                _ => panic!(
                    "Column type ({:?}) and value ({:?}) combination not implemented",
                    column.data_type, value
                ),
            }
        }

        bytes
    }

    pub(crate) fn decode_row(&self, bytes: &[u8]) -> Result<Row> {
        let mut values = Vec::new();
        let mut offset = 0;

        for column in &self.columns {
            if offset >= bytes.len() {
                return Err(miette!("Not enough data to decode column {}", column.name));
            }

            match column.data_type {
                DataType::Integer => {
                    if offset + 8 > bytes.len() {
                        return Err(miette!("Not enough bytes for integer value"));
                    }
                    let mut num_bytes = [0u8; 8];
                    num_bytes.copy_from_slice(&bytes[offset..offset + 8]);
                    let value = i64::from_le_bytes(num_bytes);
                    values.push(Value::Integer(value));
                    offset += 8;
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
                _ => todo!(),
            }
        }

        Ok(Row::new(values))
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub name: String,
    pub schema: Schema,
}

impl Relation {
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
