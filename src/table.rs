use bincode::{Decode, Encode};
use miette::Result;

use crate::{DatabaseError, page::PageId};

pub trait Table {
    fn name(&self) -> &str;
    fn schema(&self) -> &Schema;
    fn insert_row(&self) -> Result<(), DatabaseError>;
    fn get_rows(&self) -> Result<Vec<Row>, DatabaseError>;
}

#[derive(Debug)]
pub struct CatalogTable {
    /// The name of the catalog table
    pub name: String,
    /// The schema defining the structure of the table
    pub schema: Schema,
    /// The identifier for the page where table data is stored
    pub page_id: PageId,
}

impl Table for CatalogTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn insert_row(&self) -> Result<(), DatabaseError> {
        todo!()
    }

    fn get_rows(&self) -> Result<Vec<Row>, DatabaseError> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum DataType {
    Integer,
    Text,
    VarChar(usize),
    Boolean,
    Float,
}

#[derive(Debug, Clone, Encode, Decode)]
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
                "Type mismatch: {:?} cannot be stored as {:?}",
                self, data_type
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
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

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
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
}

#[derive(Debug, Clone, Encode, Decode)]
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct TableStruct {
    pub name: String,
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl TableStruct {
    pub fn new(name: String, schema: Schema) -> Self {
        Self {
            name,
            schema,
            rows: Vec::new(),
        }
    }

    pub fn insert_row(&mut self, row: Row) -> Result<(), DatabaseError> {
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

        self.rows.push(row);
        Ok(())
    }

    pub fn remove_row(&mut self, index: usize) -> Result<(), DatabaseError> {
        if index >= self.rows.len() {
            return Err(DatabaseError::InvalidQuery(format!(
                "Row index {} out of bounds",
                index
            )));
        }

        self.rows.remove(index);
        Ok(())
    }
}
