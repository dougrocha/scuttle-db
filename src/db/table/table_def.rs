use miette::Result;

use crate::{DatabaseError, core::types::Value};

use super::{Table, row::Row, schema::Schema};

/// A table (relation) with a name and schema.
///
/// Represents a table in the database with its structure definition.
/// Currently, the actual row data is stored separately in pages managed
/// by the buffer pool.
#[derive(Debug, Clone)]
pub struct TableDef {
    /// The table name.
    pub(crate) name: String,

    /// The table's schema defining its columns.
    pub(crate) schema: Schema,
}

impl TableDef {
    /// Creates a new relation (table) with the given name and schema.
    pub fn new(name: String, schema: Schema) -> Self {
        Self { name, schema }
    }
}

impl Table for TableDef {
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
