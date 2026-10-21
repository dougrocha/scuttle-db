use miette::Result;

use crate::{DatabaseError, core::types::Value};

use super::{Table, index::TableIndex, row::Row, schema::Schema};

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

    /// One index per PRIMARY KEY or UNIQUE column, kept in memory.
    pub(crate) indexes: Vec<TableIndex>,
}

impl TableDef {
    /// Creates a new relation (table) with the given name and schema. Its indexes start
    /// empty, so a table that already has rows must have them filled in.
    pub fn new(name: String, schema: Schema) -> Self {
        let indexes = TableIndex::for_schema(&schema);
        Self {
            name,
            schema,
            indexes,
        }
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
                    if !column.is_nullable() {
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
