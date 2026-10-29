use super::{Table, index::TableIndex, schema::Schema};

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

    /// The table's indexes, each in its own file. CREATE TABLE makes one per PRIMARY KEY
    /// or UNIQUE column, and CREATE INDEX makes the rest.
    pub(crate) indexes: Vec<TableIndex>,
}

impl TableDef {
    /// A table called `name` with `schema` and `indexes`. This doesn't create or read the
    /// indexes' files.
    pub(crate) fn new(name: String, schema: Schema, indexes: Vec<TableIndex>) -> Self {
        Self {
            name,
            schema,
            indexes,
        }
    }
}

impl Table for TableDef {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
