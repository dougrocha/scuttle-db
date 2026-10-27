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

    /// One index per PRIMARY KEY or UNIQUE column, each stored in its own file.
    pub(crate) indexes: Vec<TableIndex>,
}

impl TableDef {
    /// Creates a new relation (table) with the given name and schema. Its indexes' files
    /// are not created or read here.
    pub fn new(name: String, schema: Schema) -> Self {
        let indexes = TableIndex::for_schema(&name, &schema);
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
}
