use crate::core::types::DataType;

/// Definition of a single column in a table schema.
///
/// Specifies the column name, data type, and whether NULL values are allowed.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// The column name.
    pub name: String,

    /// The data type for values in this column.
    pub data_type: DataType,

    /// Whether this column can contain NULL values.
    pub nullable: bool,
}

impl ColumnDef {
    /// Creates a new column definition.
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
            nullable,
        }
    }
}
