pub mod column_def;
pub(crate) mod cursor;
pub(crate) mod index;
pub mod row;
pub mod schema;
pub mod table_def;

/// Trait for table-like structures.
///
/// Defines the common interface for tables and relations.
/// Implemented by [`table_def::TableDef`] and the system catalog.
pub trait Table {
    /// Returns the table name.
    fn name(&self) -> &str;

    /// Returns the table's schema.
    fn schema(&self) -> &schema::Schema;
}
