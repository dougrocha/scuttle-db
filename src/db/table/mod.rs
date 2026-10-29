pub mod column_def;
pub(crate) mod cursor;
pub(crate) mod index;
pub mod row;
pub mod schema;
pub mod table_def;

/// Trait for table-like structures.
pub trait Table {
    /// Returns the table's schema.
    fn schema(&self) -> &schema::Schema;
}
