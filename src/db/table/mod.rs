use miette::Result;

use crate::{DatabaseError, core::types::Value};

pub mod column_def;
pub mod row;
pub mod schema;
pub mod table_def;

/// Trait for table-like structures.
///
/// Defines the common interface for tables and relations.
/// Currently only implemented by [`Relation`].
pub trait Table {
    /// Returns the table name.
    fn name(&self) -> &str;

    /// Returns the table's schema.
    fn schema(&self) -> &schema::Schema;

    /// Inserts a row into the table (validates against schema).
    fn insert_row(&mut self, row: row::Row) -> Result<(), DatabaseError>;

    /// Gets rows matching a specific column value.
    fn get_rows(&self, column: &str, value: Value) -> Result<Vec<row::Row>, DatabaseError>;
}
