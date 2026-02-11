use crate::core::types::Value;

/// A row of data containing values for each column.
///
/// Rows are ordered collections of values that correspond to a schema's columns.
/// The number and types of values must match the schema.
#[derive(Debug, Clone)]
pub struct Row {
    /// The ordered values in this row.
    pub values: Vec<Value>,
}

impl Row {
    /// Creates a new row from a vector of values.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Gets a reference to the value at the given column index.
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}
