use miette::Diagnostic;

/// Errors that can occur during database operations.
#[derive(Debug, Diagnostic, thiserror::Error)]
pub enum DatabaseError {
    /// An I/O error occurred while reading or writing data.
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    /// Data serialization or deserialization failed.
    #[error("Serialization Error: {0}")]
    SerializationError(String),

    /// The requested table does not exist.
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// The requested column does not exist in the schema.
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// A type mismatch occurred (e.g., wrong value type for column).
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    /// The SQL query is invalid or malformed.
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}
