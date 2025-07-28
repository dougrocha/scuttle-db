use std::{error::Error, fmt};

#[derive(Debug)]
pub enum DatabaseError {
    IoError(std::io::Error),
    SerializationError(String),
    TableNotFound(String),
    ColumnNotFound(String),
    TypeMismatch(String),
    InvalidQuery(String),
}

impl Error for DatabaseError {}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseError::IoError(e) => write!(f, "IO Error: {}", e),
            DatabaseError::SerializationError(e) => write!(f, "Serialization Error: {}", e),
            DatabaseError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            DatabaseError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            DatabaseError::TypeMismatch(msg) => write!(f, "Type mismatch: {}", msg),
            DatabaseError::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
        }
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}
