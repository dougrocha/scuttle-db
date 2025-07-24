use std::fmt;

#[derive(Debug)]
pub enum DatabaseError {
    IoError(std::io::Error),
    EncodeError(bincode::error::EncodeError),
    DecodeError(bincode::error::DecodeError),
    SerializationError(String),
    TableNotFound(String),
    ColumnNotFound(String),
    TypeMismatch(String),
    InvalidQuery(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseError::IoError(e) => write!(f, "IO Error: {}", e),
            DatabaseError::SerializationError(e) => write!(f, "Serialization Error: {}", e),
            DatabaseError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            DatabaseError::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            DatabaseError::TypeMismatch(msg) => write!(f, "Type mismatch: {}", msg),
            DatabaseError::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            DatabaseError::EncodeError(encode_error) => write!(f, "Encode Error: {}", encode_error),
            DatabaseError::DecodeError(decode_error) => write!(f, "Decode Error: {}", decode_error),
        }
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<bincode::error::EncodeError> for DatabaseError {
    fn from(value: bincode::error::EncodeError) -> Self {
        Self::EncodeError(value)
    }
}

impl From<bincode::error::DecodeError> for DatabaseError {
    fn from(value: bincode::error::DecodeError) -> Self {
        Self::DecodeError(value)
    }
}
