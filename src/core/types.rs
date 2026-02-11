/// SQL data types supported by Scuttle DB.
///
/// These types define the kind of data a column can hold and how
/// it's encoded/decoded in storage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    /// 64-bit signed integer.
    ///
    /// Stored as 8 bytes in little-endian format.
    Int64,

    /// Variable-length text with no size limit.
    ///
    /// Stored as 4-byte length prefix + UTF-8 bytes.
    Text,

    /// Variable-length text with maximum length.
    ///
    /// Stored as 4-byte length prefix + UTF-8 bytes.
    /// The usize parameter specifies the maximum number of bytes allowed.
    VarChar(usize),

    /// Boolean true/false value.
    Bool,

    /// 64-bit floating point number.
    Float64,

    Timestamp,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int64 => write!(f, "Integer"),
            DataType::Text | DataType::VarChar(_) => write!(f, "String"),
            DataType::Bool => write!(f, "Boolean"),
            DataType::Float64 => write!(f, "Float"),
            DataType::Timestamp => write!(f, "Timestamp"),
        }
    }
}

impl DataType {
    /// If two types are able to coerced
    pub fn can_coerce(from: DataType, to: DataType) -> bool {
        if from == to {
            return true;
        }

        matches!(
            (from, to),
            // Int64 can be coerced to Float64
            (DataType::Int64, DataType::Float64)
            // Text and VarChar are interchangeable for comparison purposes
            | (DataType::Text, DataType::VarChar(_))
            | (DataType::VarChar(_), DataType::Text)
            | (DataType::VarChar(_), DataType::VarChar(_))
        )
    }
}

/// A value that can be stored in a database column.
///
/// Values are strongly typed and correspond to [`DataType`] definitions.
/// Each variant can be compared, ordered, and checked for type compatibility.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    /// A 64-bit signed integer value.
    Int64(i64),

    /// A 64-bit floating point number.
    Float64(f64),

    /// A UTF-8 text string.
    Text(String),

    /// A boolean value (true/false).
    Bool(bool),

    /// Represents a NULL value (absence of data).
    ///
    /// Only allowed in nullable columns.
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int64(i) => write!(f, "{}", i),
            Value::Text(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Float64(fl) => write!(f, "{}", fl),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl Value {
    /// Checks if this value can be stored in a column of the given type.
    ///
    /// Performs type checking and, for VARCHAR, length validation.
    pub fn is_compatible_with(&self, data_type: &DataType) -> Result<(), String> {
        match (self, data_type) {
            (Value::Text(s), DataType::VarChar(max_len)) => {
                if s.len() <= *max_len {
                    Ok(())
                } else {
                    Err(format!(
                        "Text length {} exceeds VARCHAR({}) limit",
                        s.len(),
                        max_len
                    ))
                }
            }
            (Value::Int64(_), DataType::Int64)
            | (Value::Bool(_), DataType::Bool)
            | (Value::Float64(_), DataType::Float64)
            | (Value::Null, _)
            | (Value::Text(_), DataType::Text) => Ok(()),

            _ => Err(format!(
                "Type mismatch: {self:?} cannot be stored as {data_type:?}"
            )),
        }
    }
}
