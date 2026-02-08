/// A literal value in SQL
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Int64(i64),
    Float64(f64),
    Text(String),
    Bool(bool),
    Null,
}
