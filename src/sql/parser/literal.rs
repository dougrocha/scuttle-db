/// A literal value in SQL
#[derive(Debug, Clone, PartialEq)]
pub enum Literal<'src> {
    Int64(i64),
    Float64(f64),
    Text(&'src str),
    Bool(bool),
    Null,
}
