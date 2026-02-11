use std::fmt;

use crate::sql::parser::{Value, operators::Operator};

pub use is_predicate::IsPredicate;

pub mod is_predicate;

/// An expression in a WHERE clause.
///
/// Expressions form a tree structure representing the filtering logic.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    BinaryOp {
        /// Left operand
        left: Box<Expression>,

        /// Operator
        op: Operator,

        /// Right operand
        right: Box<Expression>,
    },

    /// Column reference (e.g., `age`, `name`)
    Identifier(String),

    /// Literal value (e.g., `25`, `'Alice'`)
    Literal(Value),

    Is {
        expr: Box<Expression>,
        predicate: IsPredicate,
        is_negated: bool,
    },
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::BinaryOp { left, op, right } => {
                write!(f, "({left} {op:?} {right})")
            }
            Expression::Identifier(name) => write!(f, "{name}"),
            Expression::Literal(value) => match value {
                Value::Float64(num) => write!(f, "{num}"),
                Value::Int64(num) => write!(f, "{num}"),
                Value::Text(s) => write!(f, "\"{s}\""),
                Value::Bool(bool) => {
                    write!(f, "{}", bool.to_string().to_uppercase())
                }
                Value::Null => write!(f, "NULL"),
            },
            Expression::Is {
                expr,
                predicate,
                is_negated,
            } => write!(
                f,
                "{expr} {} {predicate}",
                if *is_negated { "IS NOT" } else { "IS" }
            ),
        }
    }
}

impl Expression {
    pub fn to_column_name(&self) -> &str {
        match self {
            Expression::Identifier(name) => name,
            _ => "?column?",
        }
    }
}
