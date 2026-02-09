use std::{borrow::Cow, fmt};

use crate::sql::parser::{Literal, operators::Operator};

pub use is_predicate::IsPredicate;

pub mod is_predicate;

/// An expression in a WHERE clause.
///
/// Expressions form a tree structure representing the filtering logic.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression<'src> {
    BinaryOp {
        /// Left operand
        left: Box<Expression<'src>>,

        /// Operator
        op: Operator,

        /// Right operand
        right: Box<Expression<'src>>,
    },

    /// Column reference (e.g., `age`, `name`)
    Identifier(Cow<'src, str>),

    /// Literal value (e.g., `25`, `'Alice'`)
    Literal(Literal<'src>),

    Is {
        expr: Box<Expression<'src>>,
        predicate: IsPredicate,
        is_negated: bool,
    },
}

impl fmt::Display for Expression<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::BinaryOp { left, op, right } => {
                write!(f, "({left} {op:?} {right})")
            }
            Expression::Identifier(name) => write!(f, "{name}"),
            Expression::Literal(value) => match value {
                Literal::Float64(num) => write!(f, "{num}"),
                Literal::Int64(num) => write!(f, "{num}"),
                Literal::Text(s) => write!(f, "\"{s}\""),
                Literal::Bool(bool) => {
                    write!(f, "{}", bool.to_string().to_uppercase())
                }
                Literal::Null => write!(f, "NULL"),
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

impl Expression<'_> {
    pub fn to_column_name(&self) -> &str {
        match self {
            Expression::Identifier(name) => name,
            _ => "?column?",
        }
    }
}
