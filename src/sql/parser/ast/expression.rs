use std::fmt;

use crate::sql::parser::{ScalarValue, operators::Operator};

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
    Identifier(&'src str),

    /// Literal value (e.g., `25`, `'Alice'`)
    Literal(ScalarValue<'src>),

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
                ScalarValue::Float64(num) => write!(f, "{num}"),
                ScalarValue::Int64(num) => write!(f, "{num}"),
                ScalarValue::Text(s) => write!(f, "\"{s}\""),
                ScalarValue::Bool(bool) => {
                    write!(f, "{}", bool.to_string().to_uppercase())
                }
                ScalarValue::Null => write!(f, "NULL"),
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

/// Predicates to the 'IS' keyword.
#[derive(Debug, Clone, PartialEq)]
pub enum IsPredicate {
    True,
    False,
    Null,
}

impl fmt::Display for IsPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IsPredicate::True => write!(f, "TRUE"),
            IsPredicate::False => write!(f, "FALSE"),
            IsPredicate::Null => write!(f, "NULL"),
        }
    }
}
