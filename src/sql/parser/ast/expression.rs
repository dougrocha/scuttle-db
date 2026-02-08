use std::fmt;

use crate::sql::parser::{ScalarValue, operators::Operator};

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
    Literal(ScalarValue),

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

impl Expression {
    pub fn to_column_name(&self) -> String {
        match self {
            Expression::Identifier(name) => name.clone(),
            _ => "?column?".to_string(),
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
