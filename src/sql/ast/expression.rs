use std::fmt;

use crate::{
    core::types::Value,
    sql::ast::{operator::Operator, predicate::IsPredicate},
};

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

    /// Function call (e.g., `COUNT(*)`, `SUM(price)`)
    Function { name: String, args: FunctionArgs },
}

/// Arguments passed to a function call.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArgs {
    /// `(*)`, only meaningful for `COUNT(*)`
    Star,

    /// `(expr, expr, ...)`
    List(Vec<Expression>),
}

impl fmt::Display for FunctionArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArgs::Star => write!(f, "*"),
            FunctionArgs::List(args) => {
                let args: Vec<String> = args.iter().map(ToString::to_string).collect();
                write!(f, "{}", args.join(", "))
            }
        }
    }
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
            Expression::Function { name, args } => write!(f, "{name}({args})"),
        }
    }
}

impl Expression {
    /// Name of the output column when the expression has no alias.
    ///
    /// Follows PostgreSQL: columns keep their name, function calls use the
    /// lowercased function name, and everything else becomes `?column?`.
    pub fn to_column_name(&self) -> String {
        match self {
            Expression::Identifier(name) => name.clone(),
            Expression::Function { name, .. } => name.to_lowercase(),
            _ => "?column?".to_string(),
        }
    }
}
