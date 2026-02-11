use std::fmt;

/// Binary comparison operators for WHERE clauses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operator {
    /// Equality (=)
    Equal,
    NotEqual,

    /// Logical AND
    And,
    /// Logical OR
    Or,

    /// Greater than (>)
    GreaterThan,
    GreaterThanEqual,

    /// Less than (<)
    LessThan,
    LessThanEqual,

    Add,
    Multiply,
    Divide,
    Subtract,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_symbol())
    }
}

impl Operator {
    pub fn to_symbol(self) -> &'static str {
        match self {
            Operator::Equal => "=",
            Operator::NotEqual => "!=",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::GreaterThan => ">",
            Operator::GreaterThanEqual => ">=",
            Operator::LessThan => "<",
            Operator::LessThanEqual => "<=",
            Operator::Add => "+",
            Operator::Subtract => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
        }
    }

    /// Returns the binding power (precedence) of this operator.
    ///
    /// This defines the "Order of Operations". Operators with a higher number
    /// bind tighter and are evaluated first.
    pub fn precedence(&self) -> u8 {
        match self {
            Operator::Or => 2,
            Operator::And => 3,
            Operator::NotEqual
            | Operator::Equal
            | Operator::LessThan
            | Operator::LessThanEqual
            | Operator::GreaterThan
            | Operator::GreaterThanEqual => 5,
            Operator::Add | Operator::Subtract => 7,
            Operator::Multiply | Operator::Divide => 10,
        }
    }
}

