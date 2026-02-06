use super::parser::Expression;

/// Logical Plan
///
/// This is made from the incoming AST. The result is a 'relational algebra tree'.
///
/// The output of this plan is to know what operations we need to do to get the data.
#[derive(Debug)]
pub enum LogicalPlan {
    /// Read a table
    TableScan { table: String },
    /// Discard rows that don't match a predicate
    Filter {
        predicate: Expression,
        input: Box<LogicalPlan>,
    },
    /// Select/compute specific columns
    Projection {
        expressions: Vec<Expression>,
        column_names: Vec<String>,
        input: Box<LogicalPlan>,
    },
    /// Combine rows from different inputs (usually different tables)
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expression,
        join_type: JoinType,
    },
    /// Cap the amount of rows
    Limit {
        input: Box<LogicalPlan>,
        count: usize,
    },
    /// Order By
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByExpr>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Outer,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    /// The expression to sort by (could be a Column, Math, or Function)
    pub expr: Expression,
    /// The direction: Ascending or Descending
    pub direction: SortDirection,
    /// Where to put NULLs (Postgres default is NULLS LAST for ASC)
    pub nulls: NullOrdering,
}

#[derive(Debug, Clone, Copy)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy)]
pub enum NullOrdering {
    First,
    Last,
}
