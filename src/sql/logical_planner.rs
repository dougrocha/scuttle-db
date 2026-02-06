use super::parser::Expression;

#[derive(Debug)]
pub enum LogicalPlan {
    TableScan {
        table: String,
    },
    Filter {
        predicate: Expression,
        input: Box<LogicalPlan>,
    },
    Projection {
        expressions: Vec<Expression>,
        column_names: Vec<String>,
        input: Box<LogicalPlan>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expression,
        join_type: JoinType,
    },
    Limit {
        input: Box<LogicalPlan>,
        count: usize,
    },
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
