use miette::{Result, miette};

use crate::{
    ColumnDef, DataType, Schema, Value,
    db::table::Table,
    sql::{
        ast::{
            expression::{Expression, FunctionArgs},
            operator::Operator,
            predicate::IsPredicate,
            statement::{self, DeleteStatement, InsertStatement, SelectStatement, UpdateStatement},
            target::{SelectList, SelectTarget},
        },
        catalog_context::CatalogContext,
        planner::logical::{AggregateCall, LogicalPlan, SortKey},
    },
};

#[derive(Debug)]
pub struct ColumnRef {
    pub index: usize,
    pub relation: Option<String>, // 'u' in 'u.name'
}

#[derive(Debug)]
pub enum AnalyzedExpression {
    Literal(Value),
    Column(ColumnRef, DataType),
    BinaryExpr {
        left: Box<AnalyzedExpression>,
        op: Operator,
        right: Box<AnalyzedExpression>,
        return_type: DataType,
    },
    IsPredicate {
        expr: Box<AnalyzedExpression>,
        predicate: IsPredicateTarget,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum IsPredicateTarget {
    True,
    False,
    Null,
}

impl From<&IsPredicate> for IsPredicateTarget {
    fn from(value: &IsPredicate) -> Self {
        match value {
            IsPredicate::True => IsPredicateTarget::True,
            IsPredicate::False => IsPredicateTarget::False,
            IsPredicate::Null => IsPredicateTarget::Null,
        }
    }
}

/// Aggregate functions usable in SELECT and ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunction {
    /// Looks up an aggregate by (case-insensitive) function name.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "avg" => Some(Self::Avg),
            "min" => Some(Self::Min),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    /// Result type of the aggregate given its argument type (`None` for `COUNT(*)`).
    fn return_type(self, arg: Option<DataType>) -> Result<DataType> {
        match (self, arg) {
            (Self::Count, _) => Ok(DataType::Int64),
            (Self::Sum, Some(t @ (DataType::Int64 | DataType::Float64))) => Ok(t),
            (Self::Avg, Some(DataType::Int64 | DataType::Float64)) => Ok(DataType::Float64),
            (Self::Min | Self::Max, Some(t)) => Ok(t),
            (_, Some(t)) => Err(miette!("{self:?}() cannot be applied to {t:?}")),
            (_, None) => Err(miette!("{self:?}() requires an argument")),
        }
    }
}

/// Binding state for expressions evaluated on top of an aggregate.
///
/// The aggregate's output row is `[group_by values..., aggregate values...]`.
struct GroupedScope {
    /// GROUP BY expressions (as written) and their types
    group_by: Vec<(Expression, DataType)>,

    /// Aggregate calls (as written) that have been bound so far
    aggregates: Vec<(Expression, AggregateCall)>,
}

/// Returns true if the expression calls an aggregate function anywhere.
fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Function { name, .. } => AggregateFunction::from_name(name).is_some(),
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::Is { expr, .. } => contains_aggregate(expr),
        Expression::Identifier(_) | Expression::Literal(_) => false,
    }
}

impl AnalyzedExpression {
    /// Get the type of the expression
    pub fn get_type(&self) -> DataType {
        match self {
            AnalyzedExpression::Literal(value) => match value {
                Value::Int64(_) => DataType::Int64,
                Value::Float64(_) => DataType::Float64,
                Value::Text(_) => DataType::Text,
                Value::Bool(_) => DataType::Bool,
                Value::Null => unreachable!("Null has no definite type."),
            },
            AnalyzedExpression::Column(_, column_type) => *column_type,
            AnalyzedExpression::BinaryExpr { return_type, .. } => *return_type,
            AnalyzedExpression::IsPredicate { .. } => DataType::Bool,
        }
    }

    /// Determines whether this expression can produce NULL given the input schema.
    pub fn is_nullable(&self, input_schema: &Schema) -> bool {
        match self {
            // Literals are never null (null literals are rejected during analysis)
            AnalyzedExpression::Literal(_) => false,
            // Column nullability comes from the source field
            AnalyzedExpression::Column(col_ref, _) => input_schema
                .columns
                .get(col_ref.index)
                .map(ColumnDef::is_nullable)
                .unwrap_or(true),
            // A binary expression is nullable if either operand is nullable
            AnalyzedExpression::BinaryExpr { left, right, .. } => {
                left.is_nullable(input_schema) || right.is_nullable(input_schema)
            }
            // IS TRUE / IS NULL / etc. always returns a definite bool, never null
            AnalyzedExpression::IsPredicate { .. } => false,
        }
    }
}

pub struct Analyzer<'a, 'db> {
    context: &'a CatalogContext<'db>,
}

impl<'a, 'db> Analyzer<'a, 'db> {
    pub fn new(context: &'a CatalogContext<'db>) -> Self {
        Self { context }
    }

    /// Builds the plan `Scan -> Filter -> [Aggregate] -> [Sort] -> Projection -> [Limit]`.
    pub fn analyze_select(&self, statement: SelectStatement) -> Result<LogicalPlan> {
        let SelectStatement {
            select_list,
            from_clause,
            where_clause,
            group_by,
            order_by,
            limit,
            offset,
        } = statement;
        let schema = self.context.get_table(&from_clause.table)?.schema();

        let mut plan = self.analyze_from_clause(from_clause)?;

        if let Some(expr) = where_clause {
            plan = self.analyze_where_clause(plan, &expr, schema)?;
        }

        let targets = Self::expand_targets(&select_list, schema);

        // GROUP BY prefers input columns over output aliases, ORDER BY the reverse (like PostgreSQL)
        let mut group_by_exprs = Vec::with_capacity(group_by.len());
        for expr in &group_by {
            let expr = match expr {
                Expression::Identifier(name) if schema.get_column_index(name).is_some() => expr,
                _ => Self::resolve_output_reference(expr, &targets)?,
            };
            group_by_exprs.push(expr.clone());
        }

        let mut order_by_exprs = Vec::with_capacity(order_by.len());
        for item in &order_by {
            let expr = Self::resolve_output_reference(&item.expr, &targets)?;
            order_by_exprs.push((expr.clone(), item.descending));
        }

        let is_aggregate = !group_by_exprs.is_empty()
            || targets.iter().any(|(expr, _)| contains_aggregate(expr))
            || order_by_exprs
                .iter()
                .any(|(expr, _)| contains_aggregate(expr));

        // In an aggregate query, the projection and sort run on top of the aggregate's
        // output, so their expressions are bound against that instead of the table.
        let mut bound_group_by = Vec::with_capacity(group_by_exprs.len());
        let mut grouped = if is_aggregate {
            let mut keys = Vec::with_capacity(group_by_exprs.len());
            for expr in group_by_exprs {
                let bound = self.bind_expression(&expr, schema)?;
                keys.push((expr, bound.get_type()));
                bound_group_by.push(bound);
            }

            Some(GroupedScope {
                group_by: keys,
                aggregates: Vec::new(),
            })
        } else {
            None
        };

        let mut projections = Vec::with_capacity(targets.len());
        for (expr, _) in &targets {
            projections.push(self.bind(expr, schema, grouped.as_mut())?);
        }

        let mut sort_keys = Vec::with_capacity(order_by_exprs.len());
        for (expr, descending) in &order_by_exprs {
            sort_keys.push(SortKey {
                expr: self.bind(expr, schema, grouped.as_mut())?,
                descending: *descending,
            });
        }

        if let Some(grouped) = grouped {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: bound_group_by,
                aggregates: grouped
                    .aggregates
                    .into_iter()
                    .map(|(_, call)| call)
                    .collect(),
            };
        }

        if !sort_keys.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                keys: sort_keys,
            };
        }

        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            expressions: projections,
            column_names: targets.into_iter().map(|(_, name)| name).collect(),
        };

        if limit.is_some() || offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset: offset.unwrap_or(0),
            };
        }

        Ok(plan)
    }

    pub fn analyze_update(&self, statement: UpdateStatement) -> Result<LogicalPlan> {
        let UpdateStatement {
            table: table_name,
            assignments,
            where_clause,
        } = statement;
        let schema = self.context.get_table(&table_name)?.schema();

        let mut bound_assignments: Vec<(usize, AnalyzedExpression)> = Vec::new();
        for (column, expr) in &assignments {
            let index = schema
                .get_column_index(column)
                .ok_or_else(|| miette!("Column {column} could not be found"))?;

            if bound_assignments.iter().any(|(i, _)| *i == index) {
                return Err(miette!("Column {column} is assigned more than once"));
            }

            // `SET col = NULL` is the one place a NULL literal makes sense; NOT NULL is
            // checked when the new row is validated.
            let value = if *expr == Expression::Literal(Value::Null) {
                AnalyzedExpression::Literal(Value::Null)
            } else {
                let value = self.bind_expression(expr, schema)?;
                let column_type = schema.columns[index].data_type;
                if !DataType::can_coerce(value.get_type(), column_type) {
                    return Err(miette!(
                        "Cannot assign {:?} to column {column} of type {:?}",
                        value.get_type(),
                        column_type
                    ));
                }
                value
            };

            bound_assignments.push((index, value));
        }

        let filter = where_clause
            .map(|expr| self.bind_expression(&expr, schema))
            .transpose()?;

        Ok(LogicalPlan::Update {
            table_name,
            assignments: bound_assignments,
            filter,
        })
    }

    pub fn analyze_delete(&self, statement: DeleteStatement) -> Result<LogicalPlan> {
        let DeleteStatement {
            table: table_name,
            where_clause,
        } = statement;
        let schema = self.context.get_table(&table_name)?.schema();

        let filter = where_clause
            .map(|expr| self.bind_expression(&expr, schema))
            .transpose()?;

        Ok(LogicalPlan::Delete { table_name, filter })
    }

    pub fn analyze_insert(&self, statement: InsertStatement) -> Result<LogicalPlan> {
        let InsertStatement {
            table: table_name,
            columns,
            source,
        } = statement;
        let table = self.context.get_table(&table_name)?;
        let schema = table.schema();

        // Target columns in the order the VALUES are written: the explicit column
        // list if there is one, otherwise every column in table order.
        let target_columns = match &columns {
            Some(names) => {
                let mut targets = Vec::with_capacity(names.len());
                for name in names {
                    let column = schema
                        .columns
                        .iter()
                        .find(|col| col.name == *name)
                        .ok_or_else(|| miette!("Column {name} could not be found"))?;

                    if targets.iter().any(|col: &&ColumnDef| col.name == *name) {
                        return Err(miette!("Column {name} is specified more than once"));
                    }
                    targets.push(column);
                }
                targets
            }
            None => schema.columns.iter().collect(),
        };

        for col in &schema.columns {
            if !target_columns.iter().any(|target| target.name == col.name) && !col.can_be_omitted()
            {
                return Err(miette!(
                    "Column {} is NOT NULL and must be inserted",
                    col.name
                ));
            }
        }

        // VALUES can't refer to columns, so bind them against an empty schema
        let no_columns = Schema::new(Vec::new());

        let mut analyzed_rows = Vec::with_capacity(source.len());
        for row in &source {
            if row.len() != target_columns.len() {
                return Err(miette!(
                    "INSERT has {} values but {} target columns",
                    row.len(),
                    target_columns.len()
                ));
            }

            let mut analyzed_row = Vec::with_capacity(row.len());
            for (expr, column) in row.iter().zip(&target_columns) {
                let value = if *expr == Expression::Literal(Value::Null) {
                    if !column.is_nullable() {
                        return Err(miette!("Column {} cannot be null", column.name));
                    }
                    AnalyzedExpression::Literal(Value::Null)
                } else {
                    let value = self.bind_expression(expr, &no_columns)?;
                    if !DataType::can_coerce(value.get_type(), column.data_type) {
                        return Err(miette!(
                            "Cannot insert {:?} into column {} of type {:?}",
                            value.get_type(),
                            column.name,
                            column.data_type
                        ));
                    }
                    value
                };
                analyzed_row.push(value);
            }
            analyzed_rows.push(analyzed_row);
        }

        Ok(LogicalPlan::Insert {
            table_name,
            column_names: target_columns.iter().map(|col| col.name.clone()).collect(),
            source: Box::new(LogicalPlan::Values {
                expressions: analyzed_rows,
            }),
        })
    }

    fn analyze_from_clause(&self, from_clause: statement::From) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Scan {
            table_name: from_clause.table.to_string(),
        })
    }

    /// Expands `*` and pairs every output column with its name (alias or default name).
    fn expand_targets(select_list: &SelectList, schema: &Schema) -> Vec<(Expression, String)> {
        let mut targets = Vec::new();

        for item in select_list.iter() {
            match item {
                SelectTarget::Star => {
                    for field in &schema.columns {
                        targets.push((
                            Expression::Identifier(field.name.clone()),
                            field.name.clone(),
                        ));
                    }
                }
                SelectTarget::Expression { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| expr.to_column_name());
                    targets.push((expr.clone(), name));
                }
            }
        }

        targets
    }

    /// Resolves ORDER BY / GROUP BY items that point at the select list.
    ///
    /// `2` means the second output column and a bare name may be an output alias.
    /// Anything else is returned unchanged and bound against the input.
    fn resolve_output_reference<'e>(
        expr: &'e Expression,
        targets: &'e [(Expression, String)],
    ) -> Result<&'e Expression> {
        match expr {
            Expression::Literal(Value::Int64(position)) => usize::try_from(*position)
                .ok()
                .and_then(|p| p.checked_sub(1))
                .and_then(|i| targets.get(i))
                .map(|(target, _)| target)
                .ok_or_else(|| miette!("Position {position} is not in select list")),
            Expression::Identifier(name) => Ok(targets
                .iter()
                .find(|(_, output_name)| output_name == name)
                .map_or(expr, |(target, _)| target)),
            _ => Ok(expr),
        }
    }

    fn analyze_where_clause(
        &self,
        input_plan: LogicalPlan,
        where_expr: &Expression,
        schema: &Schema,
    ) -> Result<LogicalPlan> {
        let analyzed_expr = self.bind_expression(where_expr, schema)?;

        Ok(LogicalPlan::Filter {
            input: Box::new(input_plan),
            condition: analyzed_expr,
        })
    }

    /// Binds an expression against the columns of `input_schema`.
    ///
    /// Aggregate functions are rejected here; they only make sense in a grouped
    /// context (see [`Analyzer::analyze_select`]).
    pub fn bind_expression(
        &self,
        expr: &Expression,
        input_schema: &Schema,
    ) -> Result<AnalyzedExpression> {
        self.bind(expr, input_schema, None)
    }

    /// Binds an expression, optionally on top of an aggregate.
    ///
    /// With `grouped` set, the expression may only use GROUP BY expressions and
    /// aggregate calls, and column references point into the aggregate's output row.
    /// Aggregate calls found along the way are registered in `grouped`.
    fn bind(
        &self,
        expr: &Expression,
        input_schema: &Schema,
        mut grouped: Option<&mut GroupedScope>,
    ) -> Result<AnalyzedExpression> {
        if let Some(scope) = grouped.as_deref()
            && let Some(index) = scope.group_by.iter().position(|(key, _)| key == expr)
        {
            return Ok(AnalyzedExpression::Column(
                ColumnRef {
                    index,
                    relation: None,
                },
                scope.group_by[index].1,
            ));
        }

        match expr {
            Expression::BinaryOp { left, op, right } => {
                let left = self.bind(left, input_schema, grouped.as_deref_mut())?;
                let right = self.bind(right, input_schema, grouped)?;

                let return_type = self.resolve_binary_op(left.get_type(), *op, right.get_type())?;

                Ok(AnalyzedExpression::BinaryExpr {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                    return_type,
                })
            }
            Expression::Identifier(name) if grouped.is_some() => Err(miette!(
                "Column {name} must appear in the GROUP BY clause or be used in an aggregate function"
            )),
            Expression::Identifier(name) => {
                let index = input_schema
                    .get_column_index(name)
                    .ok_or_else(|| miette!("Column {name} could not be found"))?;
                let field = &input_schema.columns[index];

                Ok(AnalyzedExpression::Column(
                    ColumnRef {
                        index,
                        relation: None,
                    },
                    field.data_type,
                ))
            }
            Expression::Literal(scalar_value) => match scalar_value {
                Value::Null => Err(miette!(
                    "NULL literal cannot be used in this context. Use 'IS NULL' or 'IS NOT NULL' instead"
                )),
                scalar_value => Ok(AnalyzedExpression::Literal(scalar_value.clone())),
            },
            Expression::Is {
                expr,
                predicate,
                is_negated,
            } => {
                let inner_analyzed = self.bind(expr, input_schema, grouped)?;

                let inner_type = inner_analyzed.get_type();
                match predicate {
                    IsPredicate::True | IsPredicate::False => {
                        if inner_type != DataType::Bool {
                            return Err(miette!("IS TRUE/FALSE requires boolean input"));
                        }
                    }
                    IsPredicate::Null => {
                        // Just works
                    }
                }

                let predicate: IsPredicateTarget = predicate.into();

                Ok(AnalyzedExpression::IsPredicate {
                    expr: Box::new(inner_analyzed),
                    predicate,
                    negated: *is_negated,
                })
            }
            Expression::Function { name, args } => {
                let function = AggregateFunction::from_name(name)
                    .ok_or_else(|| miette!("Function {name}() does not exist"))?;

                let Some(scope) = grouped else {
                    return Err(miette!(
                        "Aggregate function {name}() is not allowed in WHERE, GROUP BY, or inside another aggregate"
                    ));
                };

                // The argument is evaluated per input row, so it is bound against the table
                let arg = match args {
                    FunctionArgs::Star if function == AggregateFunction::Count => None,
                    FunctionArgs::Star => return Err(miette!("{name}(*) is not supported")),
                    FunctionArgs::List(args) if args.len() == 1 => {
                        Some(self.bind_expression(&args[0], input_schema)?)
                    }
                    FunctionArgs::List(_) => {
                        return Err(miette!("{name}() takes exactly one argument"));
                    }
                };

                let return_type = function.return_type(arg.as_ref().map(|a| a.get_type()))?;

                // Reuse the same aggregate if it appears twice (e.g. in SELECT and ORDER BY)
                let position = scope.aggregates.iter().position(|(ast, _)| ast == expr);
                let index = position.unwrap_or_else(|| {
                    scope
                        .aggregates
                        .push((expr.clone(), AggregateCall { function, arg }));
                    scope.aggregates.len() - 1
                });

                Ok(AnalyzedExpression::Column(
                    ColumnRef {
                        index: scope.group_by.len() + index,
                        relation: None,
                    },
                    return_type,
                ))
            }
        }
    }

    fn resolve_binary_op(&self, left: DataType, op: Operator, right: DataType) -> Result<DataType> {
        match op {
            Operator::Equal
            | Operator::GreaterThan
            | Operator::LessThan
            | Operator::GreaterThanEqual
            | Operator::LessThanEqual
            | Operator::NotEqual
                if DataType::can_coerce(left, right) =>
            {
                Ok(DataType::Bool)
            }
            Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                Self::resolve_arithmetic_type(left, right)
            }
            Operator::And | Operator::Or if left == DataType::Bool && right == DataType::Bool => {
                Ok(DataType::Bool)
            }
            _ => Err(miette!("Type mismatch between {left:?} {op} {right:?}")),
        }
    }

    fn resolve_arithmetic_type(left: DataType, right: DataType) -> Result<DataType> {
        if left == right {
            return Ok(left);
        }

        match (left, right) {
            (_, DataType::Float64) | (DataType::Float64, _) => Ok(DataType::Float64),
            (_, DataType::Int64) | (DataType::Int64, _) => Ok(DataType::Int64),
            _ => Err(miette!(
                "Cannot perform arithmetic between {left:?} and {right:?}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ColumnDef, Schema, core::types::DataType};

    /// Creates a test schema with common columns
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::Text),
            ColumnDef::new("email", DataType::Text),
            ColumnDef::new("age", DataType::Int64),
        ])
    }
}
