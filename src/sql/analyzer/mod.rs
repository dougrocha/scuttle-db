use miette::{Result, miette};

use crate::{
    DataType, Schema, Value,
    db::table::{Table, column_def::ColumnConstraint},
    sql::{
        ast::{
            expression::Expression,
            operator::Operator,
            predicate::IsPredicate,
            statement::{self, InsertStatement, SelectStatement},
            target::{SelectList, SelectTarget},
        },
        catalog_context::CatalogContext,
        planner::logical::LogicalPlan,
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
                .map(|f| f.has_constraint(ColumnConstraint::Nullable))
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

    pub fn analyze_from(&self, statement: SelectStatement) -> Result<LogicalPlan> {
        let SelectStatement {
            select_list,
            from_clause,
            where_clause,
        } = statement;
        let schema = self.context.get_table(&from_clause.table)?.schema();

        let mut plan = self.analyze_from_clause(from_clause)?;

        if let Some(expr) = where_clause {
            plan = self.analyze_where_clause(plan, &expr, schema)?;
        }

        plan = self.analyze_projection_clause(plan, &select_list, schema)?;

        Ok(plan)
    }

    pub fn analyze_insert(&self, statement: InsertStatement) -> Result<LogicalPlan> {
        let InsertStatement {
            table: table_name,
            columns,
            source,
        } = statement;
        let table = self.context.get_table(&table_name)?;
        let schema = table.schema();

        // Get columns that we are inserting
        let mut insert_cols = Vec::new();
        for col in &schema.columns {
            if columns.as_ref().is_some_and(|v| v.contains(&col.name)) | columns.is_none() {
                insert_cols.push(col);
            } else if col.can_be_omitted() {
                println!("Column {:?} can be either default or null", col.name);
            } else {
                return Err(miette!("Column {:?} must be inserted.", col.name));
            }
        }

        let source = {
            let analyzed_values: Vec<Vec<AnalyzedExpression>> = source
                .iter()
                .map(|expr| {
                    let analyzed_vals: Vec<AnalyzedExpression> = expr
                        .iter()
                        .map(|expr| self.bind_expression(expr, schema))
                        .collect::<Result<Vec<_>>>()?;

                    for (insert_col, analyzed_val) in insert_cols.iter().zip(analyzed_vals.iter()) {
                        if !DataType::can_coerce(insert_col.data_type, analyzed_val.get_type()) {
                            return Err(miette!(
                                "Tried to insert ({:?}, {:?}) into column ({:?}, {:?})",
                                analyzed_val,
                                analyzed_val.get_type(),
                                insert_col.name,
                                insert_col.data_type
                            ));
                        }
                    }

                    Ok(analyzed_vals)
                })
                .collect::<Result<Vec<_>>>()?;

            LogicalPlan::Values {
                expressions: analyzed_values,
            }
        };

        Ok(LogicalPlan::Insert {
            table_name,
            column_names: columns
                .unwrap_or_else(|| schema.columns.iter().map(|col| col.name.clone()).collect()),
            source: Box::new(source),
        })
    }

    fn analyze_from_clause(&self, from_clause: statement::From) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Scan {
            table_name: from_clause.table.to_string(),
        })
    }

    fn analyze_projection_clause(
        &self,
        input_plan: LogicalPlan,
        select_list: &SelectList,
        schema: &Schema,
    ) -> Result<LogicalPlan> {
        let mut analyzed_exprs = Vec::new();
        let mut output_fields = Vec::new();

        for item in select_list.iter() {
            match item {
                SelectTarget::Star => {
                    for (i, field) in schema.columns.iter().enumerate() {
                        let expr = AnalyzedExpression::Column(
                            ColumnRef {
                                index: i,
                                relation: None,
                            },
                            field.data_type,
                        );
                        analyzed_exprs.push(expr);
                        output_fields.push(field.clone());
                    }
                }
                SelectTarget::Expression { expr, alias: _ } => {
                    let analyzed_expr = self.bind_expression(expr, schema)?;

                    analyzed_exprs.push(analyzed_expr);
                }
            }
        }

        Ok(LogicalPlan::Projection {
            input: Box::new(input_plan),
            expressions: analyzed_exprs,
        })
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

    pub fn bind_expression(
        &self,
        expr: &Expression,
        input_schema: &Schema,
    ) -> Result<AnalyzedExpression> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                let left = self.bind_expression(left, input_schema)?;
                let right = self.bind_expression(right, input_schema)?;

                let return_type = self.resolve_binary_op(left.get_type(), *op, right.get_type())?;

                Ok(AnalyzedExpression::BinaryExpr {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                    return_type,
                })
            }
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
                let inner_analyzed = self.bind_expression(expr, input_schema)?;

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
