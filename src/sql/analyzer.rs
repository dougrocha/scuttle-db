use miette::{Result, miette};

use crate::{
    DataType, Table, Value,
    sql::{
        catalog_context::CatalogContext,
        parser::{
            Expression, Operator, SelectList, SelectTarget, Statement,
            expression::IsPredicate,
            statement::{FromClause, SelectStatement},
        },
    },
};

#[derive(Debug)]
pub struct ColumnRef {
    pub index: usize,
    pub relation: Option<String>, // 'u' in 'u.name'
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub alias: Option<String>,
    pub data_type: DataType,
    pub is_nullable: bool,
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
    pub fn is_nullable(&self, input_schema: &OutputSchema) -> bool {
        match self {
            // Literals are never null (null literals are rejected during analysis)
            AnalyzedExpression::Literal(_) => false,
            // Column nullability comes from the source field
            AnalyzedExpression::Column(col_ref, _) => input_schema
                .fields
                .get(col_ref.index)
                .map(|f| f.is_nullable)
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

#[derive(Debug)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        schema: OutputSchema,
    },
    Filter {
        input: Box<LogicalPlan>,
        condition: AnalyzedExpression,
    },
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<AnalyzedExpression>,
        schema: OutputSchema,
    },
}

#[derive(Debug, Clone)]
pub struct OutputSchema {
    pub fields: Vec<Field>,
}

impl OutputSchema {
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }
}

impl LogicalPlan {
    fn schema(&self) -> &OutputSchema {
        match self {
            LogicalPlan::Scan { schema, .. } | LogicalPlan::Projection { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
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

    pub fn analyze(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::Select(SelectStatement {
                select_list,
                from_clause,
                where_clause,
            }) => {
                let mut plan = self.analyze_from(from_clause)?;

                if let Some(expr) = where_clause {
                    plan = self.analyze_where(plan, &expr)?;
                }

                plan = self.analyze_projection(plan, &select_list)?;

                Ok(plan)
            }
            _ => Err(miette!("Analysis not implemented for this statement.")),
        }
    }

    fn analyze_from(&self, from_clause: FromClause) -> Result<LogicalPlan> {
        let physical_schema = self.context.get_table(&from_clause.table_name)?.schema();

        let virtual_fields = physical_schema
            .columns
            .iter()
            .map(|col| Field {
                name: col.name.clone(),
                alias: None,
                data_type: col.data_type,
                is_nullable: col.nullable,
            })
            .collect();

        let resolved_schema = OutputSchema {
            fields: virtual_fields,
        };

        Ok(LogicalPlan::Scan {
            table_name: from_clause.table_name.to_string(),
            schema: resolved_schema,
        })
    }

    fn analyze_projection(
        &self,
        input_plan: LogicalPlan,
        select_list: &SelectList,
    ) -> Result<LogicalPlan> {
        let input_schema = input_plan.schema();

        let mut analyzed_exprs = Vec::new();
        let mut output_fields = Vec::new();

        for item in select_list.iter() {
            match item {
                SelectTarget::Star => {
                    for (i, field) in input_schema.fields.iter().enumerate() {
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
                SelectTarget::Expression { expr, alias } => {
                    let analyzed_expr = self.bind_expression(expr, input_schema)?;

                    let field = Field {
                        name: expr.to_column_name().to_string(),
                        alias: alias.as_ref().map(|a| a.to_string()),
                        data_type: analyzed_expr.get_type(),
                        is_nullable: analyzed_expr.is_nullable(input_schema),
                    };

                    analyzed_exprs.push(analyzed_expr);
                    output_fields.push(field);
                }
            }
        }

        Ok(LogicalPlan::Projection {
            input: Box::new(input_plan),
            expressions: analyzed_exprs,
            schema: OutputSchema {
                fields: output_fields,
            },
        })
    }

    fn analyze_where(
        &self,
        input_plan: LogicalPlan,
        where_expr: &Expression,
    ) -> Result<LogicalPlan> {
        let schema = input_plan.schema();

        let analyzed_expr = self.bind_expression(where_expr, schema)?;

        Ok(LogicalPlan::Filter {
            input: Box::new(input_plan),
            condition: analyzed_expr,
        })
    }

    pub fn bind_expression(
        &self,
        expr: &Expression,
        input_schema: &OutputSchema,
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
                    .find_column(name)
                    .ok_or_else(|| miette!("Column {name} could not be found"))?;
                let field = &input_schema.fields[index];

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
                Self::get_common_numeric_type(left, right)
            }
            Operator::And | Operator::Or if left == DataType::Bool && right == DataType::Bool => {
                Ok(DataType::Bool)
            }
            _ => Err(miette!("Type mismatch between {left:?} {op} {right:?}")),
        }
    }

    fn get_common_numeric_type(left: DataType, right: DataType) -> Result<DataType> {
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
    use super::*;
    use crate::{ColumnDef, DataType, Schema};

    /// Creates a test schema with common columns
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64, false),
            ColumnDef::new("name", DataType::Text, false),
            ColumnDef::new("email", DataType::Text, true),
            ColumnDef::new("age", DataType::Int64, true),
        ])
    }
}
