use miette::{Result, miette};

use crate::{
    PhysicalType, Table,
    sql::{
        parser::{
            Expression, IsPredicate, Operator, ScalarValue, SelectList, SelectTarget, Statement,
            statement::{FromClause, SelectStatement},
        },
        planner_context::PlannerContext,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Float64,
    Text,
    Bool,
    Null,
}

impl From<PhysicalType> for ColumnType {
    fn from(value: PhysicalType) -> Self {
        match value {
            PhysicalType::Int64 => Self::Int64,
            PhysicalType::Text | PhysicalType::VarChar(_) => Self::Text,
            PhysicalType::Bool => Self::Bool,
            PhysicalType::Float64 => Self::Float64,
        }
    }
}

impl From<&ScalarValue> for ColumnType {
    fn from(value: &ScalarValue) -> Self {
        match value {
            ScalarValue::Int64(_) => Self::Int64,
            ScalarValue::Text(_) => Self::Text,
            ScalarValue::Bool(_) => Self::Bool,
            ScalarValue::Float64(_) => Self::Float64,
            ScalarValue::Null => Self::Null,
        }
    }
}

#[derive(Debug)]
pub struct ColumnReference {
    pub index: usize,
    pub relation: Option<String>, // 'u' in 'u.name'
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub alias: Option<String>,
    pub data_type: ColumnType,
    pub is_nullable: bool,
}

#[derive(Debug)]
pub enum AnalyzedExpression {
    Literal(ScalarValue),
    Column(ColumnReference, ColumnType),
    BinaryExpr {
        left: Box<AnalyzedExpression>,
        op: Operator,
        right: Box<AnalyzedExpression>,
        return_type: ColumnType,
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
    pub fn get_type(&self) -> ColumnType {
        match self {
            AnalyzedExpression::Literal(scalar_value) => scalar_value.into(),
            AnalyzedExpression::Column(_, column_type) => *column_type,
            AnalyzedExpression::BinaryExpr { return_type, .. } => *return_type,
            AnalyzedExpression::IsPredicate { .. } => ColumnType::Bool,
        }
    }

    /// Determines whether this expression can produce NULL given the input schema.
    pub fn is_nullable(&self, input_schema: &ResolvedSchema) -> bool {
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
pub enum AnalyzedPlan {
    Scan {
        table_name: String,
        schema: ResolvedSchema,
    },
    Filter {
        input: Box<AnalyzedPlan>,
        condition: AnalyzedExpression,
    },
    Projection {
        input: Box<AnalyzedPlan>,
        expressions: Vec<AnalyzedExpression>,
        schema: ResolvedSchema,
    },
}

#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    pub fields: Vec<Field>,
}

impl ResolvedSchema {
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }
}

impl AnalyzedPlan {
    fn schema(&self) -> &ResolvedSchema {
        match self {
            AnalyzedPlan::Scan { schema, .. } | AnalyzedPlan::Projection { schema, .. } => schema,
            AnalyzedPlan::Filter { input, .. } => input.schema(),
        }
    }
}

pub struct Analyzer<'a, 'db> {
    context: &'a PlannerContext<'db>,
}

impl<'a, 'db> Analyzer<'a, 'db> {
    pub fn new(context: &'a PlannerContext<'db>) -> Self {
        Self { context }
    }

    pub fn analyze(&self, statement: Statement) -> Result<AnalyzedPlan> {
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

                plan = self.analyze_projection(plan, select_list)?;

                Ok(plan)
            }
            _ => Err(miette!("Analysis not implemented for this statement.")),
        }
    }

    fn analyze_from(&self, from_clause: FromClause) -> Result<AnalyzedPlan> {
        let physical_schema = self.context.get_table(&from_clause.table_name)?.schema();

        let virtual_fields = physical_schema
            .columns
            .iter()
            .map(|col| Field {
                name: col.name.clone(),
                alias: None, // We don't handle alias yet here
                data_type: col.data_type.into(),
                is_nullable: col.nullable,
            })
            .collect();

        let resolved_schema = ResolvedSchema {
            fields: virtual_fields,
        };

        Ok(AnalyzedPlan::Scan {
            table_name: from_clause.table_name,
            schema: resolved_schema,
        })
    }

    fn analyze_projection(
        &self,
        input_plan: AnalyzedPlan,
        select_list: SelectList,
    ) -> Result<AnalyzedPlan> {
        let input_schema = input_plan.schema();

        let mut analyzed_exprs = Vec::new();
        let mut output_fields = Vec::new();

        for item in select_list.iter() {
            match item {
                SelectTarget::Star => {
                    for (i, field) in input_schema.fields.iter().enumerate() {
                        let expr = AnalyzedExpression::Column(
                            ColumnReference {
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
                        name: expr.to_column_name(),
                        alias: alias.clone(),
                        data_type: analyzed_expr.get_type(),
                        is_nullable: analyzed_expr.is_nullable(input_schema),
                    };

                    analyzed_exprs.push(analyzed_expr);
                    output_fields.push(field);
                }
            }
        }

        Ok(AnalyzedPlan::Projection {
            input: Box::new(input_plan),
            expressions: analyzed_exprs,
            schema: ResolvedSchema {
                fields: output_fields,
            },
        })
    }

    fn analyze_where(
        &self,
        input_plan: AnalyzedPlan,
        where_expr: &Expression,
    ) -> Result<AnalyzedPlan> {
        let schema = input_plan.schema();

        let analyzed_expr = self.bind_expression(where_expr, schema)?;

        Ok(AnalyzedPlan::Filter {
            input: Box::new(input_plan),
            condition: analyzed_expr,
        })
    }

    pub fn bind_expression(
        &self,
        expr: &Expression,
        input_schema: &ResolvedSchema,
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
                    .ok_or(miette!("Column {name} could not be found"))?;
                let field = &input_schema.fields[index];

                Ok(AnalyzedExpression::Column(
                    ColumnReference {
                        index,
                        relation: None,
                    },
                    field.data_type,
                ))
            }
            Expression::Literal(scalar_value) => match scalar_value {
                ScalarValue::Int64(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Text(_)
                | ScalarValue::Bool(_) => Ok(AnalyzedExpression::Literal(scalar_value.clone())),
                ScalarValue::Null => Err(miette!(
                    "NULL literal cannot be used in this context. Use 'IS NULL' or 'IS NOT NULL' instead"
                )),
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
                        if inner_type != ColumnType::Bool {
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

    fn resolve_binary_op(
        &self,
        left: ColumnType,
        op: Operator,
        right: ColumnType,
    ) -> Result<ColumnType> {
        match op {
            Operator::Equal
            | Operator::GreaterThan
            | Operator::LessThan
            | Operator::GreaterThanEqual
            | Operator::LessThanEqual
            | Operator::NotEqual
                if Self::can_coerce(left, right) =>
            {
                Ok(ColumnType::Bool)
            }
            Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                Self::get_common_numeric_type(left, right)
            }
            Operator::And | Operator::Or
                if left == ColumnType::Bool && right == ColumnType::Bool =>
            {
                Ok(ColumnType::Bool)
            }
            _ => Err(miette!("Type mismatch between {left:?} {op} {right:?}")),
        }
    }

    fn get_common_numeric_type(left: ColumnType, right: ColumnType) -> Result<ColumnType> {
        if left == right {
            return Ok(left);
        }

        match (left, right) {
            (_, ColumnType::Float64) | (ColumnType::Float64, _) => Ok(ColumnType::Float64),
            (_, ColumnType::Int64) | (ColumnType::Int64, _) => Ok(ColumnType::Int64),
            _ => Err(miette!(
                "Cannot perform arithmetic between {left:?} and {right:?}"
            )),
        }
    }

    fn can_coerce(from: ColumnType, to: ColumnType) -> bool {
        if from == to {
            return true;
        }

        matches!(
            (from, to),
            (ColumnType::Int64, ColumnType::Float64)
                | (ColumnType::Text, _)
                | (_, ColumnType::Text)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDefinition, PhysicalType, Schema};

    /// Creates a test schema with common columns
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            ColumnDefinition::new("id", PhysicalType::Int64, false),
            ColumnDefinition::new("name", PhysicalType::Text, false),
            ColumnDefinition::new("email", PhysicalType::Text, true),
            ColumnDefinition::new("age", PhysicalType::Int64, true),
        ])
    }
}
