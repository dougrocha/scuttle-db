//! `BindScope`: the only place column names are resolved in the analyzer.
//!
//! A scope describes the row an expression is evaluated against: every column of
//! every FROM relation, in FROM order. For `FROM users u JOIN orders o ON ...`
//! (users: id, name, dept, age; orders: id, user_id, total):
//!
//! ```text
//! index: 0     1       2       3      4     5          6
//!        u.id  u.name  u.dept  u.age  o.id  o.user_id  o.total
//! ```
//!
//! Relations are pushed one at a time, so indexes are correct by construction.
//! Lookups are exact and case-sensitive, like `Schema::get_column_index`.

use miette::{Result, miette};

use crate::{
    DataType, Schema,
    sql::ast::expression::{Expression, FunctionArgs},
};

#[derive(Debug, Clone)]
struct ScopeColumn {
    relation: String,
    name: String,
    data_type: DataType,
}

impl ScopeColumn {
    fn to_expression(&self) -> Expression {
        Expression::Qualified {
            relation: self.relation.clone(),
            column: self.name.clone(),
        }
    }
}

/// Ordered list of the columns visible to an expression, one entry per relation column.
#[derive(Debug, Default)]
pub(crate) struct BindScope {
    columns: Vec<ScopeColumn>,
}

impl BindScope {
    /// Creates a scope with no relations (used for expressions without a FROM clause).
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Creates a scope containing the columns of a single relation.
    ///
    /// # Panics
    ///
    /// Never panics in practice: the scope starts empty, so no duplicate relation can exist.
    pub(crate) fn single(relation: &str, schema: &Schema) -> Self {
        let mut scope = Self::empty();
        scope
            .push_relation(relation, schema)
            .expect("an empty scope has no duplicate relation");
        scope
    }

    /// Appends the columns of `relation` to the scope.
    ///
    /// # Errors
    ///
    /// Returns an error if a relation with the same name is already in the scope.
    pub(crate) fn push_relation(&mut self, relation: &str, schema: &Schema) -> Result<()> {
        if self.columns.iter().any(|c| c.relation == relation) {
            return Err(miette!(
                "Table name \"{relation}\" specified more than once; give one of them an alias"
            ));
        }
        self.columns
            .extend(schema.columns.iter().map(|col| ScopeColumn {
                relation: relation.to_string(),
                name: col.name.clone(),
                data_type: col.data_type,
            }));
        Ok(())
    }

    /// Returns the total number of columns across all relations in the scope.
    pub(crate) fn len(&self) -> usize {
        self.columns.len()
    }

    /// Resolves a (possibly qualified) column reference to its position and type.
    ///
    /// An unqualified name must match exactly one column across all relations.
    ///
    /// # Errors
    ///
    /// Returns an error if the relation is missing, the column does not exist,
    /// or an unqualified name is ambiguous.
    pub(crate) fn resolve(&self, qualifier: Option<&str>, name: &str) -> Result<(usize, DataType)> {
        let index = match qualifier {
            Some(relation) => {
                if !self.columns.iter().any(|c| c.relation == relation) {
                    return Err(miette!("Missing FROM-clause entry for table {relation}"));
                }
                self.columns
                    .iter()
                    .position(|c| c.relation == relation && c.name == name)
                    .ok_or_else(|| miette!("Column {relation}.{name} could not be found"))?
            }
            None => {
                let mut hits = self
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.name == name)
                    .map(|(i, _)| i);
                let first = hits
                    .next()
                    .ok_or_else(|| miette!("Column {name} could not be found"))?;
                if hits.next().is_some() {
                    return Err(miette!("Column reference \"{name}\" is ambiguous"));
                }
                first
            }
        };
        Ok((index, self.columns[index].data_type))
    }

    /// Returns true if any relation in the scope has a column with this name.
    pub(crate) fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    /// Expands `*` or `relation.*` into one expression per column, in column order.
    ///
    /// Each pair holds the column expression and its output name.
    ///
    /// # Errors
    ///
    /// Returns an error if `qualifier` names a relation that is not in the scope.
    pub(crate) fn expand_star(&self, qualifier: Option<&str>) -> Result<Vec<(Expression, String)>> {
        if let Some(relation) = qualifier
            && !self.columns.iter().any(|c| c.relation == relation)
        {
            return Err(miette!("Missing FROM-clause entry for table {relation}"));
        }
        Ok(self
            .columns
            .iter()
            .filter(|c| qualifier.is_none_or(|q| c.relation == q))
            .map(|c| (c.to_expression(), c.name.clone()))
            .collect())
    }

    /// Rewrites every column reference in `expr` to its fully qualified form.
    ///
    /// # Errors
    ///
    /// Returns an error if any column reference cannot be resolved in this scope.
    pub(crate) fn canonicalize(&self, expr: &Expression) -> Result<Expression> {
        match expr {
            Expression::Identifier(name) => {
                let (i, _) = self.resolve(None, name)?;
                Ok(self.columns[i].to_expression())
            }
            Expression::Qualified { relation, column } => {
                let (i, _) = self.resolve(Some(relation), column)?;
                Ok(self.columns[i].to_expression())
            }
            Expression::Literal(_) => Ok(expr.clone()),
            Expression::BinaryOp { left, op, right } => Ok(Expression::BinaryOp {
                left: Box::new(self.canonicalize(left)?),
                op: *op,
                right: Box::new(self.canonicalize(right)?),
            }),
            Expression::Is {
                expr: inner,
                predicate,
                is_negated,
            } => Ok(Expression::Is {
                expr: Box::new(self.canonicalize(inner)?),
                predicate: predicate.clone(),
                is_negated: *is_negated,
            }),
            Expression::Function { name, args } => {
                let args = match args {
                    FunctionArgs::Star => FunctionArgs::Star,
                    FunctionArgs::List(list) => FunctionArgs::List(
                        list.iter()
                            .map(|a| self.canonicalize(a))
                            .collect::<Result<Vec<_>>>()?,
                    ),
                };
                Ok(Expression::Function {
                    name: name.clone(),
                    args,
                })
            }
        }
    }

    /// Returns every column as a `relation.column` string, in scope order.
    pub(crate) fn qualified_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|c| format!("{}.{}", c.relation, c.name))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDef, core::types::Value, sql::ast::operator::Operator};

    /// `users(id, name, dept, age)`
    fn users_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::Text),
            ColumnDef::new("dept", DataType::Text),
            ColumnDef::new("age", DataType::Int64),
        ])
    }

    /// `orders(id, user_id, total)`
    fn orders_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("user_id", DataType::Int64),
            ColumnDef::new("total", DataType::Float64),
        ])
    }

    /// `u JOIN o` as FROM order: u.id, u.name, u.dept, u.age, o.id, o.user_id, o.total
    fn joined_scope() -> BindScope {
        let mut scope = BindScope::empty();
        scope.push_relation("u", &users_schema()).unwrap();
        scope.push_relation("o", &orders_schema()).unwrap();
        scope
    }

    fn qualified(relation: &str, column: &str) -> Expression {
        Expression::Qualified {
            relation: relation.to_string(),
            column: column.to_string(),
        }
    }

    #[test]
    fn test_scope_indexes_follow_push_order() {
        let scope = joined_scope();

        assert_eq!(
            scope.resolve(Some("o"), "total").unwrap(),
            (6, DataType::Float64)
        );
        assert_eq!(
            scope.resolve(Some("u"), "name").unwrap(),
            (1, DataType::Text)
        );
        assert_eq!(scope.len(), 7);
    }

    #[test]
    fn test_scope_unqualified_unique_resolves() {
        let scope = joined_scope();

        assert_eq!(scope.resolve(None, "user_id").unwrap().0, 5);
    }

    #[test]
    fn test_scope_unqualified_ambiguous_errors() {
        let scope = joined_scope();

        let err = scope.resolve(None, "id").unwrap_err();
        assert!(err.to_string().contains("ambiguous"), "got: {err}");
        assert!(scope.has_column("id"));
    }

    #[test]
    fn test_scope_unknown_relation_and_column() {
        let scope = joined_scope();

        assert!(scope.resolve(Some("x"), "id").is_err());
        assert!(scope.resolve(Some("u"), "missing").is_err());
        assert!(scope.resolve(None, "missing").is_err());
    }

    #[test]
    fn test_scope_duplicate_relation_errors() {
        let mut scope = BindScope::empty();
        scope.push_relation("u", &users_schema()).unwrap();

        assert!(scope.push_relation("u", &users_schema()).is_err());
    }

    #[test]
    fn test_scope_self_join_distinguishes_by_alias() {
        let pair = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::Text),
        ]);
        let mut scope = BindScope::empty();
        scope.push_relation("a", &pair).unwrap();
        scope.push_relation("b", &pair).unwrap();

        assert_eq!(
            scope.resolve(Some("b"), "id").unwrap().0,
            pair.columns.len()
        );
    }

    #[test]
    fn test_scope_expand_star_is_qualified_in_row_order() {
        let scope = joined_scope();

        let all = scope.expand_star(None).unwrap();
        assert_eq!(all.len(), 7);
        assert_eq!(all[0], (qualified("u", "id"), "id".to_string()));
        assert_eq!(all[6], (qualified("o", "total"), "total".to_string()));

        assert_eq!(scope.expand_star(Some("o")).unwrap().len(), 3);
        assert!(scope.expand_star(Some("x")).is_err());
    }

    #[test]
    fn test_scope_canonicalize_makes_bare_and_qualified_equal() {
        let mut scope = BindScope::empty();
        scope.push_relation("u", &users_schema()).unwrap();
        let canon = |expr: &Expression| scope.canonicalize(expr).unwrap();

        assert_eq!(
            canon(&Expression::Identifier("name".to_string())),
            canon(&qualified("u", "name"))
        );

        let age_plus_one = |left: Expression| Expression::BinaryOp {
            left: Box::new(left),
            op: Operator::Add,
            right: Box::new(Expression::Literal(Value::Int64(1))),
        };
        assert_eq!(
            canon(&age_plus_one(Expression::Identifier("age".to_string()))),
            canon(&age_plus_one(qualified("u", "age")))
        );

        let count = |arg: Expression| Expression::Function {
            name: "COUNT".to_string(),
            args: FunctionArgs::List(vec![arg]),
        };
        assert_eq!(
            canon(&count(Expression::Identifier("id".to_string()))),
            canon(&count(qualified("u", "id")))
        );
    }

    #[test]
    fn test_scope_qualified_names() {
        assert_eq!(
            joined_scope().qualified_names(),
            vec![
                "u.id",
                "u.name",
                "u.dept",
                "u.age",
                "o.id",
                "o.user_id",
                "o.total"
            ]
        );
    }
}
