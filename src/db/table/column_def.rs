use serde::{Deserialize, Serialize};

use crate::{core::types::DataType, sql::ast::expression::Expression};

/// Definition of a single column in a table schema.
///
/// Specifies the column name, data type, and whether NULL values are allowed.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

impl ColumnDef {
    /// Creates a new column definition.
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
            constraints: vec![ColumnConstraint::NotNull],
        }
    }

    pub fn no_constraints(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
            constraints: vec![],
        }
    }

    pub fn with_constraints(
        name: &str,
        data_type: DataType,
        constraints: Vec<ColumnConstraint>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
            constraints,
        }
    }

    /// Whether the column accepts NULL.
    ///
    /// As in SQL, columns are nullable unless declared `NOT NULL` or `PRIMARY KEY`.
    pub(crate) fn is_nullable(&self) -> bool {
        !self
            .constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::NotNull | ColumnConstraint::PrimaryKey))
    }

    /// Whether an INSERT may leave this column out (it gets its default or NULL).
    pub(crate) fn can_be_omitted(&self) -> bool {
        self.has_default() || self.is_nullable()
    }

    pub(crate) fn has_default(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::Default(_)))
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum ColumnConstraint {
    NotNull,
    Nullable,
    PrimaryKey,
    Unique,
    Default(()),
}
