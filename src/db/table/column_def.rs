use crate::{core::types::DataType, sql::ast::expression::Expression};

/// Definition of a single column in a table schema.
///
/// Specifies the column name, data type, and whether NULL values are allowed.
#[derive(Debug, Clone, PartialEq)]
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

    pub fn has_constraint(&self, constraint: ColumnConstraint) -> bool {
        self.constraints.contains(&constraint)
    }

    pub(crate) fn can_be_omitted(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::Default(_) | ColumnConstraint::Nullable))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    Nullable,
    PrimaryKey,
    Unique,
    Default(Expression),
}
