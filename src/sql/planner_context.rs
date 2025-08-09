use miette::{IntoDiagnostic, Result};

use crate::db::{
    database::Database,
    table::{Schema, Table},
};

pub struct PlannerContext<'a> {
    database: &'a Database,
}

impl<'a> PlannerContext<'a> {
    pub(crate) fn new(database: &'a Database) -> Self {
        Self { database }
    }

    pub fn get_schema(&self, table_name: &str) -> Result<&Schema> {
        let table = self.database.get_table(table_name).into_diagnostic()?;

        Ok(table.schema())
    }
}
