use miette::{IntoDiagnostic, Result};

use crate::{
    Relation,
    db::{
        database::Database,
        table::{Schema, Table},
    },
};

pub struct PlannerContext<'a> {
    database: &'a Database,
}

impl<'a> PlannerContext<'a> {
    pub(crate) fn new(database: &'a Database) -> Self {
        Self { database }
    }

    pub fn get_table(&self, table_name: &str) -> Result<&Relation> {
        self.database.get_table(table_name).into_diagnostic()
    }

    pub fn get_schema(&self, table_name: &str) -> Result<&Schema> {
        let table = self.get_table(table_name)?;

        Ok(table.schema())
    }
}
