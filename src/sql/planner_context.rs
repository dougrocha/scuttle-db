use miette::{IntoDiagnostic, Result};

use crate::{Relation, db::database::Database};

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
}
