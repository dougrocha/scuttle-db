use miette::{IntoDiagnostic, Result};

use crate::{Relation, db::database::Database};

pub struct PlannerContext<'db> {
    pub database: &'db mut Database,
}

impl<'db> PlannerContext<'db> {
    pub(crate) fn new(database: &'db mut Database) -> Self {
        Self { database }
    }

    pub fn get_table(&self, table_name: &str) -> Result<&Relation> {
        self.database.get_table(table_name).into_diagnostic()
    }
}
