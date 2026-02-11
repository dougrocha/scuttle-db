use miette::{IntoDiagnostic, Result};

use crate::db::{database::Database, table::table_def::TableDef};

pub struct CatalogContext<'db> {
    pub database: &'db mut Database,
}

impl<'db> CatalogContext<'db> {
    pub(crate) fn new(database: &'db mut Database) -> Self {
        Self { database }
    }

    pub fn get_table(&self, table_name: &str) -> Result<&TableDef> {
        self.database.get_table(table_name).into_diagnostic()
    }
}
