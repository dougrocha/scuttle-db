use crate::{
    DatabaseError,
    page::PageId,
    table::{Row, Schema, Table, Value},
};

#[derive(Debug)]
pub struct SystemCatalog {
    /// The name of the catalog table
    pub name: String,
    /// The schema defining the structure of the table
    pub schema: Schema,
    /// The identifier for the page where table data is stored
    pub page_id: PageId,
}

impl SystemCatalog {
    pub(crate) fn new() -> Self {
        Self {
            name: String::from("system_catalog"),
            schema: Schema::new(Vec::new()),
            page_id: PageId::default(),
        }
    }
}

impl Table for SystemCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn insert_row(&mut self, _row: Row) -> Result<(), DatabaseError> {
        todo!()
    }

    fn get_rows(&self, _column: &str, _value: Value) -> Result<Vec<Row>, DatabaseError> {
        todo!()
    }
}
