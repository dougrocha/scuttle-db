use miette::{IntoDiagnostic, Result, miette};

use crate::{
    ColumnDef, DataType, DatabaseError,
    core::types::Value,
    db::table::{Table, row::Row, schema::Schema},
    storage::{buffer_pool::BufferPool, page::PageHeader},
};

#[derive(Debug)]
pub struct SystemCatalog {
    /// The schema defining the structure of the table
    pub schema: Schema,
}

impl SystemCatalog {
    pub(crate) fn new() -> Self {
        Self {
            schema: Self::catalog_schema(),
        }
    }

    fn catalog_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("table_name", DataType::VarChar(255)),
                ColumnDef::no_constraints("schema_json", DataType::Text),
                ColumnDef::no_constraints("page_count", DataType::Int64),
                // ColumnDef::no_constraints("created_at", DataType::Timestamp),
            ],
        }
    }

    pub fn save_table_metadata(
        &self,
        buffer_pool: &mut BufferPool,
        name: &str,
        schema: &Schema,
        page_count: u32,
    ) -> Result<()> {
        let schema = Row::new(vec![
            Value::Text(name.to_string()),
            Value::Text(serde_json::to_string(schema).into_diagnostic()?),
            Value::Int64(page_count as i64),
            // Value::Int64(current_timestamp()),
        ]);

        let encoded = self.schema.encode_row(&schema);

        let page_header_id = {
            let page = buffer_pool.get_free_page(self.name(), encoded.len())?;
            let _ = page.add_data(&encoded);
            page.header.page_id
        };
        buffer_pool.save_page(self.name(), page_header_id)?;

        Ok(())
    }

    pub fn load_all_tables(&self, buffer_pool: &mut BufferPool) -> Result<Vec<(String, Schema)>> {
        let mut tables = vec![];

        for page_id in 0..100 {
            let page = match buffer_pool.get_page(self.name(), page_id) {
                Ok(p) => p,
                Err(_) => break,
            };

            for item_pointer in page.item_pointers() {
                if item_pointer.is_deleted() {
                    continue;
                }

                let offset = item_pointer.offset as usize - PageHeader::SIZE;
                let length = item_pointer.length as usize;
                let item_data = &page.data[offset..offset + length];

                let row = self.schema.decode_row(item_data)?;

                let table_name = match &row.values[0] {
                    Value::Text(s) => s.clone(),
                    _ => return Err(miette!("Invalid catalog")),
                };

                let schema_json = match &row.values[1] {
                    Value::Text(s) => s.as_str(),
                    _ => return Err(miette!("Invalid catalog")),
                };

                let _page_count = match row.values[2] {
                    Value::Int64(n) => n as u32,
                    _ => return Err(miette!("Invalid catalog")),
                };

                let schema: Schema = serde_json::from_str(schema_json).into_diagnostic()?;
                tables.push((table_name, schema));
            }
        }

        Ok(tables)
    }

    pub fn remove_table(name: &str) -> Result<bool> {
        todo!()
    }
}

impl Default for SystemCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Table for SystemCatalog {
    fn name(&self) -> &str {
        "__catalog__"
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
