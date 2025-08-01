use miette::Result;
use std::{
    fs::File,
    path::{Path, PathBuf},
};

use crate::{
    DatabaseError,
    buffer_manager::BufferManager,
    page::{ItemId, PageHeader, PageId},
    parser::{ColumnList, ExecutionPlan, SqlParser},
    table::{Relation, Row, Schema, Value},
};

#[derive(Debug)]
pub struct Database {
    pub tables: std::collections::BTreeMap<String, Relation>,
    pub buffer_manager: BufferManager,

    data_directory: PathBuf,
}

impl Database {
    pub fn new<P: AsRef<Path>>(data_directory: P) -> Self {
        let data_dir = data_directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).ok();

        Self {
            tables: std::collections::BTreeMap::default(),
            buffer_manager: BufferManager::new(),

            data_directory: data_directory.as_ref().to_path_buf(),
        }
    }

    pub fn initialize(&mut self) -> Result<()> {
        Ok(())
    }

    fn table_exists(&self, name: &str) -> bool {
        if self.tables.contains_key(name) {
            true
        } else {
            let table_path = self.data_directory.join(format!("{name}.table"));
            table_path.exists()
        }
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<(), DatabaseError> {
        if self.table_exists(name) {
            // return Err(DatabaseError::InvalidQuery(format!(
            //     "Table {name} already exists"
            // )));
            // Eventually save table information in a catalog table,
            // but for now just load the table with the schema normally
            println!("Table {name} already exists");
        }

        let table = Relation::new(name.to_string(), schema);
        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&Relation, DatabaseError> {
        self.tables
            .get(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }

    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Relation, DatabaseError> {
        if self.table_exists(name) {
            return Ok(self.tables.get_mut(name).unwrap());
        }

        Err(DatabaseError::TableNotFound(name.to_string()))
    }

    pub fn load_from_file(&mut self) -> Result<(), DatabaseError> {
        std::fs::create_dir_all(&self.data_directory)?;
        let entries = std::fs::read_dir(&self.data_directory)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("table") {
                let mut file = File::open(&path)?;
                // TODO: Fix this decode from file
                // let table: Relation = decode_from_std_read(&mut file, bincode::config::standard())?;

                // println!("Table: {table:#?}");
                //
                // self.tables.insert(table.name.clone(), table);
            }
        }

        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), DatabaseError> {
        self.tables
            .remove(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))?;
        Ok(())
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(PageId, ItemId)> {
        println!("Inserting row into table: {table_name}");

        // Get schema first (separate borrow scope)
        let encoded_data = {
            let table = self.get_table(table_name).unwrap();
            table.schema.encode_row(row)
        };

        // Now get the page and insert data
        let free_page = self
            .buffer_manager
            .get_free_page(table_name, encoded_data.len())?;
        let page_id = free_page.header.page_id;

        let item_id = free_page.add_data(&encoded_data)?;

        // Note: save_page method needs to be implemented in BufferManager
        self.buffer_manager.save_page(table_name, page_id)?;

        Ok((page_id, item_id))
    }

    pub fn get_rows(&mut self, table_name: &str) -> Result<Vec<Row>, DatabaseError> {
        println!("Getting rows from table: {table_name}");

        let mut found_rows: Vec<Row> = Vec::new();
        let max_pages = 1000; // To prevent infinite loops

        for current_page_id in 0..max_pages {
            // Use get_page instead of get_page_mut (which doesn't exist)
            let page_res = self.buffer_manager.get_page(table_name, current_page_id);

            let page = match page_res {
                Ok(page) => page,
                Err(_) => break,
            };

            for item_pointer in page.item_pointers() {
                if item_pointer.is_deleted() {
                    continue;
                }

                let offset = item_pointer.offset as usize - PageHeader::SIZE as usize;
                let length = item_pointer.length as usize;

                let item_data = &page.data[offset..offset + length];
                let decoded_row = self
                    .tables
                    .get(table_name)
                    .unwrap()
                    .schema
                    .decode_row(item_data)
                    .expect("Row should be decoded");

                found_rows.push(decoded_row);
            }
        }

        Ok(found_rows)
    }

    pub fn execute_query(&mut self, query: &str) -> Result<Vec<Row>, DatabaseError> {
        let mut parser = SqlParser::new(query);
        let statement = parser
            .parse()
            .map_err(|e| DatabaseError::InvalidQuery(format!("Parse error: {}", e)))?;

        let plan = ExecutionPlan::from_statement(statement)
            .map_err(|e| DatabaseError::InvalidQuery(format!("Plan error: {}", e)))?;

        self.execute_plan(plan)
    }

    fn execute_plan(&mut self, plan: ExecutionPlan) -> Result<Vec<Row>, DatabaseError> {
        match plan {
            ExecutionPlan::TableScan {
                table_name,
                columns,
            } => {
                let all_rows = self.get_rows(&table_name)?;

                match columns {
                    ColumnList::All => Ok(all_rows),
                    ColumnList::Columns(column_names) => {
                        let table = self.get_table(&table_name)?;
                        let column_indices: Result<Vec<usize>, DatabaseError> = column_names
                            .iter()
                            .map(|col_name| {
                                table
                                    .schema
                                    .columns
                                    .iter()
                                    .position(|col| col.name == *col_name)
                                    .ok_or_else(|| {
                                        DatabaseError::InvalidQuery(format!(
                                            "Column '{}' not found in table '{}'",
                                            col_name, table_name
                                        ))
                                    })
                            })
                            .collect();

                        let indices = column_indices?;

                        let projected_rows: Vec<Row> = all_rows
                            .into_iter()
                            .map(|row| {
                                let projected_values: Vec<Value> =
                                    indices.iter().map(|&idx| row.values[idx].clone()).collect();
                                Row::new(projected_values)
                            })
                            .collect();

                        Ok(projected_rows)
                    }
                }
            }
        }
    }
}
