use miette::Result;
use std::{
    fs::File,
    path::{Path, PathBuf},
};

use bincode::{decode_from_std_read, encode_into_std_write};

use crate::{
    DatabaseError,
    buffer_manager::BufferManager,
    table::{CatalogTable, ColumnDefinition, DataType, Schema, TableStruct},
};

#[derive(Debug)]
pub struct Database {
    pub tables: std::collections::BTreeMap<String, TableStruct>,
    pub catalog_tables: std::collections::HashMap<String, CatalogTable>,
    pub buffer_manager: BufferManager,

    data_directory: PathBuf,
}

impl Database {
    pub fn new<P: AsRef<Path>>(data_directory: P) -> Self {
        let mut db = Self {
            tables: std::collections::BTreeMap::default(),
            catalog_tables: std::collections::HashMap::default(),
            buffer_manager: BufferManager::new(),

            data_directory: data_directory.as_ref().to_path_buf(),
        };

        db.init_catalog_pages().expect("Failed to init catalog");

        db
    }

    fn init_catalog_pages(&mut self) -> Result<()> {
        let tables_schema = Schema::new(vec![
            ColumnDefinition::new("table_name", DataType::Text, false),
            ColumnDefinition::new("root_page_id", DataType::Integer, false),
        ]);

        let tables = CatalogTable {
            name: "tables".to_string(),
            schema: tables_schema,
            page_id: 0,
        };

        self.catalog_tables.insert(tables.name.clone(), tables);

        Ok(())
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<(), DatabaseError> {
        if self.tables.contains_key(&name) {
            return Err(DatabaseError::InvalidQuery(format!(
                "Table {} already exists",
                name
            )));
        }

        let table = TableStruct::new(name.clone(), schema);
        self.tables.insert(name, table);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&TableStruct, DatabaseError> {
        self.tables
            .get(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }

    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableStruct, DatabaseError> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }

    pub fn save(&self) -> Result<(), DatabaseError> {
        std::fs::create_dir_all(&self.data_directory)?;

        for table in self.tables.values() {
            let table_path = self.data_directory.join(format!("{}.table", table.name));
            let mut file = File::create(table_path)?;

            let _ = encode_into_std_write(&table, &mut file, bincode::config::standard())?;
        }

        Ok(())
    }

    pub fn load_from_file(&mut self) -> Result<(), DatabaseError> {
        std::fs::create_dir_all(&self.data_directory)?;
        let entries = std::fs::read_dir(&self.data_directory)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("table") {
                let mut file = File::open(&path)?;
                let table: TableStruct =
                    decode_from_std_read(&mut file, bincode::config::standard())?;

                println!("Table: {:#?}", table);

                self.tables.insert(table.name.clone(), table);
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
}
