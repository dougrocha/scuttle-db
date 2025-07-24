use miette::{Result, miette};
use std::{collections::HashMap, path::PathBuf};

use crate::page::{Page, PageType};

#[derive(Debug)]
pub struct TableFile {
    pub table_id: u32,
    pub table_name: String,
    pub file_path: PathBuf,
    pub page_count: u32,
}

#[derive(Debug)]
pub struct BufferManager {
    buffer_pool: HashMap<u32, HashMap<u32, Page>>,
    table_files: HashMap<u32, TableFile>,
}

impl BufferManager {
    pub fn new() -> Self {
        Self {
            buffer_pool: HashMap::new(),
            table_files: HashMap::new(),
        }
    }

    pub fn get_page(&mut self, table_id: u32, page_id: u32) -> Result<&mut Page> {
        // Check if page exists in cache first (using immutable borrow)
        let page_exists = self
            .buffer_pool
            .get(&table_id)
            .map(|pages| pages.contains_key(&page_id))
            .unwrap_or(false);

        if !page_exists {
            // Page not in cache, need to load from disk
            let file_path = self
                .table_files
                .get(&table_id)
                .map(|tf| tf.file_path.clone())
                .ok_or(miette!(format!("Table not found: {table_id}")))?;

            let page = self.load_page_from_file(&file_path, page_id)?;

            // Insert into cache
            self.buffer_pool
                .entry(table_id)
                .or_default()
                .insert(page_id, page);
        }

        // Return mutable reference to the cached page
        Ok(self
            .buffer_pool
            .get_mut(&table_id)
            .unwrap()
            .get_mut(&page_id)
            .unwrap())
    }

    fn load_page_from_file(&mut self, _file_path: &PathBuf, _page_id: u32) -> Result<Page> {
        // For now, create a dummy page - implement actual file loading later
        Ok(Page::new(_page_id, PageType::Table))
    }
}
