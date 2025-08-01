use miette::{IntoDiagnostic, Result, miette};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use crate::{
    Serializable,
    page::{Page, PageId, PageType},
};

#[derive(Debug)]
pub struct TableFile {
    pub table_id: u32,
    pub table_name: String,
    pub file_path: PathBuf,
    pub page_count: u32,
}

#[derive(Debug, Default)]
pub struct BufferManager {
    buffer_pool: HashMap<String, HashMap<PageId, Page>>,
}

impl BufferManager {
    pub fn new() -> Self {
        Self {
            buffer_pool: HashMap::new(),
        }
    }

    pub fn get_page(&mut self, table_name: &str, page_id: PageId) -> Result<&mut Page> {
        // Check if page exists in cache first
        let page_exists = self
            .buffer_pool
            .get(table_name)
            .map(|pages| pages.contains_key(&page_id))
            .unwrap_or(false);

        if !page_exists {
            let page = self.load_page_from_file(table_name, page_id)?;

            // Insert into cache
            self.buffer_pool
                .entry(table_name.to_string())
                .or_default()
                .insert(page_id, page);
        }

        // Return mutable reference to the cached page
        Ok(self
            .buffer_pool
            .get_mut(table_name)
            .unwrap()
            .get_mut(&page_id)
            .unwrap())
    }

    fn load_page_from_file(&self, table_name: &str, page_id: PageId) -> Result<Page> {
        let mut file = File::open(format!("./db/{table_name}.table")).into_diagnostic()?;

        let mut buffer: [u8; Page::SIZE] = [0; Page::SIZE];
        let offset = (page_id as usize) * Page::SIZE;
        file.seek(SeekFrom::Start(offset as u64))
            .into_diagnostic()?;
        file.read_exact(&mut buffer).into_diagnostic()?;

        Ok(Page::from_bytes(buffer))
    }

    pub(crate) fn get_free_page(&mut self, table_name: &str, size: usize) -> Result<&mut Page> {
        let max_pages = 1000;

        // Find a page ID that has space or does not exist
        let mut target_page_id = None;

        for page_id in 0..max_pages {
            let page_exists = self
                .buffer_pool
                .get(table_name)
                .map(|pages| pages.contains_key(&page_id))
                .unwrap_or(false);

            if page_exists {
                // Check if existing page has space
                let has_space = self
                    .buffer_pool
                    .get(table_name)
                    .unwrap()
                    .get(&page_id)
                    .unwrap()
                    .free_space()
                    > size;

                if has_space {
                    target_page_id = Some(page_id);
                    break;
                }
            } else {
                // Page does not exist so we use the last page ID
                target_page_id = Some(page_id);
                break;
            }
        }

        let page_id = target_page_id.ok_or_else(|| miette!("No free page available."))?;

        let page_exists = self
            .buffer_pool
            .get(table_name)
            .map(|pages| pages.contains_key(&page_id))
            .unwrap_or(false);

        if !page_exists {
            match self.load_page_from_file(table_name, page_id) {
                Ok(loaded_page) => {
                    self.buffer_pool
                        .entry(table_name.to_string())
                        .or_default()
                        .insert(page_id, loaded_page);
                }
                Err(_) => {
                    // Create new page
                    let new_page = Page::new(page_id, PageType::Table);
                    self.buffer_pool
                        .entry(table_name.to_string())
                        .or_default()
                        .insert(page_id, new_page);
                }
            }
        }

        Ok(self
            .buffer_pool
            .get_mut(table_name)
            .unwrap()
            .get_mut(&page_id)
            .unwrap())
    }

    pub(crate) fn save_page(&mut self, table_name: &str, page_id: PageId) -> Result<()> {
        let page = self.get_page(table_name, page_id)?;

        let mut file = File::create(format!("./db/{table_name}.table")).into_diagnostic()?;

        let offset = (page_id as usize) * Page::SIZE;
        file.seek(SeekFrom::Start(offset as u64))
            .into_diagnostic()?;
        file.write_all(&page.to_bytes()).into_diagnostic()?;

        Ok(())
    }
}
