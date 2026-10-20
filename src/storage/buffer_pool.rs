use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use miette::{IntoDiagnostic, Result, miette};

use crate::{
    core::serialization::Serializable,
    storage::page::{Page, PageId, PageType},
};

#[derive(Debug)]
pub struct TableFile {
    pub table_id: u32,
    pub table_name: String,
    pub file_path: PathBuf,
    pub page_count: u32,
}

#[derive(Debug, Default)]
pub struct BufferPool {
    pool: HashMap<String, HashMap<PageId, Page>>,

    /// Pages per table, see [`BufferPool::page_count`].
    page_counts: HashMap<String, PageId>,

    /// Directory holding the `.table` files. `None` keeps every page in memory,
    /// which is how the database runs in the browser.
    data_directory: Option<PathBuf>,
}

impl BufferPool {
    pub fn new(data_directory: impl Into<PathBuf>) -> Self {
        Self {
            pool: HashMap::new(),
            page_counts: HashMap::new(),
            data_directory: Some(data_directory.into()),
        }
    }

    pub fn in_memory() -> Self {
        Self::default()
    }

    fn table_path(&self, table_name: &str) -> Option<PathBuf> {
        self.data_directory
            .as_ref()
            .map(|dir| dir.join(format!("{table_name}.table")))
    }

    pub fn get_page(&mut self, table_name: &str, page_id: PageId) -> Result<&mut Page> {
        // Check if page exists in cache first
        let page_exists = self
            .pool
            .get(table_name)
            .is_some_and(|pages| pages.contains_key(&page_id));

        if !page_exists {
            let page = self.load_page_from_file(table_name, page_id)?;

            // Insert into cache
            self.pool
                .entry(table_name.to_string())
                .or_default()
                .insert(page_id, page);
        }

        // Return mutable reference to the cached page
        Ok(self
            .pool
            .get_mut(table_name)
            .unwrap()
            .get_mut(&page_id)
            .unwrap())
    }

    fn load_page_from_file(&self, table_name: &str, page_id: PageId) -> Result<Page> {
        let path = self
            .table_path(table_name)
            .ok_or_else(|| miette!("Page {page_id} of {table_name} is not in memory."))?;
        let mut file = File::open(path).into_diagnostic()?;

        let mut buffer: [u8; Page::SIZE] = [0; Page::SIZE];
        let offset = (page_id as usize) * Page::SIZE;
        file.seek(SeekFrom::Start(offset as u64))
            .into_diagnostic()?;
        file.read_exact(&mut buffer).into_diagnostic()?;

        Ok(Page::from_bytes(buffer))
    }

    /// Number of pages the table has, counting pages created but not yet written to disk.
    ///
    /// Seeded from the size of the table file the first time it is asked for, then kept
    /// up to date by [`BufferPool::get_free_page`], which is the only place pages are created.
    pub fn page_count(&mut self, table_name: &str) -> PageId {
        if let Some(&count) = self.page_counts.get(table_name) {
            return count;
        }

        let on_disk = self
            .table_path(table_name)
            .and_then(|path| std::fs::metadata(path).ok())
            .map_or(0, |meta| (meta.len() / Page::SIZE as u64) as PageId);
        self.page_counts.insert(table_name.to_string(), on_disk);
        on_disk
    }

    /// Returns a page with room for a tuple of `size` bytes.
    ///
    /// Tables are append-only: only the last page is considered, and a new page is
    /// added when it is full. Space freed by DELETE is never reused.
    pub(crate) fn get_free_page(&mut self, table_name: &str, size: usize) -> Result<&mut Page> {
        let page_count = self.page_count(table_name);

        if let Some(last_page_id) = page_count.checked_sub(1)
            && self.get_page(table_name, last_page_id)?.has_room_for(size)
        {
            return self.get_page(table_name, last_page_id);
        }

        let page = Page::new(page_count, PageType::Table);
        if !page.has_room_for(size) {
            return Err(miette!("A tuple of {size} bytes does not fit in a page."));
        }
        self.page_counts
            .insert(table_name.to_string(), page_count + 1);

        Ok(self
            .pool
            .entry(table_name.to_string())
            .or_default()
            .entry(page_count)
            .or_insert(page))
    }

    pub(crate) fn save_page(&mut self, table_name: &str, page_id: PageId) -> Result<()> {
        let Some(path) = self.table_path(table_name) else {
            // In-memory pages already live in the pool, so there is nothing to write.
            return Ok(());
        };

        let page = self.get_page(table_name, page_id)?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .into_diagnostic()?;

        let offset = (page_id as usize) * Page::SIZE;
        file.seek(SeekFrom::Start(offset as u64))
            .into_diagnostic()?;
        file.write_all(&page.to_bytes()).into_diagnostic()?;

        Ok(())
    }
}
