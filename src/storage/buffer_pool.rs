use std::{
    collections::{BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use miette::{IntoDiagnostic, Result, miette};

use crate::{
    core::serialization::Serializable,
    storage::page::{Page, PageId, PageType},
};

#[derive(Debug, Default)]
pub struct BufferPool {
    pool: HashMap<String, HashMap<PageId, Page>>,

    /// Pages per table, see [`BufferPool::page_count`].
    page_counts: HashMap<String, PageId>,

    /// Pages changed since the last [`BufferPool::flush`], per table. Always empty in memory.
    dirty: HashMap<String, BTreeSet<PageId>>,

    /// Each table file, opened once on first use and kept open, see [`BufferPool::file`].
    files: HashMap<String, File>,

    /// Directory holding the `.table` files. `None` keeps every page in memory,
    /// which is how the database runs in the browser.
    data_directory: Option<PathBuf>,
}

impl BufferPool {
    pub fn new(data_directory: impl Into<PathBuf>) -> Self {
        Self {
            pool: HashMap::new(),
            page_counts: HashMap::new(),
            dirty: HashMap::new(),
            files: HashMap::new(),
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

    fn load_page_from_file(&mut self, table_name: &str, page_id: PageId) -> Result<Page> {
        let file = self
            .file(table_name, false)?
            .ok_or_else(|| miette!("Page {page_id} of {table_name} is not in memory."))?;

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

    /// Writes one page to disk now. Used for the catalog, whose rows must be saved
    /// as soon as a table is created.
    pub(crate) fn save_page(&mut self, table_name: &str, page_id: PageId) -> Result<()> {
        if self.data_directory.is_none() {
            // In-memory pages already live in the pool, so there is nothing to write.
            return Ok(());
        }

        self.get_page(table_name, page_id)?;
        self.flush_pages(table_name, [page_id])
    }

    /// Records that a page changed, so the next [`BufferPool::flush`] writes it.
    ///
    /// The page itself is already updated in the pool. Marking it is cheap, so a
    /// statement can change the same page many times and it is still written once.
    pub(crate) fn mark_dirty(&mut self, table_name: &str, page_id: PageId) {
        if self.data_directory.is_none() {
            // Nothing is ever written in memory, so don't collect pages that would never be flushed.
            return;
        }

        self.dirty
            .entry(table_name.to_string())
            .or_default()
            .insert(page_id);
    }

    /// Writes every page marked dirty since the last flush, opening each table file once.
    pub(crate) fn flush(&mut self) -> Result<()> {
        for (table_name, page_ids) in std::mem::take(&mut self.dirty) {
            // BTreeSet iterates in order, so the file is written front to back
            self.flush_pages(&table_name, page_ids)?;
        }

        Ok(())
    }

    /// Writes the given pages of a table, each at its own offset in the table file.
    fn flush_pages(
        &mut self,
        table_name: &str,
        page_ids: impl IntoIterator<Item = PageId>,
    ) -> Result<()> {
        if self.file(table_name, true)?.is_none() {
            return Ok(());
        }
        // Borrow the fields directly, so the file and the pages can be used at the same time
        let file = self.files.get_mut(table_name).expect("opened above");

        for page_id in page_ids {
            let page = self
                .pool
                .get(table_name)
                .and_then(|pages| pages.get(&page_id))
                .ok_or_else(|| miette!("Dirty page {page_id} of {table_name} is not in memory."))?;

            let offset = (page_id as usize) * Page::SIZE;
            file.seek(SeekFrom::Start(offset as u64))
                .into_diagnostic()?;
            file.write_all(&page.to_bytes()).into_diagnostic()?;
        }

        Ok(())
    }

    /// The open file of a table, opening it on first use. `None` when the pool is in memory.
    ///
    /// Reads pass `create: false`, so asking for a page of a table that has no file is an
    /// error instead of leaving an empty file behind.
    fn file(&mut self, table_name: &str, create: bool) -> Result<Option<&mut File>> {
        let Some(path) = self.table_path(table_name) else {
            return Ok(None);
        };

        if !self.files.contains_key(table_name) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(create)
                .truncate(false)
                .open(path)
                .into_diagnostic()?;
            self.files.insert(table_name.to_string(), file);
        }

        Ok(self.files.get_mut(table_name))
    }
}
