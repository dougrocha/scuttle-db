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

/// Pages cached by default: 128 MB of 8 KB pages, the same as Postgres' `shared_buffers`.
pub const DEFAULT_CACHE_PAGES: usize = 16_384;

/// How far a page's usage count can climb. A page used this often survives this many
/// sweeps of the clock hand without being used again (Postgres uses the same cap).
const MAX_USAGE: u8 = 5;

/// Caches table pages in memory and writes changed ones back to their files.
///
/// On disk the cache holds at most `capacity` pages. When it is full, a "clock sweep"
/// picks the page to evict: a hand walks the slots, lowering each page's usage count,
/// and evicts the first page whose count is already zero. Pages used often keep a high
/// count and survive, while a page read once by a big scan goes on the next pass.
///
/// Nothing evicts a page while someone holds a reference to it, because evicting needs
/// `&mut self` and the borrow checker forbids that while a `&Page` from this pool is alive.
/// C databases need a pin count per page for this.
#[derive(Debug, Default)]
pub struct BufferPool {
    /// Cached pages. A page keeps its slot until it is evicted.
    frames: Vec<Option<Frame>>,

    /// Which slot holds each cached page, per table.
    slots: HashMap<String, HashMap<PageId, usize>>,

    /// Slots emptied by [`BufferPool::drop_table`], used before evicting anything.
    free_slots: Vec<usize>,

    /// Slots made dirty since the last flush, so a flush doesn't have to check every slot.
    /// May hold slots that were since written or reused; the flush skips those.
    dirty_slots: Vec<usize>,

    /// Where the clock sweep resumes.
    hand: usize,

    /// Most pages cached at once. `None` when in memory, where an evicted page would be
    /// lost because there is no file to write it to.
    capacity: Option<usize>,

    /// Pages per table, see [`BufferPool::page_count`].
    page_counts: HashMap<String, PageId>,

    /// Each table file, opened once on first use and kept open, see [`BufferPool::file`].
    files: HashMap<String, File>,

    /// Directory holding the `.table` files. `None` keeps every page in memory,
    /// which is how the database runs in the browser.
    data_directory: Option<PathBuf>,

    stats: CacheStats,
}

#[derive(Debug)]
struct Frame {
    table_name: String,
    page_id: PageId,
    page: Page,
    usage: u8,

    /// Changed since it was last written to its file.
    dirty: bool,
}

/// How many page requests the cache answered from memory and how many it read from disk.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub reads: u64,
}

impl BufferPool {
    pub fn new(data_directory: impl Into<PathBuf>) -> Self {
        Self::with_capacity(data_directory, DEFAULT_CACHE_PAGES)
    }

    /// A pool that caches at most `pages` pages (at least one).
    pub fn with_capacity(data_directory: impl Into<PathBuf>, pages: usize) -> Self {
        Self {
            capacity: Some(pages.max(1)),
            data_directory: Some(data_directory.into()),
            ..Self::default()
        }
    }

    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Requests answered so far. Read it before and after something to see what it cost.
    pub fn stats(&self) -> CacheStats {
        self.stats
    }

    /// The file holding `name`: `<name>.table` for a table. A name with an extension,
    /// such as an index's `users_id.index`, is the file name. Table names can't contain a
    /// dot, so the two never clash.
    fn table_path(&self, name: &str) -> Option<PathBuf> {
        self.data_directory.as_ref().map(|dir| {
            if name.contains('.') {
                dir.join(name)
            } else {
                dir.join(format!("{name}.table"))
            }
        })
    }

    /// A page to read, loaded from its file the first time.
    pub fn get_page(&mut self, table_name: &str, page_id: PageId) -> Result<&Page> {
        let slot = self.load(table_name, page_id)?;
        Ok(&self.frame(slot).page)
    }

    /// A page to change. It is marked dirty here, so the change is written by the next
    /// [`BufferPool::flush`], or when it is evicted, and can't be lost by a caller
    /// forgetting to say so.
    pub(crate) fn get_page_mut(&mut self, table_name: &str, page_id: PageId) -> Result<&mut Page> {
        let slot = self.load(table_name, page_id)?;
        self.mark_dirty(slot);
        Ok(&mut self.frame_mut(slot).page)
    }

    /// The slot holding the page, reading it from its file if it isn't cached.
    fn load(&mut self, table_name: &str, page_id: PageId) -> Result<usize> {
        if let Some(&slot) = self
            .slots
            .get(table_name)
            .and_then(|pages| pages.get(&page_id))
        {
            self.stats.hits += 1;
            let frame = self.frame_mut(slot);
            frame.usage = (frame.usage + 1).min(MAX_USAGE);
            return Ok(slot);
        }

        let page = self.load_page_from_file(table_name, page_id)?;
        self.stats.reads += 1;
        self.insert(table_name, page_id, page, false)
    }

    /// Puts a page in a free slot, evicting another page if the cache is full.
    fn insert(
        &mut self,
        table_name: &str,
        page_id: PageId,
        page: Page,
        dirty: bool,
    ) -> Result<usize> {
        let slot = self.free_slot()?;
        let frame = Frame {
            table_name: table_name.to_string(),
            page_id,
            page,
            usage: 1,
            dirty,
        };
        if slot == self.frames.len() {
            self.frames.push(Some(frame));
        } else {
            self.frames[slot] = Some(frame);
        }
        if dirty {
            self.dirty_slots.push(slot);
        }

        self.slots
            .entry(table_name.to_string())
            .or_default()
            .insert(page_id, slot);
        Ok(slot)
    }

    /// An empty slot: one freed by a drop, a new one while under capacity, or else the
    /// slot of the page the clock sweep evicts.
    fn free_slot(&mut self) -> Result<usize> {
        if let Some(slot) = self.free_slots.pop() {
            return Ok(slot);
        }
        if self
            .capacity
            .is_none_or(|capacity| self.frames.len() < capacity)
        {
            return Ok(self.frames.len());
        }

        loop {
            let slot = self.hand;
            self.hand = (self.hand + 1) % self.frames.len();

            let frame = self.frame_mut(slot);
            if frame.usage > 0 {
                frame.usage -= 1;
                continue;
            }

            if frame.dirty {
                self.write_frame(slot)?;
            }
            let frame = self.frames[slot].take().expect("only full slots are swept");
            if let Some(pages) = self.slots.get_mut(&frame.table_name) {
                pages.remove(&frame.page_id);
            }
            return Ok(slot);
        }
    }

    fn mark_dirty(&mut self, slot: usize) {
        let frame = self.frame_mut(slot);
        if !frame.dirty {
            frame.dirty = true;
            self.dirty_slots.push(slot);
        }
    }

    fn frame(&self, slot: usize) -> &Frame {
        self.frames[slot]
            .as_ref()
            .expect("slots only point at full frames")
    }

    fn frame_mut(&mut self, slot: usize) -> &mut Frame {
        self.frames[slot]
            .as_mut()
            .expect("slots only point at full frames")
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
    /// up to date by [`BufferPool::allocate_page`], which is the only place pages are created.
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

    /// Returns a page with room for a tuple of `size` bytes, marked dirty because the
    /// caller is about to add the tuple.
    ///
    /// Tables are append-only: only the last page is considered, and a new page is
    /// added when it is full. Space freed by DELETE is never reused.
    pub(crate) fn get_free_page(&mut self, table_name: &str, size: usize) -> Result<&mut Page> {
        let page_count = self.page_count(table_name);

        if let Some(last_page_id) = page_count.checked_sub(1)
            && self.get_page(table_name, last_page_id)?.has_room_for(size)
        {
            return self.get_page_mut(table_name, last_page_id);
        }

        if !Page::new(page_count, PageType::Table).has_room_for(size) {
            return Err(miette!("A tuple of {size} bytes does not fit in a page."));
        }

        let page_id = self.allocate_page(table_name, PageType::Table)?;
        self.get_page_mut(table_name, page_id)
    }

    /// Adds an empty page to the end of `name`'s file and returns its id. It is written
    /// to the file by the next flush or when evicted.
    pub(crate) fn allocate_page(&mut self, name: &str, page_type: PageType) -> Result<PageId> {
        let page_id = self.page_count(name);
        self.insert(name, page_id, Page::new(page_id, page_type), true)?;
        self.page_counts.insert(name.to_string(), page_id + 1);
        Ok(page_id)
    }

    /// Writes one page to disk now. Used for the catalog, whose rows must be saved
    /// as soon as a table is created.
    pub(crate) fn save_page(&mut self, table_name: &str, page_id: PageId) -> Result<()> {
        let slot = self.load(table_name, page_id)?;
        self.write_frame(slot)
    }

    /// Writes every dirty page, in file order, so each file is written front to back.
    pub(crate) fn flush(&mut self) -> Result<()> {
        let mut dirty: Vec<usize> = std::mem::take(&mut self.dirty_slots)
            .into_iter()
            .filter(|&slot| self.frames[slot].as_ref().is_some_and(|frame| frame.dirty))
            .collect();
        dirty.sort_by_key(|&slot| {
            let frame = self.frame(slot);
            (frame.table_name.clone(), frame.page_id)
        });

        // A slot evicted and filled again with another dirty page is listed twice
        dirty.dedup();
        for slot in dirty {
            self.write_frame(slot)?;
        }
        Ok(())
    }

    /// Writes a cached page to its offset in the table file and marks it clean.
    fn write_frame(&mut self, slot: usize) -> Result<()> {
        let table_name = self.frame(slot).table_name.clone();
        if self.file(&table_name, true)?.is_some() {
            // Borrow the fields directly, so the file and the page can be used at once
            let file = self.files.get_mut(&table_name).expect("opened above");
            let frame = self.frames[slot]
                .as_ref()
                .expect("slots only point at full frames");

            let offset = (frame.page_id as usize) * Page::SIZE;
            file.seek(SeekFrom::Start(offset as u64))
                .into_diagnostic()?;
            file.write_all(&frame.page.to_bytes()).into_diagnostic()?;
        }

        self.frame_mut(slot).dirty = false;
        Ok(())
    }

    /// Forgets every page of `table_name` and deletes its file, if it has one.
    pub(crate) fn drop_table(&mut self, table_name: &str) -> Result<()> {
        for (_, slot) in self.slots.remove(table_name).unwrap_or_default() {
            self.frames[slot] = None;
            self.free_slots.push(slot);
        }
        self.page_counts.remove(table_name);
        self.files.remove(table_name);

        match self.table_path(table_name).map(std::fs::remove_file) {
            Some(Err(err)) if err.kind() != std::io::ErrorKind::NotFound => {
                Err(err).into_diagnostic()
            }
            _ => Ok(()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("scuttle-pool-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Fills `pages` pages of `t`, each with one item holding its own page number.
    fn fill(pool: &mut BufferPool, pages: u8) {
        for n in 0..pages {
            // Bigger than half a page, so every item starts a new page
            let item = vec![n; Page::SIZE / 2 + 1];
            pool.get_free_page("t", item.len())
                .unwrap()
                .add_data(&item)
                .unwrap();
        }
    }

    fn first_byte(pool: &mut BufferPool, page_id: PageId) -> u8 {
        let page = pool.get_page("t", page_id).unwrap();
        let pointer = page.item_pointers().next().unwrap();
        page.data[pointer.offset as usize - crate::storage::page::PageHeader::SIZE]
    }

    #[test]
    fn test_a_full_pool_evicts_and_writes_back_changed_pages() {
        let mut pool = BufferPool::with_capacity(temp_dir("evict"), 2);
        fill(&mut pool, 6);

        // Only two pages fit, so the other four were written out when evicted
        assert!(pool.frames.len() <= 2);
        for page_id in 0..6 {
            assert_eq!(first_byte(&mut pool, page_id), page_id as u8);
        }
        assert!(pool.stats().reads >= 4, "{:?}", pool.stats());
    }

    #[test]
    fn test_pages_used_often_outlive_pages_read_once() {
        let mut pool = BufferPool::with_capacity(temp_dir("clock"), 3);
        fill(&mut pool, 8);
        pool.flush().unwrap();

        // Page 0 is used five times, then more pages than the pool holds are read once each.
        // Without usage counts, page 0 would be the oldest page and go first.
        for _ in 0..5 {
            first_byte(&mut pool, 0);
        }
        for page_id in 1..5 {
            first_byte(&mut pool, page_id);
        }

        let before = pool.stats();
        first_byte(&mut pool, 0);
        assert_eq!(pool.stats().hits, before.hits + 1, "page 0 was evicted");
    }
}
