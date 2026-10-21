use miette::{Result, miette};

use crate::{
    db::table::{index::Ctid, row::Row, schema::Schema},
    storage::{
        buffer_pool::BufferPool,
        page::{ItemId, ItemPointer, Page, PageHeader, PageId},
    },
};

/// Reads a table front to back, one page per call. This is the only code that walks
/// heap pages and decodes tuples.
///
/// It holds no borrow of the pool between calls, so any number of cursors over the same
/// table can be open at once (a self-join has two).
///
/// It reads only the pages that existed when it was opened, so it never sees pages added
/// later. It does see rows appended to those earlier pages, so it is not a snapshot. A
/// statement that writes the table it scans has to read everything first.
#[derive(Debug)]
pub(crate) struct TableCursor {
    table_name: String,
    schema: Schema,
    next_page: PageId,
    end_page: PageId,
}

impl TableCursor {
    pub(crate) fn open(pool: &mut BufferPool, table_name: String, schema: Schema) -> Self {
        let end_page = pool.page_count(&table_name);
        Self {
            table_name,
            schema,
            next_page: 0,
            end_page,
        }
    }

    /// Decodes every live row on the next unread page and passes it to `emit`, along
    /// with where it is stored (PostgreSQL's `ctid`).
    ///
    /// Returns `false` once there are no pages left. `true` can still mean nothing was
    /// emitted, when every row on the page was deleted, so callers loop instead of
    /// treating an empty page as the end of the table.
    pub(crate) fn read_next_page(
        &mut self,
        pool: &mut BufferPool,
        mut emit: impl FnMut(PageId, ItemId, Row) -> Result<()>,
    ) -> Result<bool> {
        if self.next_page >= self.end_page {
            return Ok(false);
        }
        let page_id = self.next_page;
        self.next_page += 1;

        let page = pool.get_page(&self.table_name, page_id)?;

        // The item id is the pointer's index, so enumerate before skipping deleted items
        for (item_id, item_pointer) in page.item_pointers().enumerate() {
            if item_pointer.is_deleted() {
                continue;
            }

            let row = decode_item(page, item_pointer, &self.schema)?;
            emit(page_id, item_id as ItemId, row)?;
        }

        Ok(true)
    }
}

/// Reads the row stored at `ctid`, or `None` if it was deleted. Index scans use this to
/// fetch the rows an index points to.
pub(crate) fn fetch_row(
    pool: &mut BufferPool,
    table_name: &str,
    schema: &Schema,
    (page_id, item_id): Ctid,
) -> Result<Option<Row>> {
    let page = pool.get_page(table_name, page_id)?;
    let item_pointer = page
        .item_pointers()
        .nth(item_id as usize)
        .ok_or_else(|| miette!("No item {item_id} on page {page_id} of {table_name}."))?;

    if item_pointer.is_deleted() {
        return Ok(None);
    }
    decode_item(page, item_pointer, schema).map(Some)
}

fn decode_item(page: &Page, item_pointer: ItemPointer, schema: &Schema) -> Result<Row> {
    let offset = item_pointer.offset as usize - PageHeader::SIZE;
    let length = item_pointer.length as usize;
    schema.decode_row(&page.data[offset..offset + length])
}
