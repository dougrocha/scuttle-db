use miette::Result;

const PAGE_SIZE: usize = 8192;

pub type PageId = u32;
pub type ItemId = u16;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    BTreeInternal {
        parent_page_id: Option<PageId>,
        level: u16,
    },
    BTreeLeaf {
        next_leaf_page: Option<PageId>,
        prev_leaf_page: Option<PageId>,
    },
    Table,
    Catalog,
}

#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_id: PageId,     // Logical block number
    pub page_type: PageType, // Type of the page
    pub lower: u16,          // End of item pointers
    pub upper: u16,          // Start of tuple data
    pub item_count: u16,     // Count of items
    pub special: u16,        // Start of special region (e.g., index metadata)
}

impl PageHeader {
    // Follow Postgres Size
    pub const HEADER_SIZE: u16 = 24;

    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        Self {
            page_id,
            page_type,
            lower: Self::HEADER_SIZE,
            upper: PAGE_SIZE as u16,
            special: PAGE_SIZE as u16,
            item_count: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ItemPointer {
    pub offset: u16, // Offset from start of page
    pub length: u16, // Length of tuple
    pub flags: u8,   // Status flags (live/dead)
}

impl ItemPointer {
    const SIZE: usize = std::mem::size_of::<ItemPointer>();

    const DELETED_FLAG: u8 = 0b0000_0001; // Bit 0
    const LIVE_FLAG: u8 = 0b0000_0000; // Default state

    pub fn is_deleted(&self) -> bool {
        self.flags & Self::DELETED_FLAG != 0
    }

    pub fn mark_deleted(&mut self) {
        self.flags |= Self::DELETED_FLAG;
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    pub header: PageHeader,
    pub item_pointers: Vec<ItemPointer>,
    pub data: [u8; PAGE_SIZE - PageHeader::HEADER_SIZE as usize],
}

impl Page {
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let header = PageHeader::new(page_id, page_type);

        Self {
            header,
            item_pointers: Vec::new(),
            data: [0; PAGE_SIZE - PageHeader::HEADER_SIZE as usize],
        }
    }

    pub fn add_data(&mut self, data: &[u8]) -> Result<ItemId> {
        let needed_space = data.len() + ItemPointer::SIZE;
        if self.free_space() < needed_space {
            return Err(miette::miette!("Not enough space in the page to add data."));
        }

        self.header.upper -= data.len() as u16;
        let data_offset = self.header.upper as usize - PageHeader::HEADER_SIZE as usize;
        self.data[data_offset..data_offset + data.len()].copy_from_slice(data);

        let item_pointer = ItemPointer {
            offset: self.header.upper,
            length: data.len() as u16,
            flags: 0,
        };
        self.item_pointers.push(item_pointer);

        let item_id = self.header.item_count;

        self.header.lower += ItemPointer::SIZE as u16;
        self.header.item_count += 1;

        Ok(item_id)
    }

    pub fn get_item(&self, item_id: ItemId) -> Result<&[u8]> {
        let item_pointer = self
            .item_pointers
            .get(item_id as usize)
            .ok_or(miette::miette!("Item not found."))?;

        let start = item_pointer.offset as usize - PageHeader::HEADER_SIZE as usize;
        let end = start + item_pointer.length as usize;

        Ok(&self.data[start..end])
    }

    pub fn free_space(&self) -> usize {
        (self.header.upper - self.header.lower) as usize
    }

    pub fn delete_item(&mut self, item_id: ItemId) -> Result<()> {
        let index = item_id as usize;

        if index >= self.item_pointers.len() {
            return Err(miette::miette!("Item ID out of bounds."));
        }

        // Mark item as deleted by setting flags
        self.item_pointers[index].flags = 1; // 1 = deleted
        self.header.item_count -= 1;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_create_page() {
        let page_id = 1;
        let page_type = PageType::Table;
        let page = Page::new(page_id, page_type);
        assert_eq!(page.header.page_id, page_id);
        assert_eq!(page.header.page_type, page_type);
        assert_eq!(page.header.lower, PageHeader::HEADER_SIZE);
        assert_eq!(page.header.upper, PAGE_SIZE as u16);
        assert_eq!(page.item_pointers.len(), 0);
    }

    #[test]
    pub fn test_add_data() {
        let data = b"Hello World";

        let mut page = Page::new(0, PageType::Table);

        let item_id = page.add_data(data).expect("Should add data successfully");
        assert_eq!(item_id, 0);

        // Verify upper and lower bounds
        assert_eq!(
            page.header.lower as usize,
            (PageHeader::HEADER_SIZE as usize) + ItemPointer::SIZE
        );
        assert_eq!(page.header.upper, PAGE_SIZE as u16 - data.len() as u16);

        // Verify that item is inserted and counted
        assert_eq!(page.item_pointers.len(), 1);
        assert_eq!(page.header.item_count, 1);

        // Verify if Item Pointers are correct
        assert_eq!(
            page.item_pointers[0].offset as usize,
            page.header.upper as usize
        );
        assert_eq!(page.item_pointers[0].length as usize, data.len());
        assert_eq!(page.item_pointers[0].flags as usize, 0);

        // Item exist and is the same
        let item = page.get_item(item_id).expect("Item to exist");
        assert_eq!(item, data);
    }

    #[test]
    pub fn test_add_multiple_data() {
        let mut page = Page::new(0, PageType::Table);

        let data1 = b"First Item";
        let data2 = b"Second Item";

        let item_id1 = page
            .add_data(data1)
            .expect("Should add first item successfully");
        let item_id2 = page
            .add_data(data2)
            .expect("Should add second item successfully");

        assert_eq!(item_id1, 0);
        assert_eq!(item_id2, 1);

        assert_eq!(page.header.item_count, 2);
        assert_eq!(page.item_pointers.len(), 2);

        assert_eq!(page.get_item(item_id1).expect("Item 1 to exist"), data1);
        assert_eq!(page.get_item(item_id2).expect("Item 2 to exist"), data2);
    }

    #[test]
    pub fn test_add_without_enough_space() {
        let mut page = Page::new(0, PageType::Table);
        let data = vec![0u8; PAGE_SIZE];

        let item_id_res = page.add_data(&data);

        assert!(item_id_res.is_err());
        assert_eq!(
            item_id_res.unwrap_err().to_string(),
            "Not enough space in the page to add data."
        );

        assert_eq!(page.header.item_count, 0);
        assert_eq!(page.item_pointers.len(), 0);
    }
}
