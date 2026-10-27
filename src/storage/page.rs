use miette::{Result, miette};

use crate::core::serialization::Serializable;

pub type PageId = u32;
pub type ItemId = u16;

/// Stands for "no page" where a page id is stored, as in a last leaf's `next`.
const NO_PAGE: PageId = PageId::MAX;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    /// A B-tree page above the leaves. Item `i` is a child page and the lowest key in it.
    BTreeInternal,

    /// A B-tree page holding keys. `next` is the leaf to its right, for walking a range.
    BTreeLeaf {
        next: Option<PageId>,
    },

    /// Page 0 of an index file: where the root is, and how many keys the tree holds.
    BTreeMeta,
    Table,
    Catalog,
}

#[derive(Debug, Clone, PartialEq)]
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
    pub const SIZE: usize = 24;

    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        Self {
            page_id,
            page_type,
            lower: Self::SIZE as u16,
            upper: Page::SIZE as u16,
            special: Page::SIZE as u16,
            item_count: 0,
        }
    }
}

impl Serializable<{ PageHeader::SIZE }> for PageHeader {
    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let this = &self;
        let mut data = [0; PageHeader::SIZE];
        data[0..4].copy_from_slice(&this.page_id.to_le_bytes());
        data[4] = match this.page_type {
            PageType::Table => 0,
            PageType::Catalog => 1,
            PageType::BTreeInternal => 2,
            PageType::BTreeLeaf { .. } => 3,
            PageType::BTreeMeta => 4,
        };
        data[5..7].copy_from_slice(&this.lower.to_le_bytes());
        data[7..9].copy_from_slice(&this.upper.to_le_bytes());
        data[9..11].copy_from_slice(&this.item_count.to_le_bytes());
        data[11..13].copy_from_slice(&this.special.to_le_bytes());
        if let PageType::BTreeLeaf { next } = this.page_type {
            data[13..17].copy_from_slice(&next.unwrap_or(NO_PAGE).to_le_bytes());
        }
        data
    }

    fn from_bytes(data: [u8; Self::SIZE]) -> Self {
        let page_id = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let page_type = match data[4] {
            0 => PageType::Table,
            1 => PageType::Catalog,
            2 => PageType::BTreeInternal,
            3 => {
                let next = u32::from_le_bytes(data[13..17].try_into().unwrap());
                PageType::BTreeLeaf {
                    next: (next != NO_PAGE).then_some(next),
                }
            }
            4 => PageType::BTreeMeta,
            _ => panic!("Unknown page type"),
        };
        let lower = u16::from_le_bytes(data[5..7].try_into().unwrap());
        let upper = u16::from_le_bytes(data[7..9].try_into().unwrap());
        let item_count = u16::from_le_bytes(data[9..11].try_into().unwrap());
        let special = u16::from_le_bytes(data[11..13].try_into().unwrap());

        PageHeader {
            page_id,
            page_type,
            lower,
            upper,
            item_count,
            special,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ItemPointer {
    pub offset: u16, // Offset from start of page
    pub length: u16, // Length of tuple
    pub flags: u8,   // Status flags (live/dead)
}

impl ItemPointer {
    const SIZE: usize = std::mem::size_of::<ItemPointer>();

    const DELETED_FLAG: u8 = 0b0000_0001; // Bit 0
    const LIVE_FLAG: u8 = 0b0000_0000; // Default state

    pub fn new(offset: u16, length: u16) -> Self {
        Self {
            offset,
            length,
            flags: Self::LIVE_FLAG,
        }
    }

    pub fn is_deleted(self) -> bool {
        self.flags & Self::DELETED_FLAG != 0
    }

    pub fn mark_deleted(&mut self) {
        self.flags |= Self::DELETED_FLAG;
    }
}

impl Serializable<{ ItemPointer::SIZE }> for ItemPointer {
    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut data = [0; Self::SIZE];
        data[0..2].copy_from_slice(&self.offset.to_le_bytes());
        data[2..4].copy_from_slice(&self.length.to_le_bytes());
        data[4] = self.flags;
        data
    }

    fn from_bytes(data: [u8; Self::SIZE]) -> Self {
        let offset = u16::from_le_bytes(data[0..2].try_into().unwrap());
        let length = u16::from_le_bytes(data[2..4].try_into().unwrap());
        let flags = data[4];

        ItemPointer {
            offset,
            length,
            flags,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub header: PageHeader,
    pub data: [u8; Page::SIZE - PageHeader::SIZE],
}

impl Page {
    pub const SIZE: usize = 8192;

    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let header = PageHeader::new(page_id, page_type);

        Self {
            header,
            data: [0; Page::SIZE - PageHeader::SIZE],
        }
    }

    /// Whether a tuple of `len` bytes, plus the item pointer it needs, fits in this page.
    pub fn has_room_for(&self, len: usize) -> bool {
        self.free_space() >= len + ItemPointer::SIZE
    }

    pub fn add_data(&mut self, data: &[u8]) -> Result<ItemId> {
        if !self.has_room_for(data.len()) {
            return Err(miette!("Not enough space in the page to add data."));
        }

        self.header.upper -= data.len() as u16;
        let data_offset = self.header.upper as usize - PageHeader::SIZE;
        self.data[data_offset..data_offset + data.len()].copy_from_slice(data);

        let item_pointer = ItemPointer::new(self.header.upper, data.len() as u16);

        let offset = self.header.item_count as usize * ItemPointer::SIZE;
        self.data[offset..offset + ItemPointer::SIZE].copy_from_slice(&item_pointer.to_bytes());

        let item_id = self.header.item_count;

        self.header.lower += ItemPointer::SIZE as u16;
        self.header.item_count += 1;

        Ok(item_id)
    }

    pub fn get_item(&self, item_id: ItemId) -> Result<&[u8]> {
        let item_pointer = self
            .item_pointers()
            .nth(item_id as usize)
            .ok_or_else(|| miette!("Item not found."))?;

        let start = item_pointer.offset as usize - PageHeader::SIZE;
        let end = start + item_pointer.length as usize;

        Ok(&self.data[start..end])
    }

    pub fn free_space(&self) -> usize {
        (self.header.upper - self.header.lower) as usize
    }

    pub fn delete_item(&mut self, item_id: ItemId) -> Result<()> {
        let index = item_id as usize;

        if index >= self.header.item_count as usize {
            return Err(miette!("Item ID out of bounds."));
        }

        let start = index * ItemPointer::SIZE;
        let end = start + ItemPointer::SIZE;
        let chunk = &self.data[start..end];
        let mut item_pointer = ItemPointer::from_bytes(chunk.try_into().unwrap());

        item_pointer.mark_deleted();

        self.data[start..end].copy_from_slice(&item_pointer.to_bytes());
        Ok(())
    }

    /// Adds an item so that it becomes item `pos`, moving the items from `pos` on up by
    /// one. A B-tree page keeps its items in key order this way.
    pub fn insert_at(&mut self, pos: usize, data: &[u8]) -> Result<()> {
        let count = self.header.item_count as usize;
        assert!(pos <= count, "insert at {pos} past the {count} items");

        self.add_data(data)?;
        // `add_data` put the new pointer last. Rotate it down into place.
        self.data[pos * ItemPointer::SIZE..(count + 1) * ItemPointer::SIZE]
            .rotate_right(ItemPointer::SIZE);
        Ok(())
    }

    /// Takes out item `pos`, moving the items after it down by one.
    ///
    /// Its bytes stay where they were until [`Page::compact`], so the free space doesn't
    /// grow until then.
    pub fn remove_at(&mut self, pos: usize) {
        let count = self.header.item_count as usize;
        assert!(pos < count, "remove at {pos} past the {count} items");

        self.data[pos * ItemPointer::SIZE..count * ItemPointer::SIZE]
            .rotate_left(ItemPointer::SIZE);
        self.header.item_count -= 1;
        self.header.lower -= ItemPointer::SIZE as u16;
    }

    /// Rewrites the items back to back, so the bytes of removed items become free space.
    pub fn compact(&mut self) {
        let items: Vec<Vec<u8>> = (0..self.header.item_count)
            .map(|id| self.get_item(id).expect("counted above").to_vec())
            .collect();

        let mut page = Page::new(self.header.page_id, self.header.page_type);
        for item in &items {
            page.add_data(item).expect("the items fit before");
        }
        *self = page;
    }

    /// The free space there would be after [`Page::compact`].
    pub fn free_space_if_compacted(&self) -> usize {
        let used: usize = self
            .item_pointers()
            .map(|pointer| pointer.length as usize + ItemPointer::SIZE)
            .sum();
        Page::SIZE - PageHeader::SIZE - used
    }

    pub fn item_pointers(&self) -> impl Iterator<Item = ItemPointer> {
        self.data
            .as_chunks::<{ ItemPointer::SIZE }>()
            .0
            .iter()
            .take(self.header.item_count as usize)
            .map(|chunk| ItemPointer::from_bytes(*chunk))
    }
}

impl Serializable<{ Page::SIZE }> for Page {
    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let this = &self;
        let mut data = [0; Page::SIZE];
        data[0..PageHeader::SIZE].copy_from_slice(&this.header.to_bytes());

        data[PageHeader::SIZE..].copy_from_slice(&this.data);

        data
    }

    fn from_bytes(data: [u8; Self::SIZE]) -> Self {
        let header_data: [u8; PageHeader::SIZE] = data[0..PageHeader::SIZE].try_into().unwrap();
        let header = PageHeader::from_bytes(header_data);

        let mut page_data = [0; Page::SIZE - PageHeader::SIZE];
        page_data.copy_from_slice(&data[PageHeader::SIZE..]);

        Page {
            header,
            data: page_data,
        }
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
        assert_eq!(page.header.lower, PageHeader::SIZE as u16);
        assert_eq!(page.header.upper, Page::SIZE as u16);
    }

    #[test]
    pub fn test_add_data() {
        let data = b"Hello World";

        let mut page = Page::new(0, PageType::Table);

        let item_id = page.add_data(data).expect("Should add data successfully");
        assert_eq!(item_id, 0);

        assert_eq!(
            page.header.lower as usize,
            PageHeader::SIZE + ItemPointer::SIZE
        );
        assert_eq!(page.header.upper, Page::SIZE as u16 - data.len() as u16);
        assert_eq!(page.header.item_count, 1);

        let item_pointer = page.item_pointers().next().unwrap();
        assert_eq!(item_pointer.offset, page.header.upper);
        assert_eq!(item_pointer.length as usize, data.len());
        assert_eq!(item_pointer.flags, 0);

        assert_eq!(
            page.header.lower as usize,
            PageHeader::SIZE + ItemPointer::SIZE
        );

        let item = page.get_item(item_id).expect("Item to exist");
        assert_eq!(item, data);
    }

    #[test]
    pub fn test_add_multiple_data() {
        let mut page = Page::new(0, PageType::Table);

        let data = b"First Item";
        let item_id = page
            .add_data(data)
            .expect("Should add first item successfully");
        assert_eq!(item_id, 0);
        assert_eq!(page.get_item(item_id).expect("Item 1 to exist"), data);

        let data = b"Second Item";
        let item_id = page
            .add_data(data)
            .expect("Should add second item successfully");
        assert_eq!(page.get_item(item_id).expect("Item 2 to exist"), data);
        assert_eq!(item_id, 1);

        assert_eq!(
            page.header.item_count as usize,
            page.item_pointers().collect::<Vec<_>>().len()
        );
    }

    #[test]
    pub fn test_add_without_enough_space() {
        let mut page = Page::new(0, PageType::Table);
        let data = vec![0u8; Page::SIZE];

        let item_id_res = page.add_data(&data);

        assert!(item_id_res.is_err());
        assert_eq!(
            item_id_res.unwrap_err().to_string(),
            "Not enough space in the page to add data."
        );

        assert_eq!(page.header.item_count, 0);
        assert_eq!(
            page.header.item_count as usize,
            page.item_pointers().collect::<Vec<_>>().len()
        );
    }

    #[test]
    pub fn test_header_to_bytes() {
        let page_id = 1;
        let page_type = PageType::Table;
        let header = PageHeader::new(page_id, page_type);

        let bytes = header.to_bytes();

        assert_eq!(&bytes[0..4], &page_id.to_le_bytes());
        assert_eq!(bytes[4], 0); // page_type (Table)
        assert_eq!(&bytes[5..7], &header.lower.to_le_bytes());
        assert_eq!(&bytes[7..9], &header.upper.to_le_bytes());
        assert_eq!(&bytes[9..11], &header.item_count.to_le_bytes());
        assert_eq!(&bytes[11..13], &header.special.to_le_bytes());
    }

    #[test]
    pub fn test_header_from_bytes() {
        let original_header = PageHeader {
            page_id: 42,
            page_type: PageType::Table,
            lower: 100,
            upper: 7000,
            item_count: 5,
            special: 8192,
        };

        let mut bytes = [0u8; 24];
        bytes[0..4].copy_from_slice(&42u32.to_le_bytes());
        bytes[4] = 0;
        bytes[5..7].copy_from_slice(&100u16.to_le_bytes());
        bytes[7..9].copy_from_slice(&7000u16.to_le_bytes());
        bytes[9..11].copy_from_slice(&5u16.to_le_bytes());
        bytes[11..13].copy_from_slice(&8192u16.to_le_bytes());

        let header = PageHeader::from_bytes(bytes);

        assert_eq!(header.page_id, original_header.page_id);
        assert_eq!(header.page_type, original_header.page_type);
        assert_eq!(header.lower, original_header.lower);
        assert_eq!(header.upper, original_header.upper);
        assert_eq!(header.item_count, original_header.item_count);
        assert_eq!(header.special, original_header.special);
    }

    #[test]
    pub fn test_page_from_bytes() {
        let page_id = 1;
        let page_type = PageType::Table;
        let mut header = PageHeader::new(page_id, page_type);
        header.item_count += 1;

        let mut page = [0u8; Page::SIZE];
        page[0..PageHeader::SIZE].copy_from_slice(&header.to_bytes());

        let data = b"testing page data";
        let data_offset = header.upper as usize - PageHeader::SIZE;
        page[data_offset..data_offset + data.len()].copy_from_slice(data);

        let item_pointer = ItemPointer::new((Page::SIZE - data.len()) as u16, data.len() as u16);
        let item_pointer_bytes = item_pointer.to_bytes();
        page[header.lower as usize..header.lower as usize + ItemPointer::SIZE]
            .copy_from_slice(&item_pointer_bytes);

        let page = Page::from_bytes(page);

        assert_eq!(page.header, header);
        // assert_eq!(page.item_pointers[0], item_pointer);
        assert_eq!(page.data[0..ItemPointer::SIZE], item_pointer_bytes);
    }

    #[test]
    pub fn test_page_to_from_bytes() {
        let page_id = 1;
        let page_type = PageType::Table;
        let mut original_page = Page::new(page_id, page_type);

        let test_data = b"Fake data for testing";
        let item_id = original_page
            .add_data(test_data)
            .expect("Should add data successfully");

        let bytes = original_page.to_bytes();

        let reconstructed_page = Page::from_bytes(bytes);

        assert_eq!(original_page.header, reconstructed_page.header);
        assert_eq!(
            original_page.item_pointers().collect::<Vec<_>>(),
            reconstructed_page.item_pointers().collect::<Vec<_>>()
        );

        assert_eq!(original_page.data.len(), reconstructed_page.data.len());
        assert_eq!(original_page.data, reconstructed_page.data);

        let retrieved_data = reconstructed_page
            .get_item(item_id)
            .expect("Should retrieve item");
        assert_eq!(retrieved_data, test_data);
    }

    #[test]
    pub fn test_btree_page_types_round_trip() {
        for page_type in [
            PageType::BTreeInternal,
            PageType::BTreeLeaf { next: None },
            PageType::BTreeLeaf { next: Some(7) },
            PageType::BTreeMeta,
        ] {
            let page = Page::new(3, page_type);
            assert_eq!(
                Page::from_bytes(page.to_bytes()).header.page_type,
                page_type
            );
        }
    }

    #[test]
    pub fn test_insert_at_and_remove_at_keep_items_in_position() {
        let mut page = Page::new(0, PageType::BTreeLeaf { next: None });
        for (pos, item) in [(0, "c"), (0, "a"), (1, "b"), (3, "d")] {
            page.insert_at(pos, item.as_bytes()).unwrap();
        }
        let items = |page: &Page| -> Vec<String> {
            (0..page.header.item_count)
                .map(|id| String::from_utf8(page.get_item(id).unwrap().to_vec()).unwrap())
                .collect()
        };
        assert_eq!(items(&page), ["a", "b", "c", "d"]);

        page.remove_at(1);
        page.remove_at(2);
        assert_eq!(items(&page), ["a", "c"]);

        let free = page.free_space();
        assert_eq!(page.free_space_if_compacted(), free + 2);
        page.compact();
        assert_eq!(page.free_space(), free + 2);
        assert_eq!(items(&page), ["a", "c"]);
    }
}
