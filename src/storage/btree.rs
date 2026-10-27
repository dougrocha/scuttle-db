//! A B+tree stored in the pages of one file, read and written through the buffer pool.
//!
//! Entries are byte strings, ordered by comparing bytes. An index encodes each (key, ctid)
//! pair so that byte order is key order (see `IndexKey::encode`), which keeps this tree
//! free of any knowledge of types.
//!
//! Page 0 is the metapage: where the root is and how many entries the tree holds. Every
//! other page is a node:
//! - A leaf holds entries in order, and the id of the leaf to its right, so a range scan
//!   finds its first entry and then walks along the leaves.
//! - An internal page holds items of (child page, lowest entry in that child). The first
//!   item's entry is left empty: it stands for "anything smaller than the next one".
//!
//! Inserts split full nodes on the way down (the CLRS way), so a split always has room
//! in its parent and the tree never needs to walk back up. A node counts as full when it
//! has less than [`ROOM`] bytes free: enough for the longest item a split could add.
//!
//! Deletes take the entry out of its leaf and never merge pages, as in Postgres. The
//! space comes back when the page is compacted.

use miette::{Result, miette};

use crate::storage::{
    buffer_pool::BufferPool,
    page::{ItemPointer, Page, PageHeader, PageId, PageType},
};

/// The longest entry the tree takes, in bytes. Postgres caps index entries too (at about
/// a third of a page) instead of spilling long keys to other pages.
pub(crate) const MAX_ENTRY: usize = 1024;

/// Bytes a child page id takes at the start of an internal item.
const CHILD_LEN: usize = 4;

/// Free space a node must have before an insert walks into it: one internal item with
/// the longest entry, plus its item pointer. A child's split adds at most that to its
/// parent.
const ROOM: usize = CHILD_LEN + MAX_ENTRY + ItemPointer::SIZE;

/// The metapage.
const META: PageId = 0;

/// Space [`BTree::build`] leaves free in each page, so the next inserts don't split every
/// page at once. A tenth of the page, the same as Postgres' default fillfactor of 90.
const FILL_SLACK: usize = Page::SIZE / 10;

/// A B+tree in the file `name`. It holds no pages itself: every call reads them from the
/// pool, so the tree can be bigger than memory.
#[derive(Debug, Clone)]
pub(crate) struct BTree {
    name: String,
}

/// What the metapage stores.
struct Meta {
    root: PageId,
    len: u64,
}

impl BTree {
    /// Creates the file `name` holding an empty tree: the metapage and one empty leaf.
    pub(crate) fn create(pool: &mut BufferPool, name: &str) -> Result<Self> {
        if pool.page_count(name) != 0 {
            return Err(miette!("Index file {name} already exists"));
        }

        let tree = Self::open(name);
        pool.allocate_page(name, PageType::BTreeMeta)?;
        let root = pool.allocate_page(name, PageType::BTreeLeaf { next: None })?;
        tree.write_meta(pool, Meta { root, len: 0 })?;
        Ok(tree)
    }

    /// Creates the file `name` holding `entries`, which must be sorted and unique.
    ///
    /// Much faster than inserting them one by one, and the pages come out full instead of
    /// half full. Leaves are filled left to right, leaving [`FILL_SLACK`] free in each for
    /// later inserts. Then each level above is built from the first entries of the level
    /// below, until one page is left: the root.
    pub(crate) fn build(
        pool: &mut BufferPool,
        name: &str,
        entries: impl IntoIterator<Item = Vec<u8>>,
    ) -> Result<Self> {
        let tree = Self::create(pool, name)?;
        let first_leaf = tree.meta(pool)?.root;

        // Each node of the level being built, with the lowest entry under it
        let mut level: Vec<(PageId, Vec<u8>)> = Vec::new();
        let mut leaf = first_leaf;
        let mut len = 0;
        for entry in entries {
            if entry.len() > MAX_ENTRY {
                return Err(miette!(
                    "An index entry of {} bytes is longer than the {MAX_ENTRY} an index allows",
                    entry.len()
                ));
            }
            if !pool
                .get_page(name, leaf)?
                .has_room_for(entry.len() + FILL_SLACK)
            {
                let next = pool.allocate_page(name, PageType::BTreeLeaf { next: None })?;
                pool.get_page_mut(name, leaf)?.header.page_type =
                    PageType::BTreeLeaf { next: Some(next) };
                leaf = next;
            }
            if level.last().is_none_or(|&(page_id, _)| page_id != leaf) {
                level.push((leaf, entry.clone()));
            }
            pool.get_page_mut(name, leaf)?.add_data(&entry)?;
            len += 1;
        }

        while level.len() > 1 {
            let mut above: Vec<(PageId, Vec<u8>)> = Vec::new();
            for (child, low) in level {
                let room = above.last().is_some_and(|&(page_id, _)| {
                    pool.get_page(name, page_id)
                        .is_ok_and(|page| page.has_room_for(CHILD_LEN + low.len() + FILL_SLACK))
                });
                // A page's first item leaves its entry empty, so the entry moves up
                let item = if room {
                    internal_item(child, &low)
                } else {
                    let page_id = pool.allocate_page(name, PageType::BTreeInternal)?;
                    above.push((page_id, low));
                    internal_item(child, &[])
                };
                let page_id = above.last().expect("pushed above").0;
                pool.get_page_mut(name, page_id)?.add_data(&item)?;
            }
            level = above;
        }

        let root = level.first().map_or(first_leaf, |&(page_id, _)| page_id);
        tree.write_meta(pool, Meta { root, len })?;
        Ok(tree)
    }

    /// The tree already stored in the file `name`.
    pub(crate) fn open(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// How many entries the tree holds.
    pub(crate) fn len(&self, pool: &mut BufferPool) -> Result<u64> {
        Ok(self.meta(pool)?.len)
    }

    /// Adds `entry`. Fails if the tree already holds it, or it is longer than [`MAX_ENTRY`].
    pub(crate) fn insert(&self, pool: &mut BufferPool, entry: &[u8]) -> Result<()> {
        if entry.len() > MAX_ENTRY {
            return Err(miette!(
                "An index entry of {} bytes is longer than the {MAX_ENTRY} an index allows",
                entry.len()
            ));
        }

        let mut meta = self.meta(pool)?;
        if !self.make_room(pool, meta.root)? {
            // The tree grows at the top: a new root with the old one as its only child,
            // which then splits like any other full child
            let new_root = pool.allocate_page(&self.name, PageType::BTreeInternal)?;
            pool.get_page_mut(&self.name, new_root)?
                .add_data(&internal_item(meta.root, &[]))?;
            self.split_child(pool, new_root, 0, entry, true)?;
            meta.root = new_root;
        }

        self.insert_non_full(pool, meta.root, entry, true)?;
        meta.len += 1;
        self.write_meta(pool, meta)
    }

    /// Inserts into the subtree at `page_id`, which has room for one more item.
    /// `rightmost` says the subtree is the tree's right edge: nothing sorts after it.
    fn insert_non_full(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        entry: &[u8],
        rightmost: bool,
    ) -> Result<()> {
        let page = pool.get_page(&self.name, page_id)?;
        match page.header.page_type {
            PageType::BTreeLeaf { .. } => {
                let Err(pos) = search_leaf(page, entry) else {
                    return Err(miette!("The index already holds this entry"));
                };
                pool.get_page_mut(&self.name, page_id)?
                    .insert_at(pos, entry)
            }
            PageType::BTreeInternal => {
                let mut index = child_index(page, entry);
                let mut child = child_at(page, index);
                let last = page.header.item_count as usize - 1;

                if !self.make_room(pool, child)? {
                    let child_rightmost = rightmost && index == last;
                    self.split_child(pool, page_id, index, entry, child_rightmost)?;
                    // The split put a new child right after this one. Its lowest entry
                    // decides which of the two the entry belongs in.
                    let page = pool.get_page(&self.name, page_id)?;
                    if key_at(page, index + 1) <= entry {
                        index += 1;
                    }
                    child = child_at(page, index);
                }

                // After a split the parent has one more child, so the last is one further
                let last = pool.get_page(&self.name, page_id)?.header.item_count as usize - 1;
                self.insert_non_full(pool, child, entry, rightmost && index == last)
            }
            other => Err(not_a_node(&self.name, page_id, other)),
        }
    }

    /// Splits the full child at `child_index` of `parent_id` in two, moving its upper
    /// half to a new page right after it. The parent must have room for one more item.
    ///
    /// When the child is on the tree's right edge (`rightmost`) and `entry` sorts after
    /// everything in it, keys are probably being inserted in order, like a SERIAL id. An
    /// even split would leave the left half empty forever, since every later key goes
    /// right. So the left page keeps all it can short of [`FILL_SLACK`], as Postgres does.
    fn split_child(
        &self,
        pool: &mut BufferPool,
        parent_id: PageId,
        child_index: usize,
        entry: &[u8],
        rightmost: bool,
    ) -> Result<()> {
        let child_id = child_at(pool.get_page(&self.name, parent_id)?, child_index);
        let child = pool.get_page(&self.name, child_id)?;
        let page_type = child.header.page_type;
        let at_end = rightmost && sorts_after_all(child, entry);
        let mut items = items(child);

        let split = if at_end {
            let mut used = 0;
            let fits = items
                .iter()
                .take_while(|item| {
                    used += item.len() + ItemPointer::SIZE;
                    used <= Page::SIZE - PageHeader::SIZE - FILL_SLACK
                })
                .count();
            // Both pages keep at least one item
            fits.clamp(1, items.len() - 1)
        } else {
            items.len() / 2
        };
        let mut right_items = items.split_off(split);

        let right_id = pool.allocate_page(&self.name, page_type)?;
        let (left_type, right_type) = match page_type {
            // The new leaf goes into the chain between the old one and its right neighbor
            PageType::BTreeLeaf { next } => (
                PageType::BTreeLeaf {
                    next: Some(right_id),
                },
                PageType::BTreeLeaf { next },
            ),
            page_type => (page_type, page_type),
        };

        let separator = match page_type {
            // A leaf's lowest entry is copied up: leaves hold every entry
            PageType::BTreeLeaf { .. } => right_items[0].clone(),
            // An internal page's lowest entry moves up. Its first item keeps only the child.
            _ => right_items[0].split_off(CHILD_LEN),
        };

        self.write_node(pool, child_id, left_type, &items)?;
        self.write_node(pool, right_id, right_type, &right_items)?;
        pool.get_page_mut(&self.name, parent_id)?
            .insert_at(child_index + 1, &internal_item(right_id, &separator))
    }

    /// Makes sure the node has [`ROOM`], compacting it if that frees enough. `false` means
    /// it has to be split.
    fn make_room(&self, pool: &mut BufferPool, page_id: PageId) -> Result<bool> {
        let page = pool.get_page(&self.name, page_id)?;
        if page.free_space() >= ROOM {
            return Ok(true);
        }
        if page.free_space_if_compacted() < ROOM {
            return Ok(false);
        }

        pool.get_page_mut(&self.name, page_id)?.compact();
        Ok(true)
    }

    /// Takes `entry` out of the tree. Returns whether it was there.
    pub(crate) fn remove(&self, pool: &mut BufferPool, entry: &[u8]) -> Result<bool> {
        let mut meta = self.meta(pool)?;
        let leaf = self.find_leaf(pool, meta.root, entry)?;
        let Ok(pos) = search_leaf(pool.get_page(&self.name, leaf)?, entry) else {
            return Ok(false);
        };

        pool.get_page_mut(&self.name, leaf)?.remove_at(pos);
        meta.len -= 1;
        self.write_meta(pool, meta)?;
        Ok(true)
    }

    /// A cursor at the first entry that is `>= target`.
    pub(crate) fn seek(&self, pool: &mut BufferPool, target: &[u8]) -> Result<Cursor> {
        let root = self.meta(pool)?.root;
        let leaf = self.find_leaf(pool, root, target)?;
        let (Ok(pos) | Err(pos)) = search_leaf(pool.get_page(&self.name, leaf)?, target);

        Ok(Cursor {
            name: self.name.clone(),
            page: Some(leaf),
            pos,
        })
    }

    /// The leaf where `target` is or would be.
    fn find_leaf(
        &self,
        pool: &mut BufferPool,
        mut page_id: PageId,
        target: &[u8],
    ) -> Result<PageId> {
        loop {
            let page = pool.get_page(&self.name, page_id)?;
            match page.header.page_type {
                PageType::BTreeLeaf { .. } => return Ok(page_id),
                PageType::BTreeInternal => page_id = child_at(page, child_index(page, target)),
                other => return Err(not_a_node(&self.name, page_id, other)),
            }
        }
    }

    /// Replaces the page with a fresh one holding `items`, so a split leaves no gaps.
    fn write_node(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        page_type: PageType,
        items: &[Vec<u8>],
    ) -> Result<()> {
        let page = pool.get_page_mut(&self.name, page_id)?;
        *page = Page::new(page_id, page_type);
        for item in items {
            page.add_data(item)?;
        }
        Ok(())
    }

    fn meta(&self, pool: &mut BufferPool) -> Result<Meta> {
        let bytes = pool.get_page(&self.name, META)?.get_item(0)?;
        Ok(Meta {
            root: PageId::from_le_bytes(bytes[0..4].try_into().unwrap()),
            len: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
        })
    }

    fn write_meta(&self, pool: &mut BufferPool, meta: Meta) -> Result<()> {
        let mut bytes = meta.root.to_le_bytes().to_vec();
        bytes.extend_from_slice(&meta.len.to_le_bytes());
        self.write_node(pool, META, PageType::BTreeMeta, &[bytes])
    }
}

/// A position in the tree's entries, moved forward by [`Cursor::next`].
///
/// It keeps a page id and a position, not a borrowed page, so the pool stays free for
/// other work between entries. The tree must not change while a cursor is in use.
#[derive(Debug)]
pub(crate) struct Cursor {
    name: String,

    /// The leaf being read. `None` once past the last leaf.
    page: Option<PageId>,
    pos: usize,
}

impl Cursor {
    /// The entry at the cursor, which then moves to the next one. `None` at the end.
    pub(crate) fn next(&mut self, pool: &mut BufferPool) -> Result<Option<Vec<u8>>> {
        while let Some(page_id) = self.page {
            let page = pool.get_page(&self.name, page_id)?;
            if self.pos < page.header.item_count as usize {
                self.pos += 1;
                return Ok(Some(item_at(page, self.pos - 1).to_vec()));
            }

            let PageType::BTreeLeaf { next } = page.header.page_type else {
                return Err(not_a_node(&self.name, page_id, page.header.page_type));
            };
            self.page = next;
            self.pos = 0;
        }
        Ok(None)
    }
}

/// Whether `entry` sorts after every key in the node.
fn sorts_after_all(page: &Page, entry: &[u8]) -> bool {
    let count = page.header.item_count as usize;
    match page.header.page_type {
        PageType::BTreeLeaf { .. } => count == 0 || item_at(page, count - 1) < entry,
        _ => count <= 1 || key_at(page, count - 1) <= entry,
    }
}

/// Finds `target` in a leaf: `Ok` with its position, or `Err` with where it would go.
fn search_leaf(page: &Page, target: &[u8]) -> std::result::Result<usize, usize> {
    let (mut lo, mut hi) = (0, page.header.item_count as usize);
    while lo < hi {
        let mid = (lo + hi) / 2;
        match item_at(page, mid).cmp(target) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(lo)
}

/// Which child of an internal page `target` belongs in: the last one whose lowest entry
/// is `<= target`. The first child's empty entry is below everything, so there always is one.
fn child_index(page: &Page, target: &[u8]) -> usize {
    let (mut lo, mut hi) = (1, page.header.item_count as usize);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if key_at(page, mid) <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo - 1
}

fn internal_item(child: PageId, key: &[u8]) -> Vec<u8> {
    let mut item = child.to_be_bytes().to_vec();
    item.extend_from_slice(key);
    item
}

fn child_at(page: &Page, index: usize) -> PageId {
    PageId::from_be_bytes(item_at(page, index)[..CHILD_LEN].try_into().unwrap())
}

fn key_at(page: &Page, index: usize) -> &[u8] {
    &item_at(page, index)[CHILD_LEN..]
}

fn item_at(page: &Page, index: usize) -> &[u8] {
    page.get_item(index as u16)
        .expect("positions come from the page's item count")
}

fn items(page: &Page) -> Vec<Vec<u8>> {
    (0..page.header.item_count as usize)
        .map(|index| item_at(page, index).to_vec())
        .collect()
}

fn not_a_node(name: &str, page_id: PageId, page_type: PageType) -> miette::Report {
    miette!("Page {page_id} of {name} is a {page_type:?} page, not a B-tree node")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// A fixed-seed generator, so a failure repeats.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// An entry of 1 to 40 bytes, or now and then a long one, so pages hold few.
        fn entry(&mut self) -> Vec<u8> {
            let len = match self.next() % 50 {
                0 => MAX_ENTRY - (self.next() % 100) as usize,
                _ => 1 + (self.next() % 40) as usize,
            };
            (0..len).map(|_| self.next() as u8).collect()
        }
    }

    fn scan(tree: &BTree, pool: &mut BufferPool, from: &[u8]) -> Vec<Vec<u8>> {
        let mut cursor = tree.seek(pool, from).unwrap();
        let mut entries = Vec::new();
        while let Some(entry) = cursor.next(pool).unwrap() {
            entries.push(entry);
        }
        entries
    }

    /// Checks the tree's shape:
    /// - entries in each node are in order, and within the bounds its parent gives it
    /// - every leaf is at the same depth
    /// - the leaf chain visits every leaf, left to right
    fn check(tree: &BTree, pool: &mut BufferPool) {
        fn walk(
            tree: &BTree,
            pool: &mut BufferPool,
            page_id: PageId,
            bounds: (Option<Vec<u8>>, Option<Vec<u8>>),
            depth: usize,
            leaves: &mut Vec<(PageId, usize)>,
        ) {
            let page = pool.get_page(tree.name(), page_id).unwrap().clone();
            let is_leaf = matches!(page.header.page_type, PageType::BTreeLeaf { .. });
            let keys: Vec<Vec<u8>> = (0..page.header.item_count as usize)
                .map(|i| match is_leaf {
                    true => item_at(&page, i).to_vec(),
                    false => key_at(&page, i).to_vec(),
                })
                .collect();

            // An internal page's first key is empty and stands for its lower bound
            let checked = if is_leaf { &keys[..] } else { &keys[1..] };
            assert!(
                checked.is_sorted_by(|a, b| a < b),
                "page {page_id} out of order"
            );
            for key in checked {
                assert!(
                    bounds.0.as_ref().is_none_or(|lo| key >= lo),
                    "page {page_id} below its bound"
                );
                assert!(
                    bounds.1.as_ref().is_none_or(|hi| key < hi),
                    "page {page_id} above its bound"
                );
            }

            if is_leaf {
                leaves.push((page_id, depth));
                return;
            }
            for i in 0..keys.len() {
                let lo = if i == 0 {
                    bounds.0.clone()
                } else {
                    Some(keys[i].clone())
                };
                let hi = keys.get(i + 1).cloned().or(bounds.1.clone());
                walk(tree, pool, child_at(&page, i), (lo, hi), depth + 1, leaves);
            }
        }

        let root = tree.meta(pool).unwrap().root;
        let mut leaves = Vec::new();
        walk(tree, pool, root, (None, None), 0, &mut leaves);
        assert!(
            leaves.iter().all(|&(_, depth)| depth == leaves[0].1),
            "leaves at different depths"
        );

        let mut chain = vec![leaves[0].0];
        while let PageType::BTreeLeaf { next: Some(next) } = pool
            .get_page(tree.name(), *chain.last().unwrap())
            .unwrap()
            .header
            .page_type
        {
            chain.push(next);
        }
        let in_order: Vec<PageId> = leaves.iter().map(|&(page_id, _)| page_id).collect();
        assert_eq!(chain, in_order, "the leaf chain skips or repeats leaves");
    }

    #[test]
    fn test_an_empty_tree_has_no_entries() {
        let mut pool = BufferPool::in_memory();
        let tree = BTree::create(&mut pool, "t.index").unwrap();
        assert_eq!(tree.len(&mut pool).unwrap(), 0);
        assert!(scan(&tree, &mut pool, &[]).is_empty());
    }

    #[test]
    fn test_the_root_stays_a_leaf_until_it_fills() {
        let mut pool = BufferPool::in_memory();
        let tree = BTree::create(&mut pool, "t.index").unwrap();
        let root_type = |pool: &mut BufferPool| {
            let root = tree.meta(pool).unwrap().root;
            pool.get_page(tree.name(), root).unwrap().header.page_type
        };

        for n in 0..10u32 {
            tree.insert(&mut pool, &n.to_be_bytes()).unwrap();
        }
        assert!(matches!(root_type(&mut pool), PageType::BTreeLeaf { .. }));

        for n in 10..1000u32 {
            tree.insert(&mut pool, &n.to_be_bytes()).unwrap();
        }
        assert_eq!(root_type(&mut pool), PageType::BTreeInternal);
        check(&tree, &mut pool);
    }

    #[test]
    fn test_entries_come_back_in_order_after_inserts_and_removes() {
        let mut pool = BufferPool::in_memory();
        let tree = BTree::create(&mut pool, "t.index").unwrap();
        let mut rng = Rng(0x5eed);
        let mut expected = BTreeSet::new();

        for _ in 0..20_000 {
            let entry = rng.entry();
            if expected.insert(entry.clone()) {
                tree.insert(&mut pool, &entry).unwrap();
            } else {
                assert!(
                    tree.insert(&mut pool, &entry).is_err(),
                    "a duplicate went in"
                );
            }
        }
        check(&tree, &mut pool);
        assert_eq!(
            scan(&tree, &mut pool, &[]),
            Vec::from_iter(expected.clone())
        );

        // Remove every other entry, then put some back, so pages are compacted and reused
        let removed: Vec<Vec<u8>> = expected.iter().step_by(2).cloned().collect();
        for entry in &removed {
            assert!(tree.remove(&mut pool, entry).unwrap());
            expected.remove(entry);
        }
        assert!(!tree.remove(&mut pool, &removed[0]).unwrap());
        for entry in removed.iter().step_by(3) {
            tree.insert(&mut pool, entry).unwrap();
            expected.insert(entry.clone());
        }
        check(&tree, &mut pool);
        assert_eq!(tree.len(&mut pool).unwrap(), expected.len() as u64);

        for _ in 0..200 {
            let from = rng.entry();
            let want: Vec<Vec<u8>> = expected.range(from.clone()..).cloned().collect();
            assert_eq!(scan(&tree, &mut pool, &from), want);
        }
    }

    #[test]
    fn test_a_built_tree_matches_one_made_by_inserts() {
        let mut rng = Rng(0xb01d);
        for count in [0, 1, 300, 20_000] {
            let entries: BTreeSet<Vec<u8>> = (0..count).map(|_| rng.entry()).collect();

            let mut pool = BufferPool::in_memory();
            let tree = BTree::build(&mut pool, "t.index", entries.iter().cloned()).unwrap();
            check(&tree, &mut pool);
            assert_eq!(tree.len(&mut pool).unwrap(), entries.len() as u64);
            assert_eq!(scan(&tree, &mut pool, &[]), Vec::from_iter(entries.clone()));

            // Later inserts and removes still work on the built pages
            let mut expected = entries;
            for _ in 0..2_000 {
                let entry = rng.entry();
                if expected.insert(entry.clone()) {
                    tree.insert(&mut pool, &entry).unwrap();
                }
            }
            let removed: Vec<Vec<u8>> = expected.iter().step_by(3).cloned().collect();
            for entry in &removed {
                assert!(tree.remove(&mut pool, entry).unwrap());
                expected.remove(entry);
            }
            check(&tree, &mut pool);
            assert_eq!(scan(&tree, &mut pool, &[]), Vec::from_iter(expected));
        }
    }

    #[test]
    fn test_in_order_inserts_fill_pages_as_well_as_a_build() {
        let pages = |entries: Vec<u64>, build: bool| {
            let mut pool = BufferPool::in_memory();
            let entries = entries.into_iter().map(|n| n.to_be_bytes().to_vec());
            if build {
                let mut sorted: Vec<Vec<u8>> = entries.collect();
                sorted.sort();
                BTree::build(&mut pool, "t.index", sorted).unwrap();
            } else {
                let tree = BTree::create(&mut pool, "t.index").unwrap();
                for entry in entries {
                    tree.insert(&mut pool, &entry).unwrap();
                }
                check(&tree, &mut pool);
            }
            pool.page_count("t.index")
        };

        let built = pages((0..30_000).collect(), true);
        let ascending = pages((0..30_000).collect(), false);
        // Random order splits evenly, leaving pages about 3/4 full on average
        let mut rng = Rng(0xa5c);
        let random = pages((0..30_000).map(|_| rng.next()).collect(), false);

        assert!(
            ascending * 10 < built * 11,
            "ascending {ascending} vs built {built}"
        );
        assert!(
            random > ascending,
            "random {random} vs ascending {ascending}"
        );
    }

    #[test]
    fn test_an_entry_longer_than_the_limit_is_refused() {
        let mut pool = BufferPool::in_memory();
        let tree = BTree::create(&mut pool, "t.index").unwrap();
        assert!(tree.insert(&mut pool, &[1; MAX_ENTRY]).is_ok());
        assert!(tree.insert(&mut pool, &[1; MAX_ENTRY + 1]).is_err());
    }

    #[test]
    fn test_the_tree_survives_eviction_and_reopening() {
        let dir = std::env::temp_dir().join(format!("scuttle-btree-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut rng = Rng(0xb7ee);
        let entries: BTreeSet<Vec<u8>> = (0..5_000).map(|_| rng.entry()).collect();

        // 4 cached pages, so nearly every step evicts and later reads pages back
        let mut pool = BufferPool::with_capacity(&dir, 4);
        let tree = BTree::create(&mut pool, "t.index").unwrap();
        for entry in &entries {
            tree.insert(&mut pool, entry).unwrap();
        }
        pool.flush().unwrap();

        let mut pool = BufferPool::with_capacity(&dir, 4);
        let tree = BTree::open("t.index");
        check(&tree, &mut pool);
        assert_eq!(scan(&tree, &mut pool, &[]), Vec::from_iter(entries));
        assert!(dir.join("t.index").exists());

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
