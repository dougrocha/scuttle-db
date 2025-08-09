use core::fmt;

#[derive(Default)]
pub struct InternalNode<K, V> {
    keys: Vec<K>,
    children: Vec<BTreeNode<K, V>>,
}

#[derive(Default)]
pub struct LeafNode<K, V> {
    entries: Vec<(K, V)>,

    next_node: Option<Box<LeafNode<K, V>>>,
}

pub enum BTreeNode<K, V> {
    Internal(InternalNode<K, V>),
    LeafNode(LeafNode<K, V>),
}

pub struct BTree<K, V>
where
    K: PartialOrd + Clone,
    V: Clone,
{
    root: Option<BTreeNode<K, V>>,

    min_degree: usize,
}

impl<K, V> BTree<K, V>
where
    K: PartialOrd + Clone,
    V: Clone,
{
    pub fn new(min_degree: usize) -> Self {
        assert!(min_degree >= 3);

        Self {
            root: None,
            min_degree,
        }
    }

    pub fn insert(&mut self, key: K, val: V) {
        if self.root.is_none() {
            self.root = Some(BTreeNode::LeafNode(LeafNode {
                entries: vec![(key, val)],
                next_node: None,
            }));
            return;
        }

        let root_is_full = self.is_node_full(self.root.as_ref().unwrap());
        if root_is_full {
            let old_root = self.root.take().unwrap();
            let mut new_root = InternalNode {
                keys: Vec::new(),
                children: vec![old_root],
            };
            self.split_child(&mut new_root, 0);
            self.root = Some(BTreeNode::Internal(new_root));
        }

        Self::insert_non_full_helper(self.root.as_mut().unwrap(), key, val, self.min_degree);
    }

    fn insert_non_full_helper(node: &mut BTreeNode<K, V>, key: K, val: V, min_degree: usize) {
        match node {
            BTreeNode::LeafNode(leaf) => {
                let mut index = 0;
                while index < leaf.entries.len() && leaf.entries[index].0 < key {
                    index += 1;
                }
                leaf.entries.insert(index, (key, val));
            }
            BTreeNode::Internal(internal) => {
                let mut index = 0;
                while index < internal.keys.len() && internal.keys[index] < key {
                    index += 1;
                }

                let max_keys = 2 * min_degree - 1;
                let child_is_full = match &internal.children[index] {
                    BTreeNode::LeafNode(leaf) => leaf.entries.len() == max_keys,
                    BTreeNode::Internal(internal_child) => internal_child.keys.len() == max_keys,
                };

                if child_is_full {
                    Self::split_child_helper(internal, index, min_degree);
                    if internal.keys[index] < key {
                        index += 1;
                    }
                }

                Self::insert_non_full_helper(&mut internal.children[index], key, val, min_degree);
            }
        }
    }

    fn split_child_helper(parent: &mut InternalNode<K, V>, child_index: usize, min_degree: usize) {
        let mid_index = min_degree - 1;

        match &mut parent.children[child_index] {
            BTreeNode::LeafNode(full_leaf) => {
                let new_leaf = LeafNode {
                    entries: full_leaf.entries.split_off(mid_index),
                    next_node: full_leaf.next_node.take(),
                };

                let mid_entry = new_leaf.entries[0].clone();

                parent.keys.insert(child_index, mid_entry.0);
                parent
                    .children
                    .insert(child_index + 1, BTreeNode::LeafNode(new_leaf));
            }
            BTreeNode::Internal(full_internal) => {
                let new_internal = InternalNode {
                    keys: full_internal.keys.split_off(mid_index + 1),
                    children: full_internal.children.split_off(mid_index + 1),
                };

                let mid_key = full_internal.keys.pop().unwrap();

                parent.keys.insert(child_index, mid_key);
                parent
                    .children
                    .insert(child_index + 1, BTreeNode::Internal(new_internal));
            }
        }
    }

    fn is_node_full(&self, node: &BTreeNode<K, V>) -> bool {
        let max_keys = 2 * self.min_degree - 1;
        match node {
            BTreeNode::LeafNode(leaf) => leaf.entries.len() == max_keys,
            BTreeNode::Internal(internal) => internal.keys.len() == max_keys,
        }
    }

    fn split_child(&mut self, parent: &mut InternalNode<K, V>, child_index: usize) {
        let mid_index = self.min_degree - 1;

        match &mut parent.children[child_index] {
            BTreeNode::LeafNode(full_leaf) => {
                let new_leaf = LeafNode {
                    entries: full_leaf.entries.split_off(mid_index),
                    next_node: full_leaf.next_node.take(),
                };

                let mid_entry = new_leaf.entries[0].clone();

                parent.keys.insert(child_index, mid_entry.0);
                parent
                    .children
                    .insert(child_index + 1, BTreeNode::LeafNode(new_leaf));
            }
            BTreeNode::Internal(full_internal) => {
                let new_internal = InternalNode {
                    keys: full_internal.keys.split_off(mid_index + 1),
                    children: full_internal.children.split_off(mid_index + 1),
                };

                let mid_key = full_internal.keys.pop().unwrap();

                parent.keys.insert(child_index, mid_key);
                parent
                    .children
                    .insert(child_index + 1, BTreeNode::Internal(new_internal));
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.get_node(self.root.as_ref(), key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.contains_key_node(self.root.as_ref(), key)
    }

    fn get_node<'a>(&'a self, node: Option<&'a BTreeNode<K, V>>, key: &K) -> Option<&'a V> {
        match node {
            Some(BTreeNode::LeafNode(leaf)) => {
                for (k, v) in &leaf.entries {
                    if k == key {
                        return Some(v);
                    }
                }
                None
            }
            Some(BTreeNode::Internal(internal)) => {
                let mut index = 0;
                while index < internal.keys.len() && internal.keys[index] < *key {
                    index += 1;
                }
                self.get_node(internal.children.get(index), key)
            }
            None => None,
        }
    }

    fn contains_key_node(&self, node: Option<&BTreeNode<K, V>>, key: &K) -> bool {
        match node {
            Some(BTreeNode::LeafNode(leaf)) => {
                for (k, _) in &leaf.entries {
                    if k == key {
                        return true;
                    }
                }
                false
            }
            Some(BTreeNode::Internal(internal)) => {
                let mut index = 0;
                while index < internal.keys.len() && internal.keys[index] < *key {
                    index += 1;
                }
                self.contains_key_node(internal.children.get(index), key)
            }
            None => false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }
}

impl<K, V> fmt::Debug for BTree<K, V>
where
    K: fmt::Debug + PartialOrd + Clone,
    V: fmt::Debug + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref root) = self.root {
            self.display_node(root, f, 0)
        } else {
            write!(f, "Empty BTree")
        }
    }
}

impl<K, V> BTree<K, V>
where
    K: fmt::Debug + PartialOrd + Clone,
    V: fmt::Debug + Clone,
{
    fn display_node(
        &self,
        node: &BTreeNode<K, V>,
        f: &mut fmt::Formatter<'_>,
        level: usize,
    ) -> fmt::Result {
        let indent = "  ".repeat(level);
        match node {
            BTreeNode::LeafNode(leaf) => {
                write!(f, "{}Leaf: [", indent)?;
                for (i, (key, value)) in leaf.entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}:{:?}", key, value)?;
                }
                writeln!(f, "]")
            }
            BTreeNode::Internal(internal) => {
                write!(f, "{}Internal: [", indent)?;
                for (i, key) in internal.keys.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", key)?;
                }
                writeln!(f, "]")?;

                for child in &internal.children {
                    self.display_node(child, f, level + 1)?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_btree_creation() {
        let tree: BTree<i32, String> = BTree::new(3);
        assert!(tree.is_empty());
        assert_eq!(tree.min_degree, 3);
    }

    #[test]
    fn test_first_insertion_creates_leaf_root() {
        let mut tree = BTree::new(3);

        tree.insert(10, "ten".to_string());
        tree.insert(20, "twenty".to_string());
        tree.insert(5, "five".to_string());

        assert!(!tree.is_empty());
        assert!(matches!(tree.root, Some(BTreeNode::LeafNode(_))));
    }

    #[test]
    fn test_leaf_node_splitting() {
        let mut tree = BTree::new(3);

        for i in 1..=6 {
            tree.insert(i, format!("value_{}", i));
        }

        assert!(matches!(tree.root, Some(BTreeNode::Internal(_))));
    }

    #[test]
    fn test_multiple_insertions_maintain_order() {
        let mut tree = BTree::new(3);
        let values = vec![50, 30, 70, 20, 40, 60, 80, 10, 25, 35, 45];

        for val in values {
            tree.insert(val, format!("value_{}", val));
        }

        assert!(!tree.is_empty());
    }

    #[test]
    fn test_duplicate_insertions() {
        let mut tree = BTree::new(3);

        tree.insert(10, "first".to_string());
        tree.insert(10, "second".to_string());
        tree.insert(20, "twenty".to_string());
        tree.insert(10, "third".to_string());

        assert!(!tree.is_empty());
    }
    #[test]
    fn test_contains_key() {
        let mut tree = BTree::new(3);

        tree.insert("key1".to_string(), "value1".to_string());
        tree.insert("key2".to_string(), "value2".to_string());
        tree.insert("key3".to_string(), "value3".to_string());

        assert!(tree.contains_key(&"key1".to_string()));
        assert!(tree.contains_key(&"key2".to_string()));
        assert!(tree.contains_key(&"key3".to_string()));
        assert!(!tree.contains_key(&"key4".to_string()));
        assert!(!tree.contains_key(&"nonexistent".to_string()));
    }
}
