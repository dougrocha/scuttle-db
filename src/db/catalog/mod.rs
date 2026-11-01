//! Every table the database holds: their definitions in memory, and the catalog tables
//! that store them on disk (see [`system_catalog`]).
//!
//! The planner reads tables through [`Catalog`]. Changes go through it too, so the
//! definitions in memory and the rows on disk always change together.

mod system_catalog;

use std::collections::BTreeMap;

use miette::Result;

use crate::{
    DatabaseError,
    db::table::{index::TableIndex, table_def::TableDef},
    storage::buffer_pool::BufferPool,
};
use system_catalog::SystemCatalog;

/// The tables of a database, by name, the catalog's own tables included.
#[derive(Debug)]
pub(crate) struct Catalog {
    tables: BTreeMap<String, TableDef>,
    system: SystemCatalog,
}

impl Catalog {
    /// A catalog holding only the catalog's own tables, until [`Catalog::load`].
    pub(crate) fn new() -> Self {
        Self {
            tables: SystemCatalog::tables()
                .into_iter()
                .map(|table| (table.name.clone(), table))
                .collect(),
            system: SystemCatalog,
        }
    }

    /// The table called `name`.
    pub(crate) fn table(&self, name: &str) -> Result<&TableDef, DatabaseError> {
        self.tables
            .get(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }

    /// Whether there is a table called `name`.
    ///
    /// Every table in the catalog is loaded when the database opens, so a table file that
    /// isn't here is one a failed drop left behind, not a table.
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Every table, in name order.
    pub(crate) fn tables(&self) -> impl Iterator<Item = &TableDef> {
        self.tables.values()
    }

    /// The index called `name`, with the table it is on.
    pub(crate) fn index(&self, name: &str) -> Option<(&TableDef, &TableIndex)> {
        self.tables().find_map(|table| {
            let index = table.indexes.iter().find(|index| index.name == name)?;
            Some((table, index))
        })
    }

    /// Returns `base` if no index has that name. Otherwise it appends 1, 2 and so on until
    /// the name is free. `new` holds indexes not yet added to a table. Postgres picks
    /// names the same way.
    pub(crate) fn free_index_name(&self, base: &str, new: &[TableIndex]) -> String {
        let taken = |name: &str| {
            self.tables()
                .flat_map(|table| &table.indexes)
                .chain(new)
                .any(|index| index.name == name)
        };
        let mut name = base.to_string();
        for n in 1.. {
            if !taken(&name) {
                break;
            }
            name = format!("{base}{n}");
        }
        name
    }

    /// Whether `name` is one of the catalog's own tables, which only the database writes.
    pub(crate) fn is_system(name: &str) -> bool {
        SystemCatalog::is_catalog(name)
    }

    /// Reads every table the catalog tables hold, with its indexes, and returns their
    /// names. Deletes the catalog rows of tables that were never finished.
    pub(crate) fn load(&mut self, pool: &mut BufferPool) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for table in self.system.load(pool)? {
            names.push(table.name.clone());
            self.tables.insert(table.name.clone(), table);
        }
        Ok(names)
    }

    /// Deletes the catalog rows a drop of `name` that failed partway may have left, so a
    /// new table by that name doesn't get its old columns.
    pub(crate) fn forget(&self, pool: &mut BufferPool, name: &str) -> Result<()> {
        self.system.remove_table(pool, name)
    }

    /// Adds `table`. If writing its catalog rows fails, the rows already written are
    /// deleted again, so a retry starts clean, and the table isn't added.
    pub(crate) fn add_table(&mut self, pool: &mut BufferPool, table: TableDef) -> Result<()> {
        if let Err(failed) = self.system.add_table(pool, &table) {
            self.system.remove_table(pool, &table.name)?;
            return Err(failed);
        }
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    /// Removes the table called `name` and returns it, so the caller can delete its files.
    ///
    /// It leaves memory first, so if writing the catalog fails, the table is gone until
    /// the database is opened again.
    pub(crate) fn remove_table(&mut self, pool: &mut BufferPool, name: &str) -> Result<TableDef> {
        let table = self
            .tables
            .remove(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))?;
        self.system.remove_table(pool, name)?;
        Ok(table)
    }

    /// Adds `index` to `table_name`. If writing its catalog row fails, the index isn't
    /// added.
    pub(crate) fn add_index(
        &mut self,
        pool: &mut BufferPool,
        table_name: &str,
        index: TableIndex,
    ) -> Result<()> {
        let table = self.table(table_name)?;
        if let Err(failed) = self.system.add_index(pool, table, &index) {
            self.system.remove_index(pool, &index.name)?;
            return Err(failed);
        }
        self.table_mut(table_name)?.indexes.push(index);
        Ok(())
    }

    /// Removes the index called `name` and returns it, so the caller can delete its file.
    pub(crate) fn remove_index(&mut self, pool: &mut BufferPool, name: &str) -> Result<TableIndex> {
        let (table, _) = self
            .index(name)
            .ok_or_else(|| DatabaseError::InvalidQuery(format!("index {name} does not exist")))?;
        let table_name = table.name.clone();
        self.system.remove_index(pool, name)?;

        let indexes = &mut self.table_mut(&table_name)?.indexes;
        let position = indexes
            .iter()
            .position(|index| index.name == name)
            .expect("found above");
        Ok(indexes.remove(position))
    }

    fn table_mut(&mut self, name: &str) -> Result<&mut TableDef, DatabaseError> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }
}
