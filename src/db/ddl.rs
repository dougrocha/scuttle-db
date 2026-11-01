//! Statements that change what tables and indexes exist: CREATE and DROP of each.

use std::collections::BTreeSet;

use miette::{IntoDiagnostic, Result};

use crate::{
    ColumnConstraint, ColumnDef, DatabaseError,
    db::{
        database::{Database, QueryResponse},
        table::{
            cursor::TableCursor,
            index::{IndexConstraint, TableIndex},
            schema::Schema,
            table_def::TableDef,
        },
    },
    sql::ast::statement,
    storage::external_sort::ExternalSort,
};

impl Database {
    /// Creates a new table with the given schema.
    ///
    /// The table is created in-memory and ready for use immediately.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::TableAlreadyExists`] if a table with the same name already exists.
    /// The existing table, its schema and its rows are left unchanged.
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<(), DatabaseError> {
        if self.catalog.contains(name) {
            return Err(DatabaseError::TableAlreadyExists(name.to_string()));
        }

        // A table name becomes the file name `<table>.table`, and goes into index names,
        // which become `<index>.index`. Letters, digits and `_` keep those files inside
        // the data directory.
        let names = std::iter::once(name).chain(schema.columns.iter().map(|c| c.name.as_str()));
        for name in names {
            if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(DatabaseError::InvalidQuery(format!(
                    "invalid name \"{name}\": table and column names may only use letters, \
                     digits and _"
                )));
            }
        }
        let mut seen = BTreeSet::new();
        if let Some(column) = schema.columns.iter().find(|c| !seen.insert(&c.name)) {
            return Err(DatabaseError::InvalidQuery(format!(
                "column {} is named more than once",
                column.name
            )));
        }
        // The database keeps its own tables, such as the catalog, under names like these
        if name.starts_with("__") {
            return Err(DatabaseError::InvalidQuery(format!(
                "invalid name \"{name}\": table names starting with __ are reserved"
            )));
        }

        let primary_keys = schema
            .columns
            .iter()
            .filter(|column| column.constraints.contains(&ColumnConstraint::PrimaryKey))
            .count();
        if primary_keys > 1 {
            return Err(DatabaseError::InvalidQuery(format!(
                "multiple primary keys for table {name} are not allowed"
            )));
        }

        // One index per PRIMARY KEY or UNIQUE column, with Postgres's names:
        // `<table>_pkey` and `<table>_<column>_key`
        let mut indexes: Vec<TableIndex> = Vec::new();
        for (position, column) in schema.columns.iter().enumerate() {
            let Some(constraint) = IndexConstraint::of(column) else {
                continue;
            };
            let base = match constraint {
                IndexConstraint::PrimaryKey => format!("{name}_pkey"),
                IndexConstraint::Unique => format!("{name}_{}_key", column.name),
            };
            let index_name = self.catalog.free_index_name(&base, &indexes);
            indexes.push(TableIndex::new(
                index_name,
                &schema,
                position,
                true,
                Some(constraint),
            ));
        }
        let table = TableDef::new(name.to_string(), schema, indexes);

        let error = |error: miette::Report| DatabaseError::InvalidQuery(error.to_string());
        // A drop that failed partway may have left catalog rows and files under these
        // names. Their old columns and rows must not show up in the new table.
        self.catalog.forget(&mut self.pool, name).map_err(error)?;
        let leftovers = std::iter::once(name).chain(table.indexes.iter().map(|i| i.file_name()));
        for file in leftovers {
            self.pool.drop_table(file).map_err(error)?;
        }
        for index in &table.indexes {
            index.create(&mut self.pool).map_err(error)?;
        }
        let index_files: Vec<String> = table
            .indexes
            .iter()
            .map(|i| i.file_name().to_string())
            .collect();
        if let Err(failed) = self.catalog.add_table(&mut self.pool, table) {
            // Delete the files it made, so a retry starts clean
            for file in &index_files {
                self.pool.drop_table(file).map_err(error)?;
            }
            return Err(error(failed));
        }
        self.pool.flush().map_err(error)?;

        Ok(())
    }

    /// Drops a table: its catalog row, its pages and its files.
    ///
    /// The table leaves memory and the catalog first, and the catalog is written before
    /// any file is deleted. So if a later step fails, the table is still gone, and its
    /// files are left behind until the next open deletes them (see
    /// `remove_orphan_files`). If the database stops before the catalog is
    /// written, the table is still there, files and all.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.catalog.table(name)?;
        Self::check_writable(name)?;

        let table = self.catalog.remove_table(&mut self.pool, name)?;
        self.pool.flush()?;

        self.pool.drop_table(name)?;
        for index in &table.indexes {
            self.pool.drop_table(index.file_name())?;
        }
        Ok(())
    }

    /// Creates the index `name` on `table_name`'s `column`, filled from the table's rows.
    ///
    /// # Errors
    ///
    /// Fails if the name is taken or not a valid name, if the table or column doesn't
    /// exist, or if the index is unique and two rows share a key. A failed index leaves no
    /// file behind.
    pub fn create_index(
        &mut self,
        name: &str,
        table_name: &str,
        column: &str,
        unique: bool,
    ) -> Result<()> {
        if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(DatabaseError::InvalidQuery(format!(
                "invalid index name \"{name}\": index names may only use letters, digits and _"
            ))
            .into());
        }
        if self.catalog.free_index_name(name, &[]) != name {
            return Err(DatabaseError::InvalidQuery(format!("index {name} already exists")).into());
        }
        Self::check_writable(table_name)?;
        let table = self.catalog.table(table_name)?;
        let position = table.schema.get_column_index(column).ok_or_else(|| {
            DatabaseError::ColumnNotFound(format!("{column} in table {table_name}"))
        })?;
        let index = TableIndex::new(name.to_string(), &table.schema, position, unique, None);

        // A drop that failed partway may have left a file under this name
        self.pool.drop_table(index.file_name())?;
        if let Err(error) = self.build_indexes(table_name, vec![index.clone()]) {
            self.pool.drop_table(index.file_name())?;
            return Err(error);
        }

        let file_name = index.file_name().to_string();
        if let Err(error) = self.catalog.add_index(&mut self.pool, table_name, index) {
            self.pool.drop_table(&file_name)?;
            return Err(error);
        }
        self.pool.flush()
    }

    /// Drops the index `name`: its catalog row, then its file.
    ///
    /// # Errors
    ///
    /// Fails if there is no such index, or it enforces a PRIMARY KEY or UNIQUE column.
    /// Those go away with their table.
    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        let Some((table, index)) = self.catalog.index(name) else {
            return Err(DatabaseError::InvalidQuery(format!("index {name} does not exist")).into());
        };
        if let Some(constraint) = index.constraint {
            let table_name = &table.name;
            return Err(DatabaseError::InvalidQuery(format!(
                "index {name} enforces the {} constraint of table {table_name}, so it goes \
                 only when the table does",
                constraint.as_str()
            ))
            .into());
        }

        let index = self.catalog.remove_index(&mut self.pool, name)?;
        self.pool.flush()?;
        self.pool.drop_table(index.file_name())
    }

    /// Deletes the table and index files no table owns, which a failed drop can leave,
    /// and any sort runs a stopped query left in `tmp`.
    pub(crate) fn remove_orphan_files(&mut self) -> Result<()> {
        let Some(dir) = &self.data_directory else {
            return Ok(());
        };

        let mut owned = BTreeSet::new();
        for table in self.catalog.tables() {
            owned.insert(format!("{}.table", table.name));
            owned.extend(
                table
                    .indexes
                    .iter()
                    .map(|index| index.file_name().to_string()),
            );
        }

        for entry in std::fs::read_dir(dir).into_diagnostic()? {
            let path = entry.into_diagnostic()?.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let ours = file_name.ends_with(".table") || file_name.ends_with(".index");
            if ours && !owned.contains(file_name) {
                std::fs::remove_file(&path).into_diagnostic()?;
            }
        }

        match std::fs::remove_dir_all(dir.join("tmp")) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                Err(error).into_diagnostic()
            }
            _ => Ok(()),
        }
    }

    /// Opens a table's indexes, building any whose file is missing from the table's rows.
    pub(crate) fn open_indexes(&mut self, table_name: &str) -> Result<()> {
        let mut missing = Vec::new();
        for index in &self.catalog.table(table_name)?.indexes {
            if !index.exists(&mut self.pool) {
                missing.push(index.clone());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        self.build_indexes(table_name, missing)
    }

    /// Creates the files of `indexes`, filled from `table_name`'s rows.
    pub(crate) fn build_indexes(
        &mut self,
        table_name: &str,
        missing: Vec<TableIndex>,
    ) -> Result<()> {
        // One pass over the table feeds every missing index's entries to its own sort,
        // which writes runs to disk past the sort budget. Nothing holds the whole table.
        let spill = self.sort_spill();
        let mut sorts: Vec<(TableIndex, ExternalSort<Vec<u8>>)> = missing
            .into_iter()
            .map(|index| (index, ExternalSort::new(Vec::cmp, spill.clone())))
            .collect();
        let schema = self.catalog.table(table_name)?.schema.clone();
        let mut cursor = TableCursor::open(&mut self.pool, table_name.to_string(), schema);
        while cursor.read_next_page(&mut self.pool, |page_id, item_id, row| {
            for (index, sort) in &mut sorts {
                sort.push(index.entry(&row, (page_id, item_id)))?;
            }
            Ok(())
        })? {}

        for (index, sort) in sorts {
            index.build(&mut self.pool, sort.finish()?)?;
        }
        // This doesn't flush. `initialize` flushes once every table's indexes are open, so
        // a crash partway through a rebuild still leaves the marker for the next run
        Ok(())
    }

    pub(crate) fn handle_create(
        &mut self,
        create_stmt: statement::CreateStatement,
    ) -> Result<QueryResponse> {
        let mut columns = create_stmt.columns;
        for column in &mut columns {
            Self::check_default(column)?;
        }
        let schema = Schema::new(columns);

        self.create_table(&create_stmt.table_name, schema)?;

        Ok(QueryResponse::default())
    }

    /// Converts a column's `DEFAULT` to the column type, so INSERT can use it as is.
    pub(crate) fn check_default(column: &mut ColumnDef) -> Result<()> {
        let data_type = column.data_type;
        for constraint in &mut column.constraints {
            if let ColumnConstraint::Default(value) = constraint {
                let converted = value.clone().coerce_to(&data_type).map_err(|reason| {
                    DatabaseError::TypeMismatch(format!(
                        "Column {} has an invalid DEFAULT: {}",
                        column.name, reason
                    ))
                })?;
                *value = converted;
            }
        }
        Ok(())
    }
}
