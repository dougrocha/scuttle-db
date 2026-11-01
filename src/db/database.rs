use std::path::{Path, PathBuf};

use miette::Result;

use crate::{
    DatabaseError, Value,
    db::{
        catalog::Catalog,
        result_table::ResultTable,
        table::{row::Row, table_def::TableDef},
    },
    sql::{
        ast::{parser::SqlParser, statement::Statement},
        planner::physical::{DEFAULT_SORT_MEM, ExecCtx, PhysicalPlanner},
    },
    storage::{
        buffer_pool::{BufferPool, DEFAULT_CACHE_PAGES},
        external_sort::Spill,
    },
};

/// Response from executing a SQL query.
///
/// Contains the result columns and rows.
#[derive(Debug, Default)]
pub struct QueryResponse {
    /// Output column names (aliases applied). Empty for statements that return no rows.
    pub columns: Vec<String>,

    /// The rows returned by the query.
    pub rows: Vec<Row>,

    /// Number of rows written by INSERT, UPDATE or DELETE. `None` for other statements.
    pub rows_affected: Option<usize>,
}

impl std::fmt::Display for QueryResponse {
    /// Renders the response the way the REPL shows it: a table for queries,
    /// a row count for writes.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(count) = self.rows_affected {
            return write!(f, "{count} row(s) affected");
        }

        if self.columns.is_empty() {
            return write!(f, "OK");
        }

        // One batch holds every row, so each column fits its widest value
        let mut out = String::new();
        let mut table = ResultTable::new(&self.columns, &self.rows, &mut out);
        table.push(&self.rows, &mut out);
        table.finish(&mut out);
        f.write_str(&out)
    }
}

/// The main database handle.
///
/// A statement runs through the stages under `sql/`: it is parsed, analyzed into a
/// logical plan against the catalog of tables, optimized, built into a physical plan and run.
/// The statements themselves are split by what they change:
///
/// - `query.rs`: SELECT and EXPLAIN, which change nothing.
/// - `dml.rs`: INSERT, UPDATE and DELETE, which change rows.
/// - `ddl.rs`: CREATE and DROP, which change which tables and indexes exist.
#[derive(Debug)]
pub struct Database {
    /// Every table, with its schema and indexes.
    pub(crate) catalog: Catalog,

    /// The pages of every table and index file, cached in memory.
    pub(crate) pool: BufferPool,

    /// Directory where database files are stored. `None` for an in-memory database.
    pub(crate) data_directory: Option<PathBuf>,

    /// Roughly how many bytes of rows a sort may hold in memory. A bigger sort writes
    /// sorted runs to disk and merges them. An in-memory database always sorts in memory.
    sort_mem: usize,
}

impl Database {
    /// Creates a new database instance.
    ///
    /// Creates the data directory if it doesn't exist. Call [`Database::initialize`]
    /// after creation to load the tables stored there.
    pub fn new<P: AsRef<Path>>(data_directory: P) -> Self {
        Self::with_cache_pages(data_directory, DEFAULT_CACHE_PAGES)
    }

    /// Like [`Database::new`], but caches at most `pages` pages of 8 KB in memory. Pages
    /// past that are written back if changed and dropped, then read again when needed.
    pub fn with_cache_pages<P: AsRef<Path>>(data_directory: P, pages: usize) -> Self {
        let data_dir = data_directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).ok();

        Self {
            catalog: Catalog::new(),
            pool: BufferPool::with_capacity(&data_dir, pages),
            data_directory: Some(data_dir),
            sort_mem: DEFAULT_SORT_MEM,
        }
    }

    /// Creates a database that keeps all data in memory and never touches the disk.
    ///
    /// Pages stay in the buffer pool and are never written out, so everything is
    /// lost when the database is dropped. Used when running in the browser through
    /// WebAssembly, where there is no file system.
    ///
    /// # Returns
    ///
    /// An empty database with no tables. [`Database::initialize`] is a no-op for it.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use scuttle_db::Database;
    ///
    /// let mut db = Database::in_memory();
    /// db.execute_query("CREATE TABLE users (name TEXT)").unwrap();
    /// ```
    pub fn in_memory() -> Self {
        Self {
            catalog: Catalog::new(),
            pool: BufferPool::in_memory(),
            data_directory: None,
            sort_mem: DEFAULT_SORT_MEM,
        }
    }

    /// Lets a sort hold about `bytes` of rows in memory before it writes them to disk.
    pub fn set_sort_mem(&mut self, bytes: usize) {
        self.sort_mem = bytes;
    }

    /// Where a sort writes the runs that don't fit in memory, and how much it may hold
    /// first. `None` for an in-memory database, which never touches the disk.
    pub(crate) fn sort_spill(&self) -> Option<Spill> {
        let dir = self.data_directory.as_ref()?;
        Some(Spill {
            dir: dir.join("tmp"),
            budget: self.sort_mem,
        })
    }

    /// Opens the database and loads every table the catalog holds, with its indexes.
    ///
    /// If the last run stopped with pages half written, an index may not match its table,
    /// so this builds every index again from its table's rows.
    pub fn initialize(&mut self) -> Result<()> {
        if self.data_directory.is_none() {
            return Ok(());
        }

        let rebuild = self.pool.was_left_unflushed();
        for name in self.catalog.load(&mut self.pool)? {
            if rebuild {
                for index in &self.catalog.table(&name)?.indexes {
                    self.pool.drop_table(index.file_name())?;
                }
            }
            self.open_indexes(&name)?;
        }
        // The flush writes the catalog rows that loading deleted. It also deletes the
        // unflushed marker, even when no index needed rebuilding
        self.pool.flush()?;
        self.remove_orphan_files()
    }

    /// The table called `name`.
    pub fn get_table(&self, name: &str) -> Result<&TableDef, DatabaseError> {
        self.catalog.table(name)
    }

    /// The name of every table, the catalog's own included.
    pub fn table_names(&self) -> Vec<&str> {
        self.catalog
            .tables()
            .map(|table| table.name.as_str())
            .collect()
    }

    /// Executes a SQL query and returns the results.
    ///
    /// The query goes through a complete pipeline:
    /// 1. **Lexing** - Tokenize the SQL string
    /// 2. **Parsing** - Build an Abstract Syntax Tree (AST)
    /// 3. **Logical Planning** - Convert AST to logical query plan
    /// 4. **Physical Planning** - Convert to executable physical plan
    /// 5. **Execution** - Execute the plan and return rows
    ///
    /// Every row is held in memory at once. Use [`Database::execute_query_streaming`] to
    /// handle rows a batch at a time instead.
    pub fn execute_query(&mut self, query: &str) -> Result<QueryResponse> {
        let mut rows = Vec::new();
        let mut response = self.execute_query_streaming(query, |_, batch| {
            rows.extend(batch);
            Ok(())
        })?;
        response.rows = rows;
        Ok(response)
    }

    /// Executes a SQL query, passing the rows it returns to `on_rows` a batch at a time,
    /// along with the column names.
    ///
    /// A SELECT's rows arrive in the batches the plan produces, so only one batch has to be
    /// in memory at a time. A scan's batches hold about 1,024 rows. A sort holds every row
    /// anyway and passes them in one batch. Empty batches are skipped. The response holds the column names and,
    /// for writes, the number of rows written. Its `rows` are always empty.
    ///
    /// # Errors
    ///
    /// Fails as [`Database::execute_query`] does. An error from `on_rows` stops the query
    /// and is returned.
    pub fn execute_query_streaming(
        &mut self,
        query: &str,
        mut on_rows: impl FnMut(&[String], Vec<Row>) -> Result<()>,
    ) -> Result<QueryResponse> {
        let mut parser = SqlParser::new(query);
        let statement = parser
            .parse()
            .map_err(|e| DatabaseError::InvalidQuery(format!("Parse error: {e}")))?;

        let response = match statement {
            Statement::Create(create_stmt) => self.handle_create(create_stmt),
            Statement::CreateIndex(create) => self
                .create_index(
                    &create.name,
                    &create.table_name,
                    &create.column,
                    create.unique,
                )
                .map(|()| QueryResponse::default()),
            Statement::DropTable(name) => self.drop_table(&name).map(|()| QueryResponse::default()),
            Statement::DropIndex(name) => self.drop_index(&name).map(|()| QueryResponse::default()),
            Statement::Select(select_stmt) => self.handle_select(select_stmt, &mut on_rows),
            Statement::Insert(insert_stmt) => self.handle_insert(insert_stmt),
            Statement::Update(update_stmt) => self.handle_update(update_stmt),
            Statement::Delete(delete_stmt) => self.handle_delete(delete_stmt),
            Statement::Explain { statement, analyze } => self
                .explain_statement(*statement, analyze)
                .and_then(|plan| {
                    let columns = vec!["QUERY PLAN".to_string()];
                    let rows = plan
                        .lines()
                        .map(|line| Row::new(vec![Value::Text(line.to_string())]))
                        .collect();
                    on_rows(&columns, rows)?;
                    Ok(QueryResponse {
                        columns,
                        ..QueryResponse::default()
                    })
                }),
        };

        // Write the pages this statement changed. This runs even when the statement failed,
        // so the file always matches the pages in memory.
        self.pool.flush()?;
        response
    }

    /// A planner that builds plans reading this database's tables.
    pub(crate) fn physical_planner(&mut self) -> PhysicalPlanner<'_> {
        let spill = self.sort_spill();
        PhysicalPlanner::new(&self.catalog, &mut self.pool, spill)
    }

    /// What the nodes of a running plan read from.
    pub(crate) fn exec_ctx(&mut self) -> ExecCtx<'_> {
        ExecCtx {
            pool: &mut self.pool,
        }
    }
}
