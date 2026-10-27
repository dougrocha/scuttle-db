use std::{
    collections::BTreeSet,
    fs::File,
    path::{Path, PathBuf},
};

use miette::Result;
use web_time::{Duration, Instant};

use crate::{
    ColumnConstraint, ColumnDef, DatabaseError, Value,
    db::{
        catalog::system_catalog::SystemCatalog,
        result_table::ResultTable,
        table::{
            Table, cursor::TableCursor, index::Ctid, row::Row, schema::Schema, table_def::TableDef,
        },
    },
    sql::{
        analyzer::Analyzer,
        ast::{
            parser::SqlParser,
            statement::{self, Statement},
        },
        catalog_context::CatalogContext,
        evaluator::{Evaluator, expression::ExpressionEvaluator, predicate::PredicateEvaluator},
        planner::{
            explain,
            logical::LogicalPlan,
            optimizer,
            physical::{DEFAULT_SORT_MEM, ExecCtx, PhysicalPlanner, SortSpill},
        },
    },
    storage::{
        buffer_pool::{BufferPool, DEFAULT_CACHE_PAGES},
        page::{ItemId, PageId},
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
/// `Database` is the primary interface for interacting with Scuttle DB. It manages:
/// - Table definitions and schemas
/// - Data storage via a buffer pool
/// - SQL query execution
/// - Data persistence (work in progress)
///
/// # Architecture
///
/// The database uses a page-based storage model where:
/// - Data is stored in fixed-size pages (default 8KB)
/// - A buffer pool manages pages in memory
/// - Tables are stored as relations with defined schemas
/// - Queries are parsed, planned, and executed through a pipeline
#[derive(Debug)]
pub struct Database {
    /// All tables currently loaded in the database.
    ///
    /// Maps table names to their relation definitions (schema + metadata).
    pub tables: std::collections::BTreeMap<String, TableDef>,

    /// Buffer pool managing pages in memory.
    ///
    /// Handles reading/writing data pages and caching them for performance.
    pub buffer_manager: BufferPool,
    pub catalog: SystemCatalog,

    /// Directory where database files are stored. `None` for an in-memory database.
    data_directory: Option<PathBuf>,

    /// Roughly how many bytes of rows a sort may hold in memory. A bigger sort writes
    /// sorted runs to disk and merges them. An in-memory database always sorts in memory.
    sort_mem: usize,
}

impl Database {
    /// Creates a new database instance.
    ///
    /// Creates the data directory if it doesn't exist. The database starts empty
    /// with no tables loaded. Call [`Database::initialize`] after creation to
    /// set up any necessary system catalogs (currently a no-op).
    pub fn new<P: AsRef<Path>>(data_directory: P) -> Self {
        Self::with_cache_pages(data_directory, DEFAULT_CACHE_PAGES)
    }

    /// Like [`Database::new`], but caches at most `pages` pages of 8 KB in memory. Pages
    /// past that are written back if changed and dropped, then read again when needed.
    pub fn with_cache_pages<P: AsRef<Path>>(data_directory: P, pages: usize) -> Self {
        let data_dir = data_directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).ok();

        Self {
            tables: std::collections::BTreeMap::default(),
            buffer_manager: BufferPool::with_capacity(&data_dir, pages),
            catalog: SystemCatalog::new(),

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
            tables: std::collections::BTreeMap::default(),
            buffer_manager: BufferPool::in_memory(),
            catalog: SystemCatalog::new(),

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
    pub(crate) fn sort_spill(&self) -> Option<SortSpill> {
        let dir = self.data_directory.as_ref()?;
        Some(SortSpill {
            dir: dir.join("tmp"),
            budget: self.sort_mem,
        })
    }

    /// Initializes the database.
    ///
    /// Currently a placeholder for future initialization logic such as:
    /// - Loading system catalogs
    /// - Setting up metadata tables
    /// - Recovering from crash (WAL replay)
    pub fn initialize(&mut self) -> Result<()> {
        let Some(data_directory) = &self.data_directory else {
            return Ok(());
        };
        let path = data_directory.join(format!("{}.table", self.catalog.name()));

        if Path::new(&path).exists() {
            let tables = self.catalog.load_all_tables(&mut self.buffer_manager)?;

            for (name, schema) in tables {
                self.tables
                    .insert(name.clone(), TableDef::new(name.clone(), schema));
                self.open_indexes(&name)?;
            }
        } else {
            println!("No catalog exists. Creates when adding a table");
        }

        Ok(())
    }

    /// Checks if a table exists in the database.
    ///
    /// Looks both in-memory (loaded tables) and on-disk (table files).
    fn table_exists(&self, name: &str) -> bool {
        if self.tables.contains_key(name) {
            true
        } else {
            self.data_directory
                .as_ref()
                .is_some_and(|dir| dir.join(format!("{name}.table")).exists())
        }
    }

    /// Creates a new table with the given schema.
    ///
    /// The table is created in-memory and ready for use immediately.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::TableAlreadyExists`] if a table with the same name already exists.
    /// The existing table, its schema and its rows are left unchanged.
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<(), DatabaseError> {
        if self.table_exists(name) {
            return Err(DatabaseError::TableAlreadyExists(name.to_string()));
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

        let _ = self
            .catalog
            .save_table_metadata(&mut self.buffer_manager, name, &schema, 0);

        let table = TableDef::new(name.to_string(), schema);
        for index in &table.indexes {
            index
                .create(&mut self.buffer_manager)
                .map_err(|error| DatabaseError::InvalidQuery(error.to_string()))?;
        }
        self.tables.insert(name.to_string(), table);

        Ok(())
    }

    /// Gets an immutable reference to a table.
    pub fn get_table(&self, name: &str) -> Result<&TableDef, DatabaseError> {
        self.tables
            .get(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }

    /// Gets a mutable reference to a table.
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableDef, DatabaseError> {
        if self.table_exists(name) {
            #[expect(clippy::missing_panics_doc, reason = "infallible")]
            return Ok(self.tables.get_mut(name).unwrap());
        }

        Err(DatabaseError::TableNotFound(name.to_string()))
    }

    /// Loads table metadata from disk (work in progress).
    ///
    /// Currently unimplemented. In the future, this will:
    /// - Scan the data directory for `.table` files
    /// - Deserialize table metadata
    /// - Load schemas into memory
    pub fn load_from_file(&mut self) -> Result<(), DatabaseError> {
        let Some(data_directory) = &self.data_directory else {
            return Ok(());
        };
        std::fs::create_dir_all(data_directory)?;
        let entries = std::fs::read_dir(data_directory)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("table") {
                let _file = File::open(&path)?;
                // TODO: Fix this decode from file
                // let table: Relation = decode_from_std_read(&mut file, bincode::config::standard())?;

                // println!("Table: {table:#?}");
                //
                // self.tables.insert(table.name.clone(), table);
            }
        }

        Ok(())
    }

    /// Drops a table: its catalog row, its pages and its file.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if !self.table_exists(name) {
            return Err(DatabaseError::TableNotFound(name.to_string()).into());
        }

        self.catalog.remove_table(&mut self.buffer_manager, name)?;
        self.buffer_manager.drop_table(name)?;
        for index in &table_def(&self.tables, name)?.indexes {
            self.buffer_manager.drop_table(index.file_name())?;
        }
        self.tables.remove(name);
        self.buffer_manager.flush()
    }

    /// Inserts a row into a table.
    ///
    /// The row is validated against the table's schema, encoded to bytes,
    /// and stored in a page managed by the buffer pool. The row is persisted
    /// to disk immediately.
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(PageId, ItemId)> {
        let location = self.append_row(table_name, row)?;
        self.buffer_manager.flush()?;
        Ok(location)
    }

    /// Adds a row to the table's last page and marks the page dirty, without writing it.
    ///
    /// Statements call this for every row and leave the writing to the flush at the end of
    /// [`Database::execute_query`], so each page is written once per statement, not once per row.
    ///
    /// The row is added to the table's indexes too. Uniqueness is not checked here: callers
    /// check every row of the statement first, see [`Database::check_unique`].
    fn append_row(&mut self, table_name: &str, row: Row) -> Result<Ctid> {
        // Get schema first (separate borrow scope)
        let encoded_data = {
            let table = self.get_table(table_name)?;
            table.schema().encode_row(&row)
        };

        let free_page = self
            .buffer_manager
            .get_free_page(table_name, encoded_data.len())?;
        let page_id = free_page.header.page_id;
        let item_id = free_page.add_data(&encoded_data)?;

        // Borrows only the tables, so the pool can be borrowed at the same time
        for index in &table_def(&self.tables, table_name)?.indexes {
            index.insert(&mut self.buffer_manager, &row, (page_id, item_id))?;
        }

        Ok((page_id, item_id))
    }

    /// Marks a row deleted and takes it out of the table's indexes. `row` must be the row
    /// stored at `ctid`, which is how the index finds its entry.
    fn delete_row(&mut self, table_name: &str, ctid: Ctid, row: &Row) -> Result<()> {
        let (page_id, item_id) = ctid;
        self.buffer_manager
            .get_page_mut(table_name, page_id)?
            .delete_item(item_id)?;

        for index in &table_def(&self.tables, table_name)?.indexes {
            index.remove(&mut self.buffer_manager, row, ctid)?;
        }

        Ok(())
    }

    /// Fails if writing `new_rows` would break a UNIQUE or PRIMARY KEY column.
    ///
    /// `replacing` holds the rows the statement deletes first (an UPDATE's old versions).
    /// Checked before anything is written, so a failing statement changes nothing.
    fn check_unique(
        &mut self,
        table_name: &str,
        new_rows: &[Row],
        replacing: &BTreeSet<Ctid>,
    ) -> Result<()> {
        let table = table_def(&self.tables, table_name)?;
        for index in &table.indexes {
            let column_name = &table.schema.columns[index.column].name;
            index.check_unique(&mut self.buffer_manager, column_name, new_rows, replacing)?;
        }

        Ok(())
    }

    /// Fills a table's indexes from its rows. Run when the database opens, since only
    /// the indexes' existence is stored, not their entries.
    /// What the nodes of a running plan read from.
    fn exec_ctx(&mut self) -> ExecCtx<'_> {
        ExecCtx {
            pool: &mut self.buffer_manager,
        }
    }

    /// Opens a table's indexes, building any whose file is missing from the table's rows.
    /// That covers a database written before indexes were stored on disk.
    fn open_indexes(&mut self, table_name: &str) -> Result<()> {
        let mut missing = Vec::new();
        for index in &table_def(&self.tables, table_name)?.indexes {
            if !index.exists(&mut self.buffer_manager) {
                missing.push(index.clone());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        for index in &missing {
            index.create(&mut self.buffer_manager)?;
        }
        for (page_id, item_id, row) in self.get_rows_with_location(table_name)? {
            for index in &missing {
                index.insert(&mut self.buffer_manager, &row, (page_id, item_id))?;
            }
        }
        self.buffer_manager.flush()
    }

    /// Every live row of a table, with where it is stored.
    ///
    /// The `(PageId, ItemId)` pair identifies the tuple (PostgreSQL's `ctid`), which
    /// UPDATE and DELETE need to mark the old version as deleted. They read the whole
    /// table before writing, so unlike a SELECT this doesn't stream.
    fn get_rows_with_location(&mut self, table_name: &str) -> Result<Vec<(PageId, ItemId, Row)>> {
        let schema = self.get_table(table_name)?.schema().clone();
        let mut cursor =
            TableCursor::open(&mut self.buffer_manager, table_name.to_string(), schema);

        let mut rows = Vec::new();
        while cursor.read_next_page(&mut self.buffer_manager, |page_id, item_id, row| {
            rows.push((page_id, item_id, row));
            Ok(())
        })? {}

        Ok(rows)
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
        self.buffer_manager.flush()?;
        response
    }

    /// Describes how a SQL statement would run, without running it.
    ///
    /// Parses and analyzes the statement into a logical plan, then renders the plan
    /// as an indented tree, one node per line, similar to Postgres' `EXPLAIN`.
    ///
    /// `EXPLAIN ANALYZE SELECT ...` runs the query as well and adds each node's rows, time and page cache hits and reads.
    ///
    /// # Arguments
    ///
    /// * `query` - A single SQL statement
    ///
    /// # Returns
    ///
    /// The plan tree as text. The database is not modified.
    ///
    /// # Errors
    ///
    /// Returns an error if the statement fails to parse or references a missing
    /// table or column.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use scuttle_db::Database;
    ///
    /// let mut db = Database::in_memory();
    /// db.execute_query("CREATE TABLE users (name TEXT, age INT)").unwrap();
    ///
    /// let plan = db.explain("SELECT name FROM users WHERE age > 30").unwrap();
    /// assert_eq!(plan, "Projection: name\n  Filter: age > 30\n    Scan: users");
    /// ```
    pub fn explain(&mut self, query: &str) -> Result<String> {
        let mut parser = SqlParser::new(query);
        let statement = parser
            .parse()
            .map_err(|e| DatabaseError::InvalidQuery(format!("Parse error: {e}")))?;

        match statement {
            Statement::Explain { statement, analyze } => {
                self.explain_statement(*statement, analyze)
            }
            statement => self.describe(statement),
        }
    }

    fn explain_statement(&mut self, statement: Statement, analyze: bool) -> Result<String> {
        match (statement, analyze) {
            (Statement::Select(select_stmt), true) => self.explain_analyze(select_stmt),
            (_, true) => Err(DatabaseError::InvalidQuery(
                "EXPLAIN ANALYZE only supports SELECT, since it runs the statement".into(),
            )
            .into()),
            (statement, false) => self.describe(statement),
        }
    }

    /// Runs `select_stmt` and returns its plan with the rows and time of each node, like
    /// Postgres' `EXPLAIN ANALYZE`. A node's time includes the time of the nodes under it.
    fn explain_analyze(&mut self, select_stmt: statement::SelectStatement) -> Result<String> {
        let mut context = CatalogContext::new(self);
        let mut plan = Analyzer::new(&context).analyze_select(select_stmt)?;
        let db = &mut *context.database;
        optimizer::optimize(&mut plan, &db.tables, &mut db.buffer_manager)?;
        let tree = explain::explain(&plan);

        // Building the plan includes the index lookup, so it is timed apart from running it
        let start = Instant::now();
        let mut planner = PhysicalPlanner::profiled(&mut context);
        let mut executor = planner.create_physical_plan(plan)?;
        let profile = planner.into_profile();
        let planning = start.elapsed();

        let start = Instant::now();
        while executor.next(&mut self.exec_ctx())?.is_some() {}
        let execution = start.elapsed();

        let ms = |time: Duration| time.as_secs_f64() * 1000.0;
        let mut lines: Vec<String> = tree
            .lines()
            .zip(&profile)
            .map(|(line, stats)| {
                let mut line = format!(
                    "{line} (rows={} time={:.3} ms pages: hit={} read={})",
                    stats.rows.get(),
                    ms(stats.time.get()),
                    stats.hits.get(),
                    stats.reads.get()
                );
                if let Some(detail) = &*stats.detail.borrow() {
                    line.push_str(&format!(" {detail}"));
                }
                line
            })
            .collect();
        lines.push(format!("Planning time: {:.3} ms", ms(planning)));
        lines.push(format!("Execution time: {:.3} ms", ms(execution)));
        Ok(lines.join("\n"))
    }

    /// The plan for `statement`, rendered as [`Database::explain`] returns it.
    fn describe(&mut self, statement: Statement) -> Result<String> {
        let context = CatalogContext::new(self);
        let analyzer = Analyzer::new(&context);
        let mut plan = match statement {
            Statement::Create(create_stmt) => LogicalPlan::CreateTable {
                table_name: create_stmt.table_name,
                columns: create_stmt.columns,
            },
            Statement::Select(select_stmt) => analyzer.analyze_select(select_stmt)?,
            Statement::Insert(insert_stmt) => analyzer.analyze_insert(insert_stmt)?,
            Statement::Update(update_stmt) => analyzer.analyze_update(update_stmt)?,
            Statement::Delete(delete_stmt) => analyzer.analyze_delete(delete_stmt)?,
            Statement::Explain { .. } => {
                return Err(DatabaseError::InvalidQuery("cannot EXPLAIN an EXPLAIN".into()).into());
            }
        };

        let db = &mut *context.database;
        optimizer::optimize(&mut plan, &db.tables, &mut db.buffer_manager)?;
        Ok(explain::explain(&plan))
    }

    fn handle_create(&mut self, create_stmt: statement::CreateStatement) -> Result<QueryResponse> {
        let mut columns = create_stmt.columns;
        for column in &mut columns {
            Self::check_default(column)?;
        }
        let schema = Schema::new(columns);

        self.create_table(&create_stmt.table_name, schema)?;

        Ok(QueryResponse::default())
    }

    /// Converts a column's `DEFAULT` to the column type, so INSERT can use it as is.
    fn check_default(column: &mut ColumnDef) -> Result<()> {
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

    fn handle_select(
        &mut self,
        select_stmt: statement::SelectStatement,
        on_rows: &mut impl FnMut(&[String], Vec<Row>) -> Result<()>,
    ) -> Result<QueryResponse> {
        let mut context = CatalogContext::new(self);
        let analyzer = Analyzer::new(&context);
        let mut anayzed_plan = analyzer.analyze_select(select_stmt)?;
        let db = &mut *context.database;
        optimizer::optimize(&mut anayzed_plan, &db.tables, &mut db.buffer_manager)?;
        let columns = anayzed_plan.output_schema().names();

        // TODO: Eventually add a optimizer here for logical_plan.
        // Will do a series of "pushdowns".
        //
        //  - Predicate Push Down: move filters as close together to the read source
        //  - Projection pushdown: only read columns we actually need (I think this is already implemeted)
        //  - Constant Folding: turn 'age > 10 + 5' to 'age > 25'

        let mut physical_planner = PhysicalPlanner::new(&mut context);
        let mut executor = physical_planner.create_physical_plan(anayzed_plan)?;

        while let Some(batch) = executor.next(&mut self.exec_ctx())? {
            if !batch.rows.is_empty() {
                on_rows(&columns, batch.rows)?;
            }
        }

        Ok(QueryResponse {
            columns,
            ..QueryResponse::default()
        })
    }

    /// Runs an UPDATE: each matching row is deleted and its new version inserted,
    /// the way PostgreSQL writes a new tuple version instead of updating in place.
    fn handle_update(&mut self, update_stmt: statement::UpdateStatement) -> Result<QueryResponse> {
        let context = CatalogContext::new(self);
        let analyzer = Analyzer::new(&context);
        let LogicalPlan::Update {
            table_name,
            assignments,
            filter,
            schema,
        } = analyzer.analyze_update(update_stmt)?
        else {
            unreachable!("analyze_update always returns LogicalPlan::Update")
        };

        // Compute and validate every new row before writing anything, so a bad value
        // doesn't leave the table half-updated. Scanning everything up front also keeps
        // the rows we insert from being picked up and updated a second time.
        let evaluator = ExpressionEvaluator;
        let mut updates = Vec::new();
        for (page_id, item_id, row) in self.get_rows_with_location(&table_name)? {
            if let Some(filter) = &filter
                && !PredicateEvaluator.evaluate(filter, &row)?
            {
                continue;
            }

            let mut new_row = row.clone();
            for (index, expr) in &assignments {
                // Every SET expression sees the old row, not the partially updated one
                let value = evaluator.evaluate(expr, &row)?;
                new_row.values[*index] = Self::coerce_for_column(value, &schema.columns[*index])?;
            }

            updates.push(((page_id, item_id), row, new_row));
        }

        // A row may keep its own key, so the old versions don't count as duplicates
        let replacing: BTreeSet<Ctid> = updates.iter().map(|(ctid, ..)| *ctid).collect();
        let new_rows: Vec<Row> = updates
            .iter()
            .map(|(.., new_row)| new_row.clone())
            .collect();
        self.check_unique(&table_name, &new_rows, &replacing)?;

        let rows_affected = updates.len();
        for (ctid, old_row, new_row) in updates {
            self.delete_row(&table_name, ctid, &old_row)?;
            self.append_row(&table_name, new_row)?;
        }

        Ok(QueryResponse {
            rows_affected: Some(rows_affected),
            ..QueryResponse::default()
        })
    }

    /// Converts a value for storage in `column`, checking type and NOT NULL.
    fn coerce_for_column(value: Value, column: &ColumnDef) -> Result<Value> {
        if value == Value::Null && !column.is_nullable() {
            return Err(DatabaseError::TypeMismatch(format!(
                "Column {} cannot be null",
                column.name
            ))
            .into());
        }

        Ok(value
            .coerce_to(&column.data_type)
            .map_err(DatabaseError::TypeMismatch)?)
    }

    fn handle_delete(&mut self, delete_stmt: statement::DeleteStatement) -> Result<QueryResponse> {
        let context = CatalogContext::new(self);
        let analyzer = Analyzer::new(&context);
        let LogicalPlan::Delete {
            table_name, filter, ..
        } = analyzer.analyze_delete(delete_stmt)?
        else {
            unreachable!("analyze_delete always returns LogicalPlan::Delete")
        };

        let mut rows_affected = 0;
        for (page_id, item_id, row) in self.get_rows_with_location(&table_name)? {
            if let Some(filter) = &filter
                && !PredicateEvaluator.evaluate(filter, &row)?
            {
                continue;
            }

            self.delete_row(&table_name, (page_id, item_id), &row)?;
            rows_affected += 1;
        }

        Ok(QueryResponse {
            rows_affected: Some(rows_affected),
            ..QueryResponse::default()
        })
    }

    fn handle_insert(&mut self, insert_stmt: statement::InsertStatement) -> Result<QueryResponse> {
        let context = CatalogContext::new(self);
        let analyzer = Analyzer::new(&context);
        let analyzed_plan = analyzer.analyze_insert(insert_stmt)?;
        let rows_affected;

        if let LogicalPlan::Insert {
            table_name,
            column_names: insert_columns,
            source,
        } = analyzed_plan
        {
            if let LogicalPlan::Values { expressions, .. } = *source {
                let schema = context.get_table(&table_name)?.schema().clone();
                let evaluator = ExpressionEvaluator;
                // VALUES don't reference columns, so they are evaluated against an empty row
                let empty_row = Row::new(Vec::new());

                // Build and validate every row before writing any of them
                let mut new_rows = Vec::with_capacity(expressions.len());
                for exprs in expressions {
                    let mut values = Vec::with_capacity(schema.columns.len());

                    for col_def in &schema.columns {
                        let value =
                            match insert_columns.iter().position(|name| *name == col_def.name) {
                                Some(idx) => evaluator.evaluate(&exprs[idx], &empty_row)?,
                                None => col_def.default_value().cloned().unwrap_or(Value::Null),
                            };

                        values.push(Self::coerce_for_column(value, col_def)?);
                    }

                    new_rows.push(Row::new(values));
                }

                context
                    .database
                    .check_unique(&table_name, &new_rows, &BTreeSet::new())?;

                rows_affected = new_rows.len();
                for row in new_rows {
                    context.database.append_row(&table_name, row)?;
                }
            } else {
                todo!()
            }
        } else {
            unreachable!("")
        }

        Ok(QueryResponse {
            rows_affected: Some(rows_affected),
            ..QueryResponse::default()
        })
    }
}

/// The table called `name`. Takes only the tables, unlike [`Database::get_table`], so the
/// buffer pool can be borrowed mutably at the same time.
fn table_def<'a>(
    tables: &'a std::collections::BTreeMap<String, TableDef>,
    name: &str,
) -> Result<&'a TableDef, DatabaseError> {
    tables
        .get(name)
        .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A database in a fresh temp directory, removed again on drop.
    struct TestDb {
        db: Database,
        dir: PathBuf,
    }

    impl TestDb {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("scuttle-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);

            let mut db = Database::new(&dir);
            db.initialize().unwrap();
            db.execute_query("CREATE TABLE users (id INT, name TEXT, dept TEXT, age INT)")
                .unwrap();
            db.execute_query(
                "INSERT INTO users VALUES (1, 'Alice', 'eng', 30), (2, 'Bob', 'eng', 25), \
                 (3, 'Carol', 'ops', 41), (4, 'Dan', 'ops', 35)",
            )
            .unwrap();

            Self { db, dir }
        }

        /// Reopens the database from disk, dropping everything cached in memory.
        fn reopen(&mut self) {
            self.db = Database::new(&self.dir);
            self.db.initialize().unwrap();
        }

        fn query(&mut self, sql: &str) -> Vec<Vec<Value>> {
            let response = self.db.execute_query(sql).unwrap();
            response.rows.into_iter().map(|row| row.values).collect()
        }

        fn columns(&mut self, sql: &str) -> Vec<String> {
            self.db.execute_query(sql).unwrap().columns
        }

        fn affected(&mut self, sql: &str) -> Option<usize> {
            self.db.execute_query(sql).unwrap().rows_affected
        }

        fn fails(&mut self, sql: &str) -> bool {
            self.db.execute_query(sql).is_err()
        }

        fn execute(&mut self, sql: &str) {
            self.db.execute_query(sql).unwrap();
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn text(s: &str) -> Value {
        Value::from(s)
    }

    #[test]
    fn test_select_order_by_limit() {
        let mut db = TestDb::new("order-limit");

        assert_eq!(
            db.query("SELECT name FROM users ORDER BY age DESC LIMIT 2"),
            vec![vec![text("Carol")], vec![text("Dan")]]
        );
        assert_eq!(
            db.query("SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1"),
            vec![vec![Value::Int64(2)], vec![Value::Int64(3)]]
        );
        assert_eq!(
            db.query("SELECT id FROM users LIMIT 0"),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            db.query("SELECT id FROM users OFFSET 3"),
            vec![vec![Value::Int64(4)]]
        );
    }

    #[test]
    fn test_select_order_by_multiple_keys_and_hidden_column() {
        let mut db = TestDb::new("order-keys");

        // Sort by a column that is not in the output
        assert_eq!(
            db.query("SELECT name FROM users ORDER BY dept DESC, age"),
            vec![
                vec![text("Dan")],
                vec![text("Carol")],
                vec![text("Bob")],
                vec![text("Alice")],
            ]
        );
    }

    #[test]
    fn test_select_order_by_alias_and_position() {
        let mut db = TestDb::new("order-alias");

        let youngest_first = vec![
            vec![text("Bob")],
            vec![text("Alice")],
            vec![text("Dan")],
            vec![text("Carol")],
        ];
        assert_eq!(
            db.query("SELECT name FROM users ORDER BY age"),
            youngest_first
        );

        let by_alias = db.query("SELECT name, age + 1 AS next_age FROM users ORDER BY next_age");
        let by_position = db.query("SELECT name, age + 1 AS next_age FROM users ORDER BY 2");
        assert_eq!(by_alias, by_position);
        assert_eq!(by_alias[0], vec![text("Bob"), Value::Int64(26)]);

        assert!(db.fails("SELECT name FROM users ORDER BY 2"));
    }

    #[test]
    fn test_select_aliases_name_output_columns() {
        let mut db = TestDb::new("aliases");

        assert_eq!(
            db.columns("SELECT id AS user_id, name, age + 1, COUNT(*) AS n FROM users GROUP BY id, name, age"),
            vec!["user_id", "name", "?column?", "n"]
        );
        assert_eq!(
            db.columns("SELECT * FROM users"),
            vec!["id", "name", "dept", "age"]
        );
        assert_eq!(db.columns("SELECT COUNT(*) FROM users"), vec!["count"]);
    }

    #[test]
    fn test_select_from_alias_is_accepted() {
        let mut db = TestDb::new("from-alias");

        assert_eq!(
            db.query("SELECT name FROM users u WHERE age > 30 ORDER BY id"),
            vec![vec![text("Carol")], vec![text("Dan")]]
        );
    }

    #[test]
    fn test_select_alias_and_qualified_columns() {
        let mut db = TestDb::new("alias-qualified");

        assert_eq!(
            db.query("SELECT u.name FROM users u WHERE u.age > 30 ORDER BY u.id"),
            vec![vec![text("Carol")], vec![text("Dan")]]
        );
        assert_eq!(
            db.query("SELECT u.name FROM users AS u WHERE u.age > 30 ORDER BY u.id"),
            vec![vec![text("Carol")], vec![text("Dan")]]
        );
    }

    #[test]
    fn test_select_qualified_with_table_name_when_not_aliased() {
        let mut db = TestDb::new("qualified-table-name");

        assert_eq!(
            db.query("SELECT users.name FROM users WHERE users.id = 1"),
            vec![vec![text("Alice")]]
        );
    }

    #[test]
    fn test_select_table_name_unusable_after_alias() {
        let mut db = TestDb::new("table-name-after-alias");

        assert!(db.fails("SELECT users.name FROM users u"));
        assert!(db.fails("SELECT x.name FROM users u"));
        assert!(db.fails("SELECT u.nope FROM users u"));
    }

    #[test]
    fn test_join_duplicate_alias_with_zero_column_table_fails() {
        let mut db = TestDb::new("zero-column-duplicate-alias");
        db.execute("CREATE TABLE z ()");

        let err = db
            .db
            .execute_query("SELECT * FROM users u JOIN z u ON 1 = 1")
            .unwrap_err();
        assert!(
            err.to_string().contains("specified more than once"),
            "got: {err}"
        );
    }

    #[test]
    fn test_group_by_bare_and_qualified_are_the_same_key() {
        let mut db = TestDb::new("group-by-qualified");

        assert_eq!(
            db.query("SELECT u.dept, COUNT(*) FROM users u GROUP BY dept ORDER BY u.dept"),
            vec![
                vec![text("eng"), Value::Int64(2)],
                vec![text("ops"), Value::Int64(2)]
            ]
        );
        assert_eq!(
            db.query("SELECT dept, SUM(u.age) FROM users u GROUP BY u.dept ORDER BY dept"),
            vec![
                vec![text("eng"), Value::Int64(55)],
                vec![text("ops"), Value::Int64(76)]
            ]
        );
        assert_eq!(
            db.query("SELECT age + 1 FROM users u GROUP BY u.age + 1 ORDER BY 1"),
            vec![
                vec![Value::Int64(26)],
                vec![Value::Int64(31)],
                vec![Value::Int64(36)],
                vec![Value::Int64(42)]
            ]
        );
    }

    #[test]
    fn test_order_by_duplicate_output_names() {
        let mut db = TestDb::new("order-by-duplicate-names");

        assert!(db.fails("SELECT age AS x, name AS x FROM users ORDER BY x"));
        assert!(db.fails("SELECT age AS id, id FROM users ORDER BY id"));
        assert_eq!(db.query("SELECT age AS x FROM users ORDER BY x").len(), 4);
        assert_eq!(db.query("SELECT *, id FROM users ORDER BY id").len(), 4);
        assert_eq!(
            db.query("SELECT id, id FROM users ORDER BY id DESC LIMIT 1"),
            vec![vec![Value::Int64(4), Value::Int64(4)]]
        );
    }

    #[test]
    fn test_quoted_identifier_with_dot_is_split() {
        let mut db = TestDb::new("quoted-dot");

        // Documented limitation: "a.b" is split into relation `a` and column `b`.
        assert!(db.fails(r#"SELECT "a.b" FROM users"#));
    }

    #[test]
    fn test_update_delete_accept_table_qualified_columns() {
        let mut db = TestDb::new("update-delete-qualified");

        db.execute("UPDATE users SET age = users.age + 1 WHERE users.id = 1");
        assert_eq!(
            db.query("SELECT age FROM users WHERE id = 1"),
            vec![vec![Value::Int64(31)]]
        );

        db.execute("DELETE FROM users WHERE users.id = 4");
        assert_eq!(
            db.query("SELECT COUNT(*) FROM users"),
            vec![vec![Value::Int64(3)]]
        );
    }

    #[test]
    fn test_count_star() {
        let mut db = TestDb::new("count");

        assert_eq!(
            db.query("SELECT COUNT(*) FROM users"),
            vec![vec![Value::Int64(4)]]
        );
        assert_eq!(
            db.query("SELECT COUNT(*) FROM users WHERE dept = 'eng'"),
            vec![vec![Value::Int64(2)]]
        );

        // No GROUP BY: still one row over an empty input. With GROUP BY: no groups, no rows.
        assert_eq!(
            db.query("SELECT COUNT(*), SUM(age) FROM users WHERE age > 100"),
            vec![vec![Value::Int64(0), Value::Null]]
        );
        assert!(
            db.query("SELECT dept, COUNT(*) FROM users WHERE age > 100 GROUP BY dept")
                .is_empty()
        );
    }

    #[test]
    fn test_group_by_aggregates() {
        let mut db = TestDb::new("group-by");

        assert_eq!(
            db.query(
                "SELECT dept, COUNT(*), SUM(age), AVG(age), MIN(name), MAX(age) \
                 FROM users GROUP BY dept ORDER BY dept"
            ),
            vec![
                vec![
                    text("eng"),
                    Value::Int64(2),
                    Value::Int64(55),
                    Value::Float64(27.5),
                    text("Alice"),
                    Value::Int64(30),
                ],
                vec![
                    text("ops"),
                    Value::Int64(2),
                    Value::Int64(76),
                    Value::Float64(38.0),
                    text("Carol"),
                    Value::Int64(41),
                ],
            ]
        );
    }

    #[test]
    fn test_group_by_order_by_aggregate() {
        let mut db = TestDb::new("group-order");

        // By an aggregate that is not selected, and by an aggregate's alias
        let expected = vec![vec![text("ops")], vec![text("eng")]];
        assert_eq!(
            db.query("SELECT dept FROM users GROUP BY dept ORDER BY SUM(age) DESC"),
            expected
        );
        assert_eq!(
            db.query("SELECT dept, MAX(age) AS oldest FROM users GROUP BY dept ORDER BY oldest DESC LIMIT 1"),
            vec![vec![text("ops"), Value::Int64(41)]]
        );

        // Expressions over aggregates and group keys
        assert_eq!(
            db.query("SELECT dept, SUM(age) / COUNT(*) FROM users GROUP BY dept ORDER BY 1"),
            vec![
                vec![text("eng"), Value::Int64(27)],
                vec![text("ops"), Value::Int64(38)],
            ]
        );
    }

    #[test]
    fn test_aggregate_errors() {
        let mut db = TestDb::new("aggregate-errors");

        assert!(db.fails("SELECT name, COUNT(*) FROM users"));
        assert!(db.fails("SELECT name FROM users GROUP BY dept"));
        assert!(db.fails("SELECT * FROM users WHERE COUNT(*) > 1"));
        assert!(db.fails("SELECT SUM(COUNT(*)) FROM users"));
        assert!(db.fails("SELECT SUM(name) FROM users"));
        assert!(db.fails("SELECT SUM(*) FROM users"));
        assert!(db.fails("SELECT UPPER(name) FROM users"));
    }

    #[test]
    fn test_delete() {
        let mut db = TestDb::new("delete");

        assert_eq!(db.affected("DELETE FROM users WHERE dept = 'ops'"), Some(2));
        assert_eq!(
            db.query("SELECT name FROM users ORDER BY id"),
            vec![vec![text("Alice")], vec![text("Bob")]]
        );

        assert_eq!(db.affected("DELETE FROM users WHERE id = 99"), Some(0));
        assert_eq!(db.affected("DELETE FROM users"), Some(2));
        assert!(db.query("SELECT * FROM users").is_empty());
    }

    #[test]
    fn test_update() {
        let mut db = TestDb::new("update");

        assert_eq!(
            db.affected("UPDATE users SET name = 'Alicia', age = age + 1 WHERE id = 1"),
            Some(1)
        );
        assert_eq!(
            db.query("SELECT name, age FROM users WHERE id = 1"),
            vec![vec![text("Alicia"), Value::Int64(31)]]
        );
        assert_eq!(
            db.query("SELECT COUNT(*) FROM users"),
            vec![vec![Value::Int64(4)]]
        );
    }

    #[test]
    fn test_update_all_rows_updates_each_once() {
        let mut db = TestDb::new("update-all");

        assert_eq!(db.affected("UPDATE users SET age = age + 1"), Some(4));
        assert_eq!(
            db.query("SELECT age FROM users ORDER BY id"),
            vec![
                vec![Value::Int64(31)],
                vec![Value::Int64(26)],
                vec![Value::Int64(42)],
                vec![Value::Int64(36)],
            ]
        );
    }

    #[test]
    fn test_update_set_expressions_see_old_row() {
        let mut db = TestDb::new("update-swap");

        db.affected("UPDATE users SET id = age, age = id WHERE name = 'Bob'");
        assert_eq!(
            db.query("SELECT id, age FROM users WHERE name = 'Bob'"),
            vec![vec![Value::Int64(25), Value::Int64(2)]]
        );
    }

    #[test]
    fn test_update_errors() {
        let mut db = TestDb::new("update-errors");

        assert!(db.fails("UPDATE users SET missing = 1"));
        assert!(db.fails("UPDATE users SET age = 'old'"));
        assert!(db.fails("UPDATE users SET age = 1, age = 2"));
        assert!(db.fails("UPDATE nope SET age = 1"));

        // Nothing was changed by the failed statements
        assert_eq!(
            db.query("SELECT SUM(age) FROM users"),
            vec![vec![Value::Int64(131)]]
        );
    }

    #[test]
    fn test_update_and_delete_persist() {
        let mut db = TestDb::new("persist");

        db.affected("UPDATE users SET age = 50 WHERE id = 2");
        db.affected("DELETE FROM users WHERE id = 3");
        db.reopen();

        assert_eq!(
            db.query("SELECT id, age FROM users ORDER BY id"),
            vec![
                vec![Value::Int64(1), Value::Int64(30)],
                vec![Value::Int64(2), Value::Int64(50)],
                vec![Value::Int64(4), Value::Int64(35)],
            ]
        );
    }

    #[test]
    fn test_table_larger_than_1000_pages_persists() {
        let mut db = TestDb::new("many-pages");
        db.execute("CREATE TABLE wide (id INT, body TEXT)");

        // About 2KB per row fits four rows in a page, so this needs more than 1000 pages
        let body = "x".repeat(2000);
        let rows = 4200;
        for start in (0..rows).step_by(100) {
            let values: Vec<String> = (start..start + 100)
                .map(|id| format!("({id}, '{body}')"))
                .collect();
            db.execute(&format!("INSERT INTO wide VALUES {}", values.join(", ")));
        }
        assert!(db.db.buffer_manager.page_count("wide") > 1000);

        let count = |db: &mut TestDb| db.query("SELECT COUNT(*) FROM wide");
        assert_eq!(count(&mut db), vec![vec![Value::Int64(rows)]]);

        // Reopening reads the page count back from the file, and the last page loaded
        // from disk is full, so the next insert has to start a new page
        db.reopen();
        assert_eq!(count(&mut db), vec![vec![Value::Int64(rows)]]);
        db.execute(&format!("INSERT INTO wide VALUES ({rows}, '{body}')"));
        assert_eq!(count(&mut db), vec![vec![Value::Int64(rows + 1)]]);

        // Rows past page 1000 can be found and deleted
        assert_eq!(db.affected("DELETE FROM wide WHERE id >= 4100"), Some(101));
        db.reopen();
        assert_eq!(count(&mut db), vec![vec![Value::Int64(4100)]]);
    }

    #[test]
    fn test_unique_columns_reject_duplicates_and_change_nothing() {
        let mut db = TestDb::new("unique");
        db.execute("CREATE TABLE acct (id INT PRIMARY KEY, email TEXT UNIQUE, note TEXT)");
        db.execute("INSERT INTO acct VALUES (1, 'a@x', 'one'), (2, 'b@x', 'two')");
        let ids = |db: &mut TestDb| db.query("SELECT id FROM acct ORDER BY id");
        let both = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];

        // Against stored rows, within one statement, and on either unique column. A failing
        // statement writes none of its rows.
        assert!(db.fails("INSERT INTO acct VALUES (3, 'c@x', ''), (1, 'd@x', '')"));
        assert!(db.fails("INSERT INTO acct VALUES (3, 'c@x', ''), (3, 'd@x', '')"));
        assert!(db.fails("INSERT INTO acct VALUES (3, 'a@x', '')"));
        assert_eq!(ids(&mut db), both);

        // NULL is not a key, so a UNIQUE column holds any number of them
        db.execute("INSERT INTO acct VALUES (3, NULL, ''), (4, NULL, '')");
        db.execute("DELETE FROM acct WHERE id >= 3");

        // A row may keep its key, and keys may move as long as they stay distinct
        db.execute("UPDATE acct SET note = 'same key' WHERE id = 1");
        db.execute("UPDATE acct SET id = id + 1");
        assert_eq!(
            ids(&mut db),
            vec![vec![Value::Int64(2)], vec![Value::Int64(3)]]
        );
        assert!(db.fails("UPDATE acct SET id = 7"));

        // A deleted key is free again, and the index comes back after a restart
        db.execute("DELETE FROM acct WHERE id = 3");
        db.execute("INSERT INTO acct VALUES (3, 'b2@x', '')");
        db.reopen();
        assert!(db.fails("INSERT INTO acct VALUES (3, 'z@x', '')"));
        assert!(db.fails("INSERT INTO acct VALUES (9, 'a@x', '')"));
        db.execute("INSERT INTO acct VALUES (9, 'z@x', '')");
    }

    #[test]
    fn test_index_scan_returns_the_same_rows_as_a_full_scan() {
        let mut db = TestDb::new("index-scan");
        db.execute("CREATE TABLE item (id INT PRIMARY KEY, code TEXT UNIQUE, qty INT)");
        db.execute("CREATE TABLE plain (id INT, code TEXT, qty INT)");
        for table in ["item", "plain"] {
            let values: Vec<String> = (1..=300)
                .map(|id| format!("({id}, 'c{id:03}', {})", id % 7))
                .collect();
            db.execute(&format!("INSERT INTO {table} VALUES {}", values.join(", ")));
            db.execute(&format!("DELETE FROM {table} WHERE id = 150"));
            db.execute(&format!("UPDATE {table} SET id = id + 1000 WHERE id = 151"));
        }

        // Each WHERE runs through the index on `item` and as a full scan on `plain`
        for filter in [
            "id = 42",
            "id = 150",
            "id = 1151",
            "42 = id",
            "id > 290",
            "id >= 140 AND id < 160",
            "100 < id AND id <= 103 AND qty > 2",
            "id > 50 AND id < 10",
            "id = 7 AND id = 8",
            "code = 'c007'",
            "code >= 'c298'",
        ] {
            let sql =
                |table| format!("SELECT id, code, qty FROM {table} WHERE {filter} ORDER BY id");
            assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{filter}");
            assert!(
                db.db
                    .explain(&sql("item"))
                    .unwrap()
                    .contains("IndexScan: item"),
                "{filter}"
            );
        }

        // Conditions an index can't narrow keep the full scan
        for filter in ["qty = 3", "id != 5", "id = 5 OR id = 6", "id = 5.5"] {
            let plan = db
                .db
                .explain(&format!("SELECT id FROM item WHERE {filter}"))
                .unwrap();
            assert!(
                plan.contains("Scan: item") && !plan.contains("IndexScan"),
                "{filter}"
            );
        }
    }

    #[test]
    fn test_explain_shows_the_index_range() {
        let mut db = TestDb::new("index-explain");
        db.execute("CREATE TABLE item (id INT PRIMARY KEY, qty INT)");
        let values: Vec<String> = (1..=100).map(|id| format!("({id}, 0)")).collect();
        db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

        assert_eq!(
            db.db.explain("SELECT qty FROM item WHERE id = 5").unwrap(),
            "Projection: qty\n  Filter: id = 5\n    IndexScan: item (id = 5)"
        );
        assert_eq!(
            db.db
                .explain("SELECT qty FROM item i WHERE i.id > 5 AND i.id <= 9")
                .unwrap(),
            "Projection: qty\n  Filter: (id > 5) AND (id <= 9)\n    IndexScan: item i (id > 5 AND id <= 9)"
        );

        // Past a tenth of the table, reading every page in order beats fetching rows one
        // at a time, so the index is only used for the narrower range
        let plan = |db: &mut TestDb, filter: &str| {
            db.db
                .explain(&format!("SELECT qty FROM item WHERE {filter}"))
                .unwrap()
        };
        assert!(plan(&mut db, "id > 91").ends_with("IndexScan: item (id > 91)"));
        assert!(plan(&mut db, "id > 89").ends_with("    Scan: item"));
        assert!(plan(&mut db, "id > 0").ends_with("    Scan: item"));
    }

    #[test]
    fn test_explain_statement_returns_the_plan_as_rows() {
        let mut db = TestDb::new("explain-statement");
        db.execute("CREATE TABLE item (id INT PRIMARY KEY, qty INT)");

        let response = db
            .db
            .execute_query("explain SELECT qty FROM item WHERE id = 5;")
            .unwrap();

        assert_eq!(response.columns, vec!["QUERY PLAN"]);
        let lines: Vec<_> = response
            .rows
            .iter()
            .map(|row| row.values[0].to_string())
            .collect();
        assert_eq!(
            lines,
            [
                "Projection: qty",
                "  Filter: id = 5",
                "    IndexScan: item (id = 5)"
            ]
        );

        // Explaining a write doesn't run it
        db.db
            .execute_query("EXPLAIN INSERT INTO item VALUES (1, 2)")
            .unwrap();
        assert!(
            db.db
                .execute_query("SELECT * FROM item")
                .unwrap()
                .rows
                .is_empty()
        );
    }

    #[test]
    fn test_explain_analyze_counts_the_rows_each_node_produced() {
        let mut db = TestDb::new("explain-analyze");
        db.execute("CREATE TABLE item (id INT PRIMARY KEY, qty INT)");
        let values: Vec<String> = (1..=100).map(|id| format!("({id}, 0)")).collect();
        db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

        // Times vary between runs, so only the text before them is compared
        let analyze = |db: &mut TestDb, filter: &str| -> Vec<String> {
            let sql = format!("EXPLAIN ANALYZE SELECT qty FROM item WHERE {filter}");
            let response = db.db.execute_query(&sql).unwrap();
            response
                .rows
                .iter()
                .map(|row| {
                    let line = row.values[0].to_string();
                    match line.split_once(" time=") {
                        Some((before, _)) => before.to_string(),
                        None => line.split_once(": ").unwrap().0.to_string(),
                    }
                })
                .collect()
        };

        assert_eq!(
            analyze(&mut db, "id > 80"),
            [
                "Projection: qty (rows=20",
                "  Filter: id > 80 (rows=20",
                "    Scan: item (rows=100",
                "Planning time",
                "Execution time",
            ]
        );
        assert_eq!(
            analyze(&mut db, "id = 5")[2],
            "    IndexScan: item (id = 5) (rows=1"
        );

        // It runs the statement, so writes are refused rather than applied
        assert!(
            db.db
                .execute_query("EXPLAIN ANALYZE INSERT INTO item VALUES (101, 0)")
                .is_err()
        );
        assert!(
            db.db
                .execute_query("SELECT * FROM item WHERE id = 101")
                .unwrap()
                .rows
                .is_empty()
        );
    }

    #[test]
    fn test_dropped_table_stays_dropped_after_reopen() {
        let mut db = TestDb::new("drop-table");
        db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
        db.execute("INSERT INTO item VALUES (1)");

        db.db.drop_table("item").unwrap();
        assert!(!db.dir.join("item.table").exists());
        assert!(!db.dir.join("item_id.index").exists());
        db.reopen();

        assert!(db.db.execute_query("SELECT * FROM item").is_err());
        assert_eq!(
            db.query("SELECT id FROM users WHERE id = 1"),
            [[Value::Int64(1)]]
        );

        // The name is free again, and the new table starts empty
        db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
        db.execute("INSERT INTO item VALUES (1)");
        assert_eq!(db.query("SELECT * FROM item"), [[Value::Int64(1)]]);
        assert!(db.db.drop_table("nope").is_err());
    }

    #[test]
    fn test_an_index_is_kept_in_its_own_file_and_rebuilt_if_it_goes_missing() {
        let mut db = TestDb::new("index-file");
        db.execute("CREATE TABLE item (id INT PRIMARY KEY, tag TEXT UNIQUE)");
        let values: Vec<String> = (0..2000).map(|id| format!("({id}, 't{id}')")).collect();
        db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));
        db.execute("DELETE FROM item WHERE id < 1000");
        db.execute("UPDATE item SET id = id + 5000 WHERE id >= 1990");

        let lookups = |db: &mut TestDb| {
            assert_eq!(
                db.query("SELECT tag FROM item WHERE id = 1500"),
                [[Value::Text("t1500".into())]]
            );
            assert_eq!(
                db.query("SELECT id FROM item WHERE id = 500"),
                Vec::<Vec<Value>>::new()
            );
            assert_eq!(
                db.query("SELECT count(*) FROM item WHERE id >= 6990"),
                [[Value::Int64(10)]]
            );
            assert_eq!(
                db.query("SELECT id FROM item WHERE tag = 't1995'"),
                [[Value::Int64(6995)]]
            );
            // The index still knows which keys are taken
            assert!(db.fails("INSERT INTO item VALUES (1500, 'new')"));
            assert!(db.fails("INSERT INTO item VALUES (1, 't1600')"));
        };

        let index = db.dir.join("item_id.index");
        assert!(index.exists() && db.dir.join("item_tag.index").exists());
        lookups(&mut db);

        db.reopen();
        lookups(&mut db);

        // Without its file, the index is built again from the table when the database opens
        std::fs::remove_file(&index).unwrap();
        db.reopen();
        assert!(index.exists());
        lookups(&mut db);
    }

    #[test]
    fn test_top_n_returns_the_same_rows_as_a_full_sort() {
        let mut db = TestDb::new("top-n");
        db.execute("CREATE TABLE item (id INT, grp INT, tag TEXT)");
        // Few distinct values, so most rows tie and only input order tells them apart.
        // More rows than TopNExec buffers (1,024), so it has to cut the buffer down.
        let values: Vec<String> = (0..3000)
            .map(|id| {
                let tag = if id % 5 == 0 {
                    "NULL".to_string()
                } else {
                    format!("'t{}'", id % 3)
                };
                format!("({id}, {}, {tag})", (id * 7) % 4)
            })
            .collect();
        db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

        for order in ["grp", "grp DESC", "tag", "tag DESC, grp", "grp, tag DESC"] {
            let sorted = db.query(&format!("SELECT id FROM item ORDER BY {order}"));
            for (limit, offset) in [
                (0, 0),
                (1, 0),
                (10, 0),
                (10, 25),
                (600, 0),
                (2999, 1),
                (5000, 0),
                (5, 2998),
            ] {
                let sql =
                    format!("SELECT id FROM item ORDER BY {order} LIMIT {limit} OFFSET {offset}");
                let expected: Vec<_> = sorted.iter().skip(offset).take(limit).cloned().collect();
                assert_eq!(db.query(&sql), expected, "{sql}");
            }
        }
    }

    #[test]
    fn test_a_sort_past_its_memory_budget_returns_the_same_rows() {
        let mut db = TestDb::new("external-sort");
        db.execute("CREATE TABLE item (id INT, grp INT, tag TEXT)");
        // Few distinct values, so most rows tie and only input order tells them apart
        let values: Vec<String> = (0..3000)
            .map(|id| {
                let tag = if id % 5 == 0 {
                    "NULL".to_string()
                } else {
                    format!("'t{}'", id % 3)
                };
                format!("({id}, {}, {tag})", (id * 7) % 4)
            })
            .collect();
        db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

        let orders = ["grp", "grp DESC", "tag", "tag DESC, grp", "id DESC"];
        let in_memory: Vec<_> = orders
            .iter()
            .map(|order| db.query(&format!("SELECT * FROM item ORDER BY {order}")))
            .collect();

        // Small enough to write dozens of runs
        db.db.set_sort_mem(16 * 1024);
        for (order, expected) in orders.iter().zip(&in_memory) {
            let sql = format!("SELECT * FROM item ORDER BY {order}");
            assert_eq!(&db.query(&sql), expected, "{sql}");
        }

        let plan = db.query("EXPLAIN ANALYZE SELECT id FROM item ORDER BY grp");
        let sort = plan[1][0].to_string();
        assert!(sort.contains("sort: external merge, "), "{sort}");
        // The runs are deleted once merged
        assert_eq!(std::fs::read_dir(db.dir.join("tmp")).unwrap().count(), 0);

        db.db.set_sort_mem(DEFAULT_SORT_MEM);
        let plan = db.query("EXPLAIN ANALYZE SELECT id FROM item ORDER BY grp");
        assert!(plan[1][0].to_string().ends_with("sort: in memory"));
    }

    #[test]
    fn test_a_tiny_page_cache_gives_the_same_answers() {
        let dir = std::env::temp_dir().join(format!("scuttle-tiny-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let open = || {
            let mut db = Database::with_cache_pages(&dir, 3);
            db.initialize().unwrap();
            db
        };
        let query = |db: &mut Database, sql: &str| -> Vec<Vec<Value>> {
            let response = db.execute_query(sql).unwrap();
            response.rows.into_iter().map(|row| row.values).collect()
        };

        let mut db = open();
        db.execute_query("CREATE TABLE item (id INT PRIMARY KEY, note TEXT)")
            .unwrap();
        // About 40 pages of rows, so a 3-page cache evicts on nearly every page
        let note = "x".repeat(200);
        for chunk in (0..1500).collect::<Vec<_>>().chunks(100) {
            let values: Vec<String> = chunk.iter().map(|id| format!("({id}, '{note}')")).collect();
            db.execute_query(&format!("INSERT INTO item VALUES {}", values.join(", ")))
                .unwrap();
        }
        db.execute_query("UPDATE item SET note = 'changed' WHERE id >= 1000")
            .unwrap();
        db.execute_query("DELETE FROM item WHERE id < 100").unwrap();

        for reopened in [false, true] {
            if reopened {
                db = open();
            }
            assert_eq!(
                query(&mut db, "SELECT count(*) FROM item"),
                [[Value::Int64(1400)]]
            );
            assert_eq!(
                query(&mut db, "SELECT count(*) FROM item WHERE note = 'changed'"),
                [[Value::Int64(500)]]
            );
            // Through the index, which fetches each row's page by itself
            assert_eq!(
                query(&mut db, "SELECT id, note FROM item WHERE id = 1234"),
                [[Value::Int64(1234), Value::Text("changed".into())]]
            );
            assert!(query(&mut db, "SELECT id FROM item WHERE id = 50").is_empty());
        }
        assert!(
            db.buffer_manager.stats().reads > 40,
            "{:?}",
            db.buffer_manager.stats()
        );
    }

    #[test]
    fn test_streaming_passes_rows_in_batches_and_stops_on_error() {
        let mut db = TestDb::new("streaming");
        db.execute("CREATE TABLE item (id INT)");
        let values: Vec<String> = (0..3000).map(|id| format!("({id})")).collect();
        db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

        let mut batches = Vec::new();
        let response = db
            .db
            .execute_query_streaming("SELECT id FROM item", |columns, rows| {
                assert_eq!(columns, ["id"]);
                batches.push(rows);
                Ok(())
            })
            .unwrap();

        assert_eq!(response.columns, ["id"]);
        assert!(response.rows.is_empty());
        assert!(batches.len() > 1 && batches.iter().all(|rows| !rows.is_empty()));
        let streamed: Vec<_> = batches
            .into_iter()
            .flatten()
            .map(|row| row.values)
            .collect();
        assert_eq!(streamed, db.query("SELECT id FROM item"));

        // An error from the callback ends the query after the first batch
        let mut calls = 0;
        let result = db
            .db
            .execute_query_streaming("SELECT id FROM item", |_, _| {
                calls += 1;
                Err(miette::miette!("output closed"))
            });
        assert!(result.unwrap_err().to_string().contains("output closed"));
        assert_eq!(calls, 1);
    }

    #[test]
    fn test_response_columns_widen_to_fit_their_values() {
        let response = QueryResponse {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                Row::new(vec![
                    Value::Int64(1),
                    Value::Text("a much longer name".into()),
                ]),
                Row::new(vec![Value::Int64(123456), Value::Null]),
            ],
            rows_affected: None,
        };

        assert_eq!(
            response.to_string(),
            [
                " id     | name",
                "--------+--------------------",
                " 1      | a much longer name",
                " 123456 | NULL",
                "(2 rows)",
            ]
            .join("\n")
        );
    }

    #[test]
    fn test_create_table_rejects_two_primary_keys() {
        let mut db = TestDb::new("two-pks");
        assert!(db.fails("CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)"));
        assert!(db.fails("SELECT * FROM t"));
    }

    #[test]
    fn test_insert_null() {
        let mut db = TestDb::new("insert-null");

        db.affected("INSERT INTO users VALUES (5, 'Eve', NULL, NULL)");
        // Omitted nullable columns are NULL too
        db.affected("INSERT INTO users (id, name) VALUES (6, 'Finn')");

        assert_eq!(
            db.query("SELECT id FROM users WHERE age IS NULL ORDER BY id"),
            vec![vec![Value::Int64(5)], vec![Value::Int64(6)]]
        );

        // Aggregates skip NULLs; COUNT(*) doesn't
        assert_eq!(
            db.query("SELECT COUNT(*), COUNT(age), AVG(age) FROM users"),
            vec![vec![
                Value::Int64(6),
                Value::Int64(4),
                Value::Float64(32.75)
            ]]
        );

        // NULLs sort last ascending, first descending
        assert_eq!(
            db.query("SELECT id FROM users ORDER BY age LIMIT 1"),
            vec![vec![Value::Int64(2)]]
        );
        assert_eq!(
            db.query("SELECT id FROM users ORDER BY age DESC, id LIMIT 1"),
            vec![vec![Value::Int64(5)]]
        );

        // NULL is its own group
        assert_eq!(
            db.query("SELECT dept, COUNT(*) FROM users GROUP BY dept ORDER BY dept"),
            vec![
                vec![text("eng"), Value::Int64(2)],
                vec![text("ops"), Value::Int64(2)],
                vec![Value::Null, Value::Int64(2)],
            ]
        );

        assert_eq!(
            db.affected("UPDATE users SET age = NULL WHERE id = 1"),
            Some(1)
        );
        assert_eq!(
            db.query("SELECT age FROM users WHERE id = 1"),
            vec![vec![Value::Null]]
        );
    }

    #[test]
    fn test_insert_respects_not_null() {
        let mut db = TestDb::new("insert-not-null");
        db.execute("CREATE TABLE strict (id INT PRIMARY KEY, name TEXT NOT NULL, note TEXT NULL)");

        assert!(db.fails("INSERT INTO strict VALUES (1, NULL, 'x')"));
        assert!(db.fails("INSERT INTO strict VALUES (NULL, 'a', 'x')"));
        assert!(db.fails("INSERT INTO strict (id) VALUES (1)"));

        assert_eq!(
            db.affected("INSERT INTO strict (id, name) VALUES (1, 'a')"),
            Some(1)
        );
        assert_eq!(
            db.query("SELECT * FROM strict"),
            vec![vec![Value::Int64(1), text("a"), Value::Null]]
        );
        assert!(db.fails("UPDATE strict SET name = NULL"));
    }

    #[test]
    fn test_insert_type_checks() {
        let mut db = TestDb::new("insert-types");
        db.execute("CREATE TABLE prices (id INT, price FLOAT, code VARCHAR(3))");

        // Wrong types are rejected instead of crashing the encoder
        assert!(db.fails("INSERT INTO prices VALUES (2.5, 1.0, 'abc')"));
        assert!(db.fails("INSERT INTO prices VALUES ('one', 1.0, 'abc')"));
        assert!(db.fails("INSERT INTO prices VALUES (1, 1.0, 'toolong')"));

        // INT widens to FLOAT
        db.affected("INSERT INTO prices VALUES (1, 3, 'abc')");
        assert_eq!(
            db.query("SELECT price FROM prices"),
            vec![vec![Value::Float64(3.0)]]
        );
    }

    #[test]
    fn test_insert_column_list() {
        let mut db = TestDb::new("insert-columns");

        // Values follow the column list's order, not the table's
        db.affected("INSERT INTO users (age, name, id, dept) VALUES (50, 'Gus', 7, 'ops')");
        assert_eq!(
            db.query("SELECT id, name, age FROM users WHERE id = 7"),
            vec![vec![Value::Int64(7), text("Gus"), Value::Int64(50)]]
        );

        // Expressions are evaluated, not dropped
        db.affected("INSERT INTO users VALUES (4 * 2, 'Hal', 'eng', 20 + 1)");
        assert_eq!(
            db.query("SELECT id, age FROM users WHERE name = 'Hal'"),
            vec![vec![Value::Int64(8), Value::Int64(21)]]
        );

        assert!(db.fails("INSERT INTO users (id, missing) VALUES (9, 1)"));
        assert!(db.fails("INSERT INTO users (id, id) VALUES (9, 9)"));
        assert!(db.fails("INSERT INTO users (id, name) VALUES (9)"));
        assert!(db.fails("INSERT INTO users (id) VALUES (age)"));

        // A failed multi-row insert writes nothing
        assert!(db.fails("INSERT INTO users (id) VALUES (10), ('bad')"));
        assert_eq!(
            db.query("SELECT COUNT(*) FROM users"),
            vec![vec![Value::Int64(6)]]
        );
    }

    /// `orders(id, user_id, total)` and `depts(code, title)` on top of `users`.
    fn seed_orders(db: &mut TestDb) {
        db.execute("CREATE TABLE orders (id INT, user_id INT, total INT)");
        db.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 1, 50), (12, 3, 75), (13, 9, 5)");
        db.execute("CREATE TABLE depts (code TEXT, title TEXT)");
        db.execute("INSERT INTO depts VALUES ('eng', 'Engineering'), ('ops', 'Operations')");
    }

    #[test]
    fn test_join_inner_values_from_both_sides() {
        let mut db = TestDb::new("join-inner");
        seed_orders(&mut db);

        let expected = vec![
            vec![text("Alice"), Value::Int64(100)],
            vec![text("Alice"), Value::Int64(50)],
            vec![text("Carol"), Value::Int64(75)],
        ];
        assert_eq!(
            db.query("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id"),
            expected
        );
        assert_eq!(
            db.query("SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.id"),
            expected
        );
    }

    #[test]
    fn test_join_same_column_name_both_sides() {
        let mut db = TestDb::new("join-same-name");
        seed_orders(&mut db);

        assert_eq!(
            db.query(
                "SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id"
            ),
            vec![
                vec![Value::Int64(1), Value::Int64(10)],
                vec![Value::Int64(1), Value::Int64(11)],
                vec![Value::Int64(3), Value::Int64(12)],
            ]
        );
    }

    #[test]
    fn test_join_left_pads_unmatched_with_null() {
        let mut db = TestDb::new("join-left-pad");
        seed_orders(&mut db);

        assert_eq!(
            db.query("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id"),
            vec![
                vec![text("Alice"), Value::Int64(100)],
                vec![text("Alice"), Value::Int64(50)],
                vec![text("Bob"), Value::Null],
                vec![text("Carol"), Value::Int64(75)],
                vec![text("Dan"), Value::Null],
            ]
        );
    }

    /// The NULL padding of an unmatched LEFT row must be exactly as wide as the right
    /// table: a join chained after it reads its columns at fixed positions.
    #[test]
    fn test_join_after_left_join_on_empty_table_reads_shifted_columns() {
        let mut db = TestDb::new("join-pad-width");
        seed_orders(&mut db);
        db.execute("CREATE TABLE empty_t (id INT, v INT)");

        assert_eq!(
            db.query(
                "SELECT u.name, e.v, o.total FROM users u LEFT JOIN empty_t e ON u.id = e.id \
                 JOIN orders o ON o.user_id = u.id ORDER BY o.id"
            ),
            vec![
                vec![text("Alice"), Value::Null, Value::Int64(100)],
                vec![text("Alice"), Value::Null, Value::Int64(50)],
                vec![text("Carol"), Value::Null, Value::Int64(75)],
            ]
        );
    }

    #[test]
    fn test_join_left_with_empty_right_table() {
        let mut db = TestDb::new("join-empty-right");
        db.execute("CREATE TABLE empty_t (id INT, v INT)");

        assert_eq!(
            db.query(
                "SELECT u.id, e.v FROM users u LEFT JOIN empty_t e ON u.id = e.id ORDER BY u.id"
            ),
            vec![
                vec![Value::Int64(1), Value::Null],
                vec![Value::Int64(2), Value::Null],
                vec![Value::Int64(3), Value::Null],
                vec![Value::Int64(4), Value::Null],
            ]
        );
        assert_eq!(
            db.columns("SELECT * FROM users u LEFT JOIN empty_t e ON u.id = e.id")
                .len(),
            6
        );
        assert_eq!(
            db.query("SELECT u.id FROM users u JOIN empty_t e ON u.id = e.id"),
            Vec::<Vec<Value>>::new()
        );
    }

    #[test]
    fn test_join_left_on_vs_where() {
        let mut db = TestDb::new("join-on-vs-where");
        seed_orders(&mut db);

        // The condition in ON keeps every user; unmatched users are padded.
        assert_eq!(
            db.query("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id AND o.total > 60 ORDER BY u.id"),
            vec![
                vec![text("Alice"), Value::Int64(100)],
                vec![text("Bob"), Value::Null],
                vec![text("Carol"), Value::Int64(75)],
                vec![text("Dan"), Value::Null],
            ]
        );

        // The same condition in WHERE removes the padded rows.
        assert_eq!(
            db.query("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.total > 60 ORDER BY u.id"),
            vec![
                vec![text("Alice"), Value::Int64(100)],
                vec![text("Carol"), Value::Int64(75)],
            ]
        );
    }

    #[test]
    fn test_join_left_anti_join_with_is_null() {
        let mut db = TestDb::new("join-anti");
        seed_orders(&mut db);

        assert_eq!(
            db.query("SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.id IS NULL ORDER BY u.id"),
            vec![vec![text("Bob")], vec![text("Dan")]]
        );
    }

    #[test]
    fn test_join_self_join() {
        let mut db = TestDb::new("join-self");

        assert_eq!(
            db.query("SELECT a.name, b.name FROM users a JOIN users b ON a.dept = b.dept AND a.id < b.id ORDER BY a.id, b.id"),
            vec![
                vec![text("Alice"), text("Bob")],
                vec![text("Carol"), text("Dan")],
            ]
        );
        assert_eq!(
            db.columns("SELECT a.name, b.name FROM users a JOIN users b ON a.dept = b.dept"),
            vec!["name", "name"]
        );
    }

    #[test]
    fn test_join_chain_third_table_column() {
        let mut db = TestDb::new("join-chain");
        seed_orders(&mut db);

        assert_eq!(
            db.query("SELECT u.name, o.total, d.title FROM users u JOIN orders o ON u.id = o.user_id JOIN depts d ON u.dept = d.code ORDER BY o.id"),
            vec![
                vec![text("Alice"), Value::Int64(100), text("Engineering")],
                vec![text("Alice"), Value::Int64(50), text("Engineering")],
                vec![text("Carol"), Value::Int64(75), text("Operations")],
            ]
        );
    }

    #[test]
    fn test_join_chain_left_then_left() {
        let mut db = TestDb::new("join-chain-left");
        seed_orders(&mut db);

        // `NULL = 50` is not true, so rows padded by the first LEFT JOIN stay padded.
        assert_eq!(
            db.query("SELECT u.name, o.total, d.title FROM users u LEFT JOIN orders o ON u.id = o.user_id LEFT JOIN depts d ON o.total = 50 ORDER BY u.id, o.id, d.code"),
            vec![
                vec![text("Alice"), Value::Int64(100), Value::Null],
                vec![text("Alice"), Value::Int64(50), text("Engineering")],
                vec![text("Alice"), Value::Int64(50), text("Operations")],
                vec![text("Bob"), Value::Null, Value::Null],
                vec![text("Carol"), Value::Int64(75), Value::Null],
                vec![text("Dan"), Value::Null, Value::Null],
            ]
        );
    }

    #[test]
    fn test_join_aggregate_count_unmatched_is_zero() {
        let mut db = TestDb::new("join-aggregate");
        seed_orders(&mut db);

        assert_eq!(
            db.query("SELECT u.name, COUNT(o.id) AS n FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name"),
            vec![
                vec![text("Alice"), Value::Int64(2)],
                vec![text("Bob"), Value::Int64(0)],
                vec![text("Carol"), Value::Int64(1)],
                vec![text("Dan"), Value::Int64(0)],
            ]
        );
        // Bare `dept` is unique across the join, so it resolves without a qualifier.
        assert_eq!(
            db.query("SELECT dept, SUM(o.total) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY dept ORDER BY dept"),
            vec![
                vec![text("eng"), Value::Int64(150)],
                vec![text("ops"), Value::Int64(75)],
            ]
        );
    }

    #[test]
    fn test_join_where_and_limit_over_join() {
        let mut db = TestDb::new("join-where-limit");
        seed_orders(&mut db);

        assert_eq!(
            db.query("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total >= 75 ORDER BY o.total DESC LIMIT 1"),
            vec![vec![text("Alice"), Value::Int64(100)]]
        );
    }

    #[test]
    fn test_join_rejects_right_join() {
        let mut db = TestDb::new("join-right");
        seed_orders(&mut db);

        assert!(db.fails("SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id"));
    }

    #[test]
    fn test_join_analysis_errors_fail_at_execution() {
        let mut db = TestDb::new("join-errors");
        seed_orders(&mut db);

        // Ambiguous unqualified column (`id` is in both tables).
        assert!(db.fails("SELECT id FROM users u JOIN orders o ON u.id = o.user_id"));
        // Same unaliased table twice.
        assert!(db.fails("SELECT * FROM users JOIN users ON users.id = users.id"));
        // Same alias twice.
        assert!(db.fails("SELECT * FROM users a JOIN orders a ON a.id = a.id"));
        // Unknown alias in ON.
        assert!(db.fails("SELECT * FROM users u JOIN orders o ON u.id = x.user_id"));
        // ON refers to a table that is joined later.
        assert!(db.fails(
            "SELECT * FROM users u JOIN orders o ON u.id = d.code JOIN depts d ON u.dept = d.code"
        ));
        // ON is not boolean.
        assert!(db.fails("SELECT * FROM users u JOIN orders o ON u.id"));
        // Table name is hidden once it has an alias.
        assert!(db.fails("SELECT users.name FROM users u JOIN orders o ON u.id = o.user_id"));
    }

    #[test]
    fn test_join_select_star_columns_and_values() {
        let mut db = TestDb::new("join-star");
        seed_orders(&mut db);
        let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 10";

        assert_eq!(
            db.columns(sql),
            vec!["id", "name", "dept", "age", "id", "user_id", "total"]
        );
        assert_eq!(
            db.query(sql),
            vec![vec![
                Value::Int64(1),
                text("Alice"),
                text("eng"),
                Value::Int64(30),
                Value::Int64(10),
                Value::Int64(1),
                Value::Int64(100),
            ]]
        );
    }

    #[test]
    fn test_join_select_star_self_join_is_not_ambiguous() {
        let mut db = TestDb::new("join-star-self");

        let rows = db.query("SELECT * FROM users a JOIN users b ON a.id = b.id ORDER BY a.id");
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| row.len() == 8));
        assert_eq!(
            rows[0],
            vec![
                Value::Int64(1),
                text("Alice"),
                text("eng"),
                Value::Int64(30),
                Value::Int64(1),
                text("Alice"),
                text("eng"),
                Value::Int64(30),
            ]
        );
    }

    #[test]
    fn test_join_order_by_duplicated_output_name_is_ambiguous() {
        let mut db = TestDb::new("join-order-ambiguous");
        seed_orders(&mut db);
        let select = "SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id";

        assert!(db.fails(&format!("{select} ORDER BY id")));
        assert_eq!(
            db.query(&format!("{select} ORDER BY 2")).len(),
            3,
            "ORDER BY 2 should work"
        );
        assert_eq!(
            db.query(&format!("{select} ORDER BY o.id")),
            vec![
                vec![Value::Int64(1), Value::Int64(10)],
                vec![Value::Int64(1), Value::Int64(11)],
                vec![Value::Int64(3), Value::Int64(12)],
            ]
        );
    }

    #[test]
    fn test_join_unqualified_unique_name_still_works() {
        let mut db = TestDb::new("join-unqualified-unique");
        seed_orders(&mut db);

        assert_eq!(
            db.query(
                "SELECT name, total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY total"
            ),
            vec![
                vec![text("Alice"), Value::Int64(50)],
                vec![text("Carol"), Value::Int64(75)],
                vec![text("Alice"), Value::Int64(100)],
            ]
        );
        assert!(db.fails("SELECT id FROM users u JOIN orders o ON u.id = o.user_id"));
    }

    #[test]
    fn test_join_qualified_star_expands_one_relation() {
        let mut db = TestDb::new("join-qualified-star");
        seed_orders(&mut db);

        let sql = "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id";
        assert_eq!(db.columns(sql), vec!["id", "name", "dept", "age"]);
        assert_eq!(
            db.query(sql),
            vec![
                vec![
                    Value::Int64(1),
                    text("Alice"),
                    text("eng"),
                    Value::Int64(30)
                ],
                vec![
                    Value::Int64(1),
                    text("Alice"),
                    text("eng"),
                    Value::Int64(30)
                ],
                vec![
                    Value::Int64(3),
                    text("Carol"),
                    text("ops"),
                    Value::Int64(41)
                ],
            ]
        );
    }

    #[test]
    fn test_join_qualified_star_mixed_with_column() {
        let mut db = TestDb::new("join-qualified-star-mixed");
        seed_orders(&mut db);

        let sql = "SELECT o.*, u.name FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id";
        assert_eq!(db.columns(sql), vec!["id", "user_id", "total", "name"]);
        assert_eq!(
            db.query(sql)[0],
            vec![
                Value::Int64(10),
                Value::Int64(1),
                Value::Int64(100),
                text("Alice"),
            ]
        );
    }

    #[test]
    fn test_join_qualified_star_unknown_relation_fails() {
        let mut db = TestDb::new("join-qualified-star-unknown");

        assert!(db.fails("SELECT x.* FROM users u"));
    }

    #[test]
    fn test_join_multiplication_still_works_in_select_list() {
        let mut db = TestDb::new("join-multiply-regression");

        assert_eq!(
            db.query("SELECT 2 * 3 FROM users ORDER BY id"),
            vec![vec![Value::Int64(6)]; 4]
        );
    }

    /// The error message from a statement that must fail.
    fn error_message(db: &mut Database, sql: &str) -> String {
        match db.execute_query(sql) {
            Err(err) => err.to_string(),
            Ok(_) => panic!("expected `{sql}` to fail"),
        }
    }

    #[test]
    fn test_create_table_existing_table_fails() {
        let mut db = TestDb::new("create-existing-fails");
        assert_eq!(
            error_message(&mut db.db, "CREATE TABLE users (id INT)"),
            "Table users already exists"
        );
    }

    #[test]
    fn test_create_table_existing_table_keeps_schema_and_rows() {
        let mut db = TestDb::new("create-existing-keeps");
        assert!(db.fails("CREATE TABLE users (id INT)"));
        assert_eq!(
            db.columns("SELECT * FROM users"),
            vec!["id", "name", "dept", "age"]
        );
        assert_eq!(
            db.query("SELECT COUNT(*) FROM users"),
            vec![vec![Value::Int64(4)]]
        );
    }

    #[test]
    fn test_create_table_existing_table_fails_after_reopen() {
        let mut db = TestDb::new("create-existing-reopen");
        db.reopen();
        assert!(db.fails("CREATE TABLE users (id INT)"));
        assert_eq!(
            db.query("SELECT COUNT(*) FROM users"),
            vec![vec![Value::Int64(4)]]
        );
    }

    #[test]
    fn test_create_table_new_name_then_duplicate_fails() {
        let mut db = TestDb::new("create-new-then-dup");
        db.execute("CREATE TABLE extra (id INT)");
        assert_eq!(
            error_message(&mut db.db, "CREATE TABLE extra (id INT)"),
            "Table extra already exists"
        );
    }

    #[test]
    fn test_default_applies_when_insert_omits_column() {
        let mut db = TestDb::new("default-omitted");
        db.execute("CREATE TABLE t (id INT, n INT DEFAULT 5)");
        db.execute("INSERT INTO t (id) VALUES (1)");
        assert_eq!(
            db.query("SELECT * FROM t"),
            vec![vec![Value::Int64(1), Value::Int64(5)]]
        );
    }

    #[test]
    fn test_default_literal_kinds() {
        let mut db = TestDb::new("default-kinds");
        db.execute(
            "CREATE TABLE t (id INT, f FLOAT DEFAULT 2, s TEXT DEFAULT 'hi', \
             b BOOL DEFAULT TRUE, z TEXT DEFAULT NULL)",
        );
        db.execute("INSERT INTO t (id) VALUES (1)");
        assert_eq!(
            db.query("SELECT * FROM t"),
            vec![vec![
                Value::Int64(1),
                Value::Float64(2.0),
                text("hi"),
                Value::Bool(true),
                Value::Null,
            ]]
        );
    }

    #[test]
    fn test_default_not_used_when_value_given() {
        let mut db = TestDb::new("default-given");
        db.execute("CREATE TABLE t (id INT, n INT DEFAULT 5)");
        db.execute("INSERT INTO t (id, n) VALUES (1, 9)");
        db.execute("INSERT INTO t (id, n) VALUES (2, NULL)");
        assert_eq!(
            db.query("SELECT * FROM t"),
            vec![
                vec![Value::Int64(1), Value::Int64(9)],
                vec![Value::Int64(2), Value::Null],
            ]
        );
    }

    #[test]
    fn test_default_lets_not_null_column_be_omitted() {
        let mut db = TestDb::new("default-not-null");
        db.execute("CREATE TABLE t (id INT, n INT NOT NULL DEFAULT 4)");
        db.execute("INSERT INTO t (id) VALUES (1)");
        assert_eq!(
            db.query("SELECT * FROM t"),
            vec![vec![Value::Int64(1), Value::Int64(4)]]
        );
    }

    #[test]
    fn test_default_null_on_not_null_column_fails_at_insert() {
        let mut db = TestDb::new("default-null-not-null");
        db.execute("CREATE TABLE t (id INT, n INT NOT NULL DEFAULT NULL)");
        let message = error_message(&mut db.db, "INSERT INTO t (id) VALUES (1)");
        assert!(message.contains("Column n cannot be null"), "{message}");
        assert!(db.query("SELECT * FROM t").is_empty());
    }

    #[test]
    fn test_default_rejects_wrong_type_at_create() {
        let mut db = TestDb::new("default-wrong-type");
        let message = error_message(&mut db.db, "CREATE TABLE t (id INT DEFAULT 'x')");
        assert!(
            message.contains("Column id has an invalid DEFAULT"),
            "{message}"
        );
        assert!(db.fails("SELECT * FROM t"));
    }

    #[test]
    fn test_default_survives_restart() {
        let mut db = TestDb::new("default-restart");
        db.execute("CREATE TABLE t (id INT, n INT DEFAULT 5)");
        db.reopen();
        db.execute("INSERT INTO t (id) VALUES (1)");
        assert_eq!(
            db.query("SELECT * FROM t"),
            vec![vec![Value::Int64(1), Value::Int64(5)]]
        );
    }

    #[test]
    fn test_column_without_default_still_gets_null() {
        let mut db = TestDb::new("default-none");
        db.execute("CREATE TABLE t (id INT, n INT)");
        db.execute("INSERT INTO t (id) VALUES (1)");
        assert_eq!(
            db.query("SELECT * FROM t"),
            vec![vec![Value::Int64(1), Value::Null]]
        );
    }

    fn ts(text: &str) -> Value {
        Value::Timestamp(crate::core::timestamp::parse_timestamp(text).unwrap())
    }

    /// A database with an `ev(id INT, at TIMESTAMP, note TEXT)` table holding four rows.
    fn timestamp_db(name: &str) -> TestDb {
        let mut db = TestDb::new(name);
        db.execute("CREATE TABLE ev (id INT, at TIMESTAMP, note TEXT)");
        db.execute(
            "INSERT INTO ev VALUES (1, '2026-03-01 12:00:00', 'march'), (2, '2026-01-01', 'new year'), \
             (3, TIMESTAMP '1969-12-31 23:59:59', 'before'), (4, NULL, 'unknown')",
        );
        db
    }

    #[test]
    fn test_timestamp_insert_string_and_select_formats_it() {
        let mut db = timestamp_db("ts-insert-string");
        assert_eq!(
            db.query("SELECT at FROM ev WHERE id = 2"),
            vec![vec![ts("2026-01-01 00:00:00")]]
        );
    }

    #[test]
    fn test_timestamp_insert_typed_literal() {
        let mut db = timestamp_db("ts-insert-typed");
        assert_eq!(
            db.query("SELECT at FROM ev WHERE id = 3"),
            vec![vec![ts("1969-12-31 23:59:59")]]
        );
    }

    #[test]
    fn test_timestamp_insert_named_column_leaves_others_null() {
        let mut db = timestamp_db("ts-named-column");
        db.execute("INSERT INTO ev (id) VALUES (5)");
        assert_eq!(
            db.query("SELECT at, note FROM ev WHERE id = 5"),
            vec![vec![Value::Null, Value::Null]]
        );
    }

    #[test]
    fn test_timestamp_insert_rejects_bad_text() {
        let mut db = timestamp_db("ts-bad-insert");
        assert!(db.fails("INSERT INTO ev VALUES (5, 'yesterday', 'x')"));
        assert!(db.fails("INSERT INTO ev VALUES (5, '2026-02-30', 'x')"));
        assert_eq!(
            db.query("SELECT COUNT(*) FROM ev"),
            vec![vec![Value::Int64(4)]]
        );
    }

    #[test]
    fn test_timestamp_text_column_does_not_accept_timestamps() {
        let mut db = TestDb::new("ts-text-column");
        assert!(db.fails("INSERT INTO users (id, name) VALUES (9, TIMESTAMP '2026-01-01')"));
    }

    #[test]
    fn test_timestamp_comparison_with_string_literal_both_orders() {
        let mut db = timestamp_db("ts-cmp-string");
        assert_eq!(
            db.query("SELECT id FROM ev WHERE at > '2026-01-15'"),
            vec![vec![Value::Int64(1)]]
        );
        assert_eq!(
            db.query("SELECT id FROM ev WHERE '2026-01-01 00:00:00' = at"),
            vec![vec![Value::Int64(2)]]
        );
        assert_eq!(
            db.query("SELECT id FROM ev WHERE '2026-01-15' < at"),
            vec![vec![Value::Int64(1)]]
        );
    }

    #[test]
    fn test_timestamp_comparison_with_typed_literal() {
        let mut db = timestamp_db("ts-cmp-typed");
        assert_eq!(
            db.query("SELECT id FROM ev WHERE at >= TIMESTAMP '2026-01-01' ORDER BY id"),
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]
        );
    }

    #[test]
    fn test_timestamp_comparison_rejects_bad_text() {
        let mut db = timestamp_db("ts-cmp-bad");
        assert!(db.fails("SELECT id FROM ev WHERE at > 'soon'"));
        assert!(db.fails("SELECT id FROM ev WHERE at = '2026-02-30'"));
    }

    #[test]
    fn test_timestamp_column_not_comparable_with_text_column() {
        let mut db = timestamp_db("ts-cmp-text-column");
        assert!(db.fails("SELECT id FROM ev WHERE at = note"));
    }

    #[test]
    fn test_timestamp_order_by_and_min_max() {
        let mut db = timestamp_db("ts-order-minmax");
        assert_eq!(
            db.query("SELECT id FROM ev ORDER BY at"),
            vec![
                vec![Value::Int64(3)],
                vec![Value::Int64(2)],
                vec![Value::Int64(1)],
                vec![Value::Int64(4)],
            ]
        );
        assert_eq!(
            db.query("SELECT MIN(at), MAX(at) FROM ev"),
            vec![vec![ts("1969-12-31 23:59:59"), ts("2026-03-01 12:00:00")]]
        );
    }

    #[test]
    fn test_timestamp_update_set_string_and_where() {
        let mut db = timestamp_db("ts-update");
        assert_eq!(
            db.affected("UPDATE ev SET at = '2030-05-06 07:08:09' WHERE id = 3"),
            Some(1)
        );
        assert_eq!(
            db.query("SELECT at FROM ev WHERE id = 3"),
            vec![vec![ts("2030-05-06 07:08:09")]]
        );
    }

    #[test]
    fn test_timestamp_delete_where() {
        let mut db = timestamp_db("ts-delete");
        assert_eq!(
            db.affected("DELETE FROM ev WHERE at < '2026-01-01'"),
            Some(1)
        );
        assert_eq!(
            db.query("SELECT COUNT(*) FROM ev"),
            vec![vec![Value::Int64(3)]]
        );
    }

    #[test]
    fn test_timestamp_arithmetic_is_rejected() {
        let mut db = timestamp_db("ts-arithmetic");
        assert!(db.fails("SELECT at + 1 FROM ev"));
        assert!(db.fails("SELECT at - at FROM ev"));
    }

    #[test]
    fn test_timestamp_survives_restart() {
        let mut db = timestamp_db("ts-restart");
        db.reopen();
        assert_eq!(
            db.query("SELECT id, at FROM ev ORDER BY id"),
            vec![
                vec![Value::Int64(1), ts("2026-03-01 12:00:00")],
                vec![Value::Int64(2), ts("2026-01-01 00:00:00")],
                vec![Value::Int64(3), ts("1969-12-31 23:59:59")],
                vec![Value::Int64(4), Value::Null],
            ]
        );
    }

    #[test]
    fn test_timestamp_default_literal() {
        let mut db = TestDb::new("ts-default");
        db.execute("CREATE TABLE log (id INT, at TIMESTAMP DEFAULT '2026-01-01 08:00:00')");
        db.execute("INSERT INTO log (id) VALUES (1)");
        assert_eq!(
            db.query("SELECT at FROM log"),
            vec![vec![ts("2026-01-01 08:00:00")]]
        );
    }

    #[test]
    fn test_timestamp_explain_shows_typed_literal() {
        let mut db = timestamp_db("ts-explain");
        let plan = db
            .db
            .explain("SELECT id FROM ev WHERE at > TIMESTAMP '2026-01-01'")
            .unwrap();
        assert!(
            plan.contains("TIMESTAMP '2026-01-01 00:00:00'"),
            "plan was {plan}"
        );
    }
}
