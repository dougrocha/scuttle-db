use std::{
    fs::File,
    path::{Path, PathBuf},
};

use miette::Result;

use crate::{
    DatabaseError,
    db::table::{Relation, Row, Schema, Table},
    sql::{
        analyzer::{Analyzer, ResolvedSchema},
        parser::SqlParser,
        physical_planner::PhysicalPlanner,
        planner_context::PlannerContext,
    },
    storage::{
        buffer_pool::BufferPool,
        page::{ItemId, PageHeader, PageId},
    },
};

/// Response from executing a SQL query.
///
/// Contains the table metadata and the result rows.
#[derive(Debug)]
pub struct QueryResponse {
    pub schema: ResolvedSchema,

    /// The rows returned by the query.
    pub rows: Vec<Row>,
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
    pub tables: std::collections::BTreeMap<String, Relation>,

    /// Buffer pool managing pages in memory.
    ///
    /// Handles reading/writing data pages and caching them for performance.
    pub buffer_manager: BufferPool,

    /// Directory where database files are stored.
    data_directory: PathBuf,
}

impl Database {
    /// Creates a new database instance.
    ///
    /// Creates the data directory if it doesn't exist. The database starts empty
    /// with no tables loaded. Call [`Database::initialize`] after creation to
    /// set up any necessary system catalogs (currently a no-op).
    pub fn new<P: AsRef<Path>>(data_directory: P) -> Self {
        let data_dir = data_directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).ok();

        Self {
            tables: std::collections::BTreeMap::default(),
            buffer_manager: BufferPool::new(),

            data_directory: data_directory.as_ref().to_path_buf(),
        }
    }

    /// Initializes the database.
    ///
    /// Currently a placeholder for future initialization logic such as:
    /// - Loading system catalogs
    /// - Setting up metadata tables
    /// - Recovering from crash (WAL replay)
    pub fn initialize(&mut self) -> Result<()> {
        Ok(())
    }

    /// Checks if a table exists in the database.
    ///
    /// Looks both in-memory (loaded tables) and on-disk (table files).
    fn table_exists(&self, name: &str) -> bool {
        if self.tables.contains_key(name) {
            true
        } else {
            let table_path = self.data_directory.join(format!("{name}.table"));
            table_path.exists()
        }
    }

    /// Creates a new table with the given schema.
    ///
    /// The table is created in-memory and ready for use immediately. Currently,
    /// if a table with the same name already exists, it prints a warning and
    /// continues (re-creating the table in memory).
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<(), DatabaseError> {
        if self.table_exists(name) {
            // return Err(DatabaseError::InvalidQuery(format!(
            //     "Table {name} already exists"
            // )));
            // Eventually save table information in a catalog table,
            // but for now just load the table with the schema normally
            println!("Table {name} already exists");
        }

        let table = Relation::new(name.to_string(), schema);
        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    /// Gets an immutable reference to a table.
    pub fn get_table(&self, name: &str) -> Result<&Relation, DatabaseError> {
        self.tables
            .get(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }

    /// Gets a mutable reference to a table.
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Relation, DatabaseError> {
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
        std::fs::create_dir_all(&self.data_directory)?;
        let entries = std::fs::read_dir(&self.data_directory)?;

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

    /// Drops a table from the database.
    ///
    /// Removes the table from memory. Currently does not delete on-disk files.
    pub fn drop_table(&mut self, name: &str) -> Result<(), DatabaseError> {
        self.tables
            .remove(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))?;
        Ok(())
    }

    /// Inserts a row into a table.
    ///
    /// The row is validated against the table's schema, encoded to bytes,
    /// and stored in a page managed by the buffer pool. The row is persisted
    /// to disk immediately.
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(PageId, ItemId)> {
        println!("Inserting row into table: {table_name}");

        // Get schema first (separate borrow scope)
        let encoded_data = {
            let table = self.get_table(table_name)?;
            table.schema().encode_row(&row)
        };

        // Now get the page and insert data
        let free_page = self
            .buffer_manager
            .get_free_page(table_name, encoded_data.len())?;
        let page_id = free_page.header.page_id;

        let item_id = free_page.add_data(&encoded_data)?;

        // Note: save_page method needs to be implemented in BufferManager
        self.buffer_manager.save_page(table_name, page_id)?;

        Ok((page_id, item_id))
    }

    /// Retrieves all rows from a table (full table scan).
    ///
    /// Scans all pages for the table and decodes all non-deleted rows.
    /// This is an expensive operation for large tables. Use [`Database::execute_query`]
    /// with a WHERE clause to filter rows efficiently.
    pub fn get_rows(&mut self, table_name: &str) -> Result<Vec<Row>, DatabaseError> {
        let mut found_rows: Vec<Row> = Vec::new();
        let max_pages = 1000; // To prevent infinite loops

        for current_page_id in 0..max_pages {
            // Use get_page instead of get_page_mut (which doesn't exist)
            let page_res = self.buffer_manager.get_page(table_name, current_page_id);

            let page = match page_res {
                Ok(page) => page,
                Err(_) => break,
            };

            for item_pointer in page.item_pointers() {
                if item_pointer.is_deleted() {
                    continue;
                }

                let offset = item_pointer.offset as usize - PageHeader::SIZE;
                let length = item_pointer.length as usize;

                let item_data = &page.data[offset..offset + length];
                let decoded_row = self
                    .tables
                    .get(table_name)
                    .unwrap()
                    .schema()
                    .decode_row(item_data)
                    // TODO: Eventually return a SerializationError
                    .expect("Row should be decoded");

                found_rows.push(decoded_row);
            }
        }

        Ok(found_rows)
    }

    /// Executes a SQL query and returns the results.
    ///
    /// The query goes through a complete pipeline:
    /// 1. **Lexing** - Tokenize the SQL string
    /// 2. **Parsing** - Build an Abstract Syntax Tree (AST)
    /// 3. **Logical Planning** - Convert AST to logical query plan
    /// 4. **Physical Planning** - Convert to executable physical plan
    /// 5. **Execution** - Execute the plan and return rows
    pub fn execute_query(&mut self, query: &str) -> Result<QueryResponse> {
        let mut parser = SqlParser::new(query);
        let statement = parser
            .parse()
            .map_err(|e| DatabaseError::InvalidQuery(format!("Parse error: {e}")))?;

        let mut context = PlannerContext::new(self);
        let analyzer = Analyzer::new(&context);
        let anayzed_plan = analyzer.analyze(statement)?;

        // TODO: Eventually add a optimizer here for logical_plan.
        // Will do a series of "pushdowns".
        //
        //  - Predicate Push Down: move filters as close together to the read source
        //  - Projection pushdown: only read columns we actually need (I think this is already implemeted)
        //  - Constant Folding: turn 'age > 10 + 5' to 'age > 25'

        let mut physical_planner = PhysicalPlanner::new(&mut context);
        let mut executor = physical_planner
            .create_physical_plan(anayzed_plan)
            .map_err(|e| DatabaseError::InvalidQuery(format!("Physical Plan error: {e}")))?;

        let mut batches = Vec::new();
        while let Some(batch) = executor.next()? {
            batches.push(batch);
        }

        let schema = executor.schema().clone(); // ONE clone
        let rows = batches.into_iter().flat_map(|b| b.rows).collect();
        Ok(QueryResponse { schema, rows })
    }
}
