use miette::{IntoDiagnostic, Result};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

use crate::{
    DatabaseError,
    db::table::{Relation, Row, Schema, Table, Value},
    sql::{
        logical_planner::LogicalPlan, parser::SqlParser, physical_planner::PhysicalPlan,
        planner_context::PlannerContext, predicate_evaluator::PredicateEvaluator,
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
pub struct QueryResponse<'a> {
    /// Reference to the table (relation) that was queried.
    pub relation: &'a Relation,

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
///
/// # Example
///
/// ```no_run
/// use scuttle_db::*;
///
/// let mut db = Database::new("./my_data");
/// db.initialize().expect("Failed to initialize");
///
/// // Create a table
/// let schema = Schema::new(vec![
///     ColumnDefinition::new("id", DataType::Integer, false),
///     ColumnDefinition::new("name", DataType::Text, false),
/// ]);
/// db.create_table("users", schema).expect("Failed to create table");
///
/// // Insert data
/// let row = Row::new(vec![
///     Value::Integer(1),
///     Value::Text("Alice".to_string()),
/// ]);
/// db.insert_row("users", row).expect("Failed to insert");
///
/// // Query data
/// let result = db.execute_query("SELECT * FROM users").expect("Query failed");
/// for row in result.rows {
///     println!("{:?}", row);
/// }
/// ```
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
    ///
    /// # Arguments
    ///
    /// * `data_directory` - Path where database files will be stored
    ///
    /// # Example
    ///
    /// ```no_run
    /// use scuttle_db::Database;
    ///
    /// let db = Database::new("./my_database");
    /// // Database directory created if it doesn't exist
    /// ```
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
    ///
    /// # Errors
    ///
    /// Currently always succeeds. Future versions may return errors if
    /// initialization fails.
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
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the table to create
    /// * `schema` - Schema defining the table's columns and their types
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or a [`DatabaseError`] if the operation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use scuttle_db::*;
    ///
    /// let mut db = Database::new("./data");
    ///
    /// let schema = Schema::new(vec![
    ///     ColumnDefinition::new("id", DataType::Integer, false),
    ///     ColumnDefinition::new("email", DataType::VarChar(255), false),
    ///     ColumnDefinition::new("active", DataType::Boolean, true),
    /// ]);
    ///
    /// db.create_table("users", schema).expect("Failed to create table");
    /// ```
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
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the table to retrieve
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::TableNotFound`] if the table doesn't exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scuttle_db::*;
    /// # let mut db = Database::new("./data");
    /// # let schema = Schema::new(vec![]);
    /// # db.create_table("users", schema).unwrap();
    /// let table = db.get_table("users").expect("Table not found");
    /// println!("Table '{}' has {} columns", table.name, table.schema.columns.len());
    /// ```
    pub fn get_table(&self, name: &str) -> Result<&Relation, DatabaseError> {
        self.tables
            .get(name)
            .ok_or_else(|| DatabaseError::TableNotFound(name.to_string()))
    }

    /// Gets a mutable reference to a table.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the table to retrieve
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::TableNotFound`] if the table doesn't exist.
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Relation, DatabaseError> {
        if self.table_exists(name) {
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
    ///
    /// # Errors
    ///
    /// May return I/O errors or deserialization errors.
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
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the table to drop
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::TableNotFound`] if the table doesn't exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scuttle_db::*;
    /// # let mut db = Database::new("./data");
    /// # let schema = Schema::new(vec![]);
    /// # db.create_table("temp", schema).unwrap();
    /// db.drop_table("temp").expect("Failed to drop table");
    /// ```
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
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to insert into
    /// * `row` - The row data to insert
    ///
    /// # Returns
    ///
    /// Returns `(PageId, ItemId)` indicating where the row was stored.
    /// - `PageId` - The page number where the row is stored
    /// - `ItemId` - The item slot within that page
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table doesn't exist
    /// - The row doesn't match the schema
    /// - There's no space in any page
    /// - I/O fails during persistence
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scuttle_db::*;
    /// # let mut db = Database::new("./data");
    /// # let schema = Schema::new(vec![
    /// #     ColumnDefinition::new("id", DataType::Integer, false),
    /// #     ColumnDefinition::new("name", DataType::Text, false),
    /// # ]);
    /// # db.create_table("users", schema).unwrap();
    /// let row = Row::new(vec![
    ///     Value::Integer(42),
    ///     Value::Text("Bob".to_string()),
    /// ]);
    ///
    /// let (page_id, item_id) = db.insert_row("users", row)
    ///     .expect("Failed to insert row");
    /// println!("Inserted at page {}, item {}", page_id, item_id);
    /// ```
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(PageId, ItemId)> {
        println!("Inserting row into table: {table_name}");

        // Get schema first (separate borrow scope)
        let encoded_data = {
            let table = self.get_table(table_name).unwrap();
            table.schema().encode_row(row)
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
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to scan
    ///
    /// # Returns
    ///
    /// A vector of all rows in the table.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError`] if:
    /// - The table doesn't exist
    /// - Row decoding fails (corrupted data)
    /// - I/O errors occur reading pages
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scuttle_db::*;
    /// # let mut db = Database::new("./data");
    /// # let schema = Schema::new(vec![]);
    /// # db.create_table("users", schema).unwrap();
    /// let all_rows = db.get_rows("users").expect("Failed to get rows");
    /// println!("Found {} rows", all_rows.len());
    /// ```
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
    ///
    /// # Arguments
    ///
    /// * `query` - SQL query string to execute
    ///
    /// # Returns
    ///
    /// A [`QueryResponse`] containing the table metadata and result rows.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - SQL syntax is invalid (parse error)
    /// - Referenced table doesn't exist
    /// - Column names are invalid
    /// - Query execution fails
    ///
    /// # Supported SQL
    ///
    /// Currently supports SELECT statements:
    /// - `SELECT * FROM table`
    /// - `SELECT col1, col2 FROM table`
    /// - `SELECT * FROM table WHERE condition`
    ///
    /// WHERE clause operators: `=`, `!=`, `>`, `<`
    ///
    /// # Limitations
    ///
    /// - No JOINs
    /// - No GROUP BY, ORDER BY, LIMIT
    /// - No aggregate functions (COUNT, SUM, etc.)
    /// - Table names must be unquoted
    /// - String literals use single quotes only
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scuttle_db::*;
    /// # let mut db = Database::new("./data");
    /// # let schema = Schema::new(vec![
    /// #     ColumnDefinition::new("id", DataType::Integer, false),
    /// #     ColumnDefinition::new("age", DataType::Integer, false),
    /// # ]);
    /// # db.create_table("users", schema).unwrap();
    /// // Select all rows
    /// let result = db.execute_query("SELECT * FROM users")
    ///     .expect("Query failed");
    /// println!("Found {} rows", result.rows.len());
    ///
    /// // Select with filter
    /// let result = db.execute_query("SELECT * FROM users WHERE age > 25")
    ///     .expect("Query failed");
    /// for row in result.rows {
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub fn execute_query(&mut self, query: &str) -> Result<QueryResponse<'_>> {
        let mut parser = SqlParser::new(query);
        let statement = parser
            .parse()
            .map_err(|e| DatabaseError::InvalidQuery(format!("Parse error: {e}")))?;

        let logical_plan = LogicalPlan::from_statement(statement.clone())
            .map_err(|e| DatabaseError::InvalidQuery(format!("Logical Plan error: {e}")))?;

        let context = PlannerContext::new(self);
        let physical_plan = PhysicalPlan::from_logical_plan(logical_plan, &context)
            .map_err(|e| DatabaseError::InvalidQuery(format!("Physical Plan error: {e}")))?;

        let rows = self.execute_physical_plan(physical_plan)?;
        let table = self.get_table(statement.table_name()).unwrap();

        Ok(QueryResponse {
            relation: table,
            rows,
        })
    }

    /// Executes a physical query plan.
    ///
    /// Internal method that recursively executes plan nodes:
    /// - **SeqScan** - Full table scan
    /// - **Filter** - Apply WHERE predicates
    /// - **Projection** - Select specific columns
    ///
    /// Future operators may include IndexScan, HashJoin, Sort, etc.
    fn execute_physical_plan(&mut self, plan: PhysicalPlan) -> Result<Vec<Row>> {
        match plan {
            PhysicalPlan::SeqScan { table_name } => {
                let all_rows = self.get_rows(&table_name).into_diagnostic()?;

                Ok(all_rows)
            }
            PhysicalPlan::Filter { predicate, input } => {
                let table_name = PhysicalPlan::extract_table_name(&input)?.to_string();
                let input_rows = self.execute_physical_plan(*input)?;

                let schema = self.get_table(&table_name).into_diagnostic()?.schema();

                let evaluator = PredicateEvaluator {};

                Ok(input_rows
                    .into_iter()
                    .filter(|row| evaluator.evaluate(&predicate, row, schema).unwrap_or(false))
                    .collect())
            }
            PhysicalPlan::Projection {
                columns_indices,
                input,
            } => {
                let input = self.execute_physical_plan(*input)?;

                let projected_rows: Vec<Row> = input
                    .into_iter()
                    .map(|row| {
                        let projected_values: Vec<Value> = columns_indices
                            .iter()
                            .map(|&idx| row.values[idx].clone())
                            .collect();
                        Row::new(projected_values)
                    })
                    .collect();

                Ok(projected_rows)
            }
        }
    }
}
