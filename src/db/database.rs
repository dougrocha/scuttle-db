use std::{
    fs::File,
    path::{Path, PathBuf},
};

use miette::Result;

use crate::{
    ColumnDef, DataType, DatabaseError, Value,
    db::{
        catalog::system_catalog::SystemCatalog,
        table::{Table, row::Row, schema::Schema, table_def::TableDef},
    },
    sql::{
        analyzer::Analyzer,
        ast::{
            parser::SqlParser,
            statement::{self, Statement},
        },
        catalog_context::CatalogContext,
        evaluator::{Evaluator, expression::ExpressionEvaluator, predicate::PredicateEvaluator},
        planner::{logical::LogicalPlan, physical::PhysicalPlanner},
    },
    storage::{
        buffer_pool::BufferPool,
        page::{ItemId, PageHeader, PageId},
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

        let separator = "-".repeat(8 + self.columns.len() * 15);

        writeln!(f, "{separator}")?;
        for name in &self.columns {
            write!(f, " | {name: <12}")?;
        }
        writeln!(f)?;
        writeln!(f, "{separator}")?;

        for row in &self.rows {
            for value in &row.values {
                write!(f, " | {: <12}", value.to_string())?;
            }
            writeln!(f)?;
        }

        writeln!(f, "{separator}")?;
        write!(f, "({} rows)", self.rows.len())
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
            buffer_manager: BufferPool::new(&data_dir),
            catalog: SystemCatalog::new(),

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
        let path = self
            .data_directory
            .join(format!("{}.table", self.catalog.name()));

        if Path::new(&path).exists() {
            let tables = self.catalog.load_all_tables(&mut self.buffer_manager)?;

            for (name, schema) in tables {
                self.tables.insert(name.clone(), TableDef { name, schema });
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
            // Eventually save table information in a catalog table,
            // but for now just load the table with the schema normally
            println!("Table {name} already exists");

            return Ok(());
        }

        let _ = self
            .catalog
            .save_table_metadata(&mut self.buffer_manager, name, &schema, 0);

        let table = TableDef::new(name.to_string(), schema);
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
        let rows = self.get_rows_with_location(table_name)?;
        Ok(rows.into_iter().map(|(_, _, row)| row).collect())
    }

    /// Like [`Database::get_rows`], but also returns where each row is stored.
    ///
    /// The `(PageId, ItemId)` pair identifies the tuple (PostgreSQL's `ctid`), which
    /// UPDATE and DELETE need to mark the old version as deleted.
    fn get_rows_with_location(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<(PageId, ItemId, Row)>, DatabaseError> {
        let mut found_rows = Vec::new();
        let max_pages = 1000; // To prevent infinite loops

        for current_page_id in 0..max_pages {
            // Use get_page instead of get_page_mut (which doesn't exist)
            let page_res = self.buffer_manager.get_page(table_name, current_page_id);

            let page = match page_res {
                Ok(page) => page,
                Err(_) => break,
            };

            // The item id is the pointer's index, so enumerate before skipping deleted items
            for (item_id, item_pointer) in page.item_pointers().enumerate() {
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

                found_rows.push((current_page_id, item_id as ItemId, decoded_row));
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

        match statement {
            Statement::Create(create_stmt) => self.handle_create(create_stmt),
            Statement::Select(select_stmt) => self.handle_select(select_stmt),
            Statement::Insert(insert_stmt) => self.handle_insert(insert_stmt),
            Statement::Update(update_stmt) => self.handle_update(update_stmt),
            Statement::Delete(delete_stmt) => self.handle_delete(delete_stmt),
        }
    }

    fn handle_create(&mut self, create_stmt: statement::CreateStatement) -> Result<QueryResponse> {
        let schema = Schema::new(create_stmt.columns);

        self.create_table(&create_stmt.table_name, schema)?;

        Ok(QueryResponse::default())
    }

    fn handle_select(&mut self, select_stmt: statement::SelectStatement) -> Result<QueryResponse> {
        let mut context = CatalogContext::new(self);
        let analyzer = Analyzer::new(&context);
        let anayzed_plan = analyzer.analyze_select(select_stmt)?;
        let columns = anayzed_plan.column_names();

        // TODO: Eventually add a optimizer here for logical_plan.
        // Will do a series of "pushdowns".
        //
        //  - Predicate Push Down: move filters as close together to the read source
        //  - Projection pushdown: only read columns we actually need (I think this is already implemeted)
        //  - Constant Folding: turn 'age > 10 + 5' to 'age > 25'

        let mut physical_planner = PhysicalPlanner::new(&mut context);
        let mut executor = physical_planner.create_physical_plan(anayzed_plan)?;

        let mut batches = Vec::new();
        while let Some(batch) = executor.next()? {
            batches.push(batch);
        }

        let rows = batches.into_iter().flat_map(|b| b.rows).collect();
        Ok(QueryResponse {
            columns,
            rows,
            rows_affected: None,
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
        } = analyzer.analyze_update(update_stmt)?
        else {
            unreachable!("analyze_update always returns LogicalPlan::Update")
        };

        let schema = self.get_table(&table_name)?.schema().clone();

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

            updates.push((page_id, item_id, new_row));
        }

        let rows_affected = updates.len();
        for (page_id, item_id, new_row) in updates {
            self.buffer_manager
                .get_page(&table_name, page_id)?
                .delete_item(item_id)?;
            self.buffer_manager.save_page(&table_name, page_id)?;

            self.insert_row(&table_name, new_row)?;
        }

        Ok(QueryResponse {
            rows_affected: Some(rows_affected),
            ..QueryResponse::default()
        })
    }

    /// Converts a value for storage in `column`, checking type and NOT NULL.
    fn coerce_for_column(value: Value, column: &ColumnDef) -> Result<Value> {
        let value = match (value, column.data_type) {
            (Value::Int64(n), DataType::Float64) => Value::Float64(n as f64),
            (value, _) => value,
        };

        if value == Value::Null && !column.is_nullable() {
            return Err(DatabaseError::TypeMismatch(format!(
                "Column {} cannot be null",
                column.name
            ))
            .into());
        }

        value
            .is_compatible_with(&column.data_type)
            .map_err(DatabaseError::TypeMismatch)?;

        Ok(value)
    }

    fn handle_delete(&mut self, delete_stmt: statement::DeleteStatement) -> Result<QueryResponse> {
        let context = CatalogContext::new(self);
        let analyzer = Analyzer::new(&context);
        let LogicalPlan::Delete { table_name, filter } = analyzer.analyze_delete(delete_stmt)?
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

            self.buffer_manager
                .get_page(&table_name, page_id)?
                .delete_item(item_id)?;
            self.buffer_manager.save_page(&table_name, page_id)?;
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
                                None if col_def.has_default() => {
                                    todo!("Enable values to have default")
                                }
                                None => Value::Null,
                            };

                        values.push(Self::coerce_for_column(value, col_def)?);
                    }

                    new_rows.push(Row::new(values));
                }

                rows_affected = new_rows.len();
                for row in new_rows {
                    context.database.insert_row(&table_name, row)?;
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
}
