//! Tests that run SQL against a whole database, split by what they test.

mod ddl;
mod indexes;
mod joins;
mod select;
mod timestamps;
mod writes;

use std::path::PathBuf;

use crate::{
    DEFAULT_SORT_MEM, Database, Value,
    db::{
        database::QueryResponse,
        table::{index::IndexConstraint, row::Row, table_def::TableDef},
    },
    storage::buffer_pool::UNFLUSHED,
};

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

/// The error message from a statement that must fail.
fn error_message(db: &mut Database, sql: &str) -> String {
    match db.execute_query(sql) {
        Err(err) => err.to_string(),
        Ok(_) => panic!("expected `{sql}` to fail"),
    }
}
