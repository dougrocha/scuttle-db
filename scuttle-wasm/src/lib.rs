//! Browser bindings for Scuttle DB.
//!
//! Exposes an in-memory [`Database`] to JavaScript through `wasm-bindgen`. Query
//! results are returned as JSON so the page can render them without knowing
//! about Scuttle DB's types:
//!
//! ```json
//! { "columns": ["name"], "rows": [["Scuttle DB"]], "rowsAffected": null }
//! ```

use scuttle_db::{Database, Value};
use serde_json::{Value as Json, json};
use wasm_bindgen::prelude::*;

/// An in-memory Scuttle DB instance that JavaScript can query.
///
/// A panic inside the database leaves the WebAssembly instance unusable, so
/// callers should create a new `Db` if [`Db::run`] throws a `RuntimeError`.
#[wasm_bindgen]
pub struct Db {
    inner: Database,
}

#[wasm_bindgen]
impl Db {
    /// Creates an empty in-memory database.
    ///
    /// # Returns
    ///
    /// A database with no tables.
    ///
    /// # Example
    ///
    /// ```js
    /// import init, { Db } from "./scuttle_wasm.js";
    ///
    /// await init();
    /// const db = new Db();
    /// ```
    #[wasm_bindgen(constructor)]
    pub fn new() -> Db {
        Db {
            inner: Database::in_memory(),
        }
    }

    /// Runs one SQL statement.
    ///
    /// # Arguments
    ///
    /// * `sql` - A single SQL statement, without a trailing semicolon
    ///
    /// # Returns
    ///
    /// The result as a JSON string with `columns`, `rows` and `rowsAffected`.
    ///
    /// # Errors
    ///
    /// Throws a JavaScript `Error` with the database's message when the statement
    /// fails to parse, references a missing table or column, or cannot be executed.
    ///
    /// # Example
    ///
    /// ```js
    /// db.run("CREATE TABLE users (name TEXT)");
    /// const { rows } = JSON.parse(db.run("SELECT * FROM users"));
    /// ```
    pub fn run(&mut self, sql: &str) -> Result<String, JsError> {
        let response = self
            .inner
            .execute_query(sql)
            .map_err(|err| JsError::new(&err.to_string()))?;

        let rows: Vec<Vec<Json>> = response
            .rows
            .iter()
            .map(|row| row.values.iter().map(to_json).collect())
            .collect();

        Ok(json!({
            "columns": response.columns,
            "rows": rows,
            "rowsAffected": response.rows_affected,
        })
        .to_string())
    }

    /// Describes how a SQL statement would run, without running it.
    ///
    /// # Arguments
    ///
    /// * `sql` - A single SQL statement, without a trailing semicolon
    ///
    /// # Returns
    ///
    /// The logical plan as an indented tree, one node per line.
    ///
    /// # Errors
    ///
    /// Throws a JavaScript `Error` when the statement fails to parse or references a
    /// missing table or column.
    ///
    /// # Example
    ///
    /// ```js
    /// db.explain("SELECT name FROM users WHERE age > 30");
    /// // "Projection: name\n  Filter: age > 30\n    Scan: users"
    /// ```
    pub fn explain(&mut self, sql: &str) -> Result<String, JsError> {
        self.inner
            .explain(sql)
            .map_err(|err| JsError::new(&err.to_string()))
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts a Scuttle DB value to its JSON equivalent.
fn to_json(value: &Value) -> Json {
    match value {
        Value::Int64(n) => json!(n),
        Value::Float64(n) => json!(n),
        Value::Text(s) => json!(s),
        Value::Bool(b) => json!(b),
        Value::Null => Json::Null,
    }
}
