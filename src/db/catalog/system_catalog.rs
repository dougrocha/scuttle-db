//! The system catalog is the set of tables that describe every other table.
//!
//! Like Postgres's `pg_class`, `pg_attribute` and `pg_index`, the catalog stores each
//! table's definition as rows in three tables:
//!
//! - `__tables__`: one row per table.
//! - `__columns__`: one row per column, with its position, type, NOT NULL and DEFAULT.
//!   `has_default` tells a `DEFAULT NULL` from no default, as Postgres's `atthasdef` does.
//! - `__indexes__`: one row per index, with its column, whether it is unique, and the
//!   column constraint (PRIMARY KEY or UNIQUE) it enforces, if CREATE TABLE made it for one.
//!
//! They are ordinary tables, so `SELECT * FROM __columns__` works. The database can't
//! read the catalog tables' own schemas from the catalog before it has read the catalog,
//! so the code below defines them. Postgres calls this the bootstrap problem.
//!
//! A table exists once its `__tables__` row is on disk. Creating a table writes its
//! columns and indexes, flushes them, then adds its `__tables__` row. Dropping a table
//! deletes its `__tables__` row, flushes, then deletes the rest. Opening loads the tables
//! that have a `__tables__` row and deletes the catalog rows of any other table. So a
//! crash at any point leaves either the whole table or catalog rows that the next open
//! deletes. That holds while the operating system writes the flushed pages in order,
//! which nothing makes it do until there is a write-ahead log with fsync.

use std::collections::{BTreeMap, BTreeSet};

use miette::{Result, miette};

use crate::{
    ColumnConstraint, ColumnDef, DataType, Value,
    db::table::{
        cursor::TableCursor,
        index::{IndexConstraint, TableIndex},
        row::Row,
        schema::Schema,
        table_def::TableDef,
    },
    storage::{
        buffer_pool::BufferPool,
        page::{ItemId, PageId},
    },
};

pub(crate) const TABLES: &str = "__tables__";
pub(crate) const COLUMNS: &str = "__columns__";
pub(crate) const INDEXES: &str = "__indexes__";

/// Reads and writes the catalog tables.
#[derive(Debug, Default)]
pub struct SystemCatalog;

impl SystemCatalog {
    /// The catalog's own tables. The code defines their schemas.
    pub(crate) fn tables() -> [TableDef; 3] {
        let column = ColumnDef::new;
        let nullable = ColumnDef::no_constraints;
        [
            TableDef::new(
                TABLES.to_string(),
                Schema::new(vec![column("name", DataType::Text)]),
                Vec::new(),
            ),
            TableDef::new(
                COLUMNS.to_string(),
                Schema::new(vec![
                    column("table_name", DataType::Text),
                    column("position", DataType::Int64),
                    column("name", DataType::Text),
                    column("type", DataType::Text),
                    nullable("max_len", DataType::Int64),
                    column("not_null", DataType::Bool),
                    column("has_default", DataType::Bool),
                    nullable("default_value", DataType::Text),
                ]),
                Vec::new(),
            ),
            TableDef::new(
                INDEXES.to_string(),
                Schema::new(vec![
                    column("name", DataType::Text),
                    column("table_name", DataType::Text),
                    column("column_name", DataType::Text),
                    column("is_unique", DataType::Bool),
                    nullable("constraint_type", DataType::Text),
                ]),
                Vec::new(),
            ),
        ]
    }

    /// Whether `name` is one of the catalog's tables, which only the database writes.
    pub(crate) fn is_catalog(name: &str) -> bool {
        [TABLES, COLUMNS, INDEXES].contains(&name)
    }

    /// Adds `table`'s columns and indexes, flushes them, then adds its `__tables__` row.
    ///
    /// If this fails, some of the rows may be written. [`SystemCatalog::remove_table`]
    /// deletes them.
    pub(crate) fn add_table(&self, pool: &mut BufferPool, table: &TableDef) -> Result<()> {
        for (position, column) in table.schema.columns.iter().enumerate() {
            let (type_name, max_len) = type_to_text(column.data_type);
            let row = vec![
                Value::Text(table.name.clone()),
                Value::Int64(position as i64),
                Value::Text(column.name.clone()),
                Value::Text(type_name.to_string()),
                max_len.map_or(Value::Null, |len| Value::Int64(len as i64)),
                Value::Bool(!column.is_nullable()),
                Value::Bool(column.has_default()),
                match column.default_value() {
                    Some(Value::Null) | None => Value::Null,
                    Some(value) => Value::Text(default_to_text(value)),
                },
            ];
            append(pool, COLUMNS, row)?;
        }
        for index in &table.indexes {
            self.add_index(pool, table, index)?;
        }
        pool.flush()?;
        append(pool, TABLES, vec![Value::Text(table.name.clone())])
    }

    /// Adds `index` of `table`.
    pub(crate) fn add_index(
        &self,
        pool: &mut BufferPool,
        table: &TableDef,
        index: &TableIndex,
    ) -> Result<()> {
        let row = vec![
            Value::Text(index.name.clone()),
            Value::Text(table.name.clone()),
            Value::Text(table.schema.columns[index.column].name.clone()),
            Value::Bool(index.unique),
            index
                .constraint
                .map_or(Value::Null, |c| Value::Text(c.as_str().to_string())),
        ];
        append(pool, INDEXES, row)
    }

    /// Removes `name`'s `__tables__` row, flushes, then removes its columns and indexes.
    pub(crate) fn remove_table(&self, pool: &mut BufferPool, name: &str) -> Result<()> {
        let is_table = |row: &Row| text(row, 0).is_ok_and(|t| t == name);
        delete_where(pool, TABLES, is_table)?;
        pool.flush()?;
        delete_where(pool, COLUMNS, is_table)?;
        delete_where(pool, INDEXES, |row| text(row, 1).is_ok_and(|t| t == name))
    }

    /// Removes the index called `name`.
    pub(crate) fn remove_index(&self, pool: &mut BufferPool, name: &str) -> Result<()> {
        delete_where(pool, INDEXES, |row| text(row, 0).is_ok_and(|t| t == name))
    }

    /// Every table the catalog holds, with its indexes.
    ///
    /// Deletes the catalog rows of tables that have no `__tables__` row, which a crash
    /// partway through creating or dropping a table can leave.
    pub(crate) fn load(&self, pool: &mut BufferPool) -> Result<Vec<TableDef>> {
        let mut names = BTreeSet::new();
        for row in rows(pool, TABLES)? {
            names.insert(text(&row, 0)?.to_string());
        }

        let mut columns: BTreeMap<String, Vec<(i64, ColumnDef)>> = BTreeMap::new();
        for row in rows(pool, COLUMNS)? {
            let table = text(&row, 0)?.to_string();
            let Value::Int64(position) = row.values[1] else {
                return Err(corrupt(COLUMNS));
            };
            columns
                .entry(table)
                .or_default()
                .push((position, column_from_row(&row)?));
        }

        let mut tables = Vec::new();
        for name in &names {
            // A table may have no columns, as in Postgres
            let mut table_columns = columns.remove(name).unwrap_or_default();
            table_columns.sort_by_key(|(position, _)| *position);
            let schema = Schema::new(table_columns.into_iter().map(|(_, c)| c).collect());
            tables.push(TableDef::new(name.clone(), schema, Vec::new()));
        }

        for row in rows(pool, INDEXES)? {
            let table_name = text(&row, 1)?;
            let Some(table) = tables.iter_mut().find(|t| t.name == table_name) else {
                continue;
            };
            let column = table
                .schema
                .get_column_index(text(&row, 2)?)
                .ok_or_else(|| corrupt(INDEXES))?;
            let Value::Bool(unique) = row.values[3] else {
                return Err(corrupt(INDEXES));
            };
            let constraint = match &row.values[4] {
                Value::Null => None,
                Value::Text(text) => {
                    Some(IndexConstraint::parse(text).ok_or_else(|| corrupt(INDEXES))?)
                }
                _ => return Err(corrupt(INDEXES)),
            };

            // Put the constraint back on the column, where CREATE TABLE had it
            match constraint {
                Some(IndexConstraint::PrimaryKey) => table.schema.columns[column]
                    .constraints
                    .push(ColumnConstraint::PrimaryKey),
                Some(IndexConstraint::Unique) => table.schema.columns[column]
                    .constraints
                    .push(ColumnConstraint::Unique),
                None => {}
            }
            let index = TableIndex::new(
                text(&row, 0)?.to_string(),
                &table.schema,
                column,
                unique,
                constraint,
            );
            table.indexes.push(index);
        }

        // Delete the catalog rows of tables that have no `__tables__` row
        let loaded: BTreeSet<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        let stray = |row: &Row, column| text(row, column).is_ok_and(|t| !loaded.contains(t));
        delete_where(pool, COLUMNS, |row| stray(row, 0))?;
        delete_where(pool, INDEXES, |row| stray(row, 1))?;

        Ok(tables)
    }
}

/// The column a `__columns__` row describes, without PRIMARY KEY or UNIQUE. Those come
/// from `__indexes__`.
fn column_from_row(row: &Row) -> Result<ColumnDef> {
    let max_len = match row.values[4] {
        Value::Int64(len) => Some(len as usize),
        _ => None,
    };
    let data_type = type_from_text(text(row, 3)?, max_len).ok_or_else(|| corrupt(COLUMNS))?;

    let mut constraints = Vec::new();
    if row.values[5] == Value::Bool(true) {
        constraints.push(ColumnConstraint::NotNull);
    }
    if row.values[6] == Value::Bool(true) {
        let value = match &row.values[7] {
            Value::Text(default) => {
                default_from_text(default, data_type).ok_or_else(|| corrupt(COLUMNS))?
            }
            _ => Value::Null,
        };
        constraints.push(ColumnConstraint::Default(value));
    }

    Ok(ColumnDef::with_constraints(
        text(row, 2)?,
        data_type,
        constraints,
    ))
}

/// A type's name as CREATE TABLE spells it, and a VARCHAR's length.
fn type_to_text(data_type: DataType) -> (&'static str, Option<usize>) {
    match data_type {
        DataType::Int64 => ("INT", None),
        DataType::Float64 => ("FLOAT", None),
        DataType::Text => ("TEXT", None),
        DataType::VarChar(len) => ("VARCHAR", Some(len)),
        DataType::Bool => ("BOOLEAN", None),
        DataType::Timestamp => ("TIMESTAMP", None),
    }
}

fn type_from_text(name: &str, max_len: Option<usize>) -> Option<DataType> {
    Some(match (name, max_len) {
        ("INT", None) => DataType::Int64,
        ("FLOAT", None) => DataType::Float64,
        ("TEXT", None) => DataType::Text,
        ("VARCHAR", Some(len)) => DataType::VarChar(len),
        ("BOOLEAN", None) => DataType::Bool,
        ("TIMESTAMP", None) => DataType::Timestamp,
        _ => return None,
    })
}

/// A DEFAULT as text. Loading parses it by the column's type, not as SQL, so a negative
/// number or a float like `1e300` comes back exactly. Rust prints a float with the fewest
/// digits that parse back to the same float.
fn default_to_text(value: &Value) -> String {
    match value {
        Value::Int64(n) | Value::Timestamp(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Text(text) => text.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => unreachable!("a DEFAULT NULL is stored as NULL"),
    }
}

fn default_from_text(text: &str, data_type: DataType) -> Option<Value> {
    Some(match data_type {
        DataType::Int64 => Value::Int64(text.parse().ok()?),
        DataType::Timestamp => Value::Timestamp(text.parse().ok()?),
        DataType::Float64 => Value::Float64(text.parse().ok()?),
        DataType::Text | DataType::VarChar(_) => Value::Text(text.to_string()),
        DataType::Bool => Value::Bool(text.parse().ok()?),
    })
}

/// The text in column `column` of a catalog row.
fn text(row: &Row, column: usize) -> Result<&str> {
    match &row.values[column] {
        Value::Text(text) => Ok(text),
        _ => Err(miette!("Corrupt catalog: expected text in column {column}")),
    }
}

fn corrupt(table: &str) -> miette::Report {
    miette!("Corrupt catalog: a row of {table} doesn't make sense")
}

fn schema_of(table: &str) -> Schema {
    SystemCatalog::tables()
        .into_iter()
        .find(|t| t.name == table)
        .expect("a catalog table")
        .schema
}

fn append(pool: &mut BufferPool, table: &str, values: Vec<Value>) -> Result<()> {
    let encoded = schema_of(table).encode_row(&Row::new(values));
    pool.get_free_page(table, encoded.len())?
        .add_data(&encoded)?;
    Ok(())
}

fn rows(pool: &mut BufferPool, table: &str) -> Result<Vec<Row>> {
    Ok(located_rows(pool, table)?
        .into_iter()
        .map(|(.., row)| row)
        .collect())
}

fn located_rows(pool: &mut BufferPool, table: &str) -> Result<Vec<(PageId, ItemId, Row)>> {
    let mut rows = Vec::new();
    let mut cursor = TableCursor::open(pool, table.to_string(), schema_of(table));
    while cursor.read_next_page(pool, |page_id, item_id, row| {
        rows.push((page_id, item_id, row));
        Ok(())
    })? {}
    Ok(rows)
}

fn delete_where(pool: &mut BufferPool, table: &str, matches: impl Fn(&Row) -> bool) -> Result<()> {
    for (page_id, item_id, row) in located_rows(pool, table)? {
        if matches(&row) {
            pool.get_page_mut(table, page_id)?.delete_item(item_id)?;
        }
    }
    Ok(())
}
