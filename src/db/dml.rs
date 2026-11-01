//! Statements that change rows: INSERT, UPDATE and DELETE.

use std::collections::BTreeSet;

use miette::Result;

use crate::{
    ColumnDef, DatabaseError, Value,
    db::{
        catalog::Catalog,
        database::{Database, QueryResponse},
        table::{
            cursor::{TableCursor, fetch_row},
            index::Ctid,
            row::Row,
            schema::Schema,
        },
    },
    sql::{
        analyzer::Analyzer,
        ast::statement,
        evaluator::{Evaluator, expression::ExpressionEvaluator, predicate::PredicateEvaluator},
        planner::{logical::LogicalPlan, optimizer},
    },
};

impl Database {
    pub(crate) fn handle_insert(
        &mut self,
        insert_stmt: statement::InsertStatement,
    ) -> Result<QueryResponse> {
        let analyzer = Analyzer::new(&self.catalog);
        let LogicalPlan::Insert {
            table_name,
            column_names: insert_columns,
            source,
        } = analyzer.analyze_insert(insert_stmt)?
        else {
            unreachable!("analyze_insert always returns LogicalPlan::Insert")
        };
        let LogicalPlan::Values { expressions } = *source else {
            unreachable!("an INSERT's rows always come from VALUES")
        };
        Self::check_writable(&table_name)?;

        let schema = &self.catalog.table(&table_name)?.schema;
        let evaluator = ExpressionEvaluator;
        // VALUES don't reference columns, so they are evaluated against an empty row
        let empty_row = Row::new(Vec::new());

        // Build and validate every row before writing any of them
        let mut new_rows = Vec::with_capacity(expressions.len());
        for exprs in expressions {
            let mut values = Vec::with_capacity(schema.columns.len());
            for col_def in &schema.columns {
                let value = match insert_columns.iter().position(|name| *name == col_def.name) {
                    Some(idx) => evaluator.evaluate(&exprs[idx], &empty_row)?,
                    None => col_def.default_value().cloned().unwrap_or(Value::Null),
                };
                values.push(Self::coerce_for_column(value, col_def)?);
            }
            new_rows.push(Row::new(values));
        }

        self.check_unique(&table_name, &new_rows, &BTreeSet::new())?;

        let rows_affected = new_rows.len();
        for row in new_rows {
            self.append_row(&table_name, row)?;
        }

        Ok(QueryResponse {
            rows_affected: Some(rows_affected),
            ..QueryResponse::default()
        })
    }

    /// Runs an UPDATE: each matching row is deleted and its new version inserted,
    /// the way PostgreSQL writes a new tuple version instead of updating in place.
    pub(crate) fn handle_update(
        &mut self,
        update_stmt: statement::UpdateStatement,
    ) -> Result<QueryResponse> {
        let mut plan = Analyzer::new(&self.catalog).analyze_update(update_stmt)?;
        optimizer::optimize(&mut plan, &self.catalog, &mut self.pool)?;
        let LogicalPlan::Update { input, assignments } = plan else {
            unreachable!("analyze_update always returns LogicalPlan::Update")
        };
        let RowsToChange {
            table_name,
            schema,
            rows,
        } = self.rows_to_change(&input)?;

        // Compute and validate every new row before writing anything, so a bad value
        // doesn't leave the table half-updated. Reading every row up front also keeps
        // the rows we insert from being picked up and updated a second time.
        let evaluator = ExpressionEvaluator;
        let mut updates = Vec::new();
        for (ctid, row) in rows {
            let mut new_row = row.clone();
            for (index, expr) in &assignments {
                // Every SET expression sees the old row, not the partially updated one
                let value = evaluator.evaluate(expr, &row)?;
                new_row.values[*index] = Self::coerce_for_column(value, &schema.columns[*index])?;
            }
            updates.push((ctid, row, new_row));
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

    pub(crate) fn handle_delete(
        &mut self,
        delete_stmt: statement::DeleteStatement,
    ) -> Result<QueryResponse> {
        let mut plan = Analyzer::new(&self.catalog).analyze_delete(delete_stmt)?;
        optimizer::optimize(&mut plan, &self.catalog, &mut self.pool)?;
        let LogicalPlan::Delete { input } = plan else {
            unreachable!("analyze_delete always returns LogicalPlan::Delete")
        };
        let RowsToChange {
            table_name, rows, ..
        } = self.rows_to_change(&input)?;

        let rows_affected = rows.len();
        for (ctid, row) in rows {
            self.delete_row(&table_name, ctid, &row)?;
        }

        Ok(QueryResponse {
            rows_affected: Some(rows_affected),
            ..QueryResponse::default()
        })
    }

    /// Fails for a catalog table. Only the database writes those, so they always match the
    /// tables they describe.
    pub(crate) fn check_writable(table_name: &str) -> Result<()> {
        if Catalog::is_system(table_name) {
            return Err(DatabaseError::InvalidQuery(format!(
                "{table_name} is a catalog table, which only the database changes"
            ))
            .into());
        }
        Ok(())
    }

    /// Converts a value for storage in `column`, checking type and NOT NULL.
    pub(crate) fn coerce_for_column(value: Value, column: &ColumnDef) -> Result<Value> {
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

    /// Adds a row to the table's last page and marks the page dirty, without writing it.
    ///
    /// Statements call this for every row and leave the writing to the flush at the end of
    /// [`Database::execute_query`], so each page is written once per statement, not once per row.
    ///
    /// The row is added to the table's indexes too. Uniqueness is not checked here: callers
    /// check every row of the statement first, see [`Database::check_unique`].
    pub(crate) fn append_row(&mut self, table_name: &str, row: Row) -> Result<Ctid> {
        let table = self.catalog.table(table_name)?;
        let encoded_data = table.schema.encode_row(&row);

        let free_page = self.pool.get_free_page(table_name, encoded_data.len())?;
        let page_id = free_page.header.page_id;
        let item_id = free_page.add_data(&encoded_data)?;

        for index in &table.indexes {
            index.insert(&mut self.pool, &row, (page_id, item_id))?;
        }

        Ok((page_id, item_id))
    }

    /// Marks a row deleted and takes it out of the table's indexes. `row` must be the row
    /// stored at `ctid`, which is how the index finds its entry.
    pub(crate) fn delete_row(&mut self, table_name: &str, ctid: Ctid, row: &Row) -> Result<()> {
        let (page_id, item_id) = ctid;
        self.pool
            .get_page_mut(table_name, page_id)?
            .delete_item(item_id)?;

        for index in &self.catalog.table(table_name)?.indexes {
            index.remove(&mut self.pool, row, ctid)?;
        }

        Ok(())
    }

    /// Fails if writing `new_rows` would break a unique index, or put a key too long for
    /// an index in one.
    ///
    /// `replacing` holds the rows the statement deletes first (an UPDATE's old versions).
    /// Checked before anything is written, so a failing statement changes nothing.
    pub(crate) fn check_unique(
        &mut self,
        table_name: &str,
        new_rows: &[Row],
        replacing: &BTreeSet<Ctid>,
    ) -> Result<()> {
        let table = self.catalog.table(table_name)?;
        for index in &table.indexes {
            let column_name = &table.schema.columns[index.column].name;
            index.check_rows(&mut self.pool, column_name, new_rows, replacing)?;
        }

        Ok(())
    }

    /// The rows an UPDATE's or DELETE's `input` finds, with where each is stored, and
    /// the table they are in. Fails for a catalog table.
    ///
    /// The ctid is what UPDATE and DELETE need to mark the old version deleted. Every
    /// row is read before any is written, so unlike a SELECT this doesn't stream.
    fn rows_to_change(&mut self, input: &LogicalPlan) -> Result<RowsToChange> {
        let scan = input
            .table_scan()
            .expect("the analyzer reads an UPDATE's or DELETE's rows with a Scan");
        let table_name = scan.table_name.to_string();
        let schema = scan.schema.clone();
        Self::check_writable(&table_name)?;

        let mut rows = Vec::new();
        match scan.index {
            Some(access) => {
                let mut ctids = access.open(&self.catalog.table(&table_name)?.indexes)?;
                while let Some(ctid) = ctids.next(&mut self.pool)? {
                    if let Some(row) = fetch_row(&mut self.pool, &table_name, &schema, ctid)? {
                        rows.push((ctid, row));
                    }
                }
            }
            None => {
                let mut cursor =
                    TableCursor::open(&mut self.pool, table_name.clone(), schema.clone());
                while cursor.read_next_page(&mut self.pool, |page_id, item_id, row| {
                    rows.push(((page_id, item_id), row));
                    Ok(())
                })? {}
            }
        }

        // An index finds every row the filter passes, and perhaps more
        if let Some(filter) = scan.filter {
            let mut passed = Vec::with_capacity(rows.len());
            for (ctid, row) in rows {
                if PredicateEvaluator.evaluate(filter, &row)? {
                    passed.push((ctid, row));
                }
            }
            rows = passed;
        }
        Ok(RowsToChange {
            table_name,
            schema,
            rows,
        })
    }
}

/// What [`Database::rows_to_change`] finds.
struct RowsToChange {
    table_name: String,
    schema: Schema,

    /// Each row with where it is stored.
    rows: Vec<(Ctid, Row)>,
}
