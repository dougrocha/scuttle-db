//! SELECT, and EXPLAIN of any statement that has a plan.

use miette::Result;
use web_time::{Duration, Instant};

use crate::{
    DatabaseError,
    db::{
        database::{Database, QueryResponse},
        table::row::Row,
    },
    sql::{
        analyzer::Analyzer,
        ast::{
            parser::SqlParser,
            statement::{self, Statement},
        },
        planner::{explain, logical::LogicalPlan, optimizer},
    },
};

impl Database {
    pub(crate) fn handle_select(
        &mut self,
        select_stmt: statement::SelectStatement,
        on_rows: &mut impl FnMut(&[String], Vec<Row>) -> Result<()>,
    ) -> Result<QueryResponse> {
        let mut plan = Analyzer::new(&self.catalog).analyze_select(select_stmt)?;
        optimizer::optimize(&mut plan, &self.catalog, &mut self.pool)?;
        let columns = plan.output_schema().names();

        let mut executor = self.physical_planner().create_physical_plan(plan)?;

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

    pub(crate) fn explain_statement(
        &mut self,
        statement: Statement,
        analyze: bool,
    ) -> Result<String> {
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
    pub(crate) fn explain_analyze(
        &mut self,
        select_stmt: statement::SelectStatement,
    ) -> Result<String> {
        let mut plan = Analyzer::new(&self.catalog).analyze_select(select_stmt)?;

        // Planning counts the optimizer, which walks an index to see how many rows it
        // finds, and building the plan, which finds the index range's first leaf
        let start = Instant::now();
        optimizer::optimize(&mut plan, &self.catalog, &mut self.pool)?;
        let tree = explain::explain(&plan);
        let mut planner = self.physical_planner().profiled();
        let mut executor = planner.create_physical_plan(plan)?;
        let profile = planner.into_profile();
        let planning = start.elapsed();

        // Each line of the tree is one node, in the order the planner built them
        if tree.lines().count() != profile.len() {
            return Err(miette::miette!(
                "EXPLAIN ANALYZE has {} plan lines but stats for {} nodes",
                tree.lines().count(),
                profile.len()
            ));
        }

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
    pub(crate) fn describe(&mut self, statement: Statement) -> Result<String> {
        let analyzer = Analyzer::new(&self.catalog);
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
            Statement::CreateIndex(_) | Statement::DropTable(_) | Statement::DropIndex(_) => {
                return Err(DatabaseError::InvalidQuery(
                    "only CREATE TABLE, SELECT, INSERT, UPDATE and DELETE have a plan".into(),
                )
                .into());
            }
        };

        optimizer::optimize(&mut plan, &self.catalog, &mut self.pool)?;
        Ok(explain::explain(&plan))
    }
}
