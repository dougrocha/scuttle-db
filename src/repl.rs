//! The interactive SQL read-eval-print loop.
//!
//! [`run_repl`] is shared by the `scuttle-db` and `test-repl` binaries. It is generic over its
//! input and output so it can be driven by in-memory buffers in tests.

use std::io::{self, BufRead, Write};

use miette::miette;

use crate::{Database, db::result_table::ResultTable};

/// Prompt printed before each line of input.
const PROMPT: &str = "DB: ";

/// Runs the read-eval-print loop until the input ends or `exit` / `:q` is read.
///
/// Each line is trimmed and executed as one statement. Results go to `out`. Errors go to `err`,
/// using the same rendering the binaries have always used. A failed statement does not stop the
/// loop. When the loop ends, `Exiting Scuttle` is written to `out`.
///
/// # Arguments
///
/// * `db` - The database the statements run against
/// * `input` - Where statements are read from, one per line
/// * `out` - Where prompts and results are written
/// * `err` - Where errors are written
/// * `interactive` - Whether `input` is a terminal. When it is, end of input (Ctrl-D) prints a
///   newline first so the shell prompt starts on a fresh line.
///
/// # Returns
///
/// The number of statements that failed.
///
/// # Errors
///
/// Returns an error if reading `input` or writing to `out` or `err` fails.
///
/// # Example
///
/// ```
/// use scuttle_db::{Database, run_repl};
///
/// let mut db = Database::in_memory();
/// let mut out = Vec::new();
/// let mut err = Vec::new();
/// let failed = run_repl(&mut db, "SELECT * FROM nope;\n".as_bytes(), &mut out, &mut err, false)
///     .unwrap();
/// assert_eq!(failed, 1);
/// assert!(String::from_utf8(out).unwrap().ends_with("Exiting Scuttle\n"));
/// ```
pub fn run_repl<R: BufRead, W: Write, E: Write>(
    db: &mut Database,
    mut input: R,
    mut out: W,
    mut err: E,
    interactive: bool,
) -> io::Result<usize> {
    let mut line = String::new();
    let mut failed = 0;
    loop {
        write!(out, "{PROMPT}")?;
        out.flush()?;

        line.clear();
        // End of input behaves like `exit`. `read_line` returns the bytes of a final line
        // without a newline before it returns 0, so that line is still executed.
        if input.read_line(&mut line)? == 0 {
            if interactive {
                writeln!(out)?;
            }
            break;
        }

        let statement = line.trim();
        // A blank line is not a statement. Prompt again without output (like psql).
        if statement.is_empty() {
            continue;
        }
        if statement == "exit" || statement == ":q" {
            break;
        }

        // Rows are printed as each batch arrives, so a big result never sits in memory
        let mut table: Option<ResultTable> = None;
        let mut text = String::new();
        let mut write_error = None;
        let result = db.execute_query_streaming(statement, |columns, rows| {
            let table = table.get_or_insert_with(|| ResultTable::new(columns, &rows, &mut text));
            table.push(&rows, &mut text);
            if let Err(error) = out.write_all(text.as_bytes()) {
                write_error = Some(error);
                return Err(miette!("the output was closed"));
            }
            text.clear();
            Ok(())
        });
        // The output going away (`| head` closing the pipe) is not the query's fault. Stop and
        // let the caller decide, instead of reporting it as a failed statement.
        if let Some(error) = write_error {
            return Err(error);
        }

        match result {
            Ok(_) if let Some(table) = table => {
                table.finish(&mut text);
                writeln!(out, "{text}")?;
            }
            // Writes, and queries that returned no rows
            Ok(response) => writeln!(out, "{response}")?,
            Err(error) => {
                failed += 1;
                out.flush()?;
                writeln!(err, "{:?}", error.with_source_code(statement.to_string()))?;
                err.flush()?;
            }
        }
        out.flush()?;
    }

    writeln!(out, "Exiting Scuttle")?;
    out.flush()?;
    Ok(failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What one run of the loop produced.
    struct Session {
        db: Database,
        out: String,
        err: String,
        failed: usize,
    }

    /// Runs `input` through the loop against a fresh in-memory database.
    fn run(input: &str) -> Session {
        let mut db = Database::in_memory();
        let mut out = Vec::new();
        let mut err = Vec::new();
        let failed = run_repl(&mut db, input.as_bytes(), &mut out, &mut err, false).unwrap();
        Session {
            db,
            out: String::from_utf8(out).unwrap(),
            err: String::from_utf8(err).unwrap(),
            failed,
        }
    }

    #[test]
    fn test_run_repl_exits_at_end_of_input_without_exit() {
        let s = run("CREATE TABLE t (id INT);\n");
        assert_eq!(s.out, "DB: OK\nDB: Exiting Scuttle\n");
    }

    #[test]
    fn test_run_repl_executes_last_line_without_newline() {
        let s = run("CREATE TABLE t (id INT)");
        assert_eq!(s.out, "DB: OK\nDB: Exiting Scuttle\n");
        assert!(s.db.get_table("t").is_ok());
    }

    #[test]
    fn test_run_repl_exits_on_empty_input() {
        let s = run("");
        assert_eq!(s.out, "DB: Exiting Scuttle\n");
    }

    #[test]
    fn test_run_repl_prints_a_result_that_arrives_in_several_batches() {
        // 3,000 rows come out of the scan in more than one batch
        let values: Vec<String> = (1..=3000).map(|id| format!("({id})")).collect();
        let s = run(&format!(
            "CREATE TABLE t (id INT)\nINSERT INTO t VALUES {}\nSELECT id FROM t\nSELECT id FROM t WHERE id = 0\n",
            values.join(", ")
        ));

        let table = s.out.split("DB: ").nth(3).unwrap();
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines[..3], [" id", "------", " 1"]);
        assert_eq!(lines[lines.len() - 2..], [" 3000", "(3000 rows)"]);
        assert_eq!(lines.len(), 3000 + 3);

        // No rows: the header is still printed
        assert_eq!(s.out.split("DB: ").nth(4).unwrap(), " id\n----\n(0 rows)\n");
    }

    /// Accepts `room` bytes, then fails like a pipe whose reader has gone away.
    struct ClosingPipe {
        room: usize,
    }

    impl Write for ClosingPipe {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.room < buf.len() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            self.room -= buf.len();
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_run_repl_stops_quietly_when_the_output_closes() {
        let values: Vec<String> = (1..=3000).map(|id| format!("({id})")).collect();
        let input = format!(
            "CREATE TABLE t (id INT)\nINSERT INTO t VALUES {}\nSELECT id FROM t\nSELECT id FROM t\n",
            values.join(", ")
        );
        let mut db = Database::in_memory();
        let mut err = Vec::new();

        let result = run_repl(
            &mut db,
            input.as_bytes(),
            ClosingPipe { room: 200 },
            &mut err,
            false,
        );

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert!(err.is_empty(), "{}", String::from_utf8_lossy(&err));
    }

    #[test]
    fn test_run_repl_stops_at_exit_and_colon_q() {
        for stop in ["exit", ":q"] {
            let s = run(&format!("{stop}\nCREATE TABLE t (id INT);\n"));
            assert_eq!(s.out, "DB: Exiting Scuttle\n", "stop word: {stop}");
            assert!(s.db.get_table("t").is_err(), "statement after {stop} ran");
        }
    }

    #[test]
    fn test_run_repl_keeps_going_after_a_failed_statement() {
        let s = run("SELECT * FROM nope;\nCREATE TABLE t (id INT);\n");
        assert!(s.err.contains("Table not found: nope"));
        assert!(s.out.contains("DB: OK\n"));
        assert!(s.db.get_table("t").is_ok());
    }

    #[test]
    fn test_run_repl_ignores_blank_lines() {
        let s = run("CREATE TABLE t (id INT);\n\n   \nSELECT * FROM t;\n\nexit\n");
        assert!(!s.out.contains("Unexpected"));
        assert!(s.err.is_empty());
        assert!(s.db.get_table("t").is_ok());
    }

    #[test]
    fn test_run_repl_blank_line_reprompts_silently() {
        let s = run("\n\nexit\n");
        assert_eq!(s.out, "DB: DB: DB: Exiting Scuttle\n");
    }

    #[test]
    fn test_run_repl_blank_lines_then_end_of_input_terminates() {
        let s = run("\n\n");
        assert_eq!(s.out, "DB: DB: DB: Exiting Scuttle\n");
    }

    #[test]
    fn test_run_repl_writes_errors_to_err_not_out() {
        let s = run("SELECT * FROM nope;\n");
        assert!(s.err.contains("Table not found: nope"));
        assert!(!s.out.contains("Table not found"));
    }

    #[test]
    fn test_run_repl_counts_failed_statements() {
        let s = run("SELECT * FROM nope;\nCREATE TABLE t (id INT);\nSELECT * FROM nope2;\n");
        assert_eq!(s.failed, 2);
    }

    #[test]
    fn test_run_repl_counts_no_failures_when_all_statements_succeed() {
        let s = run("CREATE TABLE t (id INT);\nSELECT * FROM t;\n");
        assert_eq!(s.failed, 0);
        assert!(s.err.is_empty());
    }

    #[test]
    fn test_run_repl_interactive_end_of_input_starts_a_new_line() {
        let mut db = Database::in_memory();
        let mut out = Vec::new();
        let mut err = Vec::new();
        run_repl(&mut db, "".as_bytes(), &mut out, &mut err, true).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "DB: \nExiting Scuttle\n");
    }
}
