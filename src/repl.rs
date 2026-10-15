//! The interactive SQL read-eval-print loop.
//!
//! [`run_repl`] is shared by the `scuttle-db` and `test-repl` binaries. It is generic over its
//! input and output so it can be driven by in-memory buffers in tests.

use std::io::{self, BufRead, Write};

use crate::Database;

/// Prompt printed before each line of input.
const PROMPT: &str = "DB: ";

/// Runs the read-eval-print loop until the input ends or `exit` / `:q` is read.
///
/// Each line is trimmed and executed as one statement. Results are written to `out`, and so are
/// errors (the same rendering the binaries have always used). A failed statement does not stop the
/// loop. When the loop ends, `Exiting Scuttle` is written.
///
/// # Arguments
///
/// * `db` - The database the statements run against
/// * `input` - Where statements are read from, one per line
/// * `out` - Where prompts, results and errors are written
/// * `interactive` - Whether `input` is a terminal. When it is, end of input (Ctrl-D) prints a
///   newline first so the shell prompt starts on a fresh line.
///
/// # Errors
///
/// Returns an error if reading `input` or writing to `out` fails.
///
/// # Example
///
/// ```
/// use scuttle_db::{Database, run_repl};
///
/// let mut db = Database::in_memory();
/// let mut out = Vec::new();
/// run_repl(&mut db, "CREATE TABLE t (id INT);\n".as_bytes(), &mut out, false).unwrap();
/// assert!(String::from_utf8(out).unwrap().ends_with("Exiting Scuttle\n"));
/// ```
pub fn run_repl<R: BufRead, W: Write>(
    db: &mut Database,
    mut input: R,
    mut out: W,
    interactive: bool,
) -> io::Result<()> {
    let mut line = String::new();
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
        if statement == "exit" || statement == ":q" {
            break;
        }

        match db.execute_query(statement) {
            Ok(response) => writeln!(out, "{response}")?,
            Err(err) => writeln!(out, "{:?}", err.with_source_code(statement.to_string()))?,
        }
        out.flush()?;
    }

    writeln!(out, "Exiting Scuttle")?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs `input` through the loop against a fresh in-memory database.
    fn run(input: &str) -> (Database, String) {
        let mut db = Database::in_memory();
        let mut out = Vec::new();
        run_repl(&mut db, input.as_bytes(), &mut out, false).unwrap();
        (db, String::from_utf8(out).unwrap())
    }

    #[test]
    fn test_run_repl_exits_at_end_of_input_without_exit() {
        let (_, out) = run("CREATE TABLE t (id INT);\n");
        assert_eq!(out, "DB: OK\nDB: Exiting Scuttle\n");
    }

    #[test]
    fn test_run_repl_executes_last_line_without_newline() {
        let (db, out) = run("CREATE TABLE t (id INT)");
        assert_eq!(out, "DB: OK\nDB: Exiting Scuttle\n");
        assert!(db.get_table("t").is_ok());
    }

    #[test]
    fn test_run_repl_exits_on_empty_input() {
        let (_, out) = run("");
        assert_eq!(out, "DB: Exiting Scuttle\n");
    }

    #[test]
    fn test_run_repl_stops_at_exit_and_colon_q() {
        for stop in ["exit", ":q"] {
            let (db, out) = run(&format!("{stop}\nCREATE TABLE t (id INT);\n"));
            assert_eq!(out, "DB: Exiting Scuttle\n", "stop word: {stop}");
            assert!(db.get_table("t").is_err(), "statement after {stop} ran");
        }
    }

    #[test]
    fn test_run_repl_keeps_going_after_a_failed_statement() {
        let (db, out) = run("SELECT * FROM nope;\nCREATE TABLE t (id INT);\n");
        assert!(out.contains("Table not found: nope"));
        assert!(out.contains("DB: OK\n"));
        assert!(db.get_table("t").is_ok());
    }

    #[test]
    fn test_run_repl_interactive_end_of_input_starts_a_new_line() {
        let mut db = Database::in_memory();
        let mut out = Vec::new();
        run_repl(&mut db, "".as_bytes(), &mut out, true).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "DB: \nExiting Scuttle\n");
    }
}
