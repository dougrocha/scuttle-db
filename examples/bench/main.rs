//! Times queries against a database on disk.
//!
//! ```text
//! cargo run --release --example bench -- gen  <dir> [rows]
//! cargo run --release --example bench -- run  <dir> [queries.sql] [rounds]
//! ```
//!
//! `gen` creates `users` (no index) and `users_pk` (`id INT PRIMARY KEY`), with the same
//! rows in each. `run` opens the database once, so the index rebuild on open isn't
//! timed, then runs every query once per round, interleaved, and prints the median.
//! The row hash shows whether two builds returned the same rows.

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    process::ExitCode,
    time::{Duration, Instant},
};

use scuttle_db::{Database, Row};

const USAGE: &str = "usage: bench gen <dir> [rows]\n       bench run <dir> [queries.sql] [rounds]";
const DEFAULT_QUERIES: &str = include_str!("queries.sql");

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    let result = match args.as_slice() {
        ["gen", dir, rest @ ..] => match rest {
            [] => generate(dir, 1_000_000),
            [rows] => match rows.parse() {
                Ok(rows) => generate(dir, rows),
                Err(_) => Err(format!("rows must be a number, got {rows}")),
            },
            _ => Err(USAGE.to_string()),
        },
        ["run", dir, rest @ ..] => {
            let (queries, rounds) = match rest {
                [] => (DEFAULT_QUERIES.to_string(), "5"),
                [file] | [file, _] => match std::fs::read_to_string(file) {
                    Ok(text) => (text, rest.get(1).copied().unwrap_or("5")),
                    Err(err) => return fail(format!("{file}: {err}")),
                },
                _ => return fail(USAGE.to_string()),
            };
            match rounds.parse() {
                Ok(rounds) if rounds > 0 => run(dir, &queries, rounds),
                _ => Err(format!("rounds must be a positive number, got {rounds}")),
            }
        }
        _ => Err(USAGE.to_string()),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => fail(err),
    }
}

fn fail(message: String) -> ExitCode {
    eprintln!("{message}");
    ExitCode::FAILURE
}

fn open(dir: &str) -> Result<Database, String> {
    let mut db = Database::new(dir);
    db.initialize().map_err(|err| format!("{err:?}"))?;
    Ok(db)
}

fn execute(db: &mut Database, sql: &str) -> Result<Vec<Row>, String> {
    db.execute_query(sql)
        .map(|response| response.rows)
        .map_err(|err| format!("{sql}\n{err:?}"))
}

fn generate(dir: &str, rows: usize) -> Result<(), String> {
    if std::fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some()) {
        return Err(format!("{dir} is not empty; pick a new directory"));
    }

    let mut db = open(dir)?;
    let start = Instant::now();
    for table in ["users", "users_pk"] {
        let id = if table == "users_pk" {
            "id INT PRIMARY KEY"
        } else {
            "id INT"
        };
        execute(
            &mut db,
            &format!("CREATE TABLE {table} ({id}, name TEXT, age INT)"),
        )?;

        let ids: Vec<usize> = (1..=rows).collect();
        for chunk in ids.chunks(1000) {
            let values: Vec<String> = chunk
                .iter()
                .map(|id| format!("({id}, 'user_{id}', {})", id % 90))
                .collect();
            execute(
                &mut db,
                &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
            )?;
        }
    }

    println!(
        "{rows} rows in users and users_pk, written to {dir} in {:.1?}",
        start.elapsed()
    );
    Ok(())
}

struct Timing<'q> {
    sql: &'q str,
    times: Vec<Duration>,
    rows: usize,
    hash: u64,
}

fn run(dir: &str, queries: &str, rounds: usize) -> Result<(), String> {
    let mut db = open(dir)?;
    let mut timings: Vec<Timing> = queries
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .map(|sql| Timing {
            sql,
            times: Vec::new(),
            rows: 0,
            hash: 0,
        })
        .collect();

    // One untimed pass loads every page into the buffer pool
    for timing in &mut timings {
        let rows = execute(&mut db, timing.sql)?;
        let mut hasher = DefaultHasher::new();
        for row in &rows {
            for value in &row.values {
                value.to_string().hash(&mut hasher);
            }
        }
        timing.rows = rows.len();
        timing.hash = hasher.finish();
    }

    // Interleaved, so a slow moment on the machine spreads over every query
    for _ in 0..rounds {
        for timing in &mut timings {
            let start = Instant::now();
            execute(&mut db, timing.sql)?;
            timing.times.push(start.elapsed());
        }
    }

    println!(
        "{:>10} {:>21} {:>8}  {:<8} query",
        "median ms", "min..max ms", "rows", "hash"
    );
    for timing in &mut timings {
        timing.times.sort();
        let ms = |time: Duration| time.as_secs_f64() * 1000.0;
        let median = ms(timing.times[timing.times.len() / 2]);
        let range = format!(
            "{:.3}..{:.3}",
            ms(timing.times[0]),
            ms(timing.times[timing.times.len() - 1])
        );
        println!(
            "{median:>10.3} {range:>21} {:>8}  {:08x} {}",
            timing.rows, timing.hash as u32, timing.sql
        );
    }
    Ok(())
}
