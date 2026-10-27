use std::io::{ErrorKind, IsTerminal, stderr, stdin, stdout};
use std::process::ExitCode;

use miette::{IntoDiagnostic, Result};
use scuttle_db::{DEFAULT_CACHE_PAGES, Database, run_repl};

const USAGE: &str = "\
Usage: scuttle-db [--data <dir> [--cache-pages <n>] | --in-memory]

Options:
  -d, --data <dir>         Store tables in <dir> (default: ./db)
      --cache-pages <n>    Keep at most <n> 8 KB pages in memory (default: 16384, 128 MB)
      --in-memory          Keep all data in memory; nothing is written to disk
  -h, --help               Print this help
";

/// Where the database keeps its tables, as chosen on the command line.
enum Storage {
    Dir { dir: String, cache_pages: usize },
    InMemory,
}

/// Parses the arguments after the program name. `Ok(None)` means `--help` was asked for.
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Option<Storage>, String> {
    let mut data = None;
    let mut in_memory = false;
    let mut cache_pages = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "-d" | "--data" => {
                data = Some(
                    args.next()
                        .ok_or_else(|| format!("{arg} needs a directory"))?,
                );
            }
            "--cache-pages" => {
                let n = args.next().ok_or_else(|| format!("{arg} needs a number"))?;
                match n.parse() {
                    Ok(n) if n > 0 => cache_pages = Some(n),
                    _ => return Err(format!("{arg} needs a positive number, got {n}")),
                }
            }
            "--in-memory" => in_memory = true,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    match (data, in_memory) {
        (Some(_), true) => Err("--data and --in-memory cannot be combined".to_string()),
        (None, true) if cache_pages.is_some() => {
            Err("--cache-pages has no effect with --in-memory, which caches every page".to_string())
        }
        (None, true) => Ok(Some(Storage::InMemory)),
        (data, false) => Ok(Some(Storage::Dir {
            dir: data.unwrap_or_else(|| "./db".to_string()),
            cache_pages: cache_pages.unwrap_or(DEFAULT_CACHE_PAGES),
        })),
    }
}

fn main() -> Result<ExitCode> {
    let storage = match parse_args(std::env::args().skip(1)) {
        Ok(Some(storage)) => storage,
        Ok(None) => {
            print!("{USAGE}");
            return Ok(ExitCode::SUCCESS);
        }
        Err(err) => {
            eprint!("error: {err}\n\n{USAGE}");
            return Ok(ExitCode::from(2));
        }
    };

    miette::set_hook(Box::new(|_| {
        Box::new(
            miette::MietteHandlerOpts::new()
                .terminal_links(true)
                .unicode(false)
                .context_lines(3)
                .tab_width(4)
                .break_words(true)
                .build(),
        )
    }))
    .into_diagnostic()?;
    miette::set_panic_hook();

    let mut db = match storage {
        Storage::Dir { dir, cache_pages } => Database::with_cache_pages(dir, cache_pages),
        Storage::InMemory => Database::in_memory(),
    };
    db.initialize().expect("Failed to init catalog");

    let stdin = stdin();
    let interactive = stdin.is_terminal();
    let failed = match run_repl(
        &mut db,
        stdin.lock(),
        stdout().lock(),
        stderr().lock(),
        interactive,
    ) {
        Ok(failed) => failed,
        // Whatever read the output stopped early, as `| head` does. Exit quietly, like psql.
        Err(error) if error.kind() == ErrorKind::BrokenPipe => return Ok(ExitCode::SUCCESS),
        Err(error) => return Err(error).into_diagnostic(),
    };
    // A script that fails a statement should fail the run. A person at a terminal has already
    // seen the error, so the exit code stays 0 there.
    Ok(if failed > 0 && !interactive {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
