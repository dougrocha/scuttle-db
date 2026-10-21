use std::io::{IsTerminal, stderr, stdin, stdout};
use std::process::ExitCode;

use miette::{IntoDiagnostic, Result};
use scuttle_db::{Database, run_repl};

const USAGE: &str = "\
Usage: scuttle-db [--data <dir> | --in-memory]

Options:
  -d, --data <dir>  Store tables in <dir> (default: ./db)
      --in-memory   Keep all data in memory; nothing is written to disk
  -h, --help        Print this help
";

/// Where the database keeps its tables, as chosen on the command line.
enum Storage {
    Dir(String),
    InMemory,
}

/// Parses the arguments after the program name. `Ok(None)` means `--help` was asked for.
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Option<Storage>, String> {
    let mut data = None;
    let mut in_memory = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "-d" | "--data" => {
                data = Some(
                    args.next()
                        .ok_or_else(|| format!("{arg} needs a directory"))?,
                );
            }
            "--in-memory" => in_memory = true,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    match (data, in_memory) {
        (Some(_), true) => Err("--data and --in-memory cannot be combined".to_string()),
        (None, true) => Ok(Some(Storage::InMemory)),
        (data, false) => Ok(Some(Storage::Dir(
            data.unwrap_or_else(|| "./db".to_string()),
        ))),
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
        Storage::Dir(dir) => Database::new(dir),
        Storage::InMemory => Database::in_memory(),
    };
    db.initialize().expect("Failed to init catalog");

    let stdin = stdin();
    let interactive = stdin.is_terminal();
    let failed = run_repl(
        &mut db,
        stdin.lock(),
        stdout().lock(),
        stderr().lock(),
        interactive,
    )
    .into_diagnostic()?;
    // A script that fails a statement should fail the run. A person at a terminal has already
    // seen the error, so the exit code stays 0 there.
    Ok(if failed > 0 && !interactive {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}
