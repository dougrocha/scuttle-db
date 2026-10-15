use std::io::{IsTerminal, stderr, stdin, stdout};
use std::process::ExitCode;

use miette::{IntoDiagnostic, Result};
use scuttle_db::{Database, run_repl};

fn main() -> Result<ExitCode> {
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

    let mut db = Database::new("./db");
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
