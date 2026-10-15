use std::io::{IsTerminal, stdin, stdout};

use miette::{IntoDiagnostic, Result};
use scuttle_db::{Database, run_repl};

fn main() -> Result<()> {
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
    run_repl(&mut db, stdin.lock(), stdout().lock(), interactive).into_diagnostic()
}
