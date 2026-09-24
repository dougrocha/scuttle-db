use std::io::{BufRead, Write, stdin, stdout};

use miette::{IntoDiagnostic, Result, miette};
use scuttle_db::Database;

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

    println!("Database created successfully!");
    println!("Tables: {:?}", db.tables.keys().collect::<Vec<_>>());

    let mut buf = String::new();

    let mut stdin = stdin().lock();
    let mut stdout = stdout().lock();

    loop {
        if buf.is_empty() {
            stdout.write_all("DB: ".as_bytes()).into_diagnostic()?;
        } else {
            stdout.write_all("*  ".as_bytes()).into_diagnostic()?;
        }
        stdout.flush().into_diagnostic()?;

        let Ok(_) = stdin.read_line(&mut buf) else {
            return Err(miette!("Input reading failed"));
        };

        let input = buf.trim();
        if input == "exit" || input == ":q" {
            break;
        }

        let query_response = match db.execute_query(input) {
            Ok(res) => res,
            Err(err) => {
                println!("{:?}", err.with_source_code(input.to_string()));
                buf.clear();
                continue;
            }
        };
        writeln!(stdout, "{query_response}").into_diagnostic()?;
        stdout.flush().into_diagnostic()?;
        buf.clear();
    }

    println!("Exiting Scuttle");

    Ok(())
}
