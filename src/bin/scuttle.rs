use std::io::{BufRead, Write, stdin, stdout};

use miette::{IntoDiagnostic, Result, miette};

use scuttle_db::{ColumnDefinition, DataType, Database, Row, Schema, Value};

fn main() -> Result<()> {
    // Delete to start from fresh right now
    std::fs::remove_dir_all("./db").ok();

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

    let schema = Schema::new(vec![
        ColumnDefinition {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
        },
        ColumnDefinition {
            name: "name".to_string(),
            data_type: DataType::VarChar(255),
            nullable: false,
        },
        ColumnDefinition {
            name: "age".to_string(),
            data_type: DataType::Integer,
            nullable: true,
        },
        ColumnDefinition {
            name: "is_active".to_string(),
            data_type: DataType::Boolean,
            nullable: true,
        },
    ]);

    let _ = db.create_table("users", schema.clone());
    let _ = db.create_table("customers", schema);

    let _ = db.insert_row(
        "users",
        Row::new(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Integer(30),
            Value::Null,
        ]),
    );
    let _ = db.insert_row(
        "users",
        Row::new(vec![
            Value::Integer(2),
            Value::Text("Bob".to_string()),
            Value::Null,
            Value::Boolean(false),
        ]),
    );
    let _ = db.insert_row(
        "users",
        Row::new(vec![
            Value::Integer(3),
            Value::Text("Charlie".to_string()),
            Value::Integer(35),
            Value::Boolean(true),
        ]),
    );

    println!("Database created successfully!");
    println!("Tables: {:?}", db.tables.keys().collect::<Vec<_>>());

    let mut buf = String::new();

    let mut stdin = stdin().lock();
    let mut stdout = stdout().lock();

    loop {
        if buf.is_empty() {
            stdout.write_all("DB: ".as_bytes()).into_diagnostic()?
        } else {
            stdout.write_all("*  ".as_bytes()).into_diagnostic()?;
        }
        stdout.flush().into_diagnostic()?;

        let Ok(_) = stdin.read_line(&mut buf) else {
            return Err(miette!("Input reading failed"));
        };

        let input = buf.trim();
        if input == "exit" {
            break;
        }

        let query_result = match db.execute_query(input) {
            Ok(res) => res,
            Err(err) => {
                println!("{:?}", err.with_source_code(input.to_string()));
                buf.clear();
                continue;
            }
        };

        stdout
            .write_all(format!("{: <8}", "Results").as_bytes())
            .into_diagnostic()?;

        // Print only the projected column names
        query_result.schema.columns.iter().for_each(|col| {
            let _ = stdout
                .write_all(format!(" | {: <8}", col.name).as_bytes())
                .into_diagnostic();
        });

        let _ = stdout.write_all(b"\n").into_diagnostic();

        query_result.rows.iter().enumerate().for_each(|(idx, row)| {
            let _ = stdout
                .write_all(format!("{: <8}", idx).as_bytes())
                .into_diagnostic();

            row.values.iter().for_each(|value| {
                let _ = stdout
                    .write_all(format!(" | {: <8}", value.to_string()).as_bytes())
                    .into_diagnostic();
            });

            let _ = stdout.write_all(b"\n").into_diagnostic();
        });

        stdout.flush().into_diagnostic()?;
        buf.clear();
    }

    println!("Exiting Scuttle");

    Ok(())
}
