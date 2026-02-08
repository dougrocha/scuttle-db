use std::io::{BufRead, Write, stdin, stdout};

use miette::{IntoDiagnostic, Result, miette};
use scuttle_db::{ColumnDefinition, Database, PhysicalType, Row, Schema, Value};

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
            data_type: PhysicalType::Int64,
            nullable: false,
        },
        ColumnDefinition {
            name: "name".to_string(),
            data_type: PhysicalType::VarChar(255),
            nullable: false,
        },
        ColumnDefinition {
            name: "age".to_string(),
            data_type: PhysicalType::Int64,
            nullable: true,
        },
        ColumnDefinition {
            name: "is_active".to_string(),
            data_type: PhysicalType::Bool,
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
        let schema = query_response.schema;
        let rows = query_response.rows;

        if rows.is_empty() {
            println!("Empty set (0 rows)");
            buf.clear();
            continue;
        }

        stdout
            .write_all(format!("{: <8}", "Row #").as_bytes())
            .into_diagnostic()?;
        for col in &schema.fields {
            stdout
                .write_all(
                    format!(" | {: <12}", col.alias.clone().unwrap_or(col.name.clone()),)
                        .as_bytes(),
                )
                .into_diagnostic()?;
        }
        stdout.write_all(b"\n").into_diagnostic()?;
        let separator_len = 8 + (schema.fields.len() * 15);
        stdout
            .write_all(&"-".repeat(separator_len).into_bytes())
            .into_diagnostic()?;
        stdout.write_all(b"\n").into_diagnostic()?;

        for (idx, row) in rows.iter().enumerate() {
            stdout
                .write_all(format!("{: <8}", idx).as_bytes())
                .into_diagnostic()?;
            for value in &row.values {
                stdout
                    .write_all(format!(" | {: <12}", value.to_string()).as_bytes())
                    .into_diagnostic()?;
            }
            stdout.write_all(b"\n").into_diagnostic()?;
        }

        stdout.flush().into_diagnostic()?;
        buf.clear();
    }

    println!("Exiting Scuttle");

    Ok(())
}
