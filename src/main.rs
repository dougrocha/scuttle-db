use miette::{IntoDiagnostic, Result};
use scuttle_db::database::Database;

use scuttle_db::table::{ColumnDefinition, DataType, Row, Schema, Value};

fn main() -> Result<()> {
    // Delete to start from fresh right now
    // std::fs::remove_dir_all("./db").ok();

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
            nullable: false,
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
        ]),
    );
    let _ = db.insert_row(
        "users",
        Row::new(vec![
            Value::Integer(2),
            Value::Text("Bob".to_string()),
            Value::Integer(25),
        ]),
    );
    let _ = db.insert_row(
        "users",
        Row::new(vec![
            Value::Integer(3),
            Value::Text("Charlie".to_string()),
            Value::Integer(35),
        ]),
    );

    println!("Database created successfully!");
    println!("Tables: {:?}", db.tables.keys().collect::<Vec<_>>());

    println!("\n=== Testing Query Execution ===");

    // Test SELECT * FROM users
    println!("\nExecuting: SELECT * FROM users");
    let query_result = db.execute_query("SELECT * FROM users").into_diagnostic()?;
    for row in &query_result {
        println!("  {:?}", row);
    }

    // Test SELECT id, name FROM users
    println!("\nExecuting: SELECT id, name FROM users");
    let query_result = db
        .execute_query("SELECT id, name FROM users")
        .into_diagnostic()?;
    for row in &query_result {
        println!("  {:?}", row);
    }

    // Test SELECT name FROM users
    println!("\nExecuting: SELECT name FROM users");
    let query_result = db
        .execute_query("SELECT name FROM users")
        .into_diagnostic()?;
    for row in &query_result {
        println!("  {:?}", row);
    }

    Ok(())
}
