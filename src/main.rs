use miette::{IntoDiagnostic, Result};
use scuttle_db::database::Database;

use scuttle_db::table::{ColumnDefinition, DataType, Schema};

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

    // db.load_from_file().into_diagnostic()?;

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

    // db.insert_row(
    //     "users",
    //     Row::new(vec![
    //         Value::Integer(1),
    //         Value::Text("Alice".to_string()),
    //         Value::Integer(30),
    //     ]),
    // )?;
    // db.insert_row(
    //     "users",
    //     Row::new(vec![
    //         Value::Integer(2),
    //         Value::Text("Alice".to_string()),
    //         Value::Integer(88),
    //     ]),
    // )?;
    // db.insert_row(
    //     "users",
    //     Row::new(vec![
    //         Value::Integer(3),
    //         Value::Text("Alice".to_string()),
    //         Value::Integer(12),
    //     ]),
    // )?;
    // db.insert_row(
    //     "users",
    //     Row::new(vec![
    //         Value::Integer(4),
    //         Value::Text("Bob".to_string()),
    //         Value::Integer(99),
    //     ]),
    // )?;
    // db.insert_row(
    //     "customers",
    //     Row::new(vec![
    //         Value::Integer(5),
    //         Value::Text("Doug".to_string()),
    //         Value::Integer(24),
    //     ]),
    // )?;

    println!("Database created successfully!");
    println!("Tables: {:?}", db.tables.keys().collect::<Vec<_>>());

    let user_rows = db.get_rows("users").into_diagnostic()?;

    for row in user_rows {
        println!("{row:?}");
    }

    Ok(())
}
