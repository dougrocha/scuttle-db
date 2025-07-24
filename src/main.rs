use rust_database::database::Database;

use rust_database::DatabaseError;
use rust_database::table::{ColumnDefinition, DataType, Row, Schema, Value};

fn main() -> Result<(), DatabaseError> {
    let mut db = Database::new("./db");

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
    ]);

    db.create_table("users".to_string(), schema.clone())?;
    db.create_table("customers".to_string(), schema)?;

    let table = db.get_table_mut("users")?;

    table.insert_row(Row::new(vec![
        Value::Integer(1),
        Value::Text("Alice".to_string()),
        Value::Integer(30),
    ]))?;

    table.insert_row(Row::new(vec![
        Value::Integer(2),
        Value::Text("Bob".to_string()),
        Value::Null,
    ]))?;

    db.get_table_mut("customers")?.insert_row(Row::new(vec![
        Value::Integer(1),
        Value::Text("Doug".to_string()),
        Value::Integer(24),
    ]))?;

    println!("Database created successfully!");
    println!("Tables: {:?}", db.tables.keys().collect::<Vec<_>>());

    let users_table = db.get_table_mut("users")?;
    println!("Users table has {} rows", users_table.rows.len());

    for (i, row) in users_table.rows.iter().enumerate() {
        println!("Row {}: {:?}", i, row.values);
    }

    let removed_user = users_table.remove_row(1)?;
    println!("Removed User: {removed_user:?}");

    db.save()?;

    let mut db = Database::new("./db");
    db.load_from_file()?;

    println!("Database loaded successfully!");
    println!("Tables: {:?}", db.tables.keys().collect::<Vec<_>>());

    let users_table = db.get_table("users")?;
    println!("Users table has {} rows", users_table.rows.len());

    for (i, row) in users_table.rows.iter().enumerate() {
        println!("Row {}: {:?}", i, row.values);
    }

    Ok(())
}
