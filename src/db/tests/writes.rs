//! INSERT, UPDATE and DELETE, with their constraints and DEFAULTs.

use super::*;
#[test]
fn test_delete() {
    let mut db = TestDb::new("delete");

    assert_eq!(db.affected("DELETE FROM users WHERE dept = 'ops'"), Some(2));
    assert_eq!(
        db.query("SELECT name FROM users ORDER BY id"),
        vec![vec![text("Alice")], vec![text("Bob")]]
    );

    assert_eq!(db.affected("DELETE FROM users WHERE id = 99"), Some(0));
    assert_eq!(db.affected("DELETE FROM users"), Some(2));
    assert!(db.query("SELECT * FROM users").is_empty());
}

#[test]
fn test_update() {
    let mut db = TestDb::new("update");

    assert_eq!(
        db.affected("UPDATE users SET name = 'Alicia', age = age + 1 WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        db.query("SELECT name, age FROM users WHERE id = 1"),
        vec![vec![text("Alicia"), Value::Int64(31)]]
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM users"),
        vec![vec![Value::Int64(4)]]
    );
}

#[test]
fn test_update_all_rows_updates_each_once() {
    let mut db = TestDb::new("update-all");

    assert_eq!(db.affected("UPDATE users SET age = age + 1"), Some(4));
    assert_eq!(
        db.query("SELECT age FROM users ORDER BY id"),
        vec![
            vec![Value::Int64(31)],
            vec![Value::Int64(26)],
            vec![Value::Int64(42)],
            vec![Value::Int64(36)],
        ]
    );
}

#[test]
fn test_update_set_expressions_see_old_row() {
    let mut db = TestDb::new("update-swap");

    db.affected("UPDATE users SET id = age, age = id WHERE name = 'Bob'");
    assert_eq!(
        db.query("SELECT id, age FROM users WHERE name = 'Bob'"),
        vec![vec![Value::Int64(25), Value::Int64(2)]]
    );
}

#[test]
fn test_update_errors() {
    let mut db = TestDb::new("update-errors");

    assert!(db.fails("UPDATE users SET missing = 1"));
    assert!(db.fails("UPDATE users SET age = 'old'"));
    assert!(db.fails("UPDATE users SET age = 1, age = 2"));
    assert!(db.fails("UPDATE nope SET age = 1"));

    // Nothing was changed by the failed statements
    assert_eq!(
        db.query("SELECT SUM(age) FROM users"),
        vec![vec![Value::Int64(131)]]
    );
}

#[test]
fn test_update_and_delete_persist() {
    let mut db = TestDb::new("persist");

    db.affected("UPDATE users SET age = 50 WHERE id = 2");
    db.affected("DELETE FROM users WHERE id = 3");
    db.reopen();

    assert_eq!(
        db.query("SELECT id, age FROM users ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Int64(30)],
            vec![Value::Int64(2), Value::Int64(50)],
            vec![Value::Int64(4), Value::Int64(35)],
        ]
    );
}

#[test]
fn test_table_larger_than_1000_pages_persists() {
    let mut db = TestDb::new("many-pages");
    db.execute("CREATE TABLE wide (id INT, body TEXT)");

    // About 2KB per row fits four rows in a page, so this needs more than 1000 pages
    let body = "x".repeat(2000);
    let rows = 4200;
    for start in (0..rows).step_by(100) {
        let values: Vec<String> = (start..start + 100)
            .map(|id| format!("({id}, '{body}')"))
            .collect();
        db.execute(&format!("INSERT INTO wide VALUES {}", values.join(", ")));
    }
    assert!(db.db.pool.page_count("wide") > 1000);

    let count = |db: &mut TestDb| db.query("SELECT COUNT(*) FROM wide");
    assert_eq!(count(&mut db), vec![vec![Value::Int64(rows)]]);

    // Reopening reads the page count back from the file, and the last page loaded
    // from disk is full, so the next insert has to start a new page
    db.reopen();
    assert_eq!(count(&mut db), vec![vec![Value::Int64(rows)]]);
    db.execute(&format!("INSERT INTO wide VALUES ({rows}, '{body}')"));
    assert_eq!(count(&mut db), vec![vec![Value::Int64(rows + 1)]]);

    // Rows past page 1000 can be found and deleted
    assert_eq!(db.affected("DELETE FROM wide WHERE id >= 4100"), Some(101));
    db.reopen();
    assert_eq!(count(&mut db), vec![vec![Value::Int64(4100)]]);
}

#[test]
fn test_unique_columns_reject_duplicates_and_change_nothing() {
    let mut db = TestDb::new("unique");
    db.execute("CREATE TABLE acct (id INT PRIMARY KEY, email TEXT UNIQUE, note TEXT)");
    db.execute("INSERT INTO acct VALUES (1, 'a@x', 'one'), (2, 'b@x', 'two')");
    let ids = |db: &mut TestDb| db.query("SELECT id FROM acct ORDER BY id");
    let both = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];

    // Against stored rows, within one statement, and on either unique column. A failing
    // statement writes none of its rows.
    assert!(db.fails("INSERT INTO acct VALUES (3, 'c@x', ''), (1, 'd@x', '')"));
    assert!(db.fails("INSERT INTO acct VALUES (3, 'c@x', ''), (3, 'd@x', '')"));
    assert!(db.fails("INSERT INTO acct VALUES (3, 'a@x', '')"));
    assert_eq!(ids(&mut db), both);

    // NULL is not a key, so a UNIQUE column holds any number of them
    db.execute("INSERT INTO acct VALUES (3, NULL, ''), (4, NULL, '')");
    db.execute("DELETE FROM acct WHERE id >= 3");

    // A row may keep its key, and keys may move as long as they stay distinct
    db.execute("UPDATE acct SET note = 'same key' WHERE id = 1");
    db.execute("UPDATE acct SET id = id + 1");
    assert_eq!(
        ids(&mut db),
        vec![vec![Value::Int64(2)], vec![Value::Int64(3)]]
    );
    assert!(db.fails("UPDATE acct SET id = 7"));

    // A deleted key is free again, and the index comes back after a restart
    db.execute("DELETE FROM acct WHERE id = 3");
    db.execute("INSERT INTO acct VALUES (3, 'b2@x', '')");
    db.reopen();
    assert!(db.fails("INSERT INTO acct VALUES (3, 'z@x', '')"));
    assert!(db.fails("INSERT INTO acct VALUES (9, 'a@x', '')"));
    db.execute("INSERT INTO acct VALUES (9, 'z@x', '')");
}

#[test]
fn test_create_table_rejects_two_primary_keys() {
    let mut db = TestDb::new("two-pks");
    assert!(db.fails("CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)"));
    assert!(db.fails("SELECT * FROM t"));
}

#[test]
fn test_insert_null() {
    let mut db = TestDb::new("insert-null");

    db.affected("INSERT INTO users VALUES (5, 'Eve', NULL, NULL)");
    // Omitted nullable columns are NULL too
    db.affected("INSERT INTO users (id, name) VALUES (6, 'Finn')");

    assert_eq!(
        db.query("SELECT id FROM users WHERE age IS NULL ORDER BY id"),
        vec![vec![Value::Int64(5)], vec![Value::Int64(6)]]
    );

    // Aggregates skip NULLs; COUNT(*) doesn't
    assert_eq!(
        db.query("SELECT COUNT(*), COUNT(age), AVG(age) FROM users"),
        vec![vec![
            Value::Int64(6),
            Value::Int64(4),
            Value::Float64(32.75)
        ]]
    );

    // NULLs sort last ascending, first descending
    assert_eq!(
        db.query("SELECT id FROM users ORDER BY age LIMIT 1"),
        vec![vec![Value::Int64(2)]]
    );
    assert_eq!(
        db.query("SELECT id FROM users ORDER BY age DESC, id LIMIT 1"),
        vec![vec![Value::Int64(5)]]
    );

    // NULL is its own group
    assert_eq!(
        db.query("SELECT dept, COUNT(*) FROM users GROUP BY dept ORDER BY dept"),
        vec![
            vec![text("eng"), Value::Int64(2)],
            vec![text("ops"), Value::Int64(2)],
            vec![Value::Null, Value::Int64(2)],
        ]
    );

    assert_eq!(
        db.affected("UPDATE users SET age = NULL WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        db.query("SELECT age FROM users WHERE id = 1"),
        vec![vec![Value::Null]]
    );
}

#[test]
fn test_insert_respects_not_null() {
    let mut db = TestDb::new("insert-not-null");
    db.execute("CREATE TABLE strict (id INT PRIMARY KEY, name TEXT NOT NULL, note TEXT NULL)");

    assert!(db.fails("INSERT INTO strict VALUES (1, NULL, 'x')"));
    assert!(db.fails("INSERT INTO strict VALUES (NULL, 'a', 'x')"));
    assert!(db.fails("INSERT INTO strict (id) VALUES (1)"));

    assert_eq!(
        db.affected("INSERT INTO strict (id, name) VALUES (1, 'a')"),
        Some(1)
    );
    assert_eq!(
        db.query("SELECT * FROM strict"),
        vec![vec![Value::Int64(1), text("a"), Value::Null]]
    );
    assert!(db.fails("UPDATE strict SET name = NULL"));
}

#[test]
fn test_insert_type_checks() {
    let mut db = TestDb::new("insert-types");
    db.execute("CREATE TABLE prices (id INT, price FLOAT, code VARCHAR(3))");

    // Wrong types are rejected instead of crashing the encoder
    assert!(db.fails("INSERT INTO prices VALUES (2.5, 1.0, 'abc')"));
    assert!(db.fails("INSERT INTO prices VALUES ('one', 1.0, 'abc')"));
    assert!(db.fails("INSERT INTO prices VALUES (1, 1.0, 'toolong')"));

    // INT widens to FLOAT
    db.affected("INSERT INTO prices VALUES (1, 3, 'abc')");
    assert_eq!(
        db.query("SELECT price FROM prices"),
        vec![vec![Value::Float64(3.0)]]
    );
}

#[test]
fn test_insert_column_list() {
    let mut db = TestDb::new("insert-columns");

    // Values follow the column list's order, not the table's
    db.affected("INSERT INTO users (age, name, id, dept) VALUES (50, 'Gus', 7, 'ops')");
    assert_eq!(
        db.query("SELECT id, name, age FROM users WHERE id = 7"),
        vec![vec![Value::Int64(7), text("Gus"), Value::Int64(50)]]
    );

    // Expressions are evaluated, not dropped
    db.affected("INSERT INTO users VALUES (4 * 2, 'Hal', 'eng', 20 + 1)");
    assert_eq!(
        db.query("SELECT id, age FROM users WHERE name = 'Hal'"),
        vec![vec![Value::Int64(8), Value::Int64(21)]]
    );

    assert!(db.fails("INSERT INTO users (id, missing) VALUES (9, 1)"));
    assert!(db.fails("INSERT INTO users (id, id) VALUES (9, 9)"));
    assert!(db.fails("INSERT INTO users (id, name) VALUES (9)"));
    assert!(db.fails("INSERT INTO users (id) VALUES (age)"));

    // A failed multi-row insert writes nothing
    assert!(db.fails("INSERT INTO users (id) VALUES (10), ('bad')"));
    assert_eq!(
        db.query("SELECT COUNT(*) FROM users"),
        vec![vec![Value::Int64(6)]]
    );
}

#[test]
fn test_create_table_existing_table_fails() {
    let mut db = TestDb::new("create-existing-fails");
    assert_eq!(
        error_message(&mut db.db, "CREATE TABLE users (id INT)"),
        "Table users already exists"
    );
}

#[test]
fn test_create_table_existing_table_keeps_schema_and_rows() {
    let mut db = TestDb::new("create-existing-keeps");
    assert!(db.fails("CREATE TABLE users (id INT)"));
    assert_eq!(
        db.columns("SELECT * FROM users"),
        vec!["id", "name", "dept", "age"]
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM users"),
        vec![vec![Value::Int64(4)]]
    );
}

#[test]
fn test_create_table_existing_table_fails_after_reopen() {
    let mut db = TestDb::new("create-existing-reopen");
    db.reopen();
    assert!(db.fails("CREATE TABLE users (id INT)"));
    assert_eq!(
        db.query("SELECT COUNT(*) FROM users"),
        vec![vec![Value::Int64(4)]]
    );
}

#[test]
fn test_create_table_new_name_then_duplicate_fails() {
    let mut db = TestDb::new("create-new-then-dup");
    db.execute("CREATE TABLE extra (id INT)");
    assert_eq!(
        error_message(&mut db.db, "CREATE TABLE extra (id INT)"),
        "Table extra already exists"
    );
}

#[test]
fn test_default_applies_when_insert_omits_column() {
    let mut db = TestDb::new("default-omitted");
    db.execute("CREATE TABLE t (id INT, n INT DEFAULT 5)");
    db.execute("INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        db.query("SELECT * FROM t"),
        vec![vec![Value::Int64(1), Value::Int64(5)]]
    );
}

#[test]
fn test_default_literal_kinds() {
    let mut db = TestDb::new("default-kinds");
    db.execute(
        "CREATE TABLE t (id INT, f FLOAT DEFAULT 2, s TEXT DEFAULT 'hi', \
         b BOOL DEFAULT TRUE, z TEXT DEFAULT NULL)",
    );
    db.execute("INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        db.query("SELECT * FROM t"),
        vec![vec![
            Value::Int64(1),
            Value::Float64(2.0),
            text("hi"),
            Value::Bool(true),
            Value::Null,
        ]]
    );
}

#[test]
fn test_default_not_used_when_value_given() {
    let mut db = TestDb::new("default-given");
    db.execute("CREATE TABLE t (id INT, n INT DEFAULT 5)");
    db.execute("INSERT INTO t (id, n) VALUES (1, 9)");
    db.execute("INSERT INTO t (id, n) VALUES (2, NULL)");
    assert_eq!(
        db.query("SELECT * FROM t"),
        vec![
            vec![Value::Int64(1), Value::Int64(9)],
            vec![Value::Int64(2), Value::Null],
        ]
    );
}

#[test]
fn test_default_lets_not_null_column_be_omitted() {
    let mut db = TestDb::new("default-not-null");
    db.execute("CREATE TABLE t (id INT, n INT NOT NULL DEFAULT 4)");
    db.execute("INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        db.query("SELECT * FROM t"),
        vec![vec![Value::Int64(1), Value::Int64(4)]]
    );
}

#[test]
fn test_default_null_on_not_null_column_fails_at_insert() {
    let mut db = TestDb::new("default-null-not-null");
    db.execute("CREATE TABLE t (id INT, n INT NOT NULL DEFAULT NULL)");
    let message = error_message(&mut db.db, "INSERT INTO t (id) VALUES (1)");
    assert!(message.contains("Column n cannot be null"), "{message}");
    assert!(db.query("SELECT * FROM t").is_empty());
}

#[test]
fn test_default_rejects_wrong_type_at_create() {
    let mut db = TestDb::new("default-wrong-type");
    let message = error_message(&mut db.db, "CREATE TABLE t (id INT DEFAULT 'x')");
    assert!(
        message.contains("Column id has an invalid DEFAULT"),
        "{message}"
    );
    assert!(db.fails("SELECT * FROM t"));
}

#[test]
fn test_default_survives_restart() {
    let mut db = TestDb::new("default-restart");
    db.execute("CREATE TABLE t (id INT, n INT DEFAULT 5)");
    db.reopen();
    db.execute("INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        db.query("SELECT * FROM t"),
        vec![vec![Value::Int64(1), Value::Int64(5)]]
    );
}

#[test]
fn test_column_without_default_still_gets_null() {
    let mut db = TestDb::new("default-none");
    db.execute("CREATE TABLE t (id INT, n INT)");
    db.execute("INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        db.query("SELECT * FROM t"),
        vec![vec![Value::Int64(1), Value::Null]]
    );
}
