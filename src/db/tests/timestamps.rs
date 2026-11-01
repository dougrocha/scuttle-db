//! TIMESTAMP columns: literals, comparisons, ordering and storage.

use super::*;
fn ts(text: &str) -> Value {
    Value::Timestamp(crate::core::timestamp::parse_timestamp(text).unwrap())
}

/// A database with an `ev(id INT, at TIMESTAMP, note TEXT)` table holding four rows.
fn timestamp_db(name: &str) -> TestDb {
    let mut db = TestDb::new(name);
    db.execute("CREATE TABLE ev (id INT, at TIMESTAMP, note TEXT)");
    db.execute(
        "INSERT INTO ev VALUES (1, '2026-03-01 12:00:00', 'march'), (2, '2026-01-01', 'new year'), \
         (3, TIMESTAMP '1969-12-31 23:59:59', 'before'), (4, NULL, 'unknown')",
    );
    db
}

#[test]
fn test_timestamp_insert_string_and_select_formats_it() {
    let mut db = timestamp_db("ts-insert-string");
    assert_eq!(
        db.query("SELECT at FROM ev WHERE id = 2"),
        vec![vec![ts("2026-01-01 00:00:00")]]
    );
}

#[test]
fn test_timestamp_insert_typed_literal() {
    let mut db = timestamp_db("ts-insert-typed");
    assert_eq!(
        db.query("SELECT at FROM ev WHERE id = 3"),
        vec![vec![ts("1969-12-31 23:59:59")]]
    );
}

#[test]
fn test_timestamp_insert_named_column_leaves_others_null() {
    let mut db = timestamp_db("ts-named-column");
    db.execute("INSERT INTO ev (id) VALUES (5)");
    assert_eq!(
        db.query("SELECT at, note FROM ev WHERE id = 5"),
        vec![vec![Value::Null, Value::Null]]
    );
}

#[test]
fn test_timestamp_insert_rejects_bad_text() {
    let mut db = timestamp_db("ts-bad-insert");
    assert!(db.fails("INSERT INTO ev VALUES (5, 'yesterday', 'x')"));
    assert!(db.fails("INSERT INTO ev VALUES (5, '2026-02-30', 'x')"));
    assert_eq!(
        db.query("SELECT COUNT(*) FROM ev"),
        vec![vec![Value::Int64(4)]]
    );
}

#[test]
fn test_timestamp_text_column_does_not_accept_timestamps() {
    let mut db = TestDb::new("ts-text-column");
    assert!(db.fails("INSERT INTO users (id, name) VALUES (9, TIMESTAMP '2026-01-01')"));
}

#[test]
fn test_timestamp_comparison_with_string_literal_both_orders() {
    let mut db = timestamp_db("ts-cmp-string");
    assert_eq!(
        db.query("SELECT id FROM ev WHERE at > '2026-01-15'"),
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        db.query("SELECT id FROM ev WHERE '2026-01-01 00:00:00' = at"),
        vec![vec![Value::Int64(2)]]
    );
    assert_eq!(
        db.query("SELECT id FROM ev WHERE '2026-01-15' < at"),
        vec![vec![Value::Int64(1)]]
    );
}

#[test]
fn test_timestamp_comparison_with_typed_literal() {
    let mut db = timestamp_db("ts-cmp-typed");
    assert_eq!(
        db.query("SELECT id FROM ev WHERE at >= TIMESTAMP '2026-01-01' ORDER BY id"),
        vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]
    );
}

#[test]
fn test_timestamp_comparison_rejects_bad_text() {
    let mut db = timestamp_db("ts-cmp-bad");
    assert!(db.fails("SELECT id FROM ev WHERE at > 'soon'"));
    assert!(db.fails("SELECT id FROM ev WHERE at = '2026-02-30'"));
}

#[test]
fn test_timestamp_column_not_comparable_with_text_column() {
    let mut db = timestamp_db("ts-cmp-text-column");
    assert!(db.fails("SELECT id FROM ev WHERE at = note"));
}

#[test]
fn test_timestamp_order_by_and_min_max() {
    let mut db = timestamp_db("ts-order-minmax");
    assert_eq!(
        db.query("SELECT id FROM ev ORDER BY at"),
        vec![
            vec![Value::Int64(3)],
            vec![Value::Int64(2)],
            vec![Value::Int64(1)],
            vec![Value::Int64(4)],
        ]
    );
    assert_eq!(
        db.query("SELECT MIN(at), MAX(at) FROM ev"),
        vec![vec![ts("1969-12-31 23:59:59"), ts("2026-03-01 12:00:00")]]
    );
}

#[test]
fn test_timestamp_update_set_string_and_where() {
    let mut db = timestamp_db("ts-update");
    assert_eq!(
        db.affected("UPDATE ev SET at = '2030-05-06 07:08:09' WHERE id = 3"),
        Some(1)
    );
    assert_eq!(
        db.query("SELECT at FROM ev WHERE id = 3"),
        vec![vec![ts("2030-05-06 07:08:09")]]
    );
}

#[test]
fn test_timestamp_delete_where() {
    let mut db = timestamp_db("ts-delete");
    assert_eq!(
        db.affected("DELETE FROM ev WHERE at < '2026-01-01'"),
        Some(1)
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM ev"),
        vec![vec![Value::Int64(3)]]
    );
}

#[test]
fn test_timestamp_arithmetic_is_rejected() {
    let mut db = timestamp_db("ts-arithmetic");
    assert!(db.fails("SELECT at + 1 FROM ev"));
    assert!(db.fails("SELECT at - at FROM ev"));
}

#[test]
fn test_timestamp_survives_restart() {
    let mut db = timestamp_db("ts-restart");
    db.reopen();
    assert_eq!(
        db.query("SELECT id, at FROM ev ORDER BY id"),
        vec![
            vec![Value::Int64(1), ts("2026-03-01 12:00:00")],
            vec![Value::Int64(2), ts("2026-01-01 00:00:00")],
            vec![Value::Int64(3), ts("1969-12-31 23:59:59")],
            vec![Value::Int64(4), Value::Null],
        ]
    );
}

#[test]
fn test_timestamp_default_literal() {
    let mut db = TestDb::new("ts-default");
    db.execute("CREATE TABLE log (id INT, at TIMESTAMP DEFAULT '2026-01-01 08:00:00')");
    db.execute("INSERT INTO log (id) VALUES (1)");
    assert_eq!(
        db.query("SELECT at FROM log"),
        vec![vec![ts("2026-01-01 08:00:00")]]
    );
}

#[test]
fn test_timestamp_explain_shows_typed_literal() {
    let mut db = timestamp_db("ts-explain");
    let plan = db
        .db
        .explain("SELECT id FROM ev WHERE at > TIMESTAMP '2026-01-01'")
        .unwrap();
    assert!(
        plan.contains("TIMESTAMP '2026-01-01 00:00:00'"),
        "plan was {plan}"
    );
}
