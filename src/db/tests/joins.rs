//! Joins: inner and left, chains, and how names resolve across tables.

use super::*;
/// `orders(id, user_id, total)` and `depts(code, title)` on top of `users`.
fn seed_orders(db: &mut TestDb) {
    db.execute("CREATE TABLE orders (id INT, user_id INT, total INT)");
    db.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 1, 50), (12, 3, 75), (13, 9, 5)");
    db.execute("CREATE TABLE depts (code TEXT, title TEXT)");
    db.execute("INSERT INTO depts VALUES ('eng', 'Engineering'), ('ops', 'Operations')");
}

#[test]
fn test_join_inner_values_from_both_sides() {
    let mut db = TestDb::new("join-inner");
    seed_orders(&mut db);

    let expected = vec![
        vec![text("Alice"), Value::Int64(100)],
        vec![text("Alice"), Value::Int64(50)],
        vec![text("Carol"), Value::Int64(75)],
    ];
    assert_eq!(
        db.query(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id"
        ),
        expected
    );
    assert_eq!(
        db.query("SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.id"),
        expected
    );
}

#[test]
fn test_join_same_column_name_both_sides() {
    let mut db = TestDb::new("join-same-name");
    seed_orders(&mut db);

    assert_eq!(
        db.query("SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id"),
        vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(1), Value::Int64(11)],
            vec![Value::Int64(3), Value::Int64(12)],
        ]
    );
}

#[test]
fn test_join_left_pads_unmatched_with_null() {
    let mut db = TestDb::new("join-left-pad");
    seed_orders(&mut db);

    assert_eq!(
        db.query("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id"),
        vec![
            vec![text("Alice"), Value::Int64(100)],
            vec![text("Alice"), Value::Int64(50)],
            vec![text("Bob"), Value::Null],
            vec![text("Carol"), Value::Int64(75)],
            vec![text("Dan"), Value::Null],
        ]
    );
}

/// The NULL padding of an unmatched LEFT row must be exactly as wide as the right
/// table: a join chained after it reads its columns at fixed positions.
#[test]
fn test_join_after_left_join_on_empty_table_reads_shifted_columns() {
    let mut db = TestDb::new("join-pad-width");
    seed_orders(&mut db);
    db.execute("CREATE TABLE empty_t (id INT, v INT)");

    assert_eq!(
        db.query(
            "SELECT u.name, e.v, o.total FROM users u LEFT JOIN empty_t e ON u.id = e.id \
             JOIN orders o ON o.user_id = u.id ORDER BY o.id"
        ),
        vec![
            vec![text("Alice"), Value::Null, Value::Int64(100)],
            vec![text("Alice"), Value::Null, Value::Int64(50)],
            vec![text("Carol"), Value::Null, Value::Int64(75)],
        ]
    );
}

#[test]
fn test_join_left_with_empty_right_table() {
    let mut db = TestDb::new("join-empty-right");
    db.execute("CREATE TABLE empty_t (id INT, v INT)");

    assert_eq!(
        db.query("SELECT u.id, e.v FROM users u LEFT JOIN empty_t e ON u.id = e.id ORDER BY u.id"),
        vec![
            vec![Value::Int64(1), Value::Null],
            vec![Value::Int64(2), Value::Null],
            vec![Value::Int64(3), Value::Null],
            vec![Value::Int64(4), Value::Null],
        ]
    );
    assert_eq!(
        db.columns("SELECT * FROM users u LEFT JOIN empty_t e ON u.id = e.id")
            .len(),
        6
    );
    assert_eq!(
        db.query("SELECT u.id FROM users u JOIN empty_t e ON u.id = e.id"),
        Vec::<Vec<Value>>::new()
    );
}

#[test]
fn test_join_left_on_vs_where() {
    let mut db = TestDb::new("join-on-vs-where");
    seed_orders(&mut db);

    // The condition in ON keeps every user; unmatched users are padded.
    assert_eq!(
        db.query("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id AND o.total > 60 ORDER BY u.id"),
        vec![
            vec![text("Alice"), Value::Int64(100)],
            vec![text("Bob"), Value::Null],
            vec![text("Carol"), Value::Int64(75)],
            vec![text("Dan"), Value::Null],
        ]
    );

    // The same condition in WHERE removes the padded rows.
    assert_eq!(
        db.query("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.total > 60 ORDER BY u.id"),
        vec![
            vec![text("Alice"), Value::Int64(100)],
            vec![text("Carol"), Value::Int64(75)],
        ]
    );
}

#[test]
fn test_join_left_anti_join_with_is_null() {
    let mut db = TestDb::new("join-anti");
    seed_orders(&mut db);

    assert_eq!(
        db.query("SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.id IS NULL ORDER BY u.id"),
        vec![vec![text("Bob")], vec![text("Dan")]]
    );
}

#[test]
fn test_join_self_join() {
    let mut db = TestDb::new("join-self");

    assert_eq!(
        db.query("SELECT a.name, b.name FROM users a JOIN users b ON a.dept = b.dept AND a.id < b.id ORDER BY a.id, b.id"),
        vec![
            vec![text("Alice"), text("Bob")],
            vec![text("Carol"), text("Dan")],
        ]
    );
    assert_eq!(
        db.columns("SELECT a.name, b.name FROM users a JOIN users b ON a.dept = b.dept"),
        vec!["name", "name"]
    );
}

#[test]
fn test_join_chain_third_table_column() {
    let mut db = TestDb::new("join-chain");
    seed_orders(&mut db);

    assert_eq!(
        db.query("SELECT u.name, o.total, d.title FROM users u JOIN orders o ON u.id = o.user_id JOIN depts d ON u.dept = d.code ORDER BY o.id"),
        vec![
            vec![text("Alice"), Value::Int64(100), text("Engineering")],
            vec![text("Alice"), Value::Int64(50), text("Engineering")],
            vec![text("Carol"), Value::Int64(75), text("Operations")],
        ]
    );
}

#[test]
fn test_join_chain_left_then_left() {
    let mut db = TestDb::new("join-chain-left");
    seed_orders(&mut db);

    // `NULL = 50` is not true, so rows padded by the first LEFT JOIN stay padded.
    assert_eq!(
        db.query("SELECT u.name, o.total, d.title FROM users u LEFT JOIN orders o ON u.id = o.user_id LEFT JOIN depts d ON o.total = 50 ORDER BY u.id, o.id, d.code"),
        vec![
            vec![text("Alice"), Value::Int64(100), Value::Null],
            vec![text("Alice"), Value::Int64(50), text("Engineering")],
            vec![text("Alice"), Value::Int64(50), text("Operations")],
            vec![text("Bob"), Value::Null, Value::Null],
            vec![text("Carol"), Value::Int64(75), Value::Null],
            vec![text("Dan"), Value::Null, Value::Null],
        ]
    );
}

#[test]
fn test_join_aggregate_count_unmatched_is_zero() {
    let mut db = TestDb::new("join-aggregate");
    seed_orders(&mut db);

    assert_eq!(
        db.query("SELECT u.name, COUNT(o.id) AS n FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name"),
        vec![
            vec![text("Alice"), Value::Int64(2)],
            vec![text("Bob"), Value::Int64(0)],
            vec![text("Carol"), Value::Int64(1)],
            vec![text("Dan"), Value::Int64(0)],
        ]
    );
    // Bare `dept` is unique across the join, so it resolves without a qualifier.
    assert_eq!(
        db.query("SELECT dept, SUM(o.total) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY dept ORDER BY dept"),
        vec![
            vec![text("eng"), Value::Int64(150)],
            vec![text("ops"), Value::Int64(75)],
        ]
    );
}

#[test]
fn test_join_where_and_limit_over_join() {
    let mut db = TestDb::new("join-where-limit");
    seed_orders(&mut db);

    assert_eq!(
        db.query("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total >= 75 ORDER BY o.total DESC LIMIT 1"),
        vec![vec![text("Alice"), Value::Int64(100)]]
    );
}

#[test]
fn test_join_rejects_right_join() {
    let mut db = TestDb::new("join-right");
    seed_orders(&mut db);

    assert!(db.fails("SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id"));
}

#[test]
fn test_join_analysis_errors_fail_at_execution() {
    let mut db = TestDb::new("join-errors");
    seed_orders(&mut db);

    // Ambiguous unqualified column (`id` is in both tables).
    assert!(db.fails("SELECT id FROM users u JOIN orders o ON u.id = o.user_id"));
    // Same unaliased table twice.
    assert!(db.fails("SELECT * FROM users JOIN users ON users.id = users.id"));
    // Same alias twice.
    assert!(db.fails("SELECT * FROM users a JOIN orders a ON a.id = a.id"));
    // Unknown alias in ON.
    assert!(db.fails("SELECT * FROM users u JOIN orders o ON u.id = x.user_id"));
    // ON refers to a table that is joined later.
    assert!(db.fails(
        "SELECT * FROM users u JOIN orders o ON u.id = d.code JOIN depts d ON u.dept = d.code"
    ));
    // ON is not boolean.
    assert!(db.fails("SELECT * FROM users u JOIN orders o ON u.id"));
    // Table name is hidden once it has an alias.
    assert!(db.fails("SELECT users.name FROM users u JOIN orders o ON u.id = o.user_id"));
}

#[test]
fn test_join_select_star_columns_and_values() {
    let mut db = TestDb::new("join-star");
    seed_orders(&mut db);
    let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 10";

    assert_eq!(
        db.columns(sql),
        vec!["id", "name", "dept", "age", "id", "user_id", "total"]
    );
    assert_eq!(
        db.query(sql),
        vec![vec![
            Value::Int64(1),
            text("Alice"),
            text("eng"),
            Value::Int64(30),
            Value::Int64(10),
            Value::Int64(1),
            Value::Int64(100),
        ]]
    );
}

#[test]
fn test_join_select_star_self_join_is_not_ambiguous() {
    let mut db = TestDb::new("join-star-self");

    let rows = db.query("SELECT * FROM users a JOIN users b ON a.id = b.id ORDER BY a.id");
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|row| row.len() == 8));
    assert_eq!(
        rows[0],
        vec![
            Value::Int64(1),
            text("Alice"),
            text("eng"),
            Value::Int64(30),
            Value::Int64(1),
            text("Alice"),
            text("eng"),
            Value::Int64(30),
        ]
    );
}

#[test]
fn test_join_order_by_duplicated_output_name_is_ambiguous() {
    let mut db = TestDb::new("join-order-ambiguous");
    seed_orders(&mut db);
    let select = "SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id";

    assert!(db.fails(&format!("{select} ORDER BY id")));
    assert_eq!(
        db.query(&format!("{select} ORDER BY 2")).len(),
        3,
        "ORDER BY 2 should work"
    );
    assert_eq!(
        db.query(&format!("{select} ORDER BY o.id")),
        vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(1), Value::Int64(11)],
            vec![Value::Int64(3), Value::Int64(12)],
        ]
    );
}

#[test]
fn test_join_unqualified_unique_name_still_works() {
    let mut db = TestDb::new("join-unqualified-unique");
    seed_orders(&mut db);

    assert_eq!(
        db.query(
            "SELECT name, total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY total"
        ),
        vec![
            vec![text("Alice"), Value::Int64(50)],
            vec![text("Carol"), Value::Int64(75)],
            vec![text("Alice"), Value::Int64(100)],
        ]
    );
    assert!(db.fails("SELECT id FROM users u JOIN orders o ON u.id = o.user_id"));
}

#[test]
fn test_join_qualified_star_expands_one_relation() {
    let mut db = TestDb::new("join-qualified-star");
    seed_orders(&mut db);

    let sql = "SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id";
    assert_eq!(db.columns(sql), vec!["id", "name", "dept", "age"]);
    assert_eq!(
        db.query(sql),
        vec![
            vec![
                Value::Int64(1),
                text("Alice"),
                text("eng"),
                Value::Int64(30)
            ],
            vec![
                Value::Int64(1),
                text("Alice"),
                text("eng"),
                Value::Int64(30)
            ],
            vec![
                Value::Int64(3),
                text("Carol"),
                text("ops"),
                Value::Int64(41)
            ],
        ]
    );
}

#[test]
fn test_join_qualified_star_mixed_with_column() {
    let mut db = TestDb::new("join-qualified-star-mixed");
    seed_orders(&mut db);

    let sql = "SELECT o.*, u.name FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id";
    assert_eq!(db.columns(sql), vec!["id", "user_id", "total", "name"]);
    assert_eq!(
        db.query(sql)[0],
        vec![
            Value::Int64(10),
            Value::Int64(1),
            Value::Int64(100),
            text("Alice"),
        ]
    );
}

#[test]
fn test_join_qualified_star_unknown_relation_fails() {
    let mut db = TestDb::new("join-qualified-star-unknown");

    assert!(db.fails("SELECT x.* FROM users u"));
}

#[test]
fn test_join_multiplication_still_works_in_select_list() {
    let mut db = TestDb::new("join-multiply-regression");

    assert_eq!(
        db.query("SELECT 2 * 3 FROM users ORDER BY id"),
        vec![vec![Value::Int64(6)]; 4]
    );
}
