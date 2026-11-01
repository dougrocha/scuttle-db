//! SELECT: ordering, aliases, grouping and aggregates, sorts and streaming.

use super::*;
#[test]
fn test_select_order_by_limit() {
    let mut db = TestDb::new("order-limit");

    assert_eq!(
        db.query("SELECT name FROM users ORDER BY age DESC LIMIT 2"),
        vec![vec![text("Carol")], vec![text("Dan")]]
    );
    assert_eq!(
        db.query("SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1"),
        vec![vec![Value::Int64(2)], vec![Value::Int64(3)]]
    );
    assert_eq!(
        db.query("SELECT id FROM users LIMIT 0"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        db.query("SELECT id FROM users OFFSET 3"),
        vec![vec![Value::Int64(4)]]
    );
}

#[test]
fn test_select_order_by_multiple_keys_and_hidden_column() {
    let mut db = TestDb::new("order-keys");

    // Sort by a column that is not in the output
    assert_eq!(
        db.query("SELECT name FROM users ORDER BY dept DESC, age"),
        vec![
            vec![text("Dan")],
            vec![text("Carol")],
            vec![text("Bob")],
            vec![text("Alice")],
        ]
    );
}

#[test]
fn test_select_order_by_alias_and_position() {
    let mut db = TestDb::new("order-alias");

    let youngest_first = vec![
        vec![text("Bob")],
        vec![text("Alice")],
        vec![text("Dan")],
        vec![text("Carol")],
    ];
    assert_eq!(
        db.query("SELECT name FROM users ORDER BY age"),
        youngest_first
    );

    let by_alias = db.query("SELECT name, age + 1 AS next_age FROM users ORDER BY next_age");
    let by_position = db.query("SELECT name, age + 1 AS next_age FROM users ORDER BY 2");
    assert_eq!(by_alias, by_position);
    assert_eq!(by_alias[0], vec![text("Bob"), Value::Int64(26)]);

    assert!(db.fails("SELECT name FROM users ORDER BY 2"));
}

#[test]
fn test_select_aliases_name_output_columns() {
    let mut db = TestDb::new("aliases");

    assert_eq!(
        db.columns(
            "SELECT id AS user_id, name, age + 1, COUNT(*) AS n FROM users GROUP BY id, name, age"
        ),
        vec!["user_id", "name", "?column?", "n"]
    );
    assert_eq!(
        db.columns("SELECT * FROM users"),
        vec!["id", "name", "dept", "age"]
    );
    assert_eq!(db.columns("SELECT COUNT(*) FROM users"), vec!["count"]);
}

#[test]
fn test_select_from_alias_is_accepted() {
    let mut db = TestDb::new("from-alias");

    assert_eq!(
        db.query("SELECT name FROM users u WHERE age > 30 ORDER BY id"),
        vec![vec![text("Carol")], vec![text("Dan")]]
    );
}

#[test]
fn test_select_alias_and_qualified_columns() {
    let mut db = TestDb::new("alias-qualified");

    assert_eq!(
        db.query("SELECT u.name FROM users u WHERE u.age > 30 ORDER BY u.id"),
        vec![vec![text("Carol")], vec![text("Dan")]]
    );
    assert_eq!(
        db.query("SELECT u.name FROM users AS u WHERE u.age > 30 ORDER BY u.id"),
        vec![vec![text("Carol")], vec![text("Dan")]]
    );
}

#[test]
fn test_select_qualified_with_table_name_when_not_aliased() {
    let mut db = TestDb::new("qualified-table-name");

    assert_eq!(
        db.query("SELECT users.name FROM users WHERE users.id = 1"),
        vec![vec![text("Alice")]]
    );
}

#[test]
fn test_select_table_name_unusable_after_alias() {
    let mut db = TestDb::new("table-name-after-alias");

    assert!(db.fails("SELECT users.name FROM users u"));
    assert!(db.fails("SELECT x.name FROM users u"));
    assert!(db.fails("SELECT u.nope FROM users u"));
}

#[test]
fn test_join_duplicate_alias_with_zero_column_table_fails() {
    let mut db = TestDb::new("zero-column-duplicate-alias");
    db.execute("CREATE TABLE z ()");

    let err = db
        .db
        .execute_query("SELECT * FROM users u JOIN z u ON 1 = 1")
        .unwrap_err();
    assert!(
        err.to_string().contains("specified more than once"),
        "got: {err}"
    );
}

#[test]
fn test_group_by_bare_and_qualified_are_the_same_key() {
    let mut db = TestDb::new("group-by-qualified");

    assert_eq!(
        db.query("SELECT u.dept, COUNT(*) FROM users u GROUP BY dept ORDER BY u.dept"),
        vec![
            vec![text("eng"), Value::Int64(2)],
            vec![text("ops"), Value::Int64(2)]
        ]
    );
    assert_eq!(
        db.query("SELECT dept, SUM(u.age) FROM users u GROUP BY u.dept ORDER BY dept"),
        vec![
            vec![text("eng"), Value::Int64(55)],
            vec![text("ops"), Value::Int64(76)]
        ]
    );
    assert_eq!(
        db.query("SELECT age + 1 FROM users u GROUP BY u.age + 1 ORDER BY 1"),
        vec![
            vec![Value::Int64(26)],
            vec![Value::Int64(31)],
            vec![Value::Int64(36)],
            vec![Value::Int64(42)]
        ]
    );
}

#[test]
fn test_order_by_duplicate_output_names() {
    let mut db = TestDb::new("order-by-duplicate-names");

    assert!(db.fails("SELECT age AS x, name AS x FROM users ORDER BY x"));
    assert!(db.fails("SELECT age AS id, id FROM users ORDER BY id"));
    assert_eq!(db.query("SELECT age AS x FROM users ORDER BY x").len(), 4);
    assert_eq!(db.query("SELECT *, id FROM users ORDER BY id").len(), 4);
    assert_eq!(
        db.query("SELECT id, id FROM users ORDER BY id DESC LIMIT 1"),
        vec![vec![Value::Int64(4), Value::Int64(4)]]
    );
}

#[test]
fn test_quoted_identifier_with_dot_is_split() {
    let mut db = TestDb::new("quoted-dot");

    // Documented limitation: "a.b" is split into relation `a` and column `b`.
    assert!(db.fails(r#"SELECT "a.b" FROM users"#));
}

#[test]
fn test_update_delete_accept_table_qualified_columns() {
    let mut db = TestDb::new("update-delete-qualified");

    db.execute("UPDATE users SET age = users.age + 1 WHERE users.id = 1");
    assert_eq!(
        db.query("SELECT age FROM users WHERE id = 1"),
        vec![vec![Value::Int64(31)]]
    );

    db.execute("DELETE FROM users WHERE users.id = 4");
    assert_eq!(
        db.query("SELECT COUNT(*) FROM users"),
        vec![vec![Value::Int64(3)]]
    );
}

#[test]
fn test_count_star() {
    let mut db = TestDb::new("count");

    assert_eq!(
        db.query("SELECT COUNT(*) FROM users"),
        vec![vec![Value::Int64(4)]]
    );
    assert_eq!(
        db.query("SELECT COUNT(*) FROM users WHERE dept = 'eng'"),
        vec![vec![Value::Int64(2)]]
    );

    // No GROUP BY: still one row over an empty input. With GROUP BY: no groups, no rows.
    assert_eq!(
        db.query("SELECT COUNT(*), SUM(age) FROM users WHERE age > 100"),
        vec![vec![Value::Int64(0), Value::Null]]
    );
    assert!(
        db.query("SELECT dept, COUNT(*) FROM users WHERE age > 100 GROUP BY dept")
            .is_empty()
    );
}

#[test]
fn test_group_by_aggregates() {
    let mut db = TestDb::new("group-by");

    assert_eq!(
        db.query(
            "SELECT dept, COUNT(*), SUM(age), AVG(age), MIN(name), MAX(age) \
             FROM users GROUP BY dept ORDER BY dept"
        ),
        vec![
            vec![
                text("eng"),
                Value::Int64(2),
                Value::Int64(55),
                Value::Float64(27.5),
                text("Alice"),
                Value::Int64(30),
            ],
            vec![
                text("ops"),
                Value::Int64(2),
                Value::Int64(76),
                Value::Float64(38.0),
                text("Carol"),
                Value::Int64(41),
            ],
        ]
    );
}

#[test]
fn test_group_by_puts_every_nan_in_one_group() {
    let mut db = TestDb::new("group-nan");
    db.execute("CREATE TABLE item (x FLOAT)");
    db.execute("INSERT INTO item VALUES (10000000000.0), (10000000000.0), (1.0)");
    // 1e10 to the 32nd overflows to infinity, and infinity minus itself is NaN
    let power = vec!["x"; 32].join(" * ");
    db.execute(&format!("UPDATE item SET x = {power}"));

    let rows = db.query("SELECT x - x AS n, COUNT(*) FROM item GROUP BY x - x");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().any(|row| row[1] == Value::Int64(2)), "{rows:?}");
}

#[test]
fn test_group_by_order_by_aggregate() {
    let mut db = TestDb::new("group-order");

    // By an aggregate that is not selected, and by an aggregate's alias
    let expected = vec![vec![text("ops")], vec![text("eng")]];
    assert_eq!(
        db.query("SELECT dept FROM users GROUP BY dept ORDER BY SUM(age) DESC"),
        expected
    );
    assert_eq!(
        db.query(
            "SELECT dept, MAX(age) AS oldest FROM users GROUP BY dept ORDER BY oldest DESC LIMIT 1"
        ),
        vec![vec![text("ops"), Value::Int64(41)]]
    );

    // Expressions over aggregates and group keys
    assert_eq!(
        db.query("SELECT dept, SUM(age) / COUNT(*) FROM users GROUP BY dept ORDER BY 1"),
        vec![
            vec![text("eng"), Value::Int64(27)],
            vec![text("ops"), Value::Int64(38)],
        ]
    );
}

#[test]
fn test_aggregate_errors() {
    let mut db = TestDb::new("aggregate-errors");

    assert!(db.fails("SELECT name, COUNT(*) FROM users"));
    assert!(db.fails("SELECT name FROM users GROUP BY dept"));
    assert!(db.fails("SELECT * FROM users WHERE COUNT(*) > 1"));
    assert!(db.fails("SELECT SUM(COUNT(*)) FROM users"));
    assert!(db.fails("SELECT SUM(name) FROM users"));
    assert!(db.fails("SELECT SUM(*) FROM users"));
    assert!(db.fails("SELECT UPPER(name) FROM users"));
}

#[test]
fn test_top_n_returns_the_same_rows_as_a_full_sort() {
    let mut db = TestDb::new("top-n");
    db.execute("CREATE TABLE item (id INT, grp INT, tag TEXT)");
    // Few distinct values, so most rows tie and only input order tells them apart.
    // More rows than TopNExec buffers (1,024), so it has to cut the buffer down.
    let values: Vec<String> = (0..3000)
        .map(|id| {
            let tag = if id % 5 == 0 {
                "NULL".to_string()
            } else {
                format!("'t{}'", id % 3)
            };
            format!("({id}, {}, {tag})", (id * 7) % 4)
        })
        .collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

    for order in ["grp", "grp DESC", "tag", "tag DESC, grp", "grp, tag DESC"] {
        let sorted = db.query(&format!("SELECT id FROM item ORDER BY {order}"));
        for (limit, offset) in [
            (0, 0),
            (1, 0),
            (10, 0),
            (10, 25),
            (600, 0),
            (2999, 1),
            (5000, 0),
            (5, 2998),
        ] {
            let sql = format!("SELECT id FROM item ORDER BY {order} LIMIT {limit} OFFSET {offset}");
            let expected: Vec<_> = sorted.iter().skip(offset).take(limit).cloned().collect();
            assert_eq!(db.query(&sql), expected, "{sql}");
        }
    }
}

#[test]
fn test_a_sort_past_its_memory_budget_returns_the_same_rows() {
    let mut db = TestDb::new("external-sort");
    db.execute("CREATE TABLE item (id INT, grp INT, tag TEXT)");
    // Few distinct values, so most rows tie and only input order tells them apart
    let values: Vec<String> = (0..3000)
        .map(|id| {
            let tag = if id % 5 == 0 {
                "NULL".to_string()
            } else {
                format!("'t{}'", id % 3)
            };
            format!("({id}, {}, {tag})", (id * 7) % 4)
        })
        .collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

    let orders = ["grp", "grp DESC", "tag", "tag DESC, grp", "id DESC"];
    let in_memory: Vec<_> = orders
        .iter()
        .map(|order| db.query(&format!("SELECT * FROM item ORDER BY {order}")))
        .collect();

    // Small enough to write dozens of runs
    db.db.set_sort_mem(16 * 1024);
    for (order, expected) in orders.iter().zip(&in_memory) {
        let sql = format!("SELECT * FROM item ORDER BY {order}");
        assert_eq!(&db.query(&sql), expected, "{sql}");
    }

    let plan = db.query("EXPLAIN ANALYZE SELECT id FROM item ORDER BY grp");
    let sort = plan[1][0].to_string();
    assert!(sort.contains("sort: external merge, "), "{sort}");
    // The runs are deleted once merged
    assert_eq!(std::fs::read_dir(db.dir.join("tmp")).unwrap().count(), 0);

    // More runs than are merged at once, so they are merged in more than one pass
    db.db.set_sort_mem(4 * 1024);
    for (order, expected) in orders.iter().zip(&in_memory) {
        let sql = format!("SELECT * FROM item ORDER BY {order}");
        assert_eq!(&db.query(&sql), expected, "{sql}");
    }
    let plan = db.query("EXPLAIN ANALYZE SELECT id FROM item ORDER BY grp");
    let sort = plan[1][0].to_string();
    assert!(sort.ends_with(" 2 passes"), "{sort}");

    db.db.set_sort_mem(DEFAULT_SORT_MEM);
    let plan = db.query("EXPLAIN ANALYZE SELECT id FROM item ORDER BY grp");
    assert!(plan[1][0].to_string().ends_with("sort: in memory"));
}

#[test]
fn test_a_tiny_page_cache_gives_the_same_answers() {
    let dir = std::env::temp_dir().join(format!("scuttle-tiny-cache-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let open = || {
        let mut db = Database::with_cache_pages(&dir, 3);
        db.initialize().unwrap();
        db
    };
    let query = |db: &mut Database, sql: &str| -> Vec<Vec<Value>> {
        let response = db.execute_query(sql).unwrap();
        response.rows.into_iter().map(|row| row.values).collect()
    };

    let mut db = open();
    db.execute_query("CREATE TABLE item (id INT PRIMARY KEY, note TEXT)")
        .unwrap();
    // About 40 pages of rows, so a 3-page cache evicts on nearly every page
    let note = "x".repeat(200);
    for chunk in (0..1500).collect::<Vec<_>>().chunks(100) {
        let values: Vec<String> = chunk.iter().map(|id| format!("({id}, '{note}')")).collect();
        db.execute_query(&format!("INSERT INTO item VALUES {}", values.join(", ")))
            .unwrap();
    }
    db.execute_query("UPDATE item SET note = 'changed' WHERE id >= 1000")
        .unwrap();
    db.execute_query("DELETE FROM item WHERE id < 100").unwrap();

    for reopened in [false, true] {
        if reopened {
            db = open();
        }
        assert_eq!(
            query(&mut db, "SELECT count(*) FROM item"),
            [[Value::Int64(1400)]]
        );
        assert_eq!(
            query(&mut db, "SELECT count(*) FROM item WHERE note = 'changed'"),
            [[Value::Int64(500)]]
        );
        // Through the index, which fetches each row's page by itself
        assert_eq!(
            query(&mut db, "SELECT id, note FROM item WHERE id = 1234"),
            [[Value::Int64(1234), Value::Text("changed".into())]]
        );
        assert!(query(&mut db, "SELECT id FROM item WHERE id = 50").is_empty());

        // The table doesn't fit in the cache, so reading all of it in index order
        // could read a page per row. It sorts instead, unless a LIMIT stops it early.
        let plan = db.explain("SELECT id FROM item ORDER BY id").unwrap();
        assert!(
            plan.contains("Sort") && !plan.contains("IndexScan"),
            "{plan}"
        );
        let plan = db
            .explain("SELECT id FROM item ORDER BY id LIMIT 5")
            .unwrap();
        assert!(
            plan.contains("IndexScan") && !plan.contains("Sort"),
            "{plan}"
        );
    }
    assert!(db.pool.stats().reads > 40, "{:?}", db.pool.stats());
}

#[test]
fn test_streaming_passes_rows_in_batches_and_stops_on_error() {
    let mut db = TestDb::new("streaming");
    db.execute("CREATE TABLE item (id INT)");
    let values: Vec<String> = (0..3000).map(|id| format!("({id})")).collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

    let mut batches = Vec::new();
    let response = db
        .db
        .execute_query_streaming("SELECT id FROM item", |columns, rows| {
            assert_eq!(columns, ["id"]);
            batches.push(rows);
            Ok(())
        })
        .unwrap();

    assert_eq!(response.columns, ["id"]);
    assert!(response.rows.is_empty());
    assert!(batches.len() > 1 && batches.iter().all(|rows| !rows.is_empty()));
    let streamed: Vec<_> = batches
        .into_iter()
        .flatten()
        .map(|row| row.values)
        .collect();
    assert_eq!(streamed, db.query("SELECT id FROM item"));

    // An error from the callback ends the query after the first batch
    let mut calls = 0;
    let result = db
        .db
        .execute_query_streaming("SELECT id FROM item", |_, _| {
            calls += 1;
            Err(miette::miette!("output closed"))
        });
    assert!(result.unwrap_err().to_string().contains("output closed"));
    assert_eq!(calls, 1);
}

#[test]
fn test_response_columns_widen_to_fit_their_values() {
    let response = QueryResponse {
        columns: vec!["id".into(), "name".into()],
        rows: vec![
            Row::new(vec![
                Value::Int64(1),
                Value::Text("a much longer name".into()),
            ]),
            Row::new(vec![Value::Int64(123456), Value::Null]),
        ],
        rows_affected: None,
    };

    assert_eq!(
        response.to_string(),
        [
            " id     | name",
            "--------+--------------------",
            " 1      | a much longer name",
            " 123456 | NULL",
            "(2 rows)",
        ]
        .join("\n")
    );
}
