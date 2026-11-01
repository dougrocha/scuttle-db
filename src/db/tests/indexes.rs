//! Reading tables through indexes: which plans use them, and that they find what a scan finds.

use super::*;
#[test]
fn test_index_scan_returns_the_same_rows_as_a_full_scan() {
    let mut db = TestDb::new("index-scan");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, code TEXT UNIQUE, qty INT)");
    db.execute("CREATE TABLE plain (id INT, code TEXT, qty INT)");
    for table in ["item", "plain"] {
        let values: Vec<String> = (1..=300)
            .map(|id| format!("({id}, 'c{id:03}', {})", id % 7))
            .collect();
        db.execute(&format!("INSERT INTO {table} VALUES {}", values.join(", ")));
        db.execute(&format!("DELETE FROM {table} WHERE id = 150"));
        db.execute(&format!("UPDATE {table} SET id = id + 1000 WHERE id = 151"));
    }

    // Each WHERE runs through the index on `item` and as a full scan on `plain`
    for filter in [
        "id = 42",
        "id = 150",
        "id = 1151",
        "42 = id",
        "id > 290",
        "id >= 140 AND id < 160",
        "100 < id AND id <= 103 AND qty > 2",
        "id > 50 AND id < 10",
        "id = 7 AND id = 8",
        "code = 'c007'",
        "code >= 'c298'",
        "id = 5 OR id = 6",
    ] {
        let sql = |table| format!("SELECT id, code, qty FROM {table} WHERE {filter} ORDER BY id");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{filter}");
        assert!(
            db.db
                .explain(&sql("item"))
                .unwrap()
                .contains("IndexScan: item"),
            "{filter}"
        );
    }

    // Conditions an index can't narrow keep the full scan
    for filter in ["qty = 3", "id != 5", "id = 5 OR qty = 6", "id = 5.5"] {
        let plan = db
            .db
            .explain(&format!("SELECT id FROM item WHERE {filter}"))
            .unwrap();
        assert!(
            plan.contains("Scan: item") && !plan.contains("IndexScan"),
            "{filter}"
        );
    }
}

#[test]
fn test_order_by_with_limit_reads_an_index_in_key_order() {
    let mut db = TestDb::new("index-order");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, grp INT NOT NULL, tag TEXT)");
    db.execute("CREATE TABLE plain (id INT, grp INT, tag TEXT)");
    // Ids out of order, and few groups so most rows tie on grp
    let values: Vec<String> = (0..3000)
        .map(|n| format!("({}, {}, 't{}')", (n * 7919) % 3000, n % 4, n % 5))
        .collect();
    for table in ["item", "plain"] {
        db.execute(&format!("INSERT INTO {table} VALUES {}", values.join(", ")));
    }
    db.execute("CREATE INDEX item_grp ON item (grp)");
    // `tag` is nullable. Its NULLs are in its index, after every value.
    db.execute("CREATE INDEX item_tag ON item (tag)");
    for table in ["item", "plain"] {
        db.execute(&format!(
            "INSERT INTO {table} VALUES (5000, 0, NULL), (5001, 1, NULL)"
        ));
    }
    // An UPDATE moves rows to the end of the table, which reorders their ties
    for table in ["item", "plain"] {
        db.execute(&format!("UPDATE {table} SET tag = 'moved' WHERE id < 100"));
    }

    for query in [
        "ORDER BY id LIMIT 5",
        "ORDER BY id LIMIT 5 OFFSET 2990",
        "WHERE id > 1500 ORDER BY id LIMIT 4",
        "WHERE id >= 10 AND id < 20 ORDER BY id LIMIT 100",
        "WHERE tag = 't3' ORDER BY id LIMIT 6",
        "ORDER BY grp LIMIT 2000",
        "WHERE grp = 2 ORDER BY grp LIMIT 50",
        "ORDER BY id DESC LIMIT 5",
        "ORDER BY id DESC LIMIT 5 OFFSET 2995",
        "WHERE id < 1500 ORDER BY id DESC LIMIT 4",
        "WHERE id <= 1500 ORDER BY id DESC LIMIT 4",
        "WHERE id > 2990 ORDER BY id DESC LIMIT 100",
        "WHERE id >= 10 AND id <= 20 ORDER BY id DESC LIMIT 100",
        "WHERE id = 42 ORDER BY id DESC LIMIT 3",
        "WHERE tag = 't3' ORDER BY id DESC LIMIT 6",
        "ORDER BY tag LIMIT 5000",
        "WHERE tag > 't3' ORDER BY tag LIMIT 5000",
        "WHERE tag < 't1' ORDER BY tag LIMIT 5000",
        // No LIMIT: the table fits in the cache, so reading it in index order wins
        "ORDER BY id",
        "ORDER BY id DESC",
        "WHERE grp = 1 ORDER BY id",
    ] {
        let sql = |table| format!("SELECT id, grp, tag FROM {table} {query}");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{query}");
        let plan = db.db.explain(&sql("item")).unwrap();
        assert!(
            plan.contains("IndexScan") && !plan.contains("Sort") && !plan.contains("TopN"),
            "{query}\n{plan}"
        );
    }

    // A backward scan stops at an excluded lower bound itself, not only in the Filter
    let plan = db
        .db
        .explain("EXPLAIN ANALYZE SELECT id FROM item WHERE id > 2990 ORDER BY id DESC LIMIT 100")
        .unwrap();
    assert!(
        plan.contains("IndexScan Backward: item (id > 2990) (rows=11 "),
        "{plan}"
    );

    // DESC returns tied rows in reverse storage order, which SQL allows. The same rows
    // come back, each group in place.
    use crate::sql::evaluator::compare_values;
    let sql = |table| {
        format!("SELECT grp, id, tag FROM {table} WHERE grp >= 2 ORDER BY grp DESC LIMIT 5000")
    };
    let mut indexed = db.query(&sql("item"));
    assert!(
        db.db
            .explain(&sql("item"))
            .unwrap()
            .contains("IndexScan Backward: item")
    );
    assert!(indexed.is_sorted_by(|a, b| compare_values(&a[0], &b[0]).is_ge()));
    let mut plain = db.query(&sql("plain"));
    indexed.sort_by(|a, b| compare_values(&a[1], &b[1]));
    plain.sort_by(|a, b| compare_values(&a[1], &b[1]));
    assert_eq!(indexed, plain);

    // The scan starts where the WHERE clause allows, not at the first key
    let plan = db
        .db
        .explain("SELECT id FROM item WHERE id > 1500 ORDER BY id LIMIT 4");
    assert!(plan.unwrap().contains("IndexScan: item (id > 1500)"));

    // DESC puts NULLs first, as the Sort does, then the largest tag
    let plan = db
        .db
        .explain("SELECT id FROM item ORDER BY tag DESC LIMIT 3");
    assert!(plan.unwrap().contains("IndexScan Backward: item (by tag)"));
    let rows = db.query("SELECT id, tag FROM item ORDER BY tag DESC LIMIT 3");
    assert_eq!(rows[0][1], Value::Null);
    assert_eq!(rows[1][1], Value::Null);
    assert_eq!(rows[2][1], Value::Text("t4".into()));

    // A WHERE range stops before the NULLs instead of reading them and filtering them out
    let plan = db
        .db
        .explain("EXPLAIN ANALYZE SELECT id FROM item WHERE tag > 't3' ORDER BY tag LIMIT 5000")
        .unwrap();
    assert!(
        plan.contains("IndexScan: item (tag > 't3') (rows=580 "),
        "{plan}"
    );
    let plan = db
        .db
        .explain(
            "EXPLAIN ANALYZE SELECT id FROM item WHERE tag > 't3' ORDER BY tag DESC LIMIT 5000",
        )
        .unwrap();
    assert!(
        plan.contains("IndexScan Backward: item (tag > 't3') (rows=580 "),
        "{plan}"
    );

    // These still sort: an expression, and two keys
    for query in ["ORDER BY id + 0 LIMIT 5", "ORDER BY grp, id LIMIT 5"] {
        let sql = |table| format!("SELECT id, grp, tag FROM {table} {query}");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{query}");
        let plan = db.db.explain(&sql("item")).unwrap();
        assert!(
            plan.contains("Sort") || plan.contains("TopN"),
            "{query}\n{plan}"
        );
    }
}

#[test]
fn test_in_and_not_in_follow_sql_null_rules() {
    let mut db = TestDb::new("in-nulls");
    db.execute("CREATE TABLE t (id INT, ts TIMESTAMP)");
    db.execute("INSERT INTO t VALUES (1, '2024-01-01 00:00:00'), (2, NULL), (NULL, NULL)");
    let ids = |db: &mut TestDb, filter: &str| {
        db.query(&format!("SELECT id FROM t WHERE {filter}"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ids(&mut db, "id IN (2, 1, 9)"),
        [Value::Int64(1), Value::Int64(2)]
    );
    // A NULL id is neither in nor not in the list
    assert_eq!(ids(&mut db, "id NOT IN (1)"), [Value::Int64(2)]);
    assert_eq!(ids(&mut db, "id + 1 IN (3)"), [Value::Int64(2)]);
    // Text in the list becomes a timestamp, as with `=`
    assert_eq!(
        ids(&mut db, "ts IN ('2024-01-01 00:00:00')"),
        [Value::Int64(1)]
    );
    // A text literal on the left becomes a timestamp too, wherever the timestamp is.
    // Row 2's ts is NULL, so only the text item can match.
    for list in ["'2024-01-01 00:00:00', ts", "ts, '2024-01-01 00:00:00'"] {
        assert_eq!(
            ids(
                &mut db,
                &format!("'2024-01-01 00:00:00' IN ({list}) AND id = 2")
            ),
            [Value::Int64(2)],
            "{list}"
        );
    }
    assert_eq!(
        db.query("SELECT id IN (1, id) FROM t"),
        [[Value::Bool(true)], [Value::Bool(true)], [Value::Null]]
    );
    assert!(db.fails("SELECT id FROM t WHERE id IN ('x')"));
    assert!(db.fails("SELECT id FROM t WHERE id IN (NULL)"));
}

#[test]
fn test_in_reads_one_index_range_per_value() {
    let mut db = TestDb::new("index-in");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, tag TEXT)");
    db.execute("CREATE TABLE plain (id INT, tag TEXT)");
    let values: Vec<String> = (0..3000)
        .map(|n| format!("({}, 't{}')", (n * 7919) % 3000, n % 50))
        .collect();
    for table in ["item", "plain"] {
        db.execute(&format!("INSERT INTO {table} VALUES {}", values.join(", ")));
        db.execute(&format!("INSERT INTO {table} VALUES (5000, NULL)"));
    }
    db.execute("CREATE INDEX item_tag ON item (tag)");

    for (filter, range) in [
        ("id IN (2999, 5, 77, 5, 0)", "id IN (0, 5, 77, 2999)"),
        ("id IN (5, 77, 1200) AND id > 5", "id IN (77, 1200)"),
        ("id IN (5, 77) AND id > 100", "no keys"),
        ("id = 5 AND id IN (5, 6)", "id = 5"),
        ("tag IN ('t7', 't3')", "tag IN ('t3', 't7')"),
        ("tag IN ('t7', 'x')", "tag IN ('t7', 'x')"),
        // The id range finds 10 rows and the tags 60, so the id index wins
        ("tag IN ('t7', 'x') AND id < 10", "id < 10"),
    ] {
        let sql = |table| format!("SELECT id, tag FROM {table} WHERE {filter} ORDER BY id");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{filter}");
        let plan = db.db.explain(&sql("item")).unwrap();
        assert!(
            plan.contains(&format!("IndexScan: item ({range})")),
            "{plan}"
        );
    }

    // Each value finds its own rows, nothing in between
    let plan = db
        .db
        .explain("EXPLAIN ANALYZE SELECT id FROM item WHERE id IN (5, 1500, 2999)")
        .unwrap();
    assert!(
        plan.contains("IndexScan: item (id IN (5, 1500, 2999)) (rows=3 "),
        "{plan}"
    );

    // The ranges come out in key order, so ORDER BY reads them instead of sorting,
    // backward for DESC
    for order in ["", " DESC"] {
        let sql = |table| {
            format!(
                "SELECT id FROM {table} WHERE id IN (9, 3, 2000, 41) ORDER BY id{order} LIMIT 3"
            )
        };
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{order}");
        let plan = db.db.explain(&sql("item")).unwrap();
        assert!(!plan.contains("Sort") && !plan.contains("TopN"), "{plan}");
    }

    // An index can't answer these, so the table is read
    for filter in [
        "id NOT IN (1, 2)",
        "id IN (1, 2.5)",
        "id IN (1, id)",
        "id + 0 IN (1)",
    ] {
        let sql = |table| format!("SELECT id FROM {table} WHERE {filter} ORDER BY id");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{filter}");
        let plan = db.db.explain(&sql("item")).unwrap();
        assert!(!plan.contains("IndexScan: item (id"), "{filter}\n{plan}");
    }
}

#[test]
fn test_or_of_equalities_on_one_column_becomes_in() {
    let mut db = TestDb::new("or-to-in");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, tag TEXT)");
    db.execute("CREATE TABLE plain (id INT, tag TEXT)");
    let values: Vec<String> = (0..3000)
        .map(|n| format!("({}, 't{}')", (n * 7919) % 3000, n % 50))
        .collect();
    for table in ["item", "plain"] {
        db.execute(&format!("INSERT INTO {table} VALUES {}", values.join(", ")));
        db.execute(&format!("INSERT INTO {table} VALUES (5000, NULL)"));
    }

    for (filter, rewritten) in [
        ("id = 5 OR id = 77 OR 1200 = id", "id IN (5, 77, 1200)"),
        ("id = 5 OR id IN (6, 7) OR id = 5", "id IN (5, 6, 7, 5)"),
        (
            "(id = 1 OR id = 2) AND tag = 't1'",
            "id IN (1, 2) AND (tag = 't1')",
        ),
        (
            "id = 5 OR tag = 't3' OR id = 7",
            "id IN (5, 7) OR (tag = 't3')",
        ),
        (
            "tag = 't1' OR tag = 't2' OR tag = 't1'",
            "tag IN ('t1', 't2', 't1')",
        ),
        // Nothing to merge
        ("id = 1 OR id > 2990", "(id = 1) OR (id > 2990)"),
        ("id = 1 OR tag = 't1'", "(id = 1) OR (tag = 't1')"),
        ("id NOT IN (1) OR id = 2", "id NOT IN (1) OR (id = 2)"),
    ] {
        let sql = |table| format!("SELECT id, tag FROM {table} WHERE {filter} ORDER BY id");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{filter}");
        let plan = db.db.explain(&sql("item")).unwrap();
        assert!(plan.contains(&format!("Filter: {rewritten}\n")), "{plan}");
    }

    // The id equalities read the index once they are an IN list
    let plan = db
        .db
        .explain("EXPLAIN ANALYZE SELECT id FROM item WHERE id = 5 OR id = 1500 OR id = 2999")
        .unwrap();
    assert!(
        plan.contains("IndexScan: item (id IN (5, 1500, 2999)) (rows=3 "),
        "{plan}"
    );

    // Inside IS, an OR of equalities stays: for a NULL id it is false, and IN would
    // be NULL
    let sql = |table| {
        format!("SELECT id FROM {table} WHERE (tag = 't1' OR tag = 't2') IS FALSE ORDER BY id")
    };
    assert_eq!(db.query(&sql("item")), db.query(&sql("plain")));
    assert!(db.query(&sql("item")).contains(&vec![Value::Int64(5000)]));
    assert!(!db.db.explain(&sql("item")).unwrap().contains(" IN "));
}

#[test]
fn test_or_across_two_indexes_unions_their_rows() {
    let mut db = TestDb::new("bitmap-or");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, tag TEXT, qty INT)");
    db.execute("CREATE TABLE plain (id INT, tag TEXT, qty INT)");
    let values: Vec<String> = (0..3000)
        .map(|n| format!("({}, 't{}', {})", (n * 7919) % 3000, n % 100, n % 3))
        .collect();
    for table in ["item", "plain"] {
        db.execute(&format!("INSERT INTO {table} VALUES {}", values.join(", ")));
        // Moved rows go to the end of the table, out of id order
        db.execute(&format!("UPDATE {table} SET qty = 9 WHERE id < 50"));
    }
    db.execute("CREATE INDEX item_tag ON item (tag)");
    let tag_of_5 = db.query("SELECT tag FROM plain WHERE id = 5")[0][0].to_string();

    for (filter, ranges) in [
        ("id = 5 OR tag = 't3'", "id = 5 OR tag = 't3'".to_string()),
        // Row 5 is found by both sides and comes back once
        (
            &format!("id IN (5, 6) OR tag = '{tag_of_5}'") as &str,
            format!("id IN (5, 6) OR tag = '{tag_of_5}'"),
        ),
        (
            "(id < 10 AND qty = 9) OR tag = 't7' OR id > 2995",
            "id < 10 OR tag = 't7' OR id > 2995".to_string(),
        ),
        (
            "qty = 1 AND (tag = 't1' OR id = 2000)",
            "tag = 't1' OR id = 2000".to_string(),
        ),
    ] {
        let sql = |table| format!("SELECT id, tag, qty FROM {table} WHERE {filter} ORDER BY id");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{filter}");
        let plan = db.db.explain(&sql("item")).unwrap();
        assert!(
            plan.contains(&format!("BitmapOr Scan: item ({ranges})")),
            "{plan}"
        );
        // Table order isn't key order, so the Sort stays
        assert!(plan.contains("Sort"), "{plan}");
    }

    let plan = db
        .db
        .explain(&format!(
            "EXPLAIN ANALYZE SELECT id FROM item WHERE id IN (5, 6) OR tag = '{tag_of_5}'"
        ))
        .unwrap();
    assert!(plan.contains("(rows=31 "), "{plan}");

    // A side no index answers, or sides that find too many rows, read the table
    for filter in [
        "id = 5 OR qty = 2",
        "id > 100 OR tag = 't1'",
        // Each side alone finds under a tenth of the table, but not both together
        "id < 200 OR id > 2850",
        "id = 5 OR id + 0 = 6",
    ] {
        let sql = |table| format!("SELECT id FROM {table} WHERE {filter} ORDER BY id");
        assert_eq!(db.query(&sql("item")), db.query(&sql("plain")), "{filter}");
        let plan = db
            .db
            .explain(&format!("SELECT id FROM item WHERE {filter}"))
            .unwrap();
        assert!(plan.ends_with("  Scan: item"), "{filter}\n{plan}");
    }
}

#[test]
fn test_explain_shows_the_index_range() {
    let mut db = TestDb::new("index-explain");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, qty INT)");
    let values: Vec<String> = (1..=100).map(|id| format!("({id}, 0)")).collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

    assert_eq!(
        db.db.explain("SELECT qty FROM item WHERE id = 5").unwrap(),
        "Projection: qty\n  Filter: id = 5\n    IndexScan: item (id = 5)"
    );
    assert_eq!(
        db.db
            .explain("SELECT qty FROM item i WHERE i.id > 5 AND i.id <= 9")
            .unwrap(),
        "Projection: qty\n  Filter: (id > 5) AND (id <= 9)\n    IndexScan: item i (id > 5 AND id <= 9)"
    );

    // Past a tenth of the table, reading every page in order beats fetching rows one
    // at a time, so the index is only used for the narrower range
    let plan = |db: &mut TestDb, filter: &str| {
        db.db
            .explain(&format!("SELECT qty FROM item WHERE {filter}"))
            .unwrap()
    };
    assert!(plan(&mut db, "id > 91").ends_with("IndexScan: item (id > 91)"));
    assert!(plan(&mut db, "id > 89").ends_with("    Scan: item"));
    assert!(plan(&mut db, "id > 0").ends_with("    Scan: item"));
}

#[test]
fn test_explain_statement_returns_the_plan_as_rows() {
    let mut db = TestDb::new("explain-statement");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, qty INT)");

    let response = db
        .db
        .execute_query("explain SELECT qty FROM item WHERE id = 5;")
        .unwrap();

    assert_eq!(response.columns, vec!["QUERY PLAN"]);
    let lines: Vec<_> = response
        .rows
        .iter()
        .map(|row| row.values[0].to_string())
        .collect();
    assert_eq!(
        lines,
        [
            "Projection: qty",
            "  Filter: id = 5",
            "    IndexScan: item (id = 5)"
        ]
    );

    // Explaining a write doesn't run it
    db.db
        .execute_query("EXPLAIN INSERT INTO item VALUES (1, 2)")
        .unwrap();
    assert!(
        db.db
            .execute_query("SELECT * FROM item")
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn test_explain_analyze_counts_the_rows_each_node_produced() {
    let mut db = TestDb::new("explain-analyze");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, qty INT)");
    let values: Vec<String> = (1..=100).map(|id| format!("({id}, 0)")).collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));

    // Times vary between runs, so only the text before them is compared
    let analyze = |db: &mut TestDb, filter: &str| -> Vec<String> {
        let sql = format!("EXPLAIN ANALYZE SELECT qty FROM item WHERE {filter}");
        let response = db.db.execute_query(&sql).unwrap();
        response
            .rows
            .iter()
            .map(|row| {
                let line = row.values[0].to_string();
                match line.split_once(" time=") {
                    Some((before, _)) => before.to_string(),
                    None => line.split_once(": ").unwrap().0.to_string(),
                }
            })
            .collect()
    };

    assert_eq!(
        analyze(&mut db, "id > 80"),
        [
            "Projection: qty (rows=20",
            "  Filter: id > 80 (rows=20",
            "    Scan: item (rows=100",
            "Planning time",
            "Execution time",
        ]
    );
    assert_eq!(
        analyze(&mut db, "id = 5")[2],
        "    IndexScan: item (id = 5) (rows=1"
    );

    // It runs the statement, so writes are refused rather than applied
    assert!(
        db.db
            .execute_query("EXPLAIN ANALYZE INSERT INTO item VALUES (101, 0)")
            .is_err()
    );
    assert!(
        db.db
            .execute_query("SELECT * FROM item WHERE id = 101")
            .unwrap()
            .rows
            .is_empty()
    );
}
