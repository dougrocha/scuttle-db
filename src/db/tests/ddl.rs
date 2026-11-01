//! CREATE and DROP of tables and indexes, the catalog, and what survives a reopen or a crash.

use super::*;
#[test]
fn test_dropped_table_stays_dropped_after_reopen() {
    let mut db = TestDb::new("drop-table");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    db.execute("INSERT INTO item VALUES (1)");

    db.db.drop_table("item").unwrap();
    assert!(!db.dir.join("item.table").exists());
    assert!(!db.dir.join("item_pkey.index").exists());
    db.reopen();

    assert!(db.db.execute_query("SELECT * FROM item").is_err());
    assert_eq!(
        db.query("SELECT id FROM users WHERE id = 1"),
        [[Value::Int64(1)]]
    );

    // The name is free again, and the new table starts empty
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    db.execute("INSERT INTO item VALUES (1)");
    assert_eq!(db.query("SELECT * FROM item"), [[Value::Int64(1)]]);
    assert!(db.db.drop_table("nope").is_err());
}

#[test]
fn test_an_index_is_kept_in_its_own_file_and_rebuilt_if_it_goes_missing() {
    let mut db = TestDb::new("index-file");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, tag TEXT UNIQUE)");
    let values: Vec<String> = (0..2000).map(|id| format!("({id}, 't{id}')")).collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));
    db.execute("DELETE FROM item WHERE id < 1000");
    db.execute("UPDATE item SET id = id + 5000 WHERE id >= 1990");

    let lookups = |db: &mut TestDb| {
        assert_eq!(
            db.query("SELECT tag FROM item WHERE id = 1500"),
            [[Value::Text("t1500".into())]]
        );
        assert_eq!(
            db.query("SELECT id FROM item WHERE id = 500"),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            db.query("SELECT count(*) FROM item WHERE id >= 6990"),
            [[Value::Int64(10)]]
        );
        assert_eq!(
            db.query("SELECT id FROM item WHERE tag = 't1995'"),
            [[Value::Int64(6995)]]
        );
        // The index still knows which keys are taken
        assert!(db.fails("INSERT INTO item VALUES (1500, 'new')"));
        assert!(db.fails("INSERT INTO item VALUES (1, 't1600')"));
    };

    let index = db.dir.join("item_pkey.index");
    assert!(index.exists() && db.dir.join("item_tag_key.index").exists());
    lookups(&mut db);

    db.reopen();
    lookups(&mut db);

    // Without its file, the index is built again from the table when the database opens
    std::fs::remove_file(&index).unwrap();
    db.reopen();
    assert!(index.exists());
    lookups(&mut db);

    // Again with a sort budget small enough that the entries are sorted in runs on disk
    std::fs::remove_file(&index).unwrap();
    db.db = Database::new(&db.dir);
    db.db.set_sort_mem(1024);
    db.db.initialize().unwrap();
    lookups(&mut db);
}

#[test]
fn test_a_table_reads_back_from_the_catalog_as_it_was_created() {
    let mut db = TestDb::new("catalog-roundtrip");
    db.execute(
        "CREATE TABLE item (id INT PRIMARY KEY, code VARCHAR(8) UNIQUE NOT NULL, \
         price FLOAT DEFAULT 2.5, made TIMESTAMP DEFAULT '2026-01-01 00:00:00', \
         done BOOLEAN NOT NULL DEFAULT false, note TEXT DEFAULT 'a b', gone INT DEFAULT NULL)",
    );
    db.execute("UPDATE item SET id = 0 WHERE id = 0");
    let before = db.db.get_table("item").unwrap().clone();

    db.reopen();
    let after = db.db.get_table("item").unwrap();
    for (old, new) in before.schema.columns.iter().zip(&after.schema.columns) {
        assert_eq!(new.name, old.name);
        assert_eq!(new.data_type, old.data_type, "{}", old.name);
        assert_eq!(new.is_nullable(), old.is_nullable(), "{}", old.name);
        assert_eq!(new.has_default(), old.has_default(), "{}", old.name);
        assert_eq!(new.default_value(), old.default_value(), "{}", old.name);
        assert_eq!(
            IndexConstraint::of(new),
            IndexConstraint::of(old),
            "{}",
            old.name
        );
    }
    assert_eq!(after.schema.columns.len(), before.schema.columns.len());
    let names = |table: &TableDef| -> Vec<String> {
        table.indexes.iter().map(|i| i.name.clone()).collect()
    };
    assert_eq!(names(after), ["item_pkey", "item_code_key"]);
    assert_eq!(names(after), names(&before));

    // The defaults still fill in an INSERT that leaves their columns out
    db.execute("INSERT INTO item (id, code) VALUES (1, 'a')");
    assert_eq!(
        db.query("SELECT price, done, note, gone FROM item"),
        [[
            Value::Float64(2.5),
            Value::Bool(false),
            text("a b"),
            Value::Null
        ]]
    );
    assert!(db.fails("INSERT INTO item (id, code) VALUES (1, 'b')"));
    assert!(db.fails("INSERT INTO item (id, code) VALUES (2, 'a')"));
}

#[test]
fn test_the_catalog_tables_can_be_read_but_not_written() {
    let mut db = TestDb::new("catalog-read");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, tag VARCHAR(4) DEFAULT 'x')");

    assert_eq!(
        db.query("SELECT name FROM __tables__ ORDER BY name"),
        [[text("item")], [text("users")]]
    );
    assert_eq!(
        db.query(
            "SELECT position, name, type, max_len, not_null, default_value \
             FROM __columns__ WHERE table_name = 'item' ORDER BY position"
        ),
        [
            vec![
                Value::Int64(0),
                text("id"),
                text("INT"),
                Value::Null,
                Value::Bool(true),
                Value::Null
            ],
            vec![
                Value::Int64(1),
                text("tag"),
                text("VARCHAR"),
                Value::Int64(4),
                Value::Bool(false),
                text("x")
            ],
        ]
    );
    assert_eq!(
        db.query("SELECT name, column_name, is_unique, constraint_type FROM __indexes__"),
        [[
            text("item_pkey"),
            text("id"),
            Value::Bool(true),
            text("PRIMARY KEY")
        ]]
    );

    assert!(db.fails("INSERT INTO __tables__ VALUES ('fake')"));
    assert!(db.fails("UPDATE __columns__ SET name = 'x'"));
    assert!(db.fails("DELETE FROM __indexes__"));
    assert!(db.db.drop_table("__tables__").is_err());
    assert!(db.fails(r#"CREATE TABLE "__tables__" (id INT)"#));
    assert_eq!(db.query("SELECT name FROM __tables__").len(), 2);
}

#[test]
fn test_a_table_without_its_tables_row_is_cleaned_up_on_open() {
    let mut db = TestDb::new("catalog-half-made");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    db.execute("INSERT INTO item VALUES (1)");

    // As if the database stopped after writing the columns but not the `__tables__` row
    let schema = db.db.get_table("__tables__").unwrap().schema.clone();
    let mut cursor = TableCursor::open(&mut db.db.pool, "__tables__".to_string(), schema);
    let mut found = Vec::new();
    while cursor
        .read_next_page(&mut db.db.pool, |page_id, item_id, row| {
            if row.values[0] == text("item") {
                found.push((page_id, item_id));
            }
            Ok(())
        })
        .unwrap()
    {}
    for (page_id, item_id) in found {
        let page = db.db.pool.get_page_mut("__tables__", page_id);
        page.unwrap().delete_item(item_id).unwrap();
    }
    db.db.pool.flush().unwrap();

    db.reopen();
    assert!(db.fails("SELECT * FROM item"));
    assert!(
        db.query("SELECT * FROM __columns__ WHERE table_name = 'item'")
            .is_empty()
    );
    assert!(db.query("SELECT * FROM __indexes__").is_empty());
    assert!(!db.dir.join("item.table").exists());
    assert!(!db.dir.join("item_pkey.index").exists());
    // `users` is untouched
    assert_eq!(db.query("SELECT id FROM users").len(), 4);
}

#[test]
fn test_a_create_table_that_fails_partway_leaves_nothing_for_a_retry() {
    let long = "x".repeat(9000);
    let create = format!("CREATE TABLE item (id INT UNIQUE, note TEXT DEFAULT '{long}')");
    let mut on_disk = TestDb::new("create-fails-partway");
    let mut in_memory = Database::in_memory();
    for db in [&mut on_disk.db, &mut in_memory] {
        // The second column's catalog row doesn't fit in a page
        assert!(db.execute_query(&create).is_err());
        let rows = db.execute_query("SELECT * FROM __columns__ WHERE table_name = 'item'");
        assert!(rows.unwrap().rows.is_empty());
        assert!(
            db.execute_query("SELECT * FROM __indexes__")
                .unwrap()
                .rows
                .is_empty()
        );

        db.execute_query("CREATE TABLE item (id INT UNIQUE)")
            .unwrap();
    }
    on_disk.reopen();
    assert_eq!(
        on_disk.db.get_table("item").unwrap().schema.columns.len(),
        1
    );
}

#[test]
fn test_a_column_name_can_only_be_used_once_per_table() {
    let mut db = TestDb::new("duplicate-columns");
    assert!(db.fails("CREATE TABLE item (x INT, x INT UNIQUE)"));
    assert!(db.fails("SELECT * FROM item"));
}

#[test]
fn test_a_table_with_no_columns_survives_a_reopen() {
    let mut db = TestDb::new("zero-columns");
    db.execute("CREATE TABLE z ()");
    db.reopen();
    assert!(db.query("SELECT * FROM z").is_empty());
    assert_eq!(
        db.query("SELECT name FROM __tables__ WHERE name = 'z'"),
        [[text("z")]]
    );
}

#[test]
fn test_index_names_that_clash_get_a_number() {
    let mut db = TestDb::new("index-names");
    // Both UNIQUE columns would be named a_b_c_key
    db.execute("CREATE TABLE a_b (c INT UNIQUE)");
    db.execute("CREATE TABLE a (b_c INT UNIQUE)");
    assert_eq!(
        db.query("SELECT name, table_name FROM __indexes__ ORDER BY name"),
        [
            [text("a_b_c_key"), text("a_b")],
            [text("a_b_c_key1"), text("a")]
        ]
    );
    db.execute("INSERT INTO a_b VALUES (1)");
    db.execute("INSERT INTO a VALUES (1)");
    db.reopen();
    assert!(db.fails("INSERT INTO a VALUES (1)"));
    db.execute("INSERT INTO a_b VALUES (2)");
}

#[test]
fn test_files_the_catalog_doesnt_list_are_deleted_on_open() {
    let mut db = TestDb::new("catalog-old-files");
    // What an older version left: its JSON catalog and a table it listed there
    std::fs::write(db.dir.join("__catalog__.table"), "old").unwrap();
    std::fs::write(db.dir.join("old.table"), "old").unwrap();
    std::fs::write(db.dir.join("old.id.index"), "old").unwrap();

    db.reopen();
    assert!(!db.dir.join("__catalog__.table").exists());
    assert!(!db.dir.join("old.table").exists());
    assert!(!db.dir.join("old.id.index").exists());
    assert_eq!(db.query("SELECT id FROM users").len(), 4);
}

#[test]
fn test_create_index_serves_queries_until_it_is_dropped() {
    let mut db = TestDb::new("create-index");
    db.execute("CREATE TABLE item (id INT, grp INT)");
    let values: Vec<String> = (0..1000)
        .map(|id| format!("({id}, {})", id % 100))
        .collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));
    let plan = |db: &mut TestDb| db.db.explain("SELECT id FROM item WHERE grp = 7").unwrap();
    let ids = |db: &mut TestDb| db.query("SELECT id FROM item WHERE grp = 7 ORDER BY id");
    let scanned = ids(&mut db);
    assert_eq!(scanned.len(), 10);

    db.execute("CREATE INDEX item_grp ON item (grp)");
    assert!(
        plan(&mut db).contains("IndexScan: item (grp = 7)"),
        "{}",
        plan(&mut db)
    );
    assert_eq!(ids(&mut db), scanned);
    assert!(db.dir.join("item_grp.index").exists());

    // Writes keep it up to date, and it outlives a restart
    db.execute("INSERT INTO item VALUES (5000, 7)");
    db.execute("DELETE FROM item WHERE id = 7");
    db.execute("UPDATE item SET grp = 7 WHERE id = 8");
    db.reopen();
    assert!(plan(&mut db).contains("IndexScan"));
    let expected: Vec<i64> = vec![8, 107, 207, 307, 407, 507, 607, 707, 807, 907, 5000];
    assert_eq!(
        ids(&mut db),
        expected
            .into_iter()
            .map(|id| vec![Value::Int64(id)])
            .collect::<Vec<_>>()
    );

    db.execute("DROP INDEX item_grp");
    assert!(!plan(&mut db).contains("IndexScan"));
    assert!(!db.dir.join("item_grp.index").exists());
    assert!(db.query("SELECT * FROM __indexes__").is_empty());
    db.reopen();
    assert_eq!(ids(&mut db).len(), 11);
    assert!(db.fails("DROP INDEX item_grp"));
}

#[test]
fn test_a_unique_index_refuses_duplicates_made_before_or_after_it() {
    let mut db = TestDb::new("unique-index");
    db.execute("CREATE TABLE item (id INT, tag TEXT)");
    db.execute("INSERT INTO item VALUES (1, 'a'), (2, 'a'), (3, NULL), (4, NULL)");

    assert!(db.fails("CREATE UNIQUE INDEX item_tag ON item (tag)"));
    assert!(!db.dir.join("item_tag.index").exists());
    assert!(db.query("SELECT * FROM __indexes__").is_empty());

    // NULLs aren't indexed, so they don't clash
    db.execute("DELETE FROM item WHERE id = 2");
    db.execute("CREATE UNIQUE INDEX item_tag ON item (tag)");
    assert!(db.fails("INSERT INTO item VALUES (5, 'a')"));
    db.execute("INSERT INTO item VALUES (5, NULL), (6, 'b')");
    db.reopen();
    assert!(db.fails("UPDATE item SET tag = 'a' WHERE id = 6"));
}

#[test]
fn test_create_and_drop_index_refuse_what_they_cant_do() {
    let mut db = TestDb::new("index-errors");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, tag TEXT UNIQUE)");

    assert!(db.fails("CREATE INDEX item_pkey ON item (tag)"));
    assert!(db.fails("CREATE INDEX item_tag_key ON users (name)"));
    assert!(db.fails(r#"CREATE INDEX "a.b" ON item (tag)"#));
    assert!(db.fails("CREATE INDEX x ON nope (tag)"));
    assert!(db.fails("CREATE INDEX x ON item (nope)"));
    assert!(db.fails("CREATE INDEX x ON item (id, tag)"));
    assert!(db.fails("CREATE INDEX x ON __tables__ (name)"));
    assert!(db.fails("DROP INDEX item_pkey"));
    assert!(db.fails("DROP INDEX item_tag_key"));
    assert!(db.fails("DROP INDEX nope"));
    assert!(db.fails("DROP TABLE __columns__"));

    // A CREATE TABLE after a CREATE INDEX took its constraint's name picks another
    db.execute("CREATE INDEX other_pkey ON item (tag)");
    db.execute("CREATE TABLE other (id INT PRIMARY KEY)");
    assert_eq!(
        db.db.get_table("other").unwrap().indexes[0].name,
        "other_pkey1"
    );
}

#[test]
fn test_drop_table_takes_its_indexes_with_it() {
    let mut db = TestDb::new("drop-table-sql");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, grp INT)");
    db.execute("INSERT INTO item VALUES (1, 1)");
    db.execute("CREATE INDEX item_grp ON item (grp)");

    db.execute("DROP TABLE item");
    assert!(db.fails("SELECT * FROM item"));
    assert!(db.fails("DROP TABLE item"));
    for file in ["item.table", "item_pkey.index", "item_grp.index"] {
        assert!(!db.dir.join(file).exists(), "{file}");
    }
    assert!(db.query("SELECT * FROM __indexes__").is_empty());

    // The names are free again
    db.execute("CREATE TABLE item (id INT PRIMARY KEY, grp INT)");
    db.execute("CREATE INDEX item_grp ON item (grp)");
    db.reopen();
    assert!(db.query("SELECT * FROM item").is_empty());
}

#[test]
fn test_a_table_cannot_take_the_catalogs_name() {
    // Before any table exists there is no catalog file, so nothing else stops this name
    let dir = std::env::temp_dir().join(format!("scuttle-reserved-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut db = Database::new(&dir);
    db.initialize().unwrap();

    assert!(
        db.execute_query(r#"CREATE TABLE "__catalog__" (id INT)"#)
            .is_err()
    );
    assert!(
        db.execute_query(r#"CREATE TABLE "__mine" (id INT)"#)
            .is_err()
    );
    db.execute_query("CREATE TABLE kept (id INT)").unwrap();
    db.execute_query("INSERT INTO kept VALUES (1)").unwrap();

    let mut db = Database::new(&dir);
    db.initialize().unwrap();
    let rows = db.execute_query("SELECT id FROM kept").unwrap().rows;
    assert_eq!(rows.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn test_names_that_would_make_unsafe_or_clashing_files_are_refused() {
    let mut db = TestDb::new("names");
    assert!(db.fails(r#"CREATE TABLE "../escape" (id INT)"#));
    assert!(db.fails(r#"CREATE TABLE "a.b" (id INT)"#));
    assert!(db.fails(r#"CREATE TABLE ok (".." INT)"#));
    assert!(!db.dir.join("../escape.table").exists());

    // These once both named their index file a_b_c.index
    db.execute("CREATE TABLE a_b (c INT PRIMARY KEY)");
    db.execute("CREATE TABLE a (b_c INT PRIMARY KEY)");
    db.execute("INSERT INTO a_b VALUES (1)");
    db.execute("INSERT INTO a VALUES (1)");
    db.reopen();
    assert_eq!(
        db.query("SELECT c FROM a_b WHERE c = 1"),
        [[Value::Int64(1)]]
    );
    assert_eq!(
        db.query("SELECT b_c FROM a WHERE b_c = 1"),
        [[Value::Int64(1)]]
    );
}

#[test]
fn test_unique_float_and_boolean_columns_refuse_duplicates() {
    let mut db = TestDb::new("unique-float-bool");
    db.execute("CREATE TABLE item (price FLOAT UNIQUE, done BOOLEAN UNIQUE)");
    db.execute("INSERT INTO item VALUES (1.5, true), (0 - 2.25, false)");

    assert!(db.fails("INSERT INTO item VALUES (1.5, NULL)"));
    assert!(db.fails("INSERT INTO item VALUES (NULL, true)"));
    // -0.0 and 0.0 are the same number
    db.execute("INSERT INTO item VALUES (0.0, NULL)");
    assert!(db.fails("INSERT INTO item VALUES (0.0 * (0 - 1), NULL)"));
    db.execute("INSERT INTO item VALUES (3.0, NULL)");

    db.reopen();
    assert!(db.fails("INSERT INTO item VALUES (1.5, NULL)"));
    assert_eq!(
        db.query("SELECT price FROM item WHERE price < 1.0 ORDER BY price"),
        [[Value::Float64(-2.25)], [Value::Float64(0.0)]]
    );
    assert_eq!(
        db.query("SELECT price FROM item WHERE done = true"),
        [[Value::Float64(1.5)]]
    );
}

#[test]
fn test_a_float_index_finds_what_a_scan_finds() {
    let mut db = TestDb::new("float-index-agrees");
    db.execute("CREATE TABLE item (price FLOAT UNIQUE)");
    // Enough rows that the optimizer picks the index for a narrow range
    let values: Vec<String> = (1..=100).map(|n| format!("({n}.0)")).collect();
    db.execute(&format!("INSERT INTO item VALUES {}", values.join(", ")));
    db.execute("INSERT INTO item VALUES (0.0000000000000001), (0.0)");

    // `price + 0.0` can't use the index, so each pair compares the index to a scan
    for filter in [
        "= 0.0",
        "< 0.0000000000000001",
        "<= 0.0000000000000001",
        "> 99.0",
        "= 1.0",
    ] {
        let indexed = format!("SELECT price FROM item WHERE price {filter} ORDER BY price");
        assert!(
            db.db.explain(&indexed).unwrap().contains("IndexScan: item"),
            "{filter}"
        );
        let scanned = db.query(&format!(
            "SELECT price FROM item WHERE price + 0.0 {filter} ORDER BY price"
        ));
        assert_eq!(db.query(&indexed), scanned, "WHERE price {filter}");
    }
    assert_eq!(
        db.query("SELECT price FROM item WHERE price = 0.0"),
        [[Value::Float64(0.0)]]
    );
}

#[test]
fn test_a_value_too_long_for_its_index_changes_nothing() {
    let mut db = TestDb::new("long-key");
    db.execute("CREATE TABLE item (id INT, tag TEXT UNIQUE)");
    db.execute("INSERT INTO item VALUES (1, 'short')");
    let long = "x".repeat(2000);

    assert!(db.fails(&format!("INSERT INTO item VALUES (2, '{long}')")));
    assert!(db.fails(&format!("UPDATE item SET tag = '{long}' WHERE id = 1")));

    db.reopen();
    assert_eq!(
        db.query("SELECT id, tag FROM item"),
        [[Value::Int64(1), Value::Text("short".into())]]
    );
    assert_eq!(
        db.query("SELECT id FROM item WHERE tag = 'short'"),
        [[Value::Int64(1)]]
    );
}

#[test]
fn test_indexes_are_rebuilt_after_a_run_that_stopped_partway() {
    let mut db = TestDb::new("stale-index");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    db.execute("INSERT INTO item VALUES (1), (2)");
    let index = db.dir.join("item_pkey.index");
    let old_index = std::fs::read(&index).unwrap();

    // A crash after the table's pages were written but before the index's: the table
    // has row 3, the index doesn't, and the marker was never deleted
    db.execute("INSERT INTO item VALUES (3)");
    std::fs::write(&index, old_index).unwrap();
    std::fs::File::create(db.dir.join(UNFLUSHED)).unwrap();

    db.reopen();
    assert_eq!(
        db.query("SELECT id FROM item WHERE id = 3"),
        [[Value::Int64(3)]]
    );
    assert!(db.fails("INSERT INTO item VALUES (3)"));
    assert!(!db.dir.join(UNFLUSHED).exists());
}

#[test]
#[cfg(unix)]
fn test_a_rebuild_that_stops_partway_leaves_the_marker() {
    use std::os::unix::fs::PermissionsExt;

    let mut db = TestDb::new("rebuild-stops");
    db.execute("CREATE TABLE first (id INT PRIMARY KEY)");
    db.execute("CREATE TABLE second (id INT PRIMARY KEY)");
    db.execute("INSERT INTO first VALUES (1)");
    db.execute("INSERT INTO second VALUES (1)");

    // A crash left the marker, and the second table can't be read, so the rebuild
    // stops after the first table, as another crash there would
    std::fs::File::create(db.dir.join(UNFLUSHED)).unwrap();
    let second = db.dir.join("second.table");
    std::fs::set_permissions(&second, std::fs::Permissions::from_mode(0o000)).unwrap();

    let mut reopened = Database::new(&db.dir);
    let failed = reopened.initialize().is_err();
    std::fs::set_permissions(&second, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(failed);
    assert!(db.dir.join(UNFLUSHED).exists());
}

#[test]
#[cfg(unix)]
fn test_a_drop_that_fails_partway_still_drops_the_table() {
    use std::os::unix::fs::PermissionsExt;

    let mut db = TestDb::new("drop-fails");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    db.execute("INSERT INTO item VALUES (1)");

    // A read-only directory: files in it can be written but not created or deleted. The
    // drop writes the catalog, then fails deleting the unflushed marker, made here
    // because it couldn't be created. Deleting the table's files would fail the same way.
    let mode = |db: &TestDb, mode| {
        std::fs::set_permissions(&db.dir, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    std::fs::File::create(db.dir.join(UNFLUSHED)).unwrap();
    mode(&db, 0o555);
    let dropped = db.db.drop_table("item");
    mode(&db, 0o755);
    assert!(dropped.is_err());

    // The table is gone, though its files are still there
    assert!(db.fails("SELECT * FROM item"));
    assert!(db.db.drop_table("item").is_err());
    assert!(db.fails("INSERT INTO item VALUES (2)"));
    assert!(db.dir.join("item.table").exists());

    // Opening the database deletes the files no table owns
    db.reopen();
    assert!(db.fails("SELECT * FROM item"));
    assert!(!db.dir.join("item.table").exists());
    assert!(!db.dir.join("item_pkey.index").exists());
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    assert_eq!(db.query("SELECT * FROM item"), Vec::<Vec<Value>>::new());
}

#[test]
#[cfg(unix)]
fn test_a_table_can_be_made_again_over_the_files_a_failed_drop_left() {
    use std::os::unix::fs::PermissionsExt;

    let mut db = TestDb::new("drop-fails-create");
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    db.execute("INSERT INTO item VALUES (1)");
    std::fs::File::create(db.dir.join(UNFLUSHED)).unwrap();
    std::fs::set_permissions(&db.dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let dropped = db.db.drop_table("item");
    std::fs::set_permissions(&db.dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(dropped.is_err());

    // Without reopening, the old rows and index entries must not come back
    db.execute("CREATE TABLE item (id INT PRIMARY KEY)");
    assert_eq!(db.query("SELECT * FROM item"), Vec::<Vec<Value>>::new());
    db.execute("INSERT INTO item VALUES (1)");
    db.reopen();
    assert_eq!(
        db.query("SELECT * FROM item WHERE id = 1"),
        [[Value::Int64(1)]]
    );
}
