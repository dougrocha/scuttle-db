# Create tables and insert rows

A user defines a table with typed columns, then adds rows: one or many per statement, all columns or
a named subset, and NULLs.

## Sub-features

- `create-table`: `CREATE TABLE name (col TYPE, ...)`. Types: `INT`/`INTEGER`, `FLOAT`, `VARCHAR`, `TEXT`/`STRING`, `BOOL`/`BOOLEAN`, `DATE`, `TIMESTAMP`.
- `insert-multi`: several `(...)` tuples in one INSERT.
- `insert-columns`: an `INSERT INTO t (col, ...)` column list. Omitted columns become NULL.
- `insert-type-check`: a value of the wrong type is rejected.

## How to get to it (user POV)

Type the statements at the `DB: ` prompt of `cargo run --bin scuttle-db`.

## Driving it with drive.sh

```bash
.claude/skills/verify-scuttle/scripts/drive.sh ddl-insert <<'SQL'
CREATE TABLE t (id INT, v TEXT);
INSERT INTO t (id) VALUES (1);
INSERT INTO t VALUES (2, 'b'), (3, 'c');
SELECT * FROM t ORDER BY id;
INSERT INTO t VALUES ('x', 1);
SELECT COUNT(*) FROM t;
SQL
```

Expected, in order: `OK`, `1 row(s) affected`, `2 row(s) affected`, then a table with rows
`1 | NULL`, `2 | b`, `3 | c` and `(3 rows)`. Next comes
`  x Cannot insert Text into column id of type Int64`, then a count of `3`, which shows the bad
row was not written. `db_files.txt` lists `__catalog__.table` and `t.table`.

## Gotchas

- Creating a table that already exists prints `Table t already exists` and then `OK`. It is not
  reported as an error. Assert on the message text, not on the absence of `OK`.
- `DEFAULT` is not parsed: `CREATE TABLE d (id INT DEFAULT 5)` fails with
  `Unexpected token 'Keyword(Default)' while parsing constraints`.
- A `TIMESTAMP` column can be created, but there is no timestamp literal. Inserting a string fails with
  `Cannot insert Text into column at of type Timestamp`, so TIMESTAMP values can't be written from the REPL yet.
