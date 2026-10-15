# Create tables and insert rows

A user defines a table with typed columns, then adds rows: one or many per statement, all columns or
a named subset, and NULLs.

## Sub-features

- `create-table`: `CREATE TABLE name (col TYPE, ...)`. Types: `INT`/`INTEGER`, `FLOAT`, `VARCHAR(n)`, `TEXT`/`STRING`, `BOOL`/`BOOLEAN`, `TIMESTAMP`.
- `insert-multi`: several `(...)` tuples in one INSERT.
- `insert-columns`: an `INSERT INTO t (col, ...)` column list. Omitted columns get their DEFAULT, or NULL when they have none.
- `insert-type-check`: a value of the wrong type is rejected.
- `column-default`: `col TYPE DEFAULT literal`. An INSERT that omits the column gets the default. An explicit NULL stays NULL.

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

## Column defaults

```bash
.claude/skills/verify-scuttle/scripts/drive.sh col-default <<'SQL'
CREATE TABLE d (id INT, n INT DEFAULT 5, f FLOAT DEFAULT 2, s TEXT DEFAULT 'hi', b BOOL DEFAULT TRUE, z TEXT DEFAULT NULL);
INSERT INTO d (id) VALUES (1);
INSERT INTO d (id, n) VALUES (2, NULL);
SELECT * FROM d ORDER BY id;
CREATE TABLE e (id INT DEFAULT 'x');
SQL
```

Expected: `OK`, then two `1 row(s) affected`. The table has rows `1 | 5 | 2 | hi | true | NULL` and
`2 | NULL | 2 | hi | true | NULL`, then `(2 rows)`. The explicit NULL in row 2 stays NULL. The last
statement fails with `Type mismatch: Column id has an invalid DEFAULT: ...`.

## Gotchas

- Creating a table that already exists fails with `  x Table t already exists`, and the existing
  table keeps its schema and rows. There is no `OK`.
- `DATE` is not a column type: `CREATE TABLE d (x DATE)` fails with
  `Parse error: Expected a column type.`
- A second `DEFAULT` on one column fails with `Parse error: Multiple DEFAULT values specified for column id`.
  `DEFAULT` takes only a literal: `DEFAULT now()` or `DEFAULT other_col` fails to parse.
- A `TIMESTAMP` column can be created, but there is no timestamp literal. Inserting a string fails with
  `Cannot insert Text into column at of type Timestamp`, so TIMESTAMP values can't be written from the REPL yet.
