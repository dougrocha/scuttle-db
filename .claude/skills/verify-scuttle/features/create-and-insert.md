# Create tables and insert rows

A user defines a table with typed columns, then adds rows: one or many per statement, all columns or
a named subset, and NULLs.

## Sub-features

- `create-table`: `CREATE TABLE name (col TYPE, ...)`. Types: `INT`/`INTEGER`, `FLOAT`, `VARCHAR(n)`, `TEXT`/`STRING`, `BOOL`/`BOOLEAN`, `TIMESTAMP`.
- `insert-multi`: several `(...)` tuples in one INSERT.
- `insert-columns`: an `INSERT INTO t (col, ...)` column list. Omitted columns get their DEFAULT, or NULL when they have none.
- `insert-type-check`: a value of the wrong type is rejected.
- `column-default`: `col TYPE DEFAULT literal`. An INSERT that omits the column gets the default. An explicit NULL stays NULL.
- `timestamp`: a `TIMESTAMP` column takes quoted text or a `TIMESTAMP '...'` literal, and values print as `YYYY-MM-DD HH:MM:SS`.

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

## Timestamps

```bash
.claude/skills/verify-scuttle/scripts/drive.sh ts-recipe <<'SQL'
CREATE TABLE ev (id INT, at TIMESTAMP);
INSERT INTO ev VALUES (1, '2026-03-01 12:00:00'), (2, '2026-03-01T08:30:00'), (3, '2024-02-29');
INSERT INTO ev VALUES (4, TIMESTAMP '1969-12-31 23:59:59');
INSERT INTO ev VALUES (5, 'yesterday');
INSERT INTO ev VALUES (6, '2026-02-30');
SELECT id, at FROM ev WHERE at > '2026-01-01' ORDER BY at;
SELECT id, at FROM ev WHERE at > TIMESTAMP '2026-01-01 00:00:00' ORDER BY id;
SQL
```

Expected: `OK`, then `3 row(s) affected` and `1 row(s) affected`. Row 5 fails with
`invalid input syntax for type timestamp: "yesterday"`, and row 6 fails with
`date/time field value out of range: "2026-02-30"`. Both bad rows are not written. The first SELECT
returns `2 | 2026-03-01 08:30:00` then `1 | 2026-03-01 12:00:00` (ordered by `at`), and the second returns
the same two rows ordered by `id`, then `(2 rows)`.

## Gotchas

- Creating a table that already exists fails with `  x Table t already exists`, and the existing
  table keeps its schema and rows. There is no `OK`.
- `DATE` is not a column type: `CREATE TABLE d (x DATE)` fails with
  `Parse error: Expected a column type.`
- A second `DEFAULT` on one column fails with `Parse error: Multiple DEFAULT values specified for column id`.
  `DEFAULT` takes only a literal: `DEFAULT now()` or `DEFAULT other_col` fails to parse.
- A `TIMESTAMP` column accepts text such as `'2026-03-01 12:00:00'` (a `T` separator or a date alone also
  works) or a `TIMESTAMP '...'` literal. Bad text fails with `invalid input syntax for type timestamp: "yesterday"`,
  and an impossible date such as `'2026-02-30'` fails with `date/time field value out of range: "2026-02-30"`.
  A `TEXT` column still rejects `TIMESTAMP` values.
