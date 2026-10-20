# Create tables and insert rows

A user defines a table with typed columns and optional defaults, then adds rows: one or many per statement, all columns or a named subset, NULLs, and timestamps.

## Sub-features

- `create-table` `CREATE TABLE name (col TYPE, ...)`. Types: `INT`/`INTEGER`, `FLOAT`, `VARCHAR(n)`, `TEXT`/`STRING`, `BOOL`/`BOOLEAN`, `TIMESTAMP`.
- `insert-multi` several `(...)` tuples in one INSERT.
- `insert-columns` an `INSERT INTO t (col, ...)` column list. Omitted columns get their DEFAULT, or NULL when they have none.
- `insert-type-check` a value of the wrong type is rejected and not written.
- `column-default` `col TYPE DEFAULT literal`. An explicit NULL stays NULL.
- `timestamp` a `TIMESTAMP` column takes quoted text or a `TIMESTAMP '...'` literal and prints `YYYY-MM-DD HH:MM:SS`.

## How to get to it (user POV)

- Type the statements at the `DB: ` prompt of `cargo run --bin scuttle-db`.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Create, insert, and hit a type error.** Run:

    ```sh
    scv drive ddl-insert <<'SQL'
    CREATE TABLE t (id INT, v TEXT);
    INSERT INTO t (id) VALUES (1);
    INSERT INTO t VALUES (2, 'b'), (3, 'c');
    SELECT * FROM t ORDER BY id;
    INSERT INTO t VALUES ('x', 1);
    SELECT COUNT(*) FROM t;
    SQL
    ```

    Expect `OK`, `1 row(s) affected`, `2 row(s) affected`, then rows `1 | NULL`, `2 | b`, `3 | c` and `(3 rows)`. Then `  x Cannot insert Text into column id of type Int64`, then a count of `3`, so the bad row was not written. `db_files.txt` lists `__catalog__.table` and `t.table`.
- **Column defaults.** Run:

    ```sh
    scv drive col-default <<'SQL'
    CREATE TABLE d (id INT, n INT DEFAULT 5, f FLOAT DEFAULT 2, s TEXT DEFAULT 'hi', b BOOL DEFAULT TRUE, z TEXT DEFAULT NULL);
    INSERT INTO d (id) VALUES (1);
    INSERT INTO d (id, n) VALUES (2, NULL);
    SELECT * FROM d ORDER BY id;
    CREATE TABLE e (id INT DEFAULT 'x');
    SQL
    ```

    Expect `OK`, two `1 row(s) affected`, then rows `1 | 5 | 2 | hi | true | NULL` and `2 | NULL | 2 | hi | true | NULL` and `(2 rows)`. The last statement fails with `Type mismatch: Column id has an invalid DEFAULT: ...`.
- **Timestamps.** Run:

    ```sh
    scv drive timestamp <<'SQL'
    CREATE TABLE ev (id INT, at TIMESTAMP);
    INSERT INTO ev VALUES (1, '2026-03-01 12:00:00'), (2, '2026-03-01T08:30:00'), (3, '2024-02-29');
    INSERT INTO ev VALUES (4, TIMESTAMP '1969-12-31 23:59:59');
    INSERT INTO ev VALUES (5, 'yesterday');
    INSERT INTO ev VALUES (6, '2026-02-30');
    SELECT id, at FROM ev WHERE at > '2026-01-01' ORDER BY at;
    SELECT id, at FROM ev WHERE at > TIMESTAMP '2026-01-01 00:00:00' ORDER BY id;
    SQL
    ```

    Expect `OK`, `3 row(s) affected`, `1 row(s) affected`, then `invalid input syntax for type timestamp: "yesterday"` and `date/time field value out of range: "2026-02-30"`. The first SELECT returns `2 | 2026-03-01 08:30:00` then `1 | 2026-03-01 12:00:00`. The second returns the same two rows ordered by `id`, then `(2 rows)`.

## Gotchas

- Creating a table that already exists fails with `  x Table t already exists`. The existing table keeps its schema and rows.
- `DATE` is not a column type. `CREATE TABLE d (x DATE)` fails with `Parse error: Expected a column type.`
- A second `DEFAULT` on one column fails with `Parse error: Multiple DEFAULT values specified for column id`.
- `DEFAULT` takes only a literal. `DEFAULT now()` and `DEFAULT other_col` fail to parse.
- A `TEXT` column rejects `TIMESTAMP` values.
