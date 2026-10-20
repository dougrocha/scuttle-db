# Update and delete rows

A user changes or removes the rows that match a WHERE clause, or every row when there is no WHERE.

## Sub-features

- `update-where` `UPDATE t SET col = expr [, ...] WHERE ...`. The expressions see the old row.
- `delete-where` `DELETE FROM t WHERE ...`.
- `update-all` and `delete-all` the same statements without WHERE.
- `update-errors` a column assigned twice, a wrong type, or an unknown column fails, and no row changes.

## How to get to it (user POV)

- Type the statement at the `DB: ` prompt. The REPL replies `N row(s) affected`.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Update one row and delete another.** Run:

    ```sh
    scv drive mutate <<'SQL'
    CREATE TABLE t (id INT, v TEXT);
    INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
    UPDATE t SET v = 'z' WHERE id = 1;
    DELETE FROM t WHERE id = 3;
    SELECT * FROM t ORDER BY id;
    SQL
    ```

    Expect `OK`, `3 row(s) affected`, `1 row(s) affected` (update), and `1 row(s) affected` (delete). The SELECT shows exactly `1 | z` and `2 | b`, then `(2 rows)`.
- **Update every row, swap columns, hit assignment errors, delete every row.** Run:

    ```sh
    scv drive mutate-all <<'SQL'
    CREATE TABLE t (a INT, b INT, s TEXT);
    INSERT INTO t VALUES (1, 10, 'x'), (2, 20, 'y');
    UPDATE t SET a = b, b = a;
    SELECT * FROM t ORDER BY a;
    UPDATE t SET a = a + 1, s = 'q' WHERE b = 2;
    SELECT * FROM t ORDER BY a;
    UPDATE t SET a = 1, a = 2;
    UPDATE t SET a = 'oops';
    UPDATE t SET nope = 1;
    UPDATE t SET a = 1 WHERE a = 999;
    DELETE FROM t;
    SELECT * FROM t;
    SQL
    ```

    Expect, in order:
    - `OK`, `2 row(s) affected`, then `2 row(s) affected` for the UPDATE without WHERE.
    - Rows `10 | 1 | x` and `20 | 2 | y`. The swap worked, so each SET expression read the old row.
    - `1 row(s) affected`, then rows `10 | 1 | x` and `21 | 2 | q`.
    - `  x Column a is assigned more than once`, `  x Cannot assign Text to column a of type Int64`, `  x Column nope could not be found`. No row changes.
    - `0 row(s) affected` when nothing matches, `2 row(s) affected` for the DELETE without WHERE, then an empty table with `(0 rows)`.

## Gotchas

- `N row(s) affected` is not proof on its own. Finish with a SELECT that shows the new state.
- UPDATE deletes each matching row and appends the new version at the end of the table. A SELECT without ORDER BY returns updated rows last.
- Neither statement takes a table alias, and a SET target must be a bare column name. `SET t.v = 1` is a parse error.
- To prove the change reached disk, add the restart step from [persistence.md](persistence.md).
