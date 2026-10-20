# Update and delete rows

A user changes or removes the rows that match a WHERE clause, or every row when there is no WHERE.

## Sub-features

- `update-where` `UPDATE t SET col = expr [, ...] WHERE ...`. The expressions see the old row.
- `delete-where` `DELETE FROM t WHERE ...`.
- `update-all` and `delete-all` the same statements without WHERE.

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

## Gotchas

- `N row(s) affected` is not proof on its own. Finish with a SELECT that shows the new state.
- To prove the change reached disk, add the restart step from [persistence.md](persistence.md).
