# Join tables

A user combines rows from several tables with `INNER JOIN` or `LEFT [OUTER] JOIN`. Columns can be qualified by table name or alias (`u.name`, `users.name`), and a table can join itself under two aliases.

## Sub-features

- `inner-join` `FROM a [INNER] JOIN b ON <expr>` keeps only matching pairs.
- `left-join` `FROM a LEFT [OUTER] JOIN b ON <expr>` also keeps unmatched left rows, with NULL in every right column, even when the right table is empty.
- `aliases` `FROM users u` or `FROM users AS u`. Once a table has an alias, only the alias is visible.
- `qualified-columns` `t.col` works in the select list, ON, WHERE, GROUP BY, ORDER BY, and aggregate arguments.
- `self-join` `users a JOIN users b`.
- `chains` `a JOIN b ON ... LEFT JOIN c ON ...` (left-deep). An ON clause sees only the tables to its left and its own table.
- `star` `*` returns every column of every table in FROM order. `t.*` returns one table's columns.
- `errors` ambiguous unqualified names, unknown or hidden aliases, duplicate table names, RIGHT/FULL/CROSS, comma joins.

## How to get to it (user POV)

- Type `SELECT ... FROM a JOIN b ON ...` at the `DB: ` prompt.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Join two tables, then relaunch.** Run:

    ```sh
    out=$(scv drive joins <<'SQL'
    CREATE TABLE users (id INT, name TEXT, dept TEXT);
    INSERT INTO users VALUES (1, 'ann', 'eng'), (2, 'bob', 'eng'), (3, 'cy', 'ops');
    CREATE TABLE orders (id INT, user_id INT, total INT);
    INSERT INTO orders VALUES (10, 1, 100), (11, 1, 50), (12, 3, 75), (13, 9, 5);
    SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id;
    SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id;
    SELECT u.name, COUNT(o.id) AS n FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name;
    SELECT a.name, b.name FROM users a JOIN users b ON a.dept = b.dept AND a.id < b.id;
    SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 10;
    SELECT id FROM users u JOIN orders o ON u.id = o.user_id;
    SELECT * FROM users u FULL JOIN orders o ON u.id = o.user_id;
    SQL
    )
    echo "$out"
    data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
    scv drive --data "$data" joins-restart <<'SQL'
    SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id;
    SQL
    ```

    Expect, in order:
    - INNER: `ann 100`, `ann 50`, `cy 75` (3 rows). Order 13 has no user, so it is absent.
    - LEFT: `ann 100`, `ann 50`, `bob NULL`, `cy 75` (4 rows). bob has no orders, so his right side is NULL.
    - LEFT with GROUP BY: `ann 2`, `bob 0`, `cy 1`. `COUNT(o.id)` skips the NULL.
    - Self-join: headers `name | name`, one row `ann | bob`.
    - `*`: headers `id | name | dept | id | user_id | total`, row `1 ann eng 10 1 100`.
    - `  x Column reference "id" is ambiguous`
    - `  x Invalid query: Parse error: FULL JOIN is not supported`
    - After the relaunch, the LEFT JOIN returns the same 4 rows from the stored tables.
- **`t.*`, a three-table chain, and scope errors.** In a new drive, run the CREATE and INSERT lines from the step above, then:

    ```sql
    CREATE TABLE depts (code TEXT, title TEXT);
    INSERT INTO depts VALUES ('eng', 'Engineering');
    SELECT o.*, u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 12;
    SELECT u.name, o.total, d.title FROM users u JOIN orders o ON u.id = o.user_id LEFT JOIN depts d ON u.dept = d.code ORDER BY o.id;
    SELECT * FROM users u JOIN orders o ON u.id = d.code JOIN depts d ON u.dept = d.code;
    SELECT users.name FROM users u;
    SELECT * FROM users JOIN users ON users.id = users.id;
    SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id;
    SELECT * FROM users CROSS JOIN orders;
    SELECT * FROM users, orders;
    ```

    Expect, in order:
    - `t.*`: headers `id | user_id | total | name`, row `12 3 75 cy`.
    - Chain: `ann 100 Engineering`, `ann 50 Engineering`, `cy 75 NULL`.
    - `  x Missing FROM-clause entry for table d`. The first ON cannot see a later table.
    - `  x Missing FROM-clause entry for table users`. The alias hides the table name.
    - `  x Table name "users" specified more than once; give one of them an alias`
    - `RIGHT JOIN is not supported`, `CROSS JOIN is not supported`, and `Unexpected Comma after end of statement`, each after `  x Invalid query: Parse error: `.

## Gotchas

- Check values from both sides. A wrong column offset gives NULLs, not an error. A LEFT JOIN whose right side is all NULL is suspicious unless the data explains it.
- Output headers are bare column names (`id`, `id`), as in PostgreSQL. `ORDER BY id` over two different `id` outputs fails with `ORDER BY "id" is ambiguous`. Use `ORDER BY o.id` or a position.
- RIGHT, FULL, CROSS, and OUTER are reserved words. `FROM users right` fails as an unsupported join, not as an alias.
- WHERE runs after the join. A right-side condition in WHERE removes the NULL rows of a LEFT JOIN. Put it in ON to keep them.
- A quoted identifier with a dot (`"a.b"`) is split into `a.b`. This is a known limitation.
- EXPLAIN is not a REPL command. The tests in `src/sql/planner/explain.rs` cover join plans.
