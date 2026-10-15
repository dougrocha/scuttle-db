# Join tables

A user combines rows from several tables with `INNER JOIN` or `LEFT [OUTER] JOIN`. Columns can be
qualified by table name or alias (`u.name`, `users.name`), and the same table can be joined with
itself under two aliases.

## Sub-features

- `inner-join`: `FROM a [INNER] JOIN b ON <expr>` keeps only matching pairs.
- `left-join`: `FROM a LEFT [OUTER] JOIN b ON <expr>` also keeps unmatched left rows, padded with NULL
  for every right column, even when the right table is empty.
- `aliases`: `FROM users u` / `FROM users AS u`. Once a table is aliased, only the alias is visible.
- `qualified-columns`: `t.col` works in the select list, ON, WHERE, GROUP BY, ORDER BY and aggregate arguments.
- `self-join`: `users a JOIN users b`.
- `chains`: `a JOIN b ON ... LEFT JOIN c ON ...` (left-deep). An ON clause sees only the tables to its left and its own table.
- `star`: `*` returns every column of every table in FROM order. `t.*` returns one table's columns.
- `errors`: ambiguous unqualified names, unknown or hidden aliases, duplicate table names, RIGHT/FULL/CROSS.

## How to get to it (user POV)

Type a `SELECT ... FROM a JOIN b ON ...` at the `DB: ` prompt.

## Driving it with drive.sh

```bash
S=.claude/skills/verify-scuttle/scripts
out=$($S/drive.sh joins <<'SQL'
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
$S/drive.sh --data "$data" joins-restart <<'SQL'
SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id;
SQL
```

Expected (verified 2026-10-14 at ebe8414):
- INNER: `ann 100`, `ann 50`, `cy 75` (3 rows). Order 13 has no user, so it is absent.
- LEFT: `ann 100`, `ann 50`, `bob NULL`, `cy 75` (4 rows). bob has no orders and is padded with NULL.
- LEFT + GROUP BY: `ann 2`, `bob 0`, `cy 1`. `COUNT(o.id)` skips the NULL pad.
- Self-join: headers `name | name`, one row `ann | bob`.
- `*`: headers `id | name | dept | id | user_id | total`, row `1 ann eng 10 1 100`.
- `  x Column reference "id" is ambiguous`
- `  x Invalid query: Parse error: FULL JOIN is not supported`
- After the restart, the LEFT JOIN returns the same 4 rows from the persisted tables.

## Gotchas

- Values from both sides must be checked. A wrong column offset produces NULLs, not an error, so a
  LEFT JOIN result that is all NULL on the right is suspicious unless the data explains it.
- Output headers are bare column names (`id`, `id`), as in PostgreSQL. `ORDER BY id` over two
  different `id` outputs is an ambiguity error. Use `ORDER BY o.id` or a position.
- WHERE runs after the join. A right-side condition in WHERE removes the NULL-padded rows of a
  LEFT JOIN. Put it in ON to keep them.
- A quoted identifier containing a dot (`"a.b"`) is split into `a.b`. This is a known limitation.
- EXPLAIN is not a REPL command. Join plans (`Join: left on u.id = o.user_id`, `Scan: users u`) are
  covered by the tests in `src/sql/planner/explain.rs`.
