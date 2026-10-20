# Query rows with SELECT

A user reads rows back from one table with filtering, sorting, paging, grouping, and computed columns.

## Sub-features

- `select-star` and `select-columns` `*` or a column list.
- `where` `= != < <= > >=`, `IS [NOT] NULL`, `AND`/`OR`, parentheses.
- `order-by` by expression, `ASC`/`DESC`, alias, 1-based position, or a column not selected. NULL sorts last with ASC.
- `limit-offset` `LIMIT n OFFSET m`, or OFFSET alone.
- `group-aggregate` `COUNT(*)`, `COUNT(x)`, `SUM`, `AVG`, `MIN`, `MAX`, with or without `GROUP BY`.
- `alias-arith` `expr AS name` and arithmetic in the select list.

## How to get to it (user POV)

- Type `SELECT ... FROM <one table> ...` at the `DB: ` prompt.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Query one table every way.** Run:

    ```sh
    export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
    scv drive select <<'SQL'
    CREATE TABLE users (id INT, name TEXT, age INT, dept TEXT);
    INSERT INTO users VALUES (1, 'ann', 30, 'eng'), (2, 'bob', NULL, 'ops'), (3, 'cy', 25, 'eng'), (4, 'di', 41, 'ops');
    SELECT * FROM users;
    SELECT name, age FROM users WHERE age > 26 ORDER BY age DESC;
    SELECT name FROM users WHERE age IS NULL;
    SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1;
    SELECT dept, COUNT(*), AVG(age), MAX(age) FROM users GROUP BY dept ORDER BY dept;
    SELECT name AS who, age + 1 FROM users WHERE dept = 'eng' ORDER BY 1;
    SQL
    ```

    Expect, in order:
    - `SELECT *` returns 4 rows. bob's age prints `NULL`.
    - `age > 26 ORDER BY age DESC` returns `di 41`, then `ann 30`. bob is excluded because NULL is not > 26.
    - `IS NULL` returns `bob` only.
    - `LIMIT 2 OFFSET 1` returns ids `2`, `3`.
    - Group by returns `eng | 2 | 27.5 | 30` and `ops | 2 | 41 | 41`. COUNT(*) counts bob. AVG and MAX skip his NULL.
    - Alias and arithmetic: headers `who | ?column?`, rows `ann 31`, `cy 26`.
- **NULL ordering, empty aggregates, integer division, and unsupported syntax.** Run:

    ```sh
    export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
    scv drive select-more <<'SQL'
    CREATE TABLE users (id INT, name TEXT, age INT, dept TEXT);
    INSERT INTO users VALUES (1, 'ann', 30, 'eng'), (2, 'bob', NULL, 'ops'), (3, 'cy', 25, 'eng'), (4, 'di', 41, 'ops');
    SELECT name FROM users WHERE age IS NOT NULL AND (age < 26 OR dept != 'eng') ORDER BY name;
    SELECT name, age FROM users ORDER BY age;
    SELECT name, age FROM users ORDER BY age DESC;
    SELECT COUNT(*), SUM(age), MIN(age) FROM users WHERE age > 100;
    SELECT dept, SUM(age) / COUNT(*) AS per FROM users GROUP BY dept ORDER BY per DESC;
    SELECT name n, age * 2 - 1 FROM users ORDER BY id DESC LIMIT 2;
    SELECT u.name FROM users u ORDER BY u.id OFFSET 3;
    SELECT DISTINCT dept FROM users;
    SELECT name FROM users WHERE NOT age > 26;
    SQL
    ```

    Expect, in order:
    - `cy`, `di`.
    - `cy 25`, `ann 30`, `di 41`, `bob NULL`. NULL sorts last with ASC.
    - `bob NULL`, `di 41`, `ann 30`, `cy 25`. NULL sorts first with DESC.
    - Headers `count | sum | min`, one row `0 | NULL | NULL`.
    - Headers `dept | per`, rows `eng 27`, `ops 20`. Integer division truncates.
    - Headers `n | ?column?`, rows `di 81`, `cy 49`. ORDER BY uses a column that is not selected.
    - `di`.
    - Two `  x Invalid query: Parse error: Expected a column or value, but found ...` lines. DISTINCT and NOT are not supported.

## Gotchas

- Multi-table queries are in [joins.md](joins.md).
- These are not supported and fail to parse: `DISTINCT`, `HAVING`, prefix `NOT`, `LIKE`, `IN`, `BETWEEN`, `<>` (use `!=`), and negative literals such as `-1` (write `0 - 1`).
- AND and OR treat NULL as false. They are not three-valued.
- An unnamed expression column is headed `?column?`, as in PostgreSQL.
