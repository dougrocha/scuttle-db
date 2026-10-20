# Query rows with SELECT

A user reads rows back from one table with filtering, sorting, paging, grouping, and computed columns.

## Sub-features

- `select-star` and `select-columns` `*` or a column list.
- `where` comparisons, `IS NULL`, `AND`/`OR`.
- `order-by` by expression, `ASC`/`DESC`, alias, or 1-based position.
- `limit-offset` `LIMIT n OFFSET m`.
- `group-aggregate` `GROUP BY` with `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`.
- `alias-arith` `expr AS name` and arithmetic in the select list.

## How to get to it (user POV)

- Type `SELECT ... FROM <one table> ...` at the `DB: ` prompt.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Query one table every way.** Run:

    ```sh
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

## Gotchas

- Multi-table queries are in [joins.md](joins.md).
- An unnamed expression column is headed `?column?`, as in PostgreSQL.
