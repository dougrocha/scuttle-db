# Query rows with SELECT

A user reads rows back with filtering, sorting, paging, grouping and computed columns, all on a
single table.

## Sub-features

- `select-star` and `select-columns`: `*` or a column list.
- `where`: comparisons, `IS NULL`, `AND`/`OR`.
- `order-by`: by expression, `ASC`/`DESC`, alias, or 1-based position.
- `limit-offset`: `LIMIT n OFFSET m`.
- `group-aggregate`: `GROUP BY` with `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`.
- `alias-arith`: `expr AS name` and arithmetic in the select list.

## How to get to it (user POV)

Type a `SELECT ... FROM <one table> ...` at the `DB: ` prompt.

## Driving it with drive.sh

```bash
.claude/skills/verify-scuttle/scripts/drive.sh select <<'SQL'
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

Expected (verified 2026-10-14 at 34c5308):
- `SELECT *` returns 4 rows. bob's age prints `NULL`.
- `age > 26 ORDER BY age DESC`: `di 41`, then `ann 30`. bob is excluded because NULL is not > 26.
- `IS NULL`: `bob` only.
- `LIMIT 2 OFFSET 1`: ids `2`, `3`.
- Group by: `eng | 2 | 27.5 | 30` and `ops | 2 | 41 | 41`. COUNT(*) counts bob, AVG/MAX skip his NULL.
- Alias and arithmetic: columns `who | ?column?`, rows `ann 31`, `cy 26`.

## Gotchas

- Multi-table queries are covered in [joins.md](joins.md).
- An unnamed expression column is headed `?column?`, as in PostgreSQL.
- Column widths in the ASCII table vary with content. Match cell values, not exact dash counts.
