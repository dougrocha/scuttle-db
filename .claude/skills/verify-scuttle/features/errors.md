# Errors and unsupported syntax

A user who types a bad statement gets a readable error, and the session continues.

## Sub-features

- `unknown-table`: a query on a table that doesn't exist.
- `parse-error`: malformed SQL or trailing junk.
- `unsupported`: syntax the engine doesn't handle yet (RIGHT/FULL/CROSS joins, EXPLAIN).
- `recovery`: the next statement after an error runs normally.

## How to get to it (user POV)

Type a bad statement at the `DB: ` prompt. The error renders as a miette report starting `  x `.

## Driving it with drive.sh

```bash
.claude/skills/verify-scuttle/scripts/drive.sh errors <<'SQL'
CREATE TABLE users (id INT);
SELECT * FROM nope;
SELECT * FROM users u FULL JOIN users v ON u.id = v.id;
EXPLAIN SELECT * FROM users;
SELECT COUNT(*) FROM users;
SQL
```

Expected, one `  x` line per bad statement:
- `  x Table not found: nope`
- `  x Invalid query: Parse error: FULL JOIN is not supported`
- `  x Invalid query: Parse error: Unexpected token: Identifier("EXPLAIN")`
- then the COUNT returns `0`, so the session recovered.

## Gotchas

- Errors go to stdout, and the exit code is still 0.
- An empty line is a parse error (`Unexpected end of input`). `drive.sh` strips blank lines for that
  reason. Driving the binary by hand doesn't.
- EOF ends the session like `exit`. (Before the fix it looped forever on `Unexpected end of input`.)
- miette wraps long messages, so match on a substring, not a whole line.
