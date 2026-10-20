# Errors and unsupported syntax

A user who types a bad statement gets a readable error, and the session continues.

## Sub-features

- `unknown-table` a query on a table that does not exist.
- `parse-error` malformed SQL or trailing junk.
- `unsupported` syntax the engine does not handle yet: RIGHT, FULL, and CROSS joins, and EXPLAIN.
- `recovery` the next statement after an error runs normally.

## How to get to it (user POV)

- Type a bad statement at the `DB: ` prompt. The error is a miette report that starts `  x `.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Send three bad statements, then a good one.** Run:

    ```sh
    scv drive errors <<'SQL'
    CREATE TABLE users (id INT);
    SELECT * FROM nope;
    SELECT * FROM users u FULL JOIN users v ON u.id = v.id;
    EXPLAIN SELECT * FROM users;
    SELECT COUNT(*) FROM users;
    SQL
    ```

    Expect one `  x` line for each bad statement:
    - `  x Table not found: nope`
    - `  x Invalid query: Parse error: FULL JOIN is not supported`
    - `  x Invalid query: Parse error: Unexpected token: Identifier("EXPLAIN")`
    - Then the COUNT returns `0`, so the session recovered.

## Gotchas

- Errors go to stderr. A piped run whose statements failed exits 1, so `exit_code` is 1 for this case by design. The session still ran every statement after the failure.
- A blank line is not an error. It prints another `DB: ` prompt.
- EOF ends the session like `exit`.
