# Errors and unsupported syntax

A user who types a bad statement gets a readable error, and the session continues.

## Sub-features

- `unknown-table` a query on a table that does not exist.
- `parse-error` malformed SQL, trailing junk, or two statements on one line.
- `unsupported` syntax the engine does not handle yet: RIGHT, FULL, and CROSS joins, EXPLAIN, and DROP.
- `recovery` the next statement after an error runs normally.
- `quit` `exit`, `:q`, and EOF end the session.

## How to get to it (user POV)

- Type a bad statement at the `DB: ` prompt. The error is a miette report that starts `  x `.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Send three bad statements, then a good one.** Run:

    ```sh
    export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
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
- **Trailing junk, two statements on a line, other unsupported syntax, then `:q`.** Run:

    ```sh
    export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
    out=$(scv drive errors-more <<'SQL'
    CREATE TABLE users (id INT);
    SELECT * FROM users u extra;
    SELECT * FROM users; SELECT * FROM users;
    SELECT * FROM users u RIGHT JOIN users v ON u.id = v.id;
    SELECT * FROM users u CROSS JOIN users v;
    DROP TABLE users;
    SELECT COUNT(*) FROM users;
    :q
    CREATE TABLE after_q (id INT);
    SQL
    )
    echo "$out"
    data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
    echo 'SELECT * FROM after_q;' | scv drive --data "$data" errors-after-q
    ```

    Expect `OK`, then these lines in `errors.txt`, each prefixed `  x Invalid query: Parse error: `:
    - `Unexpected Identifier("extra") after end of statement`
    - `Unexpected Keyword(Select) after end of statement`
    - `RIGHT JOIN is not supported`, then `CROSS JOIN is not supported`
    - `Unsupported keyword: Drop`
    - Then the COUNT returns `0`, and `Exiting Scuttle` follows `:q` with no `OK` for `after_q`. The second run prints `  x Table not found: after_q`, so `:q` stopped the session.

## Gotchas

- Errors go to stderr. A piped run whose statements failed exits 1, so `exit_code` is 1 for this case by design. The session still ran every statement after the failure.
- A blank line is not an error. It prints another `DB: ` prompt.
- EOF ends the session like `exit`.
- `exit` and `:q` must be the whole line, in lower case. `EXIT` and `exit;` are parsed as SQL and fail.
- A statement cannot span lines. Each line is parsed alone, so a line that starts `FROM t` fails with `Unsupported keyword: From`.
- `scv drive` drops blank lines, so it cannot show a blank line re-prompting. The unit tests in `src/repl.rs` cover that.
