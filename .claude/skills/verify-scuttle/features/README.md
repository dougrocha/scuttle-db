# scuttle-db verification map

This directory is the maintained source for verifying what a user can do in the scuttle-db REPL. Read this index before you drive, then use the matching feature file as the recipe.

## Baseline preconditions

- `cargo build --bin scuttle-db` ran, and `scv doctor` prints `ok` on every line.
- `scv start <feature-id>` made the run's evidence directory.
- Every case starts in a fresh data dir: `scv drive <case>` with no `--data`.
- Nothing runs from the repo root. `./db/` there is the user's own data.

## Driving conventions

- Send one SQL statement per line through `scv drive`. It drops blank lines and appends `exit`.
- Treat the SQL in the recipes as literal. The expected output is copied from real runs.
- `1 | ann` in an expectation is shorthand for one row's cells. The REPL pads each cell to 12 characters, so match the values, not the exact line.
- Read results by position: the Nth `DB: ` prompt comes before the Nth statement's output.
- Errors print `  x <message>` to stderr. A piped run exits 1 when any statement failed. Read the transcript to see which one.
- Use `--data <dir>` only to relaunch in the same directory, never to share state between unrelated cases.

## Proof and skip reporting

- Pair each statement with its result: the rows, `N row(s) affected`, or the `  x` line.
- Prove a mutation with a later `SELECT` that shows the new state. Prove a restart claim with a second `scv drive --data` run.
- Record the feature ID and the evidence path with every result line.
- Report a case that cannot run with the exact statement and the `  x` line it produced. Do not swap in an easier statement and call it verified.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph about the user-visible behavior. Then it has exactly four H2 sections in this order:

1. `Sub-features` lists short IDs with one line each.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with scv` starts with `Preconditions:`. Each bullet pairs a user action with an exact command and the result you can observe.
4. `Gotchas` lists traps that waste or invalidate a run.

## Features

- [Create and insert](./create-and-insert.md) covers CREATE TABLE, INSERT with several rows, a column list, and NULL, type checks, column defaults, and timestamps.
- [Select queries](./select-queries.md) covers projection, WHERE, IS NULL, ORDER BY, LIMIT and OFFSET, GROUP BY with aggregates, aliases, and arithmetic.
- [Update and delete](./update-delete.md) covers UPDATE ... SET ... WHERE and DELETE ... WHERE.
- [Persistence](./persistence.md) covers rows, the catalog, and mutations surviving a REPL restart or a kill, and tables larger than one page.
- [Joins](./joins.md) covers INNER and LEFT JOIN, table aliases, qualified columns, self-joins, chains, `*` and `t.*`, and scope and ambiguity errors.
- [Errors](./errors.md) covers unknown tables, parse errors, unsupported syntax, `:q`, and the session continuing.

These are not supported yet, so they have no feature file: RIGHT, FULL, CROSS, and comma joins; EXPLAIN and DROP in the REPL; DISTINCT, HAVING, NOT, LIKE, IN, and BETWEEN; negative literals; and UNIQUE enforcement. Each is rejected with an error except UNIQUE, which parses and is ignored. When one lands, add a feature file or a recipe step and list it here.
