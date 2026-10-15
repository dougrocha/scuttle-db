# scuttle-db verification map

This directory is the maintained source for verifying what a user can do in the scuttle-db REPL.
Read this index before driving, then use the matching feature file as the recipe.

## Baseline preconditions

- `cargo build --bin scuttle-db` has run, and `scripts/doctor.sh` prints `doctor: OK`.
- Every journey starts in a fresh data dir: `scripts/drive.sh NAME` with no `--data`.
- Never run the binary from the repo root. `./db/` there is the user's own data.

## Features

| ID | File | What it covers |
|---|---|---|
| `ddl-insert` | [create-and-insert.md](create-and-insert.md) | CREATE TABLE, INSERT (multi-row, column list, NULL), type checks |
| `select` | [select-queries.md](select-queries.md) | projection, WHERE, IS NULL, ORDER BY, LIMIT/OFFSET, GROUP BY, aggregates, aliases, arithmetic |
| `mutate` | [update-delete.md](update-delete.md) | UPDATE ... SET ... WHERE, DELETE ... WHERE |
| `persist` | [persistence.md](persistence.md) | data and catalog survive a REPL restart |
| `joins` | [joins.md](joins.md) | INNER/LEFT JOIN, table aliases, qualified columns, self-joins, chains, `*`/`t.*`, ambiguity errors |
| `errors` | [errors.md](errors.md) | unknown table, parse errors, unsupported syntax; session keeps going |

Not supported yet, so there is no journey: RIGHT/FULL/CROSS joins (rejected with an error), and
EXPLAIN in the REPL (it exists only as `Database::explain` and in `scuttle-wasm`). When one of these
lands, add a feature file for it.

## Driving conventions

- One SQL statement per line. `drive.sh` drops blank lines and appends `exit`.
- Treat SQL in the recipes as literal. The expected output is copied from real runs.
- Read results positionally: the Nth `DB: ` prompt comes before the Nth statement's output.
- Errors print `  x <message>` to stdout and the exit code stays 0, so check the transcript, not
  the exit code.

## Proof and skip reporting

- Report the evidence dir path printed by `drive.sh` for every journey you ran.
- A mutation is proven by a later `SELECT` showing the new state. A restart-dependent claim is
  proven by a second `drive.sh --data` run.
- Report a journey that cannot run with the exact statement and the `  x` line it produced.
  Don't swap in an easier statement and call it verified.
