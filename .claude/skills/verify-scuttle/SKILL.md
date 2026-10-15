---
name: verify-scuttle
description: Drive the scuttle-db SQL REPL (the `scuttle-db` binary) the way a user does and capture transcript evidence. Use to prove a parser, planner, executor or storage change works end to end, not just in unit tests. Covers CREATE/INSERT/SELECT/UPDATE/DELETE, errors, and on-disk persistence across restarts.
---

# Verify scuttle-db

The user-facing surface is a line-oriented SQL REPL (`src/bin/scuttle.rs`). It reads stdin one line
at a time, prints the prompt `DB: `, executes each line as one statement, and stores tables under
`./db/` **relative to the working directory**. Piped stdin works, so no PTY or tmux is needed.

Other surfaces, not covered here:
- `test-repl` (`src/bin/test-repl.rs`) is the same REPL with a startup banner.
- `scuttle-wasm/` holds the WebAssembly bindings. `EXPLAIN` exists only there and as
  `Database::explain` in the library. The REPL rejects `EXPLAIN ...` as a parse error.

All helpers live in `.claude/skills/verify-scuttle/scripts/`. Run them from anywhere in the repo.

## Launch

The REPL is short-lived, so there is no server to start. Build once:

```bash
cargo build --bin scuttle-db
```

Ready means `target/debug/scuttle-db` exists and is newer than every file in `src/`. `doctor.sh`
checks this. A fresh data dir needs no setup: the first `CREATE TABLE` creates `db/__catalog__.table`.
An empty dir prints `No catalog exists. Creates when adding a table` once at startup.

Each drive is its own process in its own data dir, so runs never share state unless you pass `--data`.

## Doctor

```bash
.claude/skills/verify-scuttle/scripts/doctor.sh
```

Read-only. It prints the commit (and whether the tree is dirty) and the binary path. It ends with
`doctor: OK`, or `doctor: NOT READY` plus the fix (build missing, or a source file newer than the
binary). Run it before driving and whenever output looks wrong.

## Drive

```bash
# Fresh, isolated data dir. SQL from a file or a heredoc.
.claude/skills/verify-scuttle/scripts/drive.sh NAME path/to/file.sql
.claude/skills/verify-scuttle/scripts/drive.sh NAME <<'SQL'
CREATE TABLE users (id INT, name TEXT, age INT);
INSERT INTO users VALUES (1, 'ann', 30);
SELECT * FROM users;
SQL

# Reuse a data dir, e.g. to restart and prove persistence. drive.sh prints `--- exit=N data=DIR`.
out=$(.claude/skills/verify-scuttle/scripts/drive.sh NAME <<'SQL'
CREATE TABLE users (id INT);
SQL
)
data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
.claude/skills/verify-scuttle/scripts/drive.sh --data "$data" NAME-restart <<'SQL'
SELECT * FROM users;
SQL
```

How the REPL behaves (all observed):
- One statement per line. A trailing `;` is optional. Blank lines are ignored (the prompt is just
  printed again). `drive.sh` still strips blank lines from the input, so when you pipe to the
  binary by hand, blank lines are harmless.
- Prompt `DB: ` comes before each result, so result N follows the Nth `DB: `.
- Success outputs: `OK` (CREATE), `N row(s) affected` (INSERT/UPDATE/DELETE), and a dashed table
  ending `(N rows)` (SELECT). NULL prints as `NULL`.
- Errors print to **stdout** as a miette report starting `  x `, e.g.
  `  x Table not found: nope`. The process keeps going and exits 0. A failed statement is
  visible only in the transcript, never in the exit code.
- `exit` or `:q` ends the session and prints `Exiting Scuttle`. End of input (EOF) does the same,
  so a script without `exit` also ends cleanly. `drive.sh` still appends `exit` and wraps the run
  in `timeout 20`. Exit code 124 means it hung.

Never run the binary from the repo root. It would read and write the user's own `./db/`.
`drive.sh` refuses to.

The feature map in `features/` lists the journeys and the exact SQL that proves each one.

## Evidence

Each run writes `target/verify-scuttle/evidence/<stamp>-NAME/`:
- `input.sql`: exactly what was sent, including the appended `exit`
- `transcript.txt`: full stdout and stderr
- `exit_code`, and `data_dir` (where the tables went)
- `db_files.txt`: listing of the `db/` dir after the run, the on-disk side effect

`drive.sh` prints the transcript, then the evidence path as its last line.

Proof standards:
- Drive the real binary through stdin. Calling `Database::execute_query` from a test is a unit
  test, not this proof.
- Show the action and the resulting state. After an INSERT/UPDATE/DELETE, run a `SELECT` that shows
  the changed rows. `N row(s) affected` alone is not proof.
- For storage changes, prove persistence by restarting: a second `drive.sh --data` run against the
  same dir must return the data.
- For error paths, quote the exact `  x ...` line, and show that the next statement still works.
- Compare against the expected output in the feature file. Any extra `  x ` line in a transcript
  you expected to be clean is a failure.

## Cleanup

```bash
.claude/skills/verify-scuttle/scripts/cleanup.sh
```

Removes `target/verify-scuttle/data/` (scratch tables). It keeps `target/verify-scuttle/evidence/`.
No long-lived processes are started, and `timeout` bounds each run, so nothing needs killing.
If you started a REPL by hand, kill it by its PID, never by name. The user may have their own
REPL open. `cargo clean` deletes the evidence too, so copy anything you need to keep first.
