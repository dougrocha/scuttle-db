---
name: verify-scuttle
description: Drive the scuttle-db SQL REPL (the `scuttle-db` binary) the way a user does, through piped stdin, and capture transcripts and on-disk files as proof. Use it to prove that a parser, planner, executor, or storage change works end to end, not only in unit tests. It covers CREATE, INSERT, SELECT, JOIN, UPDATE, DELETE, errors, and persistence across restarts.
---

# Verify scuttle-db

The user types SQL into a line-oriented REPL (`src/bin/scuttle.rs`, with the loop in `src/repl.rs`). It prints the prompt `DB: `, runs each line as one statement, and stores tables under `./db/` relative to its working directory (`--data <dir>` changes that, `--in-memory` writes nothing). Piped stdin works, so this skill needs no PTY or tmux.

The helper is `.claude/skills/verify-scuttle/bin/scv`, relative to the repo root. Run `scv help` for every subcommand. The examples call it as `scv`. Bash tool calls do not keep `PATH`, so start every call with:

```sh
export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
```

Every recipe in `features/` starts with that line.

The feature recipes are in `features/`. Read `features/README.md` first.

Other surfaces are not covered here:

- `test-repl` (`src/bin/test-repl.rs`) is the same REPL with a startup banner.
- `scuttle-wasm/` holds the WebAssembly bindings. `EXPLAIN` exists only there and as `Database::explain` in the library. The REPL rejects `EXPLAIN ...` as a parse error.

## 1. Launch

Nothing long-lived to launch. Each `scv drive` starts the REPL, pipes the SQL, and waits for it to exit. Build first:

```sh
cargo build --bin scuttle-db
```

- WARNING: Never run the binary from the repo root. It reads and writes the user's own `./db/`. `scv drive` refuses to.
- Ready means `scv doctor` prints `ok` on every line.
- Run `scv start <feature-id>` before you drive. It makes the run directory and prints its path. Later commands use the last started run.
- WARNING: When another agent may be verifying at the same time, prefix every `scv` call with `SCV_RUN=<path start printed>`. Otherwise both agents write into the newest run.

A fresh data dir needs no setup. The first `CREATE TABLE` creates `db/__catalog__.table`. An empty dir prints `No catalog exists. Creates when adding a table` once at startup.

## 2. Doctor

Run `scv doctor` before you drive, and again when a result looks wrong. It is read only. It checks:

- `target/debug/scuttle-db` exists.
- No `.rs` file under `src/`, and neither `Cargo.toml` nor `Cargo.lock`, is newer than the binary. If one is, rebuild.
- `fd`, `rg`, `eza`, and `timeout` are installed.

It prints the commit, whether the tree is dirty, and the binary path.

## 3. Drive

Use real stdin. Calling `Database::execute_query` from a test is a unit test, not this proof.

| User action | Command | Notes |
|---|---|---|
| Type statements in a new database | `scv drive <case> <<'SQL' ... SQL` | Fresh data dir under `<run dir>/data/`. |
| Run a saved script | `scv drive <case> path/to/file.sql` | |
| Quit and relaunch in the same directory | `scv drive --data <dir> <case> <<'SQL' ... SQL` | Take `<dir>` from the `--- exit=N data=<dir>` line of the earlier drive. `scv` refuses a dir with no `db/`. |
| Kill the REPL before it quits | `scv drive --kill-after <secs> <case> <<'SQL' ... SQL` | Sends no `exit`, kills the REPL after `<secs>`, and exits 124. |

Capture the data dir of one drive for a restart:

```sh
export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
out=$(scv drive persist-1 <<'SQL'
CREATE TABLE users (id INT);
SQL
)
data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
scv drive --data "$data" persist-2 <<'SQL'
SELECT * FROM users;
SQL
```

How the REPL behaves:

- It runs one statement per line. A trailing `;` is optional. A second statement on the same line is a parse error, and a statement cannot span lines. A blank line prints the prompt again. `scv drive` drops blank lines anyway, so the transcript stays positional.
- The Nth `DB: ` prompt comes before the Nth statement's output.
- Success prints `OK` (CREATE), `N row(s) affected` (INSERT, UPDATE, DELETE), or a dashed table that ends `(N rows)` (SELECT). NULL prints as `NULL`.
- An error prints to stderr as a miette report that starts `  x `, for example `  x Table not found: nope`. The session continues. `scv drive` merges stderr into the transcript.
- With piped stdin, the exit code is 1 when any statement failed and 0 otherwise. On a terminal it is always 0.
- `exit`, `:q`, and EOF end the session and print `Exiting Scuttle`. `exit` and `:q` must be the whole line, in lower case. `scv drive` appends `exit` and wraps the run in `timeout 20`. Exit code 124 without `--kill-after` means it hung.

Each drive without `--data` starts in its own data dir, so cases never share state. `scv reset` is needed only to free the scratch dirs.

`scv drive` exits with the REPL's exit code: 1 when a statement failed. It exits 2 on a harness error, such as no run started, a missing SQL file, empty input, or a bad `--data` dir. Exit 2 means nothing was verified.

## 4. Evidence

Each drive writes `<run dir>/<case>/` (with `.2`, `.3` appended for repeats):

| File | Content |
|---|---|
| `input.sql` | Exactly what was sent, including the appended `exit`. |
| `errors.txt` | Each `x` report joined onto one line. Match error text here. |
| `transcript.txt` | Full stdout and stderr. |
| `exit_code` | The REPL's exit code. |
| `data_dir` | Where the tables went. |
| `db_files.txt` | A listing of the data dir's `db/` after the run: the on-disk side effect. |

`scv drive` prints the transcript, then `--- exit=N data=<dir>`, then the evidence path as its last line.

- Show the action and the resulting state. After an INSERT, UPDATE, or DELETE, run a `SELECT` that shows the changed rows. `N row(s) affected` alone is not proof.
- For a storage change, prove persistence with a second `scv drive --data` against the same dir.
- For an error path, quote the line from `errors.txt`, and show that the next statement still works.
- Compare against the expected output in the feature file. An extra `  x ` line in a transcript you expected to be clean is a failure.
- Write each result with the expected value: `<feature-id>, expect <value>: <actual>`.
- Name the feature ID and the evidence path for every case you ran.

## 5. Cleanup

Run `scv cleanup` at the end and after each failed attempt. It deletes the run's `data/` directory and nothing else. It keeps the evidence in the run directory.

`scv` starts no long-lived process, and `timeout` bounds each run, so nothing needs killing. If you started a REPL by hand, kill it by its PID, never by name. The user may have their own REPL open.

## 6. Gotchas

- **The data dir is the working directory.** A relaunch in another dir sees an empty database. That looks like data loss but is not. Pass `--data`.
- **`./db/` in the repo root is the user's data.** Do not read it as test output, and do not delete it.
- **miette wraps long messages in `transcript.txt`.** A message can break onto a `  | ` line. Match error text against `errors.txt`.
- **Column widths vary with content.** Match cell values, not dash counts.
- **A stale binary passes old behavior.** If `scv doctor` reports a newer source or Cargo file, rebuild before you believe a transcript.
