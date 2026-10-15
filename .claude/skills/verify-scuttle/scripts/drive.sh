#!/usr/bin/env bash
# Drive the scuttle-db REPL with SQL from a file (or stdin) in an isolated data dir.
#
#   drive.sh NAME [SQL_FILE]            fresh data dir, named NAME
#   drive.sh --data DIR NAME [SQL_FILE] reuse DIR (e.g. to prove persistence across restarts)
#
# Writes evidence to target/verify-scuttle/evidence/<stamp>-NAME/ and prints that path last.
set -euo pipefail
repo="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
bin="$repo/target/debug/scuttle-db"
root="$repo/target/verify-scuttle"

data=""
if [[ "${1:-}" == "--data" ]]; then data="$2"; shift 2; fi
name="${1:?usage: drive.sh [--data DIR] NAME [SQL_FILE]}"
sql="${2:-/dev/stdin}"

[[ -x "$bin" ]] || { echo "binary missing; run: cargo build --bin scuttle-db" >&2; exit 2; }
if [[ -z "$data" ]]; then
  mkdir -p "$root/data"
  data="$(mktemp -d "$root/data/$name.XXXX")"
fi
mkdir -p "$data"
data="$(cd "$data" && pwd)"
[[ "$data" == "$repo" ]] && { echo "refusing to run in the repo root (would use the user's ./db)" >&2; exit 2; }

ev="$root/evidence/$(date +%Y%m%d-%H%M%S)-$name"
mkdir -p "$ev"
# Blank lines are harmless to the REPL now, but drop them so the transcript stays positional.
# EOF also ends the REPL now; the explicit exit stays so the transcript shows a clean end,
# and the timeout still catches a hang.
{ rg -v '^\s*$' "$sql" || true; echo exit; } > "$ev/input.sql"

set +e
(cd "$data" && timeout 20 "$bin" < "$ev/input.sql" > "$ev/transcript.txt" 2>&1)
code=$?
set -e

echo "$code" > "$ev/exit_code"
echo "$data" > "$ev/data_dir"
eza -la --no-user --no-time "$data/db" > "$ev/db_files.txt" 2>&1 || echo "(no db dir)" > "$ev/db_files.txt"

cat "$ev/transcript.txt"
echo "--- exit=$code data=$data"
[[ $code -eq 124 ]] && echo "--- TIMEOUT: REPL did not exit (missing 'exit' or a hang)"
echo "$ev"
