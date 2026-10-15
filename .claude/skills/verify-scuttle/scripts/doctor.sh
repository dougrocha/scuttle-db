#!/usr/bin/env bash
# Read-only check: is the scuttle-db REPL binary built from the current source?
set -u
repo="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
bin="$repo/target/debug/scuttle-db"
ok=1
echo "repo:   $repo"
echo "commit: $(git -C "$repo" rev-parse --short HEAD)$(git -C "$repo" diff --quiet || echo ' (dirty)')"
if [[ -x "$bin" ]]; then
  echo "binary: $bin"
  stale=$(fd -t f -e rs . "$repo/src" -x bash -c '[[ "$1" -nt "$2" ]] && echo "$1"' _ {} "$bin" | head -1)
  if [[ -n "$stale" ]]; then echo "STALE:  $stale is newer than the binary; run: cargo build --bin scuttle-db"; ok=0; fi
else
  echo "MISSING: $bin; run: cargo build --bin scuttle-db"; ok=0
fi
if [[ -d "$repo/db" ]]; then
  echo "note:   $repo/db exists (the user's REPL data). drive.sh never touches it."
fi
(( ok )) && echo "doctor: OK" || { echo "doctor: NOT READY"; exit 1; }
