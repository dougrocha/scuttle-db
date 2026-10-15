#!/usr/bin/env bash
# Remove scratch data dirs created by drive.sh. Evidence is kept.
set -euo pipefail
repo="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
root="$repo/target/verify-scuttle"
rm -rf "$root/data"
echo "removed $root/data"
echo "evidence kept in $root/evidence"
