# Data survives a restart

Tables and rows live in `db/<table>.table` files plus a `db/__catalog__.table` system catalog under
the working directory. A user who quits and relaunches the REPL from the same directory sees the same data.

## Sub-features

- `persist-rows`: rows written in one session are readable in the next.
- `persist-catalog`: table schemas are reloaded from `__catalog__.table`. Column names and types survive.
- `persist-mutations`: UPDATE/DELETE results survive.

## How to get to it (user POV)

Run the REPL, write data, type `exit`, then run the REPL again from the same directory.

## Driving it with drive.sh

```bash
S=.claude/skills/verify-scuttle/scripts
out=$($S/drive.sh persist-1 <<'SQL'
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'ann'), (2, 'bob');
DELETE FROM users WHERE id = 2;
SQL
)
echo "$out"
data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
$S/drive.sh --data "$data" persist-2 <<'SQL'
SELECT * FROM users;
SQL
```

Expected: the first run's `db_files.txt` lists `__catalog__.table` and `users.table`. The second
run's transcript has no `No catalog exists` line, which shows the catalog was found. Its SELECT
returns only `1 | ann`.

## Gotchas

- The data dir is the process's working directory. A second run in a different dir sees an empty
  database, which looks like data loss but isn't.
- Each `.table` file is at least 8 KB (one page), even when empty.
