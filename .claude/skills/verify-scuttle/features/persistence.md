# Data survives a restart

Tables and rows live in `db/<table>.table` files and a `db/__catalog__.table` system catalog under the working directory. A user who quits and relaunches the REPL from the same directory sees the same data, including tables that span many pages.

## Sub-features

- `persist-rows` rows written in one session are readable in the next.
- `persist-catalog` table schemas reload from `__catalog__.table`. Column names and types survive, including a table with no rows.
- `persist-mutations` UPDATE and DELETE results survive.
- `persist-multipage` a table larger than one 8 KB page is written and read back in full.
- `persist-kill` every finished statement is on disk, even when the process is killed before `exit`.

## How to get to it (user POV)

- Run the REPL, write data, type `exit` or press Ctrl-D, then run the REPL again from the same directory.
- Kill the REPL after a statement finishes, then run it again from the same directory.

## Driving it with scv

Preconditions:

- Baseline from `README.md`.

- **Write, quit, relaunch, read.** Run:

    ```sh
    export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
    out=$(scv drive persist-1 <<'SQL'
    CREATE TABLE users (id INT, name TEXT);
    INSERT INTO users VALUES (1, 'ann'), (2, 'bob');
    UPDATE users SET name = 'amy' WHERE id = 1;
    DELETE FROM users WHERE id = 2;
    CREATE TABLE empty_t (id INT);
    SQL
    )
    echo "$out"
    data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
    scv drive --data "$data" persist-2 <<'SQL'
    SELECT * FROM users;
    SELECT * FROM empty_t;
    SQL
    ```

    Expect the first run's `db_files.txt` to list `__catalog__.table` and `users.table` (8.2k each) and no `empty_t.table`. The second transcript has no `No catalog exists` line, so the catalog was found. Its first SELECT returns only `1 | amy`. The second returns headers and `(0 rows)`, with no error.
- **Fill several pages.** Run:

    ```sh
    export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
    out=$( { echo "CREATE TABLE big (id INT, pad TEXT);"
      for i in $(seq 1 600); do echo "INSERT INTO big VALUES ($i, '$(printf 'x%.0s' {1..50})');"; done
      echo "SELECT COUNT(*), MIN(id), MAX(id) FROM big;"; } | scv drive big-1 )
    echo "$out" | tail -8
    data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
    scv drive --data "$data" big-2 <<'SQL'
    SELECT COUNT(*), MIN(id), MAX(id) FROM big;
    DELETE FROM big WHERE id > 10;
    SELECT COUNT(*) FROM big;
    SQL
    ```

    Expect `600 | 1 | 600` in both runs. `big-1/db_files.txt` lists `big.table` at 49k (six pages). Then `590 row(s) affected` and a count of `10`.
- **Kill before exit.** Run:

    ```sh
    export PATH="$(git rev-parse --show-toplevel)/.claude/skills/verify-scuttle/bin:$PATH"
    out=$(scv drive --kill-after 2 kill-1 <<'SQL'
    CREATE TABLE k (id INT);
    INSERT INTO k VALUES (7);
    SQL
    )
    echo "$out"
    data=$(echo "$out" | rg -o 'data=(.*)' -r '$1')
    echo 'SELECT * FROM k;' | scv drive --data "$data" kill-2
    ```

    Expect the first drive to print `--- exit=124` after about 2 seconds, with no `Exiting Scuttle`. The second run returns `7`.

## Gotchas

- The data dir is the process's working directory. A second run in another dir sees an empty database. That looks like data loss but is not.
- CREATE TABLE writes only the catalog. `<table>.table` appears with the first INSERT, then grows in whole 8 KB pages.
- Dirty pages are written after every statement, even a failed one. There is no fsync, so only an OS crash can lose a finished statement.
- Tables are append-only. DELETE never frees space, so the file does not shrink.
- A row larger than one page fails with `A tuple of N bytes does not fit in a page.` and is not written.
