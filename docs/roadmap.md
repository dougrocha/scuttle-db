# Roadmap: indexes on disk and a smarter planner

A plan to learn from as much as to build from. Each phase names the idea it teaches, the
ways real databases do it, the way we pick and why, then small steps that each end in
something you can test. Verify every phase with `EXPLAIN ANALYZE` on a 1M-row table
(`cargo run --release --example bench -- gen <dir>` makes one).

```
Phase 0  Foundations ── Phase 1  Top-N ── Phase 2  Memory limits ── Phase 3  On-disk B+tree ──┬── Phase 4  Catalog + CREATE INDEX
   (done)                  (done)                                                              ├── Phase 5  ORDER BY via index, IN / OR
                                                                                               └── Phase 6  Joins + statistics
```

**Why memory limits come before the B+tree:** the B+tree's pages go through the buffer
pool, and today the pool never evicts a page. A B+tree that has to fit in RAM loses half
the reason for putting it on disk.

**Why the B+tree comes before CREATE INDEX and ORDER BY:**
- The B+tree is keyed on (key, ctid), so non-unique indexes come with it.
- It has a leaf chain, so a lazy cursor that can stop early comes with it too.
- Building both in memory first would mean writing them twice.

---

## Phase 0: Foundations (done)

| Step | Done when |
|---|---|
| 0.1 Commit a benchmark (`examples/bench/`) that generates the 1M-row tables and runs queries in interleaved rounds, in one process so the index rebuild on open isn't timed. `/tmp` got wiped once already. | `cargo run --release --example bench -- run <dir>` prints the median time per query. |
| 0.2 Pass an execution context to `next()`. Nodes can then read an index while running, not only while being built. | All tests pass, and EXPLAIN ANALYZE output is unchanged. |
| 0.3 Make catalog rows deletable, then make `drop_table` delete its catalog row and file. Before this it only forgot the table in memory, so the table came back after a restart. | A test drops a table, reopens the database, and finds the table gone. |

**What happened:**
- `ExecCtx` holds only the pool for now. Nothing reads `tables` until Phase 3, and an unread field only gets a compiler warning.
- The benchmark's interleaving evens out noise from the machine, but not effects from earlier queries in the same process. A query can run slower after heavy queries. To compare one query across two builds, run it on its own.

---

## Phase 1: Top-N for `ORDER BY … LIMIT n` (done)

**Idea:** to keep the best n rows out of M, you don't need to sort all M.

| Approach | Used by | Trade-off |
|---|---|---|
| Bounded max-heap, with the bound passed from Limit down into Sort | Postgres (`top-N heapsort`) | M·log n time, n rows in memory. Slow when n is close to M |
| A separate TopN node made by an optimizer rule | DuckDB, DataFusion | The plan says plainly what runs; easier to test |
| Buffer rows and cut back to the best n with `select_nth_unstable` every 2n rows | — | Constant time per row on average, at most 2n rows in memory |
| Running threshold: drop rows worse than the current n-th before storing them | DuckDB, DataFusion | An add-on to either of the above |

**What we built:**
- A TopN node, made by a rule from `Limit(Projection(Sort))` with `k = limit + offset`. The Limit stays on top to skip the offset.
- A buffer cut by `select_nth`, plus a running threshold. Input position breaks ties, so results match the stable Sort.

The first version used a heap. It was fastest for small k, but `LIMIT 10 OFFSET 990000` took 1093 ms against 394 ms for a plain sort, so it was replaced:

| Query on 1M rows | Sort (before) | Heap | Buffer + select_nth (kept) |
|---|---|---|---|
| `LIMIT 10` | 371 ms | 85 ms | ~95 ms |
| `LIMIT 100000` | 366 ms | 286 ms | ~200 ms |
| `LIMIT 10 OFFSET 990000` | ~394 ms | 1080 ms | ~540 ms |

When k is close to M, TopN is still slower than a plain sort, because it compares the extra input position on every row.

**Read:** CMU 15-445 sorting lecture; DuckDB "TopN" blog post.

---

## Phase 2: Memory limits (medium)

**Idea:** today three things hold everything in memory, and on a 1M-row table you can see each one:

| What | Where | Peak memory on 1M rows |
|---|---|---|
| The buffer pool never evicts, so every page read stays cached | `src/storage/buffer_pool.rs` | 330 MB after one scan |
| Results are collected into a `Vec` before returning | `handle_select`, `src/db/database.rs` | +290 MB for `SELECT *` |
| Sort holds every row it sorts | `SortExec` | +290 MB for `ORDER BY` with no LIMIT |

A table bigger than RAM crashes on the first scan. This phase puts a cap on each one.

### 2.1 Buffer pool eviction

**Done.** Clock sweep with a usage count up to 5, the dirty flag on each slot, `get_page_mut` marks pages dirty, `--cache-pages` (default 16,384), and page hits and reads per node in EXPLAIN ANALYZE. On 1M rows with no index, peak memory went from 41 MB to 4 MB with a 100-page cache. The ~170 MB left with `users_pk` is its in-memory index, which Phase 3 moves to pages.

| Policy | Used by | Trade-off |
|---|---|---|
| LRU | textbooks | A single big scan pushes out every useful page ("sequential flooding") |
| **Clock sweep**: one usage counter per page, a hand that sweeps and decrements | Postgres (counter up to 5) | Close to LRU, cheap to maintain |
| Midpoint insertion: new pages enter the middle of the LRU list | MySQL InnoDB | Scans don't push out pages that are used often |
| Ring buffer for big sequential scans | Postgres (256 KB ring) | Combine with any of the above |

**Pick:** clock sweep.
- **Pinning:** a page in use can't be evicted. Today the pool is lent for one `next()` call at a time, which helps.
- **Dirty pages** are written out before eviction.

**Verify:**
- Cap the pool at 1,000 pages (8 MB) and scan the 1M-row table. The rows hash the same, and peak memory stays near the cap.
- Measure the slowdown from re-reading pages.

### 2.2 Stream results out of `execute_query`

**Done.** `execute_query_streaming` hands rows to a callback a batch at a time, and the REPL prints each batch as it arrives, with columns sized from the first batch like psql with `FETCH_COUNT`. `SELECT *` on 1M rows went from 356 MB to 4 MB peak, and printing it from 0.63 s to 0.43 s. `ORDER BY` still holds every row in the Sort until 2.3.

Pass batches to a callback, or return an iterator, instead of building a `Vec`. Keep the current `execute_query` as a wrapper that collects, for the browser build and the tests.

**Verify:** `SELECT *` on 1M rows no longer adds 290 MB, and the CLI prints the first rows before the scan finishes.

### 2.3 External merge sort

**Done.** `SortExec` writes a sorted run to a temp file in `<data>/tmp` each time its rows reach the budget (`--sort-mem`, default 64 MB), then merges the runs with a min-heap and streams the result a batch at a time. Runs use their own tagged encoding, not the table page format, because a sort's rows may come from a join or an aggregate and match no table. Ties go to the earlier run, so the sort stays stable. EXPLAIN ANALYZE ends the Sort line with `sort: in memory` or `sort: external merge, N runs`. On 1M rows, `ORDER BY age` used about 333 MB above the startup floor before. It now uses about 130 MB at 64 MB, 39 MB at 16 MB, and 8 MB at 1 MB, and runs slightly faster (1.19 s down to 1.03 s at 1 MB). The budget is an estimate that misses malloc's overhead, so the real use is about twice the budget. The output matches the in-memory sort row for row.

1. Sort until the rows held reach a memory budget (Postgres calls it `work_mem`).
2. Write each sorted run to a temporary file, using the same row encoding as the table pages.
3. Merge the runs with a min-heap of each run's next row, holding one page per run.

With B pages of memory, a sort of N pages takes 1 + ⌈log_{B−1}(N/B)⌉ passes. That's 2 for nearly anything here.

**Rules:**
- An in-memory database (the browser build) has nowhere to spill, so it keeps sorting in memory.
- TopN already holds at most 2k rows.

**Verify:**
- With a small budget, `ORDER BY age` on 1M rows gives the same hash as the in-memory sort, and peak memory stays near the budget.
- EXPLAIN ANALYZE says how many runs it wrote, the way Postgres prints `Sort Method: external merge`.

**Teaches:** page replacement, pinning, sequential flooding, and why external sorting is counted in I/O passes.

**Read:**
- CMU 15-445 buffer pool and sorting lectures
- [Postgres buffer manager README](https://github.com/postgres/postgres/blob/master/src/backend/storage/buffer/README)

---

## Phase 3: On-disk B+tree index (large: the core of the plan)

**Idea:** the in-memory index is rebuilt by a full scan each time the database opens, and
all of it has to fit in RAM. A B+tree stored in pages fixes both. At 8 KB per page, each
page holds about 400 i64 entries. That means 3 levels cover 81M rows, so a lookup is 1–3
page reads.

| Approach | How | Fits scuttle? |
|---|---|---|
| **Postgres nbtree: one file per index** | B+tree with a metapage at block 0 (root, height, count). Leaves hold (key, heap TID). Right-links and high keys between siblings. | **Yes.** The buffer pool already works per file, the heap and ctids stay, and a root split only changes the metapage. |
| SQLite: every table and index is a B-tree in one file | Tables are clustered by rowid. Varint cells and overflow pages. Root page numbers live in `sqlite_schema`. | It would mean rewriting the heap. Read about it, don't build it. |
| Copy-on-write B+tree (LMDB, BoltDB) | Never overwrite a page. Copy the leaf-to-root path, then swap the meta page. | Crash-safe without a log. It's a good second project. |
| LSM tree (RocksDB) | Memtable, sorted files, compaction | Not page-based. Read about it for contrast. |

**Starting point: `src/storage/btree.rs`.** This is the in-memory B+tree from the first commit. It was removed in Phase 0, then brought back and fixed (keys equal to a separator now go right). Phase 3 moves it onto pages instead of writing a new tree, and it replaces the `BTreeMap` in `TableIndex`:
- A node becomes a page, and a `Box` child becomes a `PageId`.
- `next_node` becomes the right sibling's page id. In memory it could never be set, because the parent already owns the sibling.
- "Full" means the page has no room for the entry, instead of `2t - 1` keys, because text keys vary in size.
- It splits full nodes on the way down (CLRS style). A split's separator always fits in the parent, so the tree needs neither parent pointers nor a stack of pages. The cost is that some nodes split before they had to.

**Choices inside the design:**
- **B+tree, not B-tree.** Values live only in leaves and leaves are chained, so a range scan is one descent and then a walk along the chain.
- **Key = (key, ctid).** Every entry is unique, a delete finds exactly its entry, and non-unique indexes come free. Postgres has done this since version 12.
- **Byte-comparable key encoding.** For i64, flip the sign bit and write big-endian. Text gets an escaped terminator. Comparing keys is then just comparing bytes.
- **No parent pointers.** Splitting on the way down already makes them unnecessary. The `parent_page_id` field already in `PageType` should go, because each split would have to rewrite every child's parent pointer.
- **Long keys.** Cap the key size (Postgres caps at about 1/3 of a page) instead of building overflow pages.
- **Deletes.** Remove the entry right away and never merge pages. Postgres doesn't merge either.
- **Crash safety without a WAL.** The index can always be rebuilt from the heap. Store a generation number in the metapage, and if it doesn't match on open, do a bulk rebuild.

**Done.** Each PRIMARY KEY or UNIQUE column has its own `<table>.<column>.index` file, read through the buffer pool. On the 1M-row bench:
- Opening the database no longer rebuilds anything. A run with one index lookup takes 0.6 ms and 3.4 MB, down from about 200 ms and 210 MB.
- A point lookup reads 4 pages: the metapage, the root, a leaf, then the heap page.
- A missing index file is built from the table with a bulk build: 0.23 s and 22 MB. Inserting the rows one at a time instead took 0.67 s and 46 MB, because in-order inserts leave every leaf half full.

Where it differs from the steps below:
- The sibling link is stored in the page header, which had 11 unused bytes, not in a special area.
- IndexScan holds a cursor that names the index file, so `ExecCtx` didn't need `tables`.
- The bulk build sorts entries in memory, about 40 bytes per INT entry. It doesn't use the external sort yet.
- Entries are capped at 1,024 bytes (`MAX_ENTRY`). A node splits when it has less room than one more entry, so a split always fits in its parent.

After step 8:
- **Inserts in key order.** When the rightmost page splits and the new key sorts after everything in it, the left page keeps about 90% instead of half, as Postgres does. Inserting 200K rising ids makes a 4.6 MB index, against 4.5 MB from a bulk build. With even splits it was about twice that.
- **Step 9 used a marker file instead of a generation number.** The pool creates `<data>/unflushed` when the first page changes and deletes it after a complete flush. If the database opens and finds the file, the last run stopped partway, so every index is rebuilt from its table. It costs about 3.5 µs per writing statement, for the file's create and delete.
- **No fsync yet.** Without a write-ahead log, fsync alone can't make a statement atomic: a crash can still land between two page writes. It would cost about 1 ms on every statement. It belongs with a WAL, where one fsync of the log can commit a batch of statements together (group commit). Until then, the marker covers a crash of the process, but not a power cut.

g.**
   - Serialize the B-tree page types. Today `PageHeader::to_bytes` panics on them.
   - Store sibling links in a special area at the end of the page.
   - Add `Page::insert_at(pos)`.
   - Test: page round-trips, and sorted insert.
2. **Key encoding.** Property-test that byte order equals `(IndexKey, Ctid)` order, including negative numbers and text that is a prefix of other text.
3. **One leaf page.** Search, insert, delete and range within a single page. Test against a `BTreeMap`.
4. **Allocate pages and add the metapage.** Add `BufferPool::allocate_page(name, PageType)` and a metapage at page 0. Test with the in-memory pool.
5. **Splits.** Leaf split, then interior pages, then root split. Test by inserting 100K random keys and comparing with a `BTreeMap`, plus an invariant checker: keys in order, sibling links correct, every leaf at the same depth.
6. **Swap it in behind `TableIndex`.**
   - Range scan along the leaf chain, `count_up_to`, and `check_unique`.
   - Pass the pool into the optimizer.
   - Make IndexScan lazy, so it holds a cursor instead of a `Vec<Ctid>` collected up front. Add `tables` to `ExecCtx` for this.
   - Test: the existing IndexScan and EXPLAIN tests pass.
7. **Delete.** Test that a deleted key can be inserted again.
8. **Bulk build.** Sort the (key, ctid) pairs, fill leaves left to right, then build each level above from the first keys of the level below. This replaces `rebuild_indexes`. It can reuse Phase 2's external sort.
9. **Persistence.** Restart tests, the generation check, and `sync_all` in `flush` (nothing calls fsync today).

**Verify:**
- Opening the 1M-row database no longer takes a full-scan rebuild.
- `id > 900000` drops its ~1.3 ms of Planning time, because the index range is no longer collected up front.

**Read:**
- Petrov, *Database Internals*, chapters 2–4 and 6
- [Postgres nbtree README](https://github.com/postgres/postgres/blob/master/src/backend/access/nbtree/README)
- [SQLite file format](https://www.sqlite.org/fileformat2.html)
- CMU 15-445 B+tree lectures

---

## Phase 4: Catalog without JSON, plus CREATE / DROP INDEX (medium)

| Approach | Used by | Trade-off |
|---|---|---|
| Store the `CREATE` SQL text and parse it on open | SQLite `sqlite_schema` | Small change, but needs a schema printer that round-trips |
| **Typed catalog rows: tables, columns, indexes** | Postgres `pg_class`, `pg_attribute`, `pg_index` | The most to learn, and `SELECT * FROM __columns__` works for free |
| Binary schema blob | — | Still opaque, like JSON |
| Keep JSON | today | Teaches nothing new |

**Pick:** typed rows. The hardcoded schemas of the catalog tables themselves are the
"bootstrap problem" Postgres also has.

**Steps:**
1. Add `__tables__`, `__columns__` and `__indexes__`. Write them alongside the JSON at first, and test that both produce equal `Schema`s.
2. Remove the JSON.
3. Add `CREATE [UNIQUE] INDEX name ON t (col)` and `DROP INDEX name`. PRIMARY KEY and UNIQUE become indexes they create automatically.
4. Optional: multi-column keys. Their order is lexicographic, so an index on `(a, b)` also serves queries on just `a` (the leftmost prefix).

**Verify:** on a 1M-row table, `WHERE grp = 7` changes from Scan with rows=1000000 to IndexScan with rows≈1000. After `DROP INDEX` it goes back to Scan.

---

## Phase 5: Using index order, IN and OR (medium)

**ORDER BY through an index.**
- A B+tree already returns keys in order. When Sort's key is an indexed column, replace the Scan with a full-range IndexScan and delete the Sort. For DESC, walk the leaf chain backwards.
- With LIMIT, the scan stops after the first batch.
- This is a simple version of System R's "interesting orders". The general version tracks the order each candidate plan produces.
- Trap: NULLs aren't in the index. The scan must add them itself (last for ASC, first for DESC) or only be used on NOT NULL columns.

**IN and OR.**
- Parse `IN (...)` first: the keyword exists but nothing parses it.
- Then let a Scan carry a list of index ranges, sorted and merged. Ranges that don't overlap can't return the same row twice, so no dedup is needed. This is how SQLite handles IN and how Postgres handles `= ANY`.
- Rewrite `a = 1 OR a = 2` into IN.
- An OR across two different indexes unions their row locations in a `BTreeSet<Ctid>`, which also sorts them into page order. That is a small version of Postgres's BitmapOr.

**Verify:**
- `ORDER BY id DESC LIMIT 10` has no Sort node, and IndexScan shows about 1024 rows, not 1M.
- `id IN (5, 500000, 999999)` shows rows=3.

---

## Phase 6: Joins and statistics (medium to large)

Today's join compares every left row with every right row: 1M × 10K = 10^10 comparisons.

| Join | How | When it wins |
|---|---|---|
| **Hash join** | Build a hash table from the smaller side, then stream the other side through it | Equality joins of any size |
| **Index nested-loop** | For each left row, look up the right side in its index | Small left side, with an index on the right key |
| Sort-merge | Sort both sides, then read them in step | Inputs already sorted (Phase 5) |

**Steps:**
1. **Push WHERE conditions into join inputs.** Today the Filter sits above the Join, so a table inside a join never uses an index.
2. **Hash join.** It needs a hashable key, because `Value` holds an f64. `Int64(1)` must equal `Float64(1.0)`, and NULL never matches. LEFT JOIN works for free, because the side being streamed is the one that keeps unmatched rows.
3. **Minimal statistics:** table row count, distinct keys per index, and min/max per index.
4. **Index nested-loop join**, chosen using those statistics.

**Verify:**
- A 1M-row table joined to a 10K-row table finishes with a hash join and times out without one.
- With `WHERE u.id < 100`, the plan shows about 100 index lookups.

**Read:**
- [SQLite query planner](https://www.sqlite.org/queryplanner.html) and its [optimizer overview](https://www.sqlite.org/optoverview.html)
- [Postgres "Using EXPLAIN"](https://www.postgresql.org/docs/current/using-explain.html)
- CMU 15-445 joins and optimization lectures

---

## Known bugs to fix along the way

- The external sort keeps every run open while merging. A tiny `--sort-mem` on a big table could run out of file descriptors, and each open run holds a read buffer outside the budget. A multi-pass merge, a few runs at a time, would bound both.
- An index's bulk build sorts all of its entries in memory, about 40 bytes per INT entry. It runs when an index file is missing and after a crash, so it should use the external sort.

## Suggested pace

| Week | Work |
|---|---|
| 1 | Phase 0, Phase 1 (done) |
| 2 | Phase 2 |
| 3–4 | Phase 3, steps 1–5 (the B+tree itself) |
| 5 | Phase 3, steps 6–9 (swap it in, persist it) |
| 6 | Phase 4 |
| 7 | Phase 5 |
| 8+ | Phase 6 |

Before each phase, we agree on the design before any code is written.
