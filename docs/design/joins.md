> **Status:** implemented (commits 700b9e0..ebe8414). The `Join.column_names` / `right_width` fields and `explain::output_columns` described here were later removed by the output-schema refactor; see [output-schema.md](output-schema.md).

# JOIN support: analyzer-owned `BindScope`, name-free plans

## Problem

Everything in the SELECT path assumes one table: `From { table: String }`, one `&Schema` threaded through `analyze_select`/`bind`, `Schema::get_column_index` (first match, no ambiguity), and positional `ColumnRef.index`. JOIN must add aliases, qualified names (`u.id`), ambiguity errors, left-deep chains and `LEFT` null-padding without breaking those constraints (from `grounding.md`):

- The evaluator reads `row.values[index]` and returns NULL when the index is out of range, so an off-by-offset binding is a **silent wrong answer**, not an error.
- `LogicalPlan` nodes have no schema. `column_names()` (used for `QueryResponse.columns`) and `explain::output_columns` are two independent derivations, and `output_columns` has a `_ => Vec::new()` fallback that would swallow a new `Join`.
- `GroupedScope` matches GROUP BY keys and aggregates by structural `Expression` equality, so `u.dept` and `dept` would be "different" columns.
- The lexer lexes `a.id` as one `Identifier("a.id")` and `a.*` as `Identifier("a.")` + `Asterisk` (which today parses as multiplication).
- `test_parse_rejects_trailing_tokens` currently expects `SELECT * FROM users u` to be an error.

## Usage (caller's view)

Fixture used below (users is the existing `TestDb` table; `orders` and `depts` are created by the tests):

```sql
-- users(id, name, dept, age): (1 Alice eng 30) (2 Bob eng 25) (3 Carol ops 41) (4 Dan ops 35)
CREATE TABLE orders (id INT, user_id INT, total INT);
INSERT INTO orders VALUES (10, 1, 100), (11, 1, 50), (12, 3, 75), (13, 9, 5);
CREATE TABLE depts (code TEXT, title TEXT);
INSERT INTO depts VALUES ('eng', 'Engineering'), ('ops', 'Operations');
```

Inner join with aliases and qualified references:

```
> SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id;
--------------------------------------
 | name         | total       
--------------------------------------
 | Alice        | 100         
 | Alice        | 50          
 | Carol        | 75          
--------------------------------------
(3 rows)
```

`LEFT [OUTER] JOIN`: unmatched left rows are padded with NULL for every right column:

```
> SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id, o.id;
--------------------------------------
 | name         | total       
--------------------------------------
 | Alice        | 100         
 | Alice        | 50          
 | Bob          | NULL        
 | Carol        | 75          
 | Dan          | NULL        
--------------------------------------
(5 rows)
```

ON decides matching, WHERE filters afterwards. A right-side condition in ON keeps unmatched left rows; the same condition in WHERE drops them:

```
> SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id AND o.total > 60 ORDER BY u.id;
--------------------------------------
 | name         | total       
--------------------------------------
 | Alice        | 100         
 | Bob          | NULL        
 | Carol        | 75          
 | Dan          | NULL        
--------------------------------------
(4 rows)
> SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.total > 60 ORDER BY u.id;
--------------------------------------
 | name         | total       
--------------------------------------
 | Alice        | 100         
 | Carol        | 75          
--------------------------------------
(2 rows)
```

Aggregates over a join (`COUNT(o.id)` skips the NULL pad, so unmatched users count 0; `u.name` in ORDER BY matches the GROUP BY key):

```
> SELECT u.name, COUNT(o.id) AS n FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name;
--------------------------------------
 | name         | n           
--------------------------------------
 | Alice        | 2           
 | Bob          | 0           
 | Carol        | 1           
 | Dan          | 0           
--------------------------------------
(4 rows)
```

Self-join. Output column names are bare, as in PostgreSQL, so duplicates appear as-is:

```
> SELECT a.name, b.name FROM users a JOIN users b ON a.dept = b.dept AND a.id < b.id ORDER BY a.id, b.id;
--------------------------------------
 | name         | name        
--------------------------------------
 | Alice        | Bob         
 | Carol        | Dan         
--------------------------------------
(2 rows)
```

`SELECT *` is every column of every table in FROM order (shown with `WHERE o.id = 10`); `t.*` is in scope for the first cut and expands the same way for one relation:

```
> SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 10;
-----------------------------------------------------------------------------------------------------------------
 | id           | name         | dept         | age          | id           | user_id      | total       
-----------------------------------------------------------------------------------------------------------------
 | 1            | Alice        | eng          | 30           | 10           | 1            | 100         
-----------------------------------------------------------------------------------------------------------------
(1 rows)
```

Chains are left-deep and an ON clause sees only the tables to its left plus its own:

```sql
SELECT u.name, o.total, d.title
FROM users u JOIN orders o ON u.id = o.user_id
             JOIN depts d  ON u.dept = d.code
ORDER BY o.id;
-- (Alice, 100, Engineering) (Alice, 50, Engineering) (Carol, 75, Operations)
```

Errors (each makes `execute_query` return `Err`; none may panic):

```sql
SELECT id FROM users u JOIN orders o ON u.id = o.user_id          -- column reference "id" is ambiguous
SELECT users.name FROM users u                                      -- aliased: only `u` is visible
SELECT x.id FROM users u                                            -- missing FROM-clause entry for table x
SELECT * FROM users JOIN users ON users.id = users.id               -- table name "users" specified more than once
SELECT * FROM users u JOIN orders o ON u.id = d.code JOIN depts d ON true   -- ON cannot see later tables
SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id       -- RIGHT JOIN is not supported
```

`Database::explain` (there is no EXPLAIN keyword; this is the Rust API). Names under a `Join` are qualified, single-table output is byte-identical to today:

```
db.explain("SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.total > 10")
Projection: name, total
  Filter: o.total > 10
    Join: left on u.id = o.user_id
      Scan: users
      Scan: orders
```

Rust test call sites (`database.rs`, `TestDb` style; `seed_orders` runs the CREATE/INSERT above):

```rust
#[test]
fn test_join_inner_values_from_both_sides() {
    let mut db = TestDb::new("join-inner");
    seed_orders(&mut db);
    assert_eq!(
        db.query("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.id"),
        vec![vec![text("Alice"), Value::Int64(100)], vec![text("Alice"), Value::Int64(50)], vec![text("Carol"), Value::Int64(75)]],
    );
}

#[test]
fn test_join_left_pads_nulls_even_for_empty_right_table() {
    let mut db = TestDb::new("join-left-empty");
    db.execute("CREATE TABLE empty_t (id INT, v INT)");
    assert_eq!(
        db.query("SELECT u.id, e.v FROM users u LEFT JOIN empty_t e ON u.id = e.id ORDER BY u.id")[0],
        vec![Value::Int64(1), Value::Null],
    );
    assert_eq!(db.columns("SELECT * FROM users u LEFT JOIN empty_t e ON u.id = e.id").len(), 6);
}

#[test]
fn test_join_rejects_ambiguous_and_unknown_names() {
    let mut db = TestDb::new("join-errors");
    seed_orders(&mut db);
    assert!(db.fails("SELECT id FROM users u JOIN orders o ON u.id = o.user_id"));
    assert!(db.fails("SELECT users.name FROM users u"));
}
```

## Shape

**Data structures.** One new analyzer-side type, `BindScope`, replaces `&Schema` everywhere in `analyze_select`:

```text
BindScope { columns: Vec<ScopeColumn { relation, name, data_type }> }   // index == position in the joined row
```

For `users u JOIN orders o` it is `u.id u.name u.dept u.age o.id o.user_id o.total` at indexes 0..6. There is **no stored offset**: a column's index is its position, so the "right side shifts by `left_width`" rule cannot be mis-computed by an implementer. Every dominant access pattern traces through it directly:

- resolve `u.name` -> linear scan on `(relation, name)` -> `(1, Text)`.
- resolve bare `id` -> scan on `name`: 0 hits = not found, 1 = ok, 2+ = ambiguous (the thing `Schema::get_column_index` could not say).
- `*` / `t.*` -> `expand_star` returns `Qualified` references (never bare names, which would be ambiguous in a self-join).
- build for chain `a JOIN b JOIN c`: push a, push b, bind ON#1, push c, bind ON#2. An ON clause sees exactly what is to its left plus its own table, by construction.

**Interface of `BindScope` (9 methods):** `empty`, `single`, `push_relation`, `len`, `resolve`, `has_column`, `expand_star`, `canonicalize`, `qualified_names`. Callers never touch `columns`, never compute an offset, never check ambiguity. Everything about names (aliases, ambiguity, unknown relation, duplicate alias, star expansion, display names) lives in `analyzer/scope.rs`; this is the single source of truth per invariant (per *boundary-discipline*: names are resolved once at the analyzer boundary, the plan and executor trust indexes).

**Plan stays name-free.** `LogicalPlan::Join { left, right, kind, on, right_width, column_names }`. Execution needs `left, right, kind, on, right_width` (the pad width must be explicit because the right table may be empty). `column_names` (`"u.id"`, ...) is **display-only** and is produced by `scope.qualified_names()` at the single point where the node is built. `explain::output_columns(Join)` just returns it, so EXPLAIN needs no resolution logic of its own and `u.id = o.user_id` / `a.id = b.id` render correctly. `ColumnRef.relation` (always `None`, never read) is deleted: plans carry only indexes.

`JoinKind { Inner, Left }` is defined once in `ast::statement` and reused in the plan (like `Operator`), so there is no second enum to keep in sync.

**Qualifier representation.** Keep the lexer untouched. Add one variant `Expression::Qualified { relation, column }` alongside `Identifier(String)`; the parser splits the lexer's `"u.name"` word on the first `.` (`column_from_word`). This is additive: no existing `Identifier(..)` construction or match changes, so all existing parser tests stay as they are. The compiler flags the 3 exhaustive matches (`Display`, `contains_aggregate`, `bind`); one `_ =>` site (`to_column_name`) is named in PLAN.md. After the analyzer's `canonicalize`, every column in GROUP BY keys/aggregates is `Qualified`, so downstream analyzer code sees a single shape.

**GROUP BY / aggregate matching.** `GroupedScope` stores keys and aggregate ASTs in canonical form (`scope.canonicalize`: every column ref rewritten to `Qualified{real relation, column}`), and `bind` canonicalizes before comparing. `SELECT * ... GROUP BY id`, `SELECT u.dept ... GROUP BY dept`, `COUNT(id)` vs `COUNT(o.id)`, `age + 1` vs `u.age + 1` all compare equal. Canonicalizing is one extra resolve per grouped subexpression; this is a learning project (clarity over speed), and `bind` stays the one place that handles grouped mode.

**Execution.** `JoinExec` is a nested-loop join: drain `right` once into `Vec<Row>` on first `next()` (precedent: `AggregateExec`/`SortExec` `done` flag), stream `left` batch by batch, concatenate rows, evaluate `on` with `PredicateEvaluator` (NULL -> false, exactly the right ON semantics), LEFT-pad `right_width` NULLs, and loop past batches that produce nothing (like `FilterExec`). The evaluator, `ScanExec`, and `Row` are unchanged. WHERE stays a `Filter` above the `Join`; nothing pushes it into ON.

**Interface depth.** Callers of the analyzer (database.rs) see no change: `Analyzer::analyze_select(SelectStatement) -> LogicalPlan`. The public surface added is one AST variant, one `From` shape, one plan variant. All the policy (ambiguity, aliasing, offsets, star expansion, canonical form, duplicate alias) hides behind `BindScope`'s 9 methods. Nothing outside the analyzer can resolve a name; the physical layer cannot see a name at all.

**Traps encoded rather than remembered.** (1) `resolve_output_reference` follows the PostgreSQL rule: several outputs with the same name are accepted only if they are the same value, otherwise it is an ambiguity error (`SELECT a.id, b.id ... ORDER BY id`). (2) The GROUP BY "input column beats alias" test uses `has_column` (any column with that name) so an ambiguous name is not silently reinterpreted as an alias. (3) `a.*` is handled before `parse_expression` in `parse_targets` (it would parse as multiplication). (4) `explain::output_columns` loses its `_ =>` fallback.

## Synthesis decision

**Base: candidate B (analyzer-owned `BindScope`, Sonnet).** Candidate A (`LogicalPlan::output_schema()` / `RelationSchema`, Opus) is the stronger architecture: it has one source of truth for node columns and deletes the duplicate name derivation in explain.rs. B won on implementability. The implementer is Claude Haiku. B's changes are additive, its `scope.rs` and `JoinExec` were type-checked against the repo, and its call-site table marks which sites the compiler will not flag. The cross-judge (Opus; the configured GPT-6-Sol seat was unavailable) scored A 25 and B 22, and recommended B as the base on the same grounds. My own rubric reading agreed.

Grafts from A:
1. **`FULL` and `CROSS` are reserved and rejected** in `parse_from_clause`. Without this, B parses `users FULL JOIN orders` as alias `full` plus an INNER JOIN and silently returns wrong rows.
2. **PostgreSQL ORDER BY output-name rule** (`resolve_output_reference`). A name matching several outputs is accepted only if they are the same value (compared with `canonicalize`), otherwise it is an ambiguity error. B's fall-through let `SELECT age AS id, id … ORDER BY id` silently sort by the input `id`.
3. **`Scan.alias` for EXPLAIN** (`Scan: users u`), so self-joins are readable. It is display-only.

Also from the review: the dead `is_nullable(&Schema)` is deleted rather than left stale; the quoted `"a.b"` limitation is pinned by a test; comma joins and `FULL` after an alias are added to the parser reject tests.

Rejected from A for now: the `Dot` token and the `Identifier` → `Column` rename (high churn for Haiku), and the `output_schema()` refactor. That refactor is a good follow-up once JOIN works. Meanwhile `output_columns` loses its `_` fallback, so a forgotten arm is a compile error.

## Tradeoffs accepted

- We accept **two derivations of column names remain** (`LogicalPlan::column_names` vs `explain::output_columns`) in exchange for not refactoring every plan node. The new `Join.column_names` is the only plan-held name list, display-only, and generated from the scope in one place.
- We accept **duplicate bare headers** (`id`, `id`) in `QueryResponse.columns` for `SELECT *` over a join, as PostgreSQL does, in exchange for no collision-renaming policy; `ORDER BY id` over them is an ambiguity error.
- We accept **quoted identifiers containing a dot (`"a.b"`) are split**, because the lexer drops quote information; the alternative (a `Dot` token) is the riskiest lexer change.
- We accept **`canonicalize` resolves twice** (once in `canonicalize`, once in the bind arm) in exchange for equal-by-construction GROUP BY/aggregate matching for any expression shape.
- We accept **no nullability tracking**: right columns of a LEFT JOIN are not marked nullable anywhere, because the only reader (`AnalyzedExpression::is_nullable`) is dead code and `ScopeColumn` deliberately omits `ColumnDef` (its NOT NULL would be wrong for the right side). If needed later, add `nullable: bool` to `ScopeColumn`, set in `push_relation`.
- We accept **Scan lines in EXPLAIN do not show aliases** (`Scan: users` twice in a self-join); the qualified names on the Join and above identify them.
- We accept **`ON` must be Bool** is checked (WHERE is not checked today) because a non-boolean ON would silently match nothing.
- We accept that **`AND`/`OR` are not fully 3-valued** (pre-existing); ON with NULL-padded columns from an earlier LEFT JOIN works for comparisons but can differ from standard SQL for OR/NOT.
- `OUTER` becomes a reserved word (needed for `LEFT OUTER JOIN`).

## Alternatives considered

- **Plan-node output schema** (`LogicalPlan::output_schema()` on every node, `ColumnRef` carrying names). Would delete the `column_names`/`output_columns` duplication and make EXPLAIN trivial, but touches every node, the physical planner and explain, and puts names in plans. Wider surface for an invariant we can hold in one analyzer struct. Rejected for this candidate (it is the other runner's direction).
- **Structured identifier `Expression::Column { qualifier: Option<String>, name }` replacing `Identifier`.** Cleaner type, but rewrites ~10 test constructions, the `"*"` hack, `to_column_name`, `resolve_output_reference`, and every `Identifier` match. Churn for a small implementer with no behavioural gain over an additive variant plus canonicalization.
- **`Dot` token in the lexer.** Cleanest tokens, but `consume_word` and number lexing (`1.5`) must change and every lexer test is exposed; `a.*` still needs parser lookahead.
- **Compare GROUP BY keys by resolved column index only.** Handles plain column refs but not `age + 1` vs `u.age + 1` nor aggregate args. Canonical AST handles every shape with one function.
- **Per-relation `(alias, Schema, offset)` entries.** Stores an offset that every consumer must add; a flat column list makes the offset implicit and unmissable.
- **Keep a `&Schema` overload of `bind_expression` next to `&BindScope`.** Two ways to do one task; an agent would copy whichever it sees. UPDATE/DELETE/INSERT use `BindScope::single/empty` instead.
- **Render EXPLAIN names from the bound expression.** `ColumnRef` would have to carry the name, re-adding names to plans for every node, not just Join.

## Open questions and risks

- Should duplicate output headers be disambiguated (`id`, `id` -> `u.id`, `o.id`) for joined queries? This candidate keeps bare names; the wasm JSON `columns` is an array so nothing collapses, but a consumer keying rows by name would.
- Is reserving `outer` acceptable, or should `LEFT OUTER JOIN` be rejected?
- `ORDER BY name` where two outputs share `name` now falls through to input binding (usually an ambiguity error) where it used to pick the first. Is that acceptable for non-join queries too? (It matches PostgreSQL.)
- Should EXPLAIN show aliases on `Scan` lines (`Scan: users as a`)? Needs an alias on `Scan`, which this design avoids.
- Is `t.*` wanted in the first cut? It costs one parser block, one `SelectTarget` variant and reuses `expand_star`; PLAN.md makes it the last, droppable step.
- Risk: a future contributor adds a `LogicalPlan` variant that sits above a Join and relies on `output_columns`; the explicit arm list makes that a compile error.

## Next implementation step

Step 1 of PLAN.md: change `statement::From` to `{ table: TableRef, joins: Vec<JoinClause> }`, add `Keyword::Outer`, write `parse_from_clause`/`parse_table_ref`, make the analyzer reject non-empty `joins` for now, and update the two parser tests that mention `From`/`FROM users u`.
