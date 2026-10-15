> **Status:** implemented (commits 97b48a5..feec3ba). One deviation from the plan: `BindScope::of_join` takes the right relation's binding name (`TableRef::binding_name` was kept), because a zero-column table contributes no columns and `FROM users u JOIN z u` must still fail as a duplicate name. Pinned by `test_join_duplicate_alias_with_zero_column_table_fails`.

# One source of truth for a plan node's output columns: derived `output_schema()` beside `BindScope`

## Problem

Today no plan node states what its output row is. Each consumer works it out again for its own purpose:

- `LogicalPlan::column_names()` derives the client headers, with a `_ => Vec::new()` fallback.
- `explain::output_columns` and `table_columns` derive EXPLAIN names, re-reading the catalog.
- `Join.column_names` is a stored snapshot of `BindScope::qualified_names()`.
- `Join.right_width` is stored as `scope.len() - left_width`.
- `BindScope::push_relation` derives the FROM row layout a second time, in parallel with the Join tree that defines it.
- The aggregate layout `[keys..., aggs...]` is implicit in the binder, in `AggregateExec` and in EXPLAIN.

The evaluator returns NULL for an out-of-range index, so any drift between these produces wrong answers silently, never an error. I confirmed this while grounding: **no existing test pins the LEFT pad width.** A mutation that pads with the left width instead of the right passes every pre-existing test, because a too-wide or too-narrow pad also reads as NULL. Step 0 adds the missing test.

The shape is not obvious because "a column's name" means three things that a single field cannot hold:

- the client header (`name`, `count`, `?column?`, with duplicates kept);
- the qualified scope name used for resolution (`u.name`);
- the EXPLAIN text of a column seen from the node above. That text is bare over a Scan, qualified over a Join, and rendered expression text over an Aggregate.

Every EXPLAIN string has to stay byte-identical, including the unpinned Sort over Aggregate over Join (`Sort: u.name`). So do every resolution error, `QueryResponse.columns`, positional execution and the plan order.

## Usage (caller's view)

There is one question, "what does this node output?", and one call answers it. Every in-crate call site after the refactor:

```rust
// database.rs, handle_select: client headers (bare, duplicates kept)
let columns = anayzed_plan.output_schema().names();

// physical.rs, Join arm: LEFT pad width, read before `right` is moved
let right_width = right.output_schema().len();

// explain.rs: print an expression with the labels of the row it reads
LogicalPlan::Filter { input, condition } => format!("{indent}Filter: {}", input.output_schema().render(condition)),
LogicalPlan::Join { on, .. }             => format!("{indent}Join: {kind} on {}", plan.output_schema().render(on)),
LogicalPlan::Sort { input, keys }        => { let columns = input.output_schema(); /* columns.render(&key.expr) */ }
LogicalPlan::Projection { .. }           => format!("{indent}Projection: {}", plan.output_schema().names().join(", ")),

// analyzer, analyze_from: name resolution is a policy over the plan's own schema
let right = self.scan(&join.table)?;
let on_scope = BindScope::of_join(&plan, &right)?;  // "Table name ... specified more than once" lives here
let on = self.bind_expression(&join.on, &on_scope)?;
plan = LogicalPlan::Join { left: Box::new(plan), right: Box::new(right), kind: join.kind, on };
// ...after the loop
let scope = BindScope::of(&plan);                   // WHERE, select list, GROUP BY, ORDER BY bind here
```

A contributor who adds a node, for example `Distinct { input }`, gets a compile error in `output_schema`, `explain::write_node` and `create_physical_plan`. All three are exhaustive, with no `_` arm. The fix is one line in the first of them (`LogicalPlan::Distinct { input } => input.output_schema()`). From then on, EXPLAIN, response headers and binding all agree about the node.

## Shape

**Data structures** live in `src/sql/planner/relation_schema.rs`, a new file:

```text
RelationSchema { columns: Vec<OutputColumn> }        // private; built only by 4 constructors
OutputColumn   { relation: Option<String>, name, label, data_type }
```

The invariant is that `columns[i]` describes `row.values[i]` for every row the node produces. The schema is **derived, never stored**. `LogicalPlan::output_schema(&self) -> RelationSchema` is one exhaustive match, catalog-free and infallible. To make that possible:

- `Scan` gains `schema: Schema`, a snapshot taken at analysis.
- `AggregateCall` gains `return_type: DataType`, already computed in `bind`.

`Join` loses `right_width` and `column_names`, and `LogicalPlan::column_names()` is deleted. Derive instead of sync, per single source of truth.

**The hard part: three naming schemes, byte-identical.** There is no global qualification rule. Each scheme is set by the one constructor that creates that kind of column, and every parent inherits labels unchanged:

| Constructor | Used by | `label` |
|---|---|---|
| `RelationSchema::table(relation, &Schema)` | Scan, `BindScope::single`, EXPLAIN Update/Delete | bare `name`, even with an alias |
| `RelationSchema::join(&left, &right)` | Join, `BindScope::of_join` | `relation.name` for every column that has a relation. **The only place labels become qualified.** |
| `RelationSchema::computed((name, type)...)` | Aggregate (name = `input.render(key)` / `input.render_aggregate(call)`), Projection (name = output name) | `== name` |
| `RelationSchema::empty()` | Values, Insert, Update, Delete, CreateTable | none |

Filter, Sort and Limit return `input.output_schema()` unchanged. The Aggregate arm renders its columns with the *input's* labels. So the rendered key over a join is `u.name`, and that becomes the Aggregate column's label, which a Sort above prints. That is today's output, reached without a special case. The renderers (`expression`, `operand`, `literal`, `aggregate`) move from explain.rs into `RelationSchema` (`render`, `render_aggregate`). The reason: an Aggregate's column labels *are* rendered text, so the renderer belongs to the label derivation, and the plan does not depend on its printer.

Trace for every EXPLAIN shape (all verified byte-identical; the starred rows were unpinned and are now pinned in step 0):

| Node prints | Reads labels of | Labels come from | Example |
|---|---|---|---|
| Filter over Scan (aliased or not) | Scan | `table`: bare | `Filter: age > 20` over `Scan: users u` |
| `*` Sort over Filter over Scan | Filter, which inherits from Scan | bare | `Sort: age` |
| Join ON | the Join itself | `join`: qualified | `Join: inner on u.id = o.user_id` |
| Filter over Join | Join | qualified | `Filter: o.total > 10` |
| `*` Sort over Join | Join | qualified | `Sort: o.total DESC` |
| Aggregate over Scan / Join / Filter(Join) | its input | bare / qualified | `group by u.name; COUNT(o.id)` |
| `*` Sort over Aggregate (single table) | Aggregate | `computed` from bare | `Sort: COUNT(*) DESC, dept`; `Sort: age + 1` |
| `*` Sort over Aggregate over Join | Aggregate | `computed` from qualified | `Sort: u.name, COUNT(o.id) DESC` |
| Projection | itself | `names()` | `Projection: id, name, dept, age, id, user_id, total, ...` |
| Update / Delete | `RelationSchema::table(t, schema)` | bare | `Update: users set age = age + 1` |
| out-of-range index | any | `label()` fallback | `#7` (unchanged) |

**`BindScope` stays, built from the plan schema.** `BindScope { schema: RelationSchema }` is the analyzer's policy layer over the layout the plan owns. That policy is qualifier matching, ambiguity, unknown relation, duplicate relation, `*` and `t.*` expansion, and AST canonicalization for GROUP BY. I chose to build it from the plan rather than keep it independent. The reason: `push_relation` order is an independent derivation of the Join layout, and if the two drift, ON and WHERE indexes stop matching what `JoinExec` emits. After the change, `RelationSchema::join` is the single function behind both `output_schema(Join)` and the ON scope.

The policy stays out of `RelationSchema` because:

- it speaks AST (`Expression::Qualified`, `canonicalize`), and the planner module should not;
- its error texts and tests (`test_scope_*`) carry over almost verbatim.

`ScopeColumn`, `push_relation`, `len`, `qualified_names` and `TableRef::binding_name` are deleted. The alias-else-table rule now exists only in `output_schema`'s Scan arm.

**Interface depth.** The surface is:

- one method on the plan, `output_schema`;
- on `RelationSchema`: four constructors and six readers (`len`, `columns`, `names`, `label`, `render`, `render_aggregate`);
- two new `BindScope` constructors (`of`, `of_join`).

Behind it sit the following, and none of them is visible to callers:

- the row layout of every node;
- the three naming schemes;
- the aggregate layout as EXPLAIN sees it;
- the pad width;
- the type of every output column.

explain.rs shrinks to tree layout (about 100 lines shorter net).

**Optional depth (each its own step, droppable by synthesis without touching the others):**

- **Step 6 `ProjectionColumn { name, expr }`** replaces the parallel `expressions` / `column_names` Vecs. Their equal length becomes a property of the type, so `output_schema(Projection)` needs no `zip`, which would silently truncate.
- **Step 7 `schema: Schema` on Update/Delete.** EXPLAIN becomes `explain(&LogicalPlan) -> String`, with no catalog and no error path. `handle_update` stops re-reading the catalog.

**Deliberately not done:**

- typed or nullable `QueryResponse`;
- routing UPDATE SET targets or the INSERT column list through `BindScope`;
- caching schemas on nodes;
- bound-expression GROUP BY matching;
- typing VALUES.

## Synthesis decision

Three runners raced: A (derived `output_schema()` beside `BindScope`, Opus), B (`BindScope` promoted to `RowSchema`, Sonnet), and C (private `LogicalPlan` struct with stored output built by smart constructors, GPT-6-Sol).

**Base: A.** The cross-judge (GPT-6-Sol, given the three candidates anonymized) recommended C, the most structurally airtight design. Its scores were A 16, B 14, C 18. I overruled it for three reasons:

1. **The implementer is Haiku.** The judge scored C 2/5 on implementability. Its step 4 swaps the plan's representation and migrates nearly every caller in one go. A scored 5/5.
2. **The judge discounted evidence it didn't have.** It treated A's and B's "implemented in scratch" claims as unproven only because the anonymized folders didn't include the reference patches. I have them: every step of A was gated in a scratch copy, with all EXPLAIN bytes identical. C was only format-checked.
3. **A's main weakness is narrow.** Because A derives the schema, a node can't disagree with its children. The judge's "split ownership" points were the optional Projection pairing and the aggregate index arithmetic, and both are fixable inside A.

Grafts and fixes:

- **Projection pairing (step 6) is now required.** This comes from C's `(expr, name)` constructor. Header count and expression count can no longer drift.
- **One owner for the aggregate output order (new step 6b).** This is a lighter version of C's `AggregateSpec` and B's grouped schema. `GroupedScope::key_column`/`call_column` are the only place the analyzer writes `[keys, calls]`. A `debug_assert` checks them against `output_schema()`, and the step 0 EXPLAIN pins catch a reorder.
- **The DML snapshot and catalog-free EXPLAIN (step 7) are now required.** This leaves EXPLAIN reading exactly one thing.
- **B's zero-column test was added to step 0.**
- **Not taken from B:** the analyzer calling EXPLAIN's renderers (information leakage), and the zero-column `z JOIN z` behavior change.
- **Not taken from C:** the enum-to-struct migration. Its guarantee is worth revisiting once the codebase settles. Exhaustive `output_schema()`, `write_node` and `create_physical_plan` already force every new node to state its output.

## Tradeoffs accepted

- We accept that `output_schema()` is recomputed recursively on every call: O(depth²) string work in EXPLAIN, and a few calls per SELECT in the analyzer. In exchange, no node field can disagree with its children.
- We accept a `Schema` clone per Scan (and per Update/Delete in step 7). In exchange, `output_schema()` needs no catalog and cannot fail. The snapshot lives for one statement, so it is not a second owner of catalog state.
- We accept that the aggregate layout still has one writer outside the schema: `bind` emits `group_by.len() + call_pos`, and `AggregateExec` emits keys then accumulators. These are an index producer and a row producer, not describers. Merging them would mean binding through a placeholder and then rewriting indexes. The drift detector is a set of step-0 EXPLAIN pins (`Sort: COUNT(*) DESC, dept` and `Sort: u.name, COUNT(o.id) DESC`). They fail if any one of the three orders changes. I verified this by mutating the `output_schema` chain order.
- We accept two strings per column (`name`, `label`) that differ only for table columns under a Join. In exchange, each naming scheme is decided by one constructor instead of a "qualify iff more than one relation" rule, which would change the Sort-over-Aggregate-over-Join output.
- We accept that `output_schema()` over Projection/Aggregate calls `AnalyzedExpression::get_type()`, which panics on `Literal(Null)`. That is safe only because the binder rejects NULL literals in SELECT, and the invariant is documented on `output_schema`. VALUES reports an empty schema for the same reason.
- We accept that `BindScope::of_join` checks duplicate relations column by column, like today's `push_relation`. A zero-column table would be invisible to the check, which is unchanged behavior.

## Alternatives considered

- **Keep `BindScope` independent and derive only plan-side consumers from `output_schema`.** This is a smaller diff in scope.rs. But FROM layout keeps two independent derivations: `push_relation` order, and the Join arm plus `JoinExec`. That violates the goal's "no two places derive the same layout" exactly where drift becomes silently wrong indexes. It hides less: the binder still owns its own copy of the layout.
- **Fold `BindScope` into `RelationSchema` (candidate-1's `resolve`/`relation_columns` on the schema).** This gives one type instead of two. But AST-specific policy (`canonicalize` into `Expression::Qualified`, star expansion into `(Expression, String)`) would move into the planner. Every planner reader would then learn the resolution rules, and the `test_scope_*` suite would be rewritten rather than ported. That interface is shallower, not deeper.
- **Store a schema field on every node.** Cheap reads, but every constructor must keep the field in sync with its children: the split-ownership red flag. It also adds a field to every match arm.
- **One name per column plus a schema-level `qualified: bool` set by `join`.** Equivalent output, but the qualification bit becomes a rule every parent must propagate correctly. A stored per-column label states the answer and inherits trivially.
- **`Option<RelationSchema>` for non-row nodes.** It would force `expect` at `handle_select` and at EXPLAIN, for nodes that never reach those paths. The empty schema with a doc comment says the same thing with no panic site.

## Open questions and risks

- Is the unpinned output "Sort over Aggregate over Join prints `u.name`" the behavior you want to freeze? This design preserves it and step 0 pins it. PostgreSQL would print the same.
- Should Values carry its target column types (it does produce rows), or stay empty until something binds names against it, such as INSERT ... SELECT?
- Parallel fix #6 (TIMESTAMP literals) will likely add a `Value` arm to `explain::literal`. PLAN step 1 moves the renderers by name at implementation time, so the arm moves with them. Do not paste today's text.
- The physical planner's `_ => todo!()` becomes an explicit error arm. The panic becomes an `Err` on paths that are unreachable today (DML never goes through the physical planner). Is an error, rather than `unreachable!`, the right call?
- Both doc examples that assert EXPLAIN output (`database.rs` `no_run`, `scuttle-wasm` doc) are compiled but never run. The equivalent string is pinned by `test_explain_select_with_filter`.

## Resolution of grounding.md "Open design choices"

1. **Derive vs store:** (a). `output_schema()` is derived. Scan carries a `Schema` snapshot, `AggregateCall` carries `return_type`, and Values/DML report an empty schema. Update/Delete gain a snapshot in optional step 7, which makes EXPLAIN catalog-free.
2. **One type or two:** one type, two strings per column. `name` is the client header and `label` is the EXPLAIN text. Both are computed at derivation; they differ only for table columns under a Join.
3. **Qualification rule:** none global. `RelationSchema::join` qualifies, Scan labels are bare, and Aggregate labels are rendered against the input. Byte-identical, including Sort over Aggregate over Join (`u.name`), now pinned.
4. **`BindScope`:** kept as the analyzer's naming policy and built from the plan schema (`of`, `of_join`, `single`). `canonicalize` stays, and there is no `PartialEq` on `AnalyzedExpression`.
5. **Join fields:** both deleted. Pad width is `right.output_schema().len()`, read before `*right` is moved, and `JoinExec` keeps its own `right_width` field.
6. **Response typing:** `QueryResponse.columns: Vec<String>` is unchanged, with no types or nullability.
7. **Exhaustiveness:** `output_schema`, `write_node` and `create_physical_plan` are all exhaustive with no `_`. Non-row nodes report `RelationSchema::empty()`, not `Option`.
8. **DML paths:** UPDATE SET targets and INSERT column lists stay on `Schema` lookups. Their error text and semantics are unchanged.
9. **Candidate-1 items:**
   - Ported: the exhaustive `output_schema`, `AggregateCall.return_type`, the Scan snapshot, the schema-derived pad width, and the renderers moved into the schema.
   - Not ported: the "qualify iff >1 relation" rule (it would change Sort-over-Aggregate-over-Join), bound-expression GROUP BY, nullability, alias hiding, `{kind} Join:`, and the `Dot` token.
10. **`plan_node.rs`:** deleted in step 5 (orphan).

## Next implementation step

Add the step-0 pin tests (`test_explain_column_names_follow_input_shape` and `test_join_after_left_join_on_empty_table_reads_shifted_columns`) to main as they are and see them pass. After that, create `relation_schema.rs` and switch EXPLAIN to `output_schema()` (PLAN step 1).
