<!-- todo:guide — managed by todo; this block is rewritten on save. Docs: https://github.com/andresbott/todo
This file is a todo list managed by "todo", a terminal TODO app:
https://github.com/andresbott/todo

todo watches this file and reloads it automatically when it changes on disk, so
you — human or agent — can edit it directly in any editor. Keep to this format
so todo can parse what you write:

  # Heading           Headings ("#" to "######") are categories; they nest by
                      heading level.
  - [ ] Open task     A "- [ ]" line is an open task; "- [x]" marks it done.
  - [/] In progress   "- [/]" flags a task in progress, "- [>]" defers it.
  - [x] Done task     Tasks must live under a category heading.
    - [ ] Subtask     Indent by two spaces to nest a subtask under a task.
    Description text  An indented, non-checkbox line is the task's description.

Notes for editors:
- Text above the first heading (this block included) is preserved on save.
- todo rewrites the file into the canonical form above on every change, so any
  other free-form markdown placed between items is not kept.
-->

# New features

# Code review

- [x] Launch code review and capture findings
  Ran 4 expert subagents (Pike/Go idioms, Go architecture, Go code review, DB/SQL). Findings below, deduped and ranked. Positives confirmed by multiple agents: SQL-injection surface airtight, move-as-duplicate fix correct, resource handling clean, GetOrAdd well-tested.

## A. Correctness bugs - fix before 0.10

- [x] Update reorder-only path silently succeeds for a missing / wrong-tenant id
  closuretree.go:492-536 - reorder-only Update (item=nil, newParentID=nil) never verifies the node exists and ignores RowsAffected, returning nil instead of ErrNodeNotFound [go-code-reviewer, conf 88]. Fix: check RowsAffected on the final sort_order UPDATE / that the current-parent lookup found a row.
- [x] Update field-update falsely returns ErrNodeNotFound on MySQL for a no-op update
  closuretree.go:446-453 - MySQL RowsAffected counts rows *changed*, so an idempotent Update of unchanged values returns 0 and spuriously reports not-found [go-code-reviewer, conf 90]. Fix: distinguish matched-vs-changed (clientFoundRows DSN or a separate existence check). TestUpdateSortOrder already dodges this ("avoid MySQL no-op").
- [x] Typed-nil pointer panics instead of returning an error
  closuretree.go:304-317 + node.go:29-44 - hasNode passes a typed-nil pointer, then stripNodeCopy's reflect.Set panics; crashes Add/Update/GetNode/FindChild/GetOrAdd on caller input [Pike conf 92 + go-code-reviewer conf 80; reproduced]. Fix: reject a nil pointer with a sentinel error at the hasNode guard.
- [x] DeleteRecurse leaks closure_tree_meta rows for inner sub-nodes
  closuretree.go:822-831 - the meta cleanup SELECTs descendants from the relations table AFTER those rows were deleted at :815-819, so it matches 0 -> permanent no-op, unbounded meta leak per subtree delete [go-code-reviewer, conf 86]. Fix: capture descendant ids (or delete meta) before deleting the closure rows.
- [x] Move-only does not maintain the sort_order / meta invariants that Add and reorder uphold
  closuretree.go:702-743 (+455-464, 1206-1232, 633) - a re-parent without afterNodeID never rescales the moved node's sort_order to its new siblings nor updates the destination parent's min_halvings -> sibling sort_order collisions and false-negative NeedsRenormalize [go-architect-reviewer, conf 78]. Decision first: should move own re-placement (like Add/reorder), or must callers pair move+reorder? Then fix accordingly.

## B. Concurrency hardening - needs a design decision (no row locks / FKs under READ COMMITTED)

- [x] Concurrent conflicting moves can create a cycle or double-parent (CRITICAL)
  closuretree.go:702-743 - lock-free same-parent/cycle guards; "move A under B" || "move B under A" both pass and commit -> cycle -> runaway TreeDescendants CTE (or MySQL err 3636) [DB/SQL, conf 85]. Fixed: every structural write takes a per-tenant lock first (SELECT ... FOR UPDATE on PG/MySQL, anchor upsert on SQLite) via writeTx/lockTenant in lock.go, serializing conflicting moves. See TestConcurrentConflictingMovesNoCycle.
- [x] Concurrent Add-under-parent vs DeleteRecurse(parent) orphans / phantoms closure rows
  closuretree.go:337-350, 390-404 vs 792-843 - Add's parent check is lock-free and there are no FKs, so a concurrent delete yields a phantom (unreachable) node or orphan closure rows; the "avoid TOCTOU" comment is misleading [DB/SQL, conf 80]. Fixed: the same per-tenant write lock serializes Add against DeleteRecurse. See TestConcurrentAddVsDeleteConsistency.
- [x] Closure unique index includes depth, so (ancestor_id, descendant_id, tenant) is not enforced unique
  closuretree.go:252-257 - dropping depth from idx_closure_uniq turns concurrent double-parent / duplicate-path corruption into a loud UNIQUE violation instead of silent duplicates [DB/SQL, conf 80]. Note: AutoMigrate is additive-only and cannot alter this index - needs a real migration.
- [x] GetOrAdd duplicate-sibling race - fixed by the per-tenant write lock
  getoradd.go:79-90 - concurrent GetOrAdd of a not-yet-existing child both create it (distinct node_ids) -> duplicate siblings; no DB constraint can catch it as modeled [DB/SQL, conf 90]. Resolved: the per-tenant write lock (lock.go) now serializes concurrent GetOrAdd of the same new child, so exactly one is created; TestGetOrAddConcurrent asserts childNodes==1.
- [x] upsertMetaHalvings upsert can keep the higher min_halvings under concurrency
  closuretree.go:1209-1232 - two concurrent first-Adds under a parent race; the loser's lower min_halvings is dropped -> NeedsRenormalize warns slightly late (self-heals) [DB/SQL, conf 75]. Optional fix: ON CONFLICT DO UPDATE SET min_halvings = LEAST(...).

## C. API sharp edges - decide before the 0.10 API freeze

- [x] Query-by-example silently ignores zero-valued match fields
  getoradd.go:131-161 (skip at 148-151) - a match on a legitimately false/0/"" field contributes nothing to the WHERE -> GetOrAdd can reuse the wrong sibling or create a duplicate [Pike, conf 85]. Fix: document loudly and/or add a map-based match form.
- [x] afterNodeID has inconsistent zero-value semantics between Add and Update
  closuretree.go:274 vs 421 - Add uses bare uint (0 = "first"); Update uses *uint (nil = "don't reorder", &0 = "first"); parentID likewise uint-vs-*uint [architect conf 85 + Pike conf 70]. Fix: unify the sentinel convention / document prominently.
- [x] GetOrAdd overwrites caller payload on the found path but preserves it on create
  getoradd.go:95-106 - found path replaces the whole item with the loaded row; create path keeps caller fields -> surprising path-dependent result [Pike, conf 75]. Fix: document the asymmetry on the exported method.
- [x] Get-prefixed accessors violate the Go getter convention
  closuretree.go:233, 239 - rename GetNodeTableName / GetClosureTableName -> NodeTableName / ClosureTableName before the API freezes [Pike, conf 80].
- [x] GetLeaves hand-reconstructs GORM join-column names (breaks under custom naming)
  leaves.go:156-159, 166-172 - builds the m2m FK via inflection.Singular instead of reading stmt.Schema.Relationships -> SQL error under a custom NamingStrategy / SingularTable; also the sole reason for the inflection dependency [Pike, conf 65]. Fix: resolve FK / References from the parsed schema.

## D. Docs and error-handling consistency

- [x] Inconsistent error wrapping on the write / closure paths
  closuretree.go:385-403, 532-536, 730-760 + leaves.go:157-163 - the trickiest write paths return bare driver errors while query methods wrap with %w + context [Pike, conf 85]. Fix: wrap with a short op context.
- [x] Validation errors are ad-hoc strings, not sentinels, with duplicate wording
  closuretree.go:945-956, 1099-1115 + leaves.go:28-81 - promote recurring ones to exported sentinels; unify the "pointer to a slice" message drift [Pike, conf 80].
- [x] Document the Tree concurrency contract
  closuretree.go:42-50 - state "safe for concurrent use by multiple goroutines" (with the GetOrAdd / move races as the exceptions) [Pike, conf 90].
- [x] DeleteRecurse is exported but undocumented
  closuretree.go:792 - add a doc comment (subtree semantics, tenant scoping, ErrNodeNotFound) [Pike, conf 95].
- [x] Document the virtual-root (0, node, level) closure rows
  closuretree.go:408-413 - every node (not just roots) gets an ancestor_id=0 row whose depth = its absolute level; it is load-bearing for the depth filter [DB/SQL, conf 95].
- [x] Document the MySQL cte_max_recursion_depth ceiling (or set it per session)
  closuretree.go:1369-1391, 1466-1490 - trees deeper than 1000 levels error on MySQL 8 (err 3636) but work on SQLite / Postgres [DB/SQL, conf 70].
- [x] buildMatchConditions drops ctx (uses context.Background())
  getoradd.go:131, 148 - thread the caller ctx through so context-honoring GORM valuers / serializers are not silently un-cancellable [go-code-reviewer, conf 80].
- [x] Use any instead of interface{} on the public surface
  closuretree.go:944 + node.go:59, 103 - cosmetic consistency (Go 1.25) [Pike, conf 90].

## E. Structure and refactors - lower urgency

- [x] Split the 1503-line closuretree.go by concern
  closuretree.go (whole) - mixes construction/migration, sort-order/halvings, CRUD + closure SQL, query/CTE, and reflection scanning; getoradd.go already set the split precedent [architect conf 88 + Pike conf 70]. Mechanical move, no behavior change (e.g. sortorder.go, move.go, scan.go, query.go).
- [x] De-duplicate the closure-maintenance choreography and the current-parent lookup
  closuretree.go:353-365 vs 517-530; 474-481 vs 504-513 - the compute-sort -> upsert-meta sequence and the "find my current parent" query are written twice and coupled by convention [architect, conf 88]. Fix: extract a place-among-siblings helper and a currentParent(tx, id, tenant) helper.
- [x] Two row->struct scanning strategies can diverge
  closuretree.go:983 (GORM ScanRows) vs 1084/1118/1257 (hand-rolled mapRowToStruct) - Descendants and TreeDescendants can disagree on type coercion for the same model/driver [architect, conf 85].
- [x] Migration is fused into construction, additive-only and unversioned
  closuretree.go:53-67 -> 112-126 - New always AutoMigrates (needs DDL privilege; concurrent starts race); cannot drop the stale index it tells users to DROP by hand (:251); no schema version / backfill for 0.10's sort_order + meta [architect, conf 82]. Consider exposing newTree as a migration-free constructor + an explicit Migrate().
- [x] Dialect handling has no home (hand-maintained portability despite GORM)
  closuretree.go:211-223, 781, 1222-1231 - scattered isMySQLDialect checks (version gate, upsert branch, CTE-in-DELETE workaround); consider a single dialect-capabilities seam if the DB matrix grows [architect, conf 72]. Fixed: dialect.go is now the single seam - dialectOf classifies once, and the per-dialect behaviors (checkVersion, metaUpsertSQL, lockTenant) live on the dialect enum; isMySQLDialect and the raw driver-name switch are gone. Non-branching portable SQL (recursive-CTE ceiling, CTE-in-DELETE) left as-is.
- [ ] Two sources of truth for column names (parsed map vs hardcoded literals)
  closuretree.go:152-154, 170, 533 etc. hardcode node_id/tenant/sort_order/parent_id while readers use col2FieldMap -> split-brain if Node's mapping changes [architect, conf 78]. Fix: centralize as named constants used by both.
- [x] Standardize not-found detection and slice validation across operations
  closuretree.go mixes RowsAffected==0 vs gorm.ErrRecordNotFound; Descendants (949-957) inlines validation while TreeDescendants uses validateItems (1099-1116) [architect, conf 75].
- [x] Reconsider exposing internal table names as a first-class pattern
  closuretree.go:233, 239 + example_test.go:223-227 - teaching the manual raw-JOIN path next to the safe GetLeaves invites tenant / closure-invariant bypass; the meta table has no accessor (asymmetric) [architect, conf 60].
- [x] GetLeaves validates target after a DB round-trip; checkMySQLVersion has a dead branch
  leaves.go:120-136 (move isLeaveSlice above the DescendantIds call) + closuretree.go:220-223 (SplitN can never yield len < 1) [Pike, conf 85].
- [x] GetLeaves passes the whole descendant id set as IN (?) - bind-param limit on huge subtrees
  leaves.go:126-161 - a subtree over the driver's placeholder cap (e.g. Postgres 65535) errors; chunk or join through the closure table [DB/SQL, conf 80].
- [x] Verify DeleteRecurse's single-reference CTEs materialize on MySQL 8 (err 1093)
  closuretree.go:845-863 - deleteNodesRec / deleteRelationsQuery reference a CTE that selects from the table being deleted; confirm MySQL materializes it (make test-full exercises MySQL) [DB/SQL, conf 55].
