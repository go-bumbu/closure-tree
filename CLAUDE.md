# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make test          # SQLite only (fast)
make test-full     # All DBs via Docker (MySQL, PostgreSQL, SQLite)
make lint          # golangci-lint
make verify        # test-full + lint + license-check + benchmark + coverage
make clean         # Remove generated .sqlite files
```

Run a single test:
```bash
go test -run TestTreeGetNode ./...
```

Inspect SQLite databases during development:
```bash
LOCAL_SQLITE=true go test -run TestTreeGetNode ./...
```

## Architecture

**Closure tree pattern**: two tables per tree — the user's node table (embedding `Node`) and a `closure_tree_rel_<nodetable>` table that stores every ancestor/descendant pair with a `depth` column, enabling O(1) subtree reads.

### Core files

- **`closuretree.go`** — `Tree` struct and all CRUD methods. Write SQL is raw via `fmt.Sprintf` with table name substitution (never user input); read SQL is built in `readsql.go`. `New` validates table names with a regex before any migration. Write operations go through `writeTx` in `lock.go`, not `db.Transaction(...)` directly.
- **`node.go`** — `Node` struct to embed in user types. `ParentId` is read-only (populated via LEFT JOIN on read, ignored on write). Reflection helpers `hasNode`/`hasNodeType`/`findNodeValue` support multi-level embedding.
- **`leaves.go`** — `Leaf` struct for many-to-many targets. `GetLeaves` auto-discovers the `many2many:` GORM tag, validates the m2m table name, then joins through the closure table.
- **`tx.go`** — `WithTx` (a copy of the Tree bound to the caller's transaction), `LockTenant`, `ErrNotInTransaction`.
- **`lock.go`** — `writeTx`: runs a write in its own transaction, or in a savepoint inside the caller's; takes the tenant lock.
- **`placement.go`** — `AddOption`, `PlaceLast`, `ErrConflictingPlacement` for `Add` and `GetOrAdd`.
- **`bulkdelete.go`** — `DeleteRecurseMany` (set semantics, chunked ids) and `DeleteTenant`.
- **`readoptions.go`** — the read options (`Filter`, `Eq`, `IsNull`, `NotNull`, `In`, `PerNode`, `Unfiltered`, `Columns`), `planRead` (validates them into a `readPlan`), `WithFilter` and `ErrUnknownColumn`.
- **`readsql.go`** — builds the read statements from parts (`sqlBuf`, select list, match and not-match predicates); identifiers are validated and quoted, values are bound.
- **`scan.go`** — scans each row with gorm's scanner into the Tree's model, then copies it into the caller's element type (`modelToElem`).
- **`build.go`** — `BuildFromParents`: builds the closure rows of existing nodes from `ParentLink`s, validated in Go before any write; options `Rebuild` and `OrphansToRoot`.

### Key invariants

- **Multi-tenancy**: every operation requires a non-empty `tenant` string. Empty string returns `ErrEmptyTenant`. Use `closuretree.DefaultTenant` as the fallback constant.
- **Schema**: the closure table has a unique constraint on `(ancestor_id, descendant_id, tenant, depth)`. A reflexive self-row at `depth=0` exists for every node; root nodes also have an ancestor row with `ancestor_id=0, depth=1`.
- **`Add`/`Update` use reflection** to copy only non-`Node` fields, using a `map[string]any` for `Update` to preserve zero values. Fields with `gorm:"-"` (empty `DBName`) are excluded.
- **Transactions**: a write on a Tree bound to a transaction (`WithTx`) runs in its own savepoint; on PostgreSQL the library's own transactions run at READ COMMITTED.
- **`New` rejects a model with its own field mapped to `parent_id`** (`ErrParentIDColumn`); only `Node.ParentId` may use that column.
- **`TreeDescendants`/`TreeDescendantsIds`** use `WITH RECURSIVE` CTEs. The internal depth counter is named `cte_depth` (not `depth`) to avoid shadowing any `depth` column in the user struct.
- **MySQL 8.0+** is enforced at `New` time via `checkMySQLVersion`.

### `TreeDescendants` requirements

The items slice must be `[]*YourType`. A `Children` field of `YourType` must be a `[]*YourType`, or the read fails before it queries; without a `Children` field the elements come back unlinked. Each row is scanned with gorm's scanner into the Tree's model (parsed at `New` time), then copied into `YourType` (see `modelToElem` in `scan.go`): as a whole when `YourType` is the model or embeds it directly; otherwise each field goes to the field at the same path of Go names (gorm's `Field.BindNames`), or, when there is none, to the field with its Go name if no other model field has that name. gorm skips result columns the model has no field for (like `cte_depth`). A field of a type that cannot be assigned, converted or dereferenced fails the read before it queries. Children are linked through the scanned row's node id and parent id, so `YourType` need not embed `Node`. `Descendants` follows the same copy rule and rejects an element that is not a struct or a pointer to a struct.

### Read options

`GetNode`, `FindChild`, `Descendants`, `DescendantIds`, `TreeDescendants` and `TreeDescendantsIds` take `opts ...ReadOption`; `GetLeaves`, `IsDescendant` and `IsChildOf` take none and are not filtered. `planRead` validates all options first, so an unknown column returns `ErrUnknownColumn` from every read, also from one that ignores the option. A column can be named when its field has a column name and is not both read-only and excluded from migration (`hasColumn`); the computed parent cannot be named. `Eq` and `In` reject a value that is a list or a gorm expression. An empty `Filter()` has no effect; only `Unfiltered()` switches the Tree's default filter off, and only a `Filter` with a cond replaces it. `WithFilter` with conds replaces any existing default filter; without conds it keeps it. Writes are never filtered.

Without `Columns` a read selects every column of the node table (`nodes.*`); with `Columns`, `node_id`, `tenant`, `sort_order` and the named columns. The parent is always computed from the closure rows, as the last column, so it overrides a leftover physical `parent_id` column. The recursive step of a tree read keeps its join order (`orderedJoin`), and the prune check of the flat reads walks the candidate's own ancestor rows, so filtered reads stay fast on tables without statistics.
