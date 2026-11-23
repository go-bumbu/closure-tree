# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make test          # SQLite only (fast)
make test-full     # All DBs via Docker (MySQL, PostgreSQL, SQLite)
make test-race     # Race detector, SQLite only, skips the large-tree tests (-short)
make lint          # golangci-lint
make verify        # lint + test-full + test-race + license-check + benchmark + coverage
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

- **`closuretree.go`** — `Tree` struct, `New`, `Migrate`, `Add` and `DeleteRecurse`. Write SQL is raw via `fmt.Sprintf` with table name substitution (never user input); read SQL is built in `readsql.go`. `New` validates table names with a regex before any migration. Write operations go through `writeTx` in `lock.go`, not `db.Transaction(...)` directly.
- **`node.go`** — `Node` struct to embed in user types. `ParentId` is read-only (populated via LEFT JOIN on read, ignored on write). Reflection helpers `hasNode`/`hasNodeType`/`findNodeValue` support multi-level embedding.
- **`query.go`** — the node reads `GetNode`, `Descendants`, `DescendantIds`, `TreeDescendants`, `TreeDescendantsIds` (with `TreeNode`, `SortTree`), and the unfiltered checks `IsDescendant`, `IsChildOf`.
- **`move.go`** — `Update`: changes a node's fields, moves it (`moveInTx`: rejects a cycle or a same-parent move with `ErrInvalidMove`, deletes the subtree's outside paths and inserts the new ones) and reorders it, in one write.
- **`getoradd.go`** — `FindChild` and `GetOrAdd`: match a direct child of a parent on the named fields (`buildMatchConditions`, NULL matched as `IS NULL`); `GetOrAdd` adds the item when no child matches.
- **`sortorder.go`** — sibling order: `sort_order` bisection between neighbours (`computeSortOrder`), the halvings left per parent in the meta table (`closureTreeMeta`), `Renormalize`, `RenormalizeAll`, `NeedsRenormalize`, `NeedsRenormalizeAny`, `DefaultHalvingsBuffer`.
- **`leaves.go`** — `GetLeaves` for any model with a many2many relation to the nodes (`[]T` or `[]*T`, one primary key column). It picks the relation that points to the node table (`Relation` names it when several do), validates the table names and the join columns, and selects the rows with `IN` over the `SubtreeIDs` subquery, ordered by the primary key. `Leaf` is optional and deprecated: embedding it only adds a tenant condition on the model's own table.
- **`ancestors.go`** — `Ancestors`, `AncestorIDs`: one statement over the closure rows of the node, plus an existence check (`requireNode`).
- **`childcounts.go`** — `ChildCounts`: direct children per id, ids deduplicated and sent in chunks of `idChunkSize`.
- **`subtree.go`** — `SubtreeIDs`, `SubtreeIDsOf`, `ClosurePairs`, `ErrNoAncestors`: they build a `*gorm.DB` subquery (`subquery`) and execute nothing; the caller embeds it in its own query. The output column names are public API.
- **`tx.go`** — `WithTx` (a copy of the Tree bound to the caller's transaction), `LockTenant`, `ErrNotInTransaction`, `ErrSavepointRollback`.
- **`lock.go`** — `writeTx`: runs a write in its own transaction, or in a savepoint inside the caller's; takes the tenant lock.
- **`placement.go`** — `AddOption`, `PlaceLast`, `ErrConflictingPlacement` for `Add` and `GetOrAdd`.
- **`bulkdelete.go`** — `DeleteRecurseMany` (set semantics, chunked ids) and `DeleteTenant`.
- **`readoptions.go`** — the read options (`Filter`, `Eq`, `IsNull`, `NotNull`, `In`, `PerNode`, `Unfiltered`, `Columns`, `DepthRange`, `ForAncestors`, `IncludeVirtualRoot`, `Relation`, `NoPreload`), `planRead` (validates them into a `readPlan`), `WithFilter`, `ErrUnknownColumn` and `ErrInvalidDepthRange`.
- **`readsql.go`** — builds the read statements from parts (`sqlBuf`, select list, match and not-match predicates); identifiers are validated and quoted, values are bound.
- **`scan.go`** — scans each row with gorm's scanner into the Tree's model, then copies it into the caller's element type (`modelToElem`).
- **`collation.go`** — `ensureTenantCollation`, the last step of `Migrate` on MySQL: gives the tenant columns of a tree without rows the collation `utf8mb4_0900_bin` (the statement from `alterTenantCollationSQL`, which accepts only a `varchar(n)` type), and returns `ErrTenantCollation` when the four columns disagree.
- **`build.go`** — `BuildFromParents`: builds the closure rows of existing nodes from `ParentLink`s, validated in Go before any write; options `Rebuild` and `OrphansToRoot`.
- **`dialect.go`** — `dialect`: the places PostgreSQL, MySQL and SQLite differ — the MySQL version gate (`checkVersion`), the meta upsert (`metaUpsertSQL`), the tenant lock (`lockTenant`, `ErrLockNotAcquired`) and the isolation level of the library's transactions (`txOptions`).
- **`columns.go`** — the constants for the column names the raw SQL uses (`colNodeID`, `colTenant`, …); `TestColumnNamesMatchSchema` checks them against the names gorm derives.

### Key invariants

- **Multi-tenancy**: every operation requires a non-empty `tenant` string. Empty string returns `ErrEmptyTenant`. Use `closuretree.DefaultTenant` as the fallback constant. Tenants are compared exactly on every database: "Acme", "acme", "acmé" and "acme " are four tenants. On MySQL, `Migrate` sets the collation `utf8mb4_0900_bin` on the tenant column of the tree's four tables while the tree holds no rows (`ensureTenantCollation`). A tree with rows keeps its collation unless gorm's `AutoMigrate`, which `Migrate` runs first, modifies its tenant column: it does so on the node table or the closure table when the column's type, size, default or comment differs from the model, and without a collation, which gives the column the default collation of its table; it leaves the tenant columns of the meta and lock tables alone, as they are part of the primary key, so `Migrate` then returns `ErrTenantCollation` unless that default is the collation of the other tenant columns (README, "Adopting an existing table"). Under MySQL's default, `utf8mb4_0900_ai_ci`, tenants that differ only in case or accents are one tenant, and `DeleteTenant` deletes all of them. To change it, callers stop writes, store one spelling per tenant, then run on each of the four tables, with the column's current type, `ALTER TABLE <table> MODIFY tenant varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL;` (without `NOT NULL` for a `Leaf` table). Each rebuilds its table, and until all four have run the tables compare tenants differently. `Migrate` returns `ErrTenantCollation` until all four agree.
- **Schema**: the closure table has a unique constraint on `(ancestor_id, descendant_id, tenant, depth)`. A reflexive self-row at `depth=0` exists for every node; root nodes also have an ancestor row with `ancestor_id=0, depth=1`. A node row without a closure row at `depth=1`, as before `BuildFromParents`, is not part of the tree: `Add`, `GetOrAdd` and a move under it return `ErrParentNotFound` wrapped with a message naming `BuildFromParents` (`requireParent` in `closuretree.go`, one statement).
- **`Add`/`Update` use reflection** to copy only non-`Node` fields, using a `map[string]any` for `Update` to preserve zero values. Fields with `gorm:"-"` (empty `DBName`) are excluded.
- **Transactions**: a write on a Tree bound to a transaction (`WithTx`) runs in its own savepoint; the rollback to it ignores the write's cancellation but runs under a 30-second timeout (`savepointRollbackTimeout` in `lock.go`), and a failed rollback to it is wrapped in `ErrSavepointRollback` (the caller's transaction is then unusable); on PostgreSQL and MySQL the library's own transactions run at READ COMMITTED.
- **`New` rejects a model with its own field mapped to a column of `Node`** (`node_id`, `parent_id`, `tenant`, `sort_order`, taken from the Node-owned fields of the schema) with `ErrNodeColumn`, which for `parent_id` also matches `ErrParentIDColumn` (`checkNodeColumns` in `closuretree.go`); only Node's own fields may use those columns, so a model's own `Tenant` field must be on another column or `gorm:"-"`. `Node` itself is a valid model: `nodeOwned` counts all its fields as Node-owned, and `findNodeValue` returns the value itself.
- **`TreeDescendants`/`TreeDescendantsIds`** use `WITH RECURSIVE` CTEs. The internal depth counter is named `cte_depth` (not `depth`) to avoid shadowing any `depth` column in the user struct.
- **MySQL 8.0.17+** is enforced at `New` time via `checkVersion` in `dialect.go`.

### `TreeDescendants` requirements

The items slice must be `[]*YourType`, and `YourType` must have a `Children` field of type `[]*YourType` that can be linked; otherwise the read returns a plain error naming the type and the case before it validates the tenant and before it queries, for every `maxDepth`, also 1 (`childrenIndex` in `scan.go`). The cases are: no `Children` field; one in each of two structs embedded at the same depth (Go's `FieldByName` promotes neither, so `embedsField` tells this apart from none); one of another type; one promoted through an embedded pointer to an unexported type, which reflection cannot allocate. An unexported embedded struct value is no obstacle. A caller without the field adds it, or reads with `Descendants` (same parameters; without `PerNode`, at `maxDepth` 1 it returns the same nodes in the same order). A `Children` field promoted through an embedded pointer struct is reached on every element, leaves included, by allocating the nil pointer (see `linkTree` in `scan.go`). Each row is scanned with gorm's scanner into the Tree's model (parsed at `New` time), then copied into `YourType` (see `modelToElem` in `scan.go`): as a whole when `YourType` is the model or embeds it directly; otherwise each field goes to the field at the same path of Go names (gorm's `Field.BindNames`), or, when there is none, to the field with its Go name if no other model field has that name. gorm skips result columns the model has no field for (like `cte_depth`). A field of a type that cannot be assigned, converted or dereferenced fails the read before it queries. Children are linked through the scanned row's node id and parent id, so `YourType` need not embed `Node`. `Descendants` follows the same copy rule and rejects an element that is not a struct or a pointer to a struct.

### Read options

Every read takes `opts ...ReadOption` except `IsDescendant` and `IsChildOf`, which take none and are not filtered. The table in the README section "Narrowing reads" says which option each read honours; an option a read does not honour has no effect there. `planRead` validates all options first, so an unknown column returns `ErrUnknownColumn` and an invalid depth range `ErrInvalidDepthRange` from every read, also from one that ignores the option. A column can be named when the field gorm maps it to (`FieldsByDBName`) has a column name and is not both read-only and excluded from migration (`hasColumn`); the computed parent cannot be named. A column name wins over a Go field name; a Go field name that fields of several nameable columns share (the model's own field and one of a struct embedded with a prefix) returns `ErrUnknownColumn` (`Tree.column`), because gorm's `LookUpField` would return the last field declared. The `FindChild`/`GetOrAdd` match fields resolve by the same rule (`lookUpColumn`, shared with `Tree.column`) on the schema of the match item's type, counting the columns of fields that are not Node-owned (so a model's own `Tenant` field on another column resolves there, while it is ambiguous for the read options) and without the `hasColumn` check (so a read-only field excluded from migration is accepted); a shared Go name returns `ErrUnknownMatchField`. `Eq`, `In` and the match values reject a value that gorm would not bind as one value (`checkBindValue`): a list, a gorm expression, a `gorm.Valuer` or a `sql.NamedArg`; the match checks the value gorm reads from the field, so a serializer field (a `driver.Valuer` wrapper) is matched by its serialized value. The match also rejects a field with a column default (`HasDefaultValue`: a `default:` tag or `autoIncrement`) whose value gorm reports as zero, also a nil pointer, because `Create` stores the default instead. A byte slice that `Add` stores as NULL matches as `IS NULL` (`addedAsNull`), except on MySQL behind a pointer: the driver renders that value itself, and with `interpolateParams=true` stores a nil `json.RawMessage` as `''`, so the match binds the same pointer with `<=>`. An empty `Filter()` has no effect; only `Unfiltered()` switches the Tree's default filter off, and only a `Filter` with a cond replaces it. `WithFilter` with conds replaces any existing default filter; without conds it keeps it. Writes are never filtered.

Without `Columns` a read selects every column of the node table (`nodes.*`); with `Columns`, `node_id`, `tenant`, `sort_order` and the named columns. The parent is always computed from the closure rows, as the last column, so it overrides a leftover physical `parent_id` column. The recursive step of a tree read keeps its join order (`orderedJoin`), and the prune check of the flat reads walks the candidate's own ancestor rows, so filtered reads stay fast on tables without statistics.
