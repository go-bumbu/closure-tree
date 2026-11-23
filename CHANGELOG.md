# Changelog

## v0.11.0

### Breaking changes

- **MySQL 8.0.17 or later.** `New` returns an error for a MySQL server older than 8.0.17; v0.10.0
  accepted any 8.0. Upgrade the server. TiDB, which by default reports itself as 8.0.11, is
  rejected; it is not supported.
- **MySQL: tenants are compared exactly.** On MySQL, `Migrate` gives the tenant column of the
  tree's four tables the collation `utf8mb4_0900_bin` while none of them has a row, so "Acme",
  "acme" and "acmé" are three tenants, as on PostgreSQL and SQLite. The library does not convert a
  tree that has rows: it keeps its collation, unless gorm's `AutoMigrate` modifies its tenant
  column, and under MySQL's default `utf8mb4_0900_ai_ci` tenants that differ only in case or accents
  stay one tenant (`DeleteTenant` deletes all of them). `Migrate` returns the new
  `ErrTenantCollation` when the four tenant columns do not share one collation. To change a tree
  with rows, follow the paragraph "Tenants" under "Closure Tree methods" in the README. On a tree
  without rows, `Migrate` sets that collation with `ALTER TABLE ... MODIFY`, also on the first
  `Migrate` of a new tree, so the database user needs the ALTER privilege, where v0.10.0 needed
  only CREATE for a new tree: grant it.
- **MySQL: the library's own transactions run at READ COMMITTED**, whatever the connection's
  default level is (every write on a Tree that is not inside a caller's transaction). A server with
  binary logging on and `binlog_format=STATEMENT` refuses these writes (error 1665): switch it to
  `ROW`, the default, or `MIXED`.
- **Tenant index on the closure table, and repeated writes after a MySQL deadlock.** On a tree
  created before v0.11.0, the next `Migrate` adds an index on the tenant column of the closure
  table, `idx_closure_tree_rel_<node table>_tenant` (gorm's naming strategy shortens a name that is
  too long), unless an index of exactly that name exists. On a large closure table the build takes
  time: PostgreSQL blocks writes to the closure table until it is built, so build it beforehand
  with `CREATE INDEX CONCURRENTLY` under that name (README, the paragraph on `Migrate` under
  "Usage"); MySQL builds it online, but at its start and end waits for a metadata lock until the
  open transactions that used the closure table end, and while it waits every new statement on that
  table queues behind it, so run `Migrate` when no long transaction uses the tree. On MySQL, a
  write in a transaction the library opens itself (a Tree not bound with `WithTx`, or bound to a
  handle that is not in a transaction) that loses a deadlock (error 1213) now runs again, up to 5
  attempts in all, so your model's gorm hooks run once per attempt, also in attempts MySQL rolled
  back: keep them free of effects outside the database, or let those effects tolerate repeats. See
  "Concurrent writes on MySQL" in the README.
- **`New` rejects a model with a field of its own on a column of `Node`** (`node_id`,
  `parent_id`, `tenant` or `sort_order`, for example its own `Tenant string`) with the new
  `ErrNodeColumn`; for `parent_id` the error also matches the new `ErrParentIDColumn`. v0.10.0
  accepted such a model, and `Add` stored the field's value instead of the tenant, id or sort order
  the tree sets. Map the field to another column (``gorm:"column:owner_tenant"``) or remove it.
  `New(db, closuretree.Node{})` stays accepted.
- **Nine methods gained a trailing variadic parameter:** `Add` and `GetOrAdd` (`...AddOption`),
  `GetNode`, `FindChild`, `Descendants`, `DescendantIds`, `TreeDescendants`,
  `TreeDescendantsIds` and `GetLeaves` (`...ReadOption`). Calls compile unchanged; an interface,
  mock or function type with the old signatures no longer matches `*Tree`: add the parameter.
- **`GetOrAdd` and `FindChild` check their match fields before the query:**
  - a Go field name that fields of several columns share (the model's own `Name` and the `Name` of
    a struct embedded with a column prefix) returns `ErrUnknownMatchField`; v0.10.0 used the last
    declared field. Name the column instead.
  - a field whose value is a list (a slice other than a byte slice, or an array), a gorm
    expression, a `gorm.Valuer` or a `sql.NamedArg` returns an error. Such a field cannot be a match
    field.
  - a field with a column default (a `default:` tag, or `autoIncrement`, which gorm treats as a
    default) whose value is the zero value returns an error. gorm stores the default instead of the
    zero value (for `autoIncrement`, the next number); where that differs from the zero value,
    v0.10.0 never found the node and `GetOrAdd` added a new one on every call. A field whose default
    is the zero value (`default:0`) is rejected as well, though v0.10.0 found its node. Set a value,
    or remove the default.
  - on a model that embeds `Node` through another struct, a field of `Node` (`NodeId`, `Tenant`,
    `SortOrder`) returns `ErrUnknownMatchField`, as it already did for a model that embeds `Node`
    directly; v0.10.0 matched it. Remove it from the match fields.
- **`GetLeaves` on a model with several many2many relations to the node table** returns the new
  `ErrAmbiguousRelation`; v0.10.0 used the first one. Name one with the `Relation` option, for
  example `Relation("Tags")`.
- **`TreeDescendants` needs a `Children []*T` field it can link into.** Without one (none, one of
  another type, one in each of two structs embedded at the same depth, or one promoted through an
  embedded pointer to an unexported type), it returns an error before it queries, for every
  `maxDepth`, also 1; v0.10.0 returned only the top-level nodes of a type without the field. Add
  the field, or read with `Descendants`.
- **`TreeDescendants` allocates the embedded pointer struct that holds `Children`** on every
  element, leaves included. v0.10.0 left it nil at `maxDepth` 1 and panicked on a read that
  returned children.
  Code that tested the pointer for nil to find leaves tests `len(Children)` instead.
- **`Descendants` and `TreeDescendants` copy fields more strictly.** Whether an element field can
  take a model field is now decided by the model field's type, before the query, also when the read
  would return no rows; v0.10.0 decided per row by the value the driver returned. An element field
  that cannot be assigned, converted or dereferenced from the model field returns an error, and so
  does an integer field read into a string field (v0.10.0 stored the code point: 65 became "A"). A
  number outside the range of a smaller element field (an `int64` into an `int8`), or a NaN or
  infinite float read into an integer, fails the read with an error naming the field and the node,
  where v0.10.0 stored a wrapped value. Give the element field the model field's type, or convert
  the value yourself.
- **`Add`, `GetOrAdd` and a move with `Update` under a node row that is not linked into the tree**
  (no closure row at depth 1, as for rows not yet built with `BuildFromParents`) return a wrapped
  `ErrParentNotFound` whose message names `BuildFromParents`, and write nothing; v0.10.0 wrote a
  node cut off from the tree. Build the tree with `BuildFromParents` first.

### New

- Run a Tree inside your own transaction: `WithTx`, `LockTenant`, `ErrNotInTransaction`,
  `ErrSavepointRollback`, and `ErrLockNotAcquired` for a write or `LockTenant` on PostgreSQL or
  MySQL whose tenant lock row was deleted between ensuring and locking it on each of its 3
  attempts (README "Using closure-tree inside your own transaction").
- The ancestors of a node, root first: `Ancestors` and `AncestorIDs`.
- Filter the nodes of a read by their columns: `Filter` with the `Cond`s `Eq`, `IsNull`, `NotNull`
  and `In`, `PerNode`, `Unfiltered`, a default filter with `WithFilter`, and `ErrUnknownColumn`;
  all read options are of type `ReadOption` (README "Narrowing reads").
- Turn an existing table whose rows know their parent into a tree: `BuildFromParents` with
  `ParentLink`, the `BuildOption`s `Rebuild` and `OrphansToRoot`, and the errors
  `ErrLinksMismatch`, `ErrOrphanLink`, `ErrLinkCycle` and `ErrClosureExists` (README "Adopting an
  existing table").
- The number of direct children of many nodes in one call: `ChildCounts`.
- Read only some columns: `Columns`.
- Place a new node last among its siblings: the `AddOption` `PlaceLast` for `Add` and `GetOrAdd`,
  and `ErrConflictingPlacement` when it is combined with an `afterNodeID`.
- Delete several subtrees in one transaction: `DeleteRecurseMany`, and `ErrVirtualRoot` for id 0.
- Delete a whole tenant: `DeleteTenant`.
- Subtree subqueries to embed in your own queries: `SubtreeIDs`, `SubtreeIDsOf`, `ClosurePairs`,
  the options `DepthRange`, `ForAncestors` and `IncludeVirtualRoot`, and the errors
  `ErrNoAncestors` and `ErrInvalidDepthRange` (README "Subtree subqueries"). `GetLeaves` reads any
  model with a many2many relation to the nodes, into `[]T` or `[]*T`, and takes the read options,
  `Relation` and `NoPreload`; `Leaf` is optional (also embedded by pointer) and deprecated, and so
  is `ErrItemIsNotTreeLeaf`, which `GetLeaves` no longer returns (README "Querying leaves").

### Fixed

- `Descendants` and `TreeDescendants` failed with "cannot assign type ..." on a model with a
  pointer field (`*string`, `*time.Time`) or a `sql.Scanner` field (`sql.NullString`) holding a
  value.
- `Descendants` and `TreeDescendants` panicked on a model that embeds by pointer a struct with
  columns; the pointer is allocated now.
- `Descendants` into a slice whose element is not a struct panicked; it returns an error.
- A `Descendants` read that failed after the query started left the rows read so far in the
  caller's slice; the slice keeps its length now.
- `TreeDescendants` ordered siblings by node id alone when the element type had no `SortOrder`
  field, and panicked when that field was not a float; siblings are ordered by the row's
  `sort_order`, then node id.
- `GetOrAdd` added a duplicate on every call, and `FindChild` did not find the node, when a match
  field was NULL other than through a nil pointer: a `driver.Valuer` whose `Value` is nil (such as
  `sql.NullString{}`), also behind a pointer, a nil byte slice, an empty `json.RawMessage`, or a
  pointer to a nil `json.RawMessage`.
- On a model that embeds `Node` through another struct, `Update` with an item reset the node's
  `sort_order` to 0.
- `Update` that moved a missing node to the root and reordered it returned nil and wrote a
  metadata row; it returns `ErrNodeNotFound`.
- `Add` on a tree of `closuretree.Node` itself (`New(db, closuretree.Node{})`) stored an empty
  tenant.
- A Tree made from a chained handle, such as `db.Set("gorm:table_options", ...)`, failed its second
  `Migrate`, because `Migrate` left the lock table on the handle.
- `GetLeaves` used the model's first many2many field even when it pointed to another table, and
  returned the wrong rows; it uses the relation to the node table, and returns `ErrLeafMissingM2M`
  when there is none.
- On a PostgreSQL server whose default isolation level is REPEATABLE READ, the checks inside the
  library's own write transactions, such as the cycle check of a move, read a snapshot taken before
  the tenant lock was granted, so two concurrent moves could form a cycle. These transactions now
  run at READ COMMITTED.
