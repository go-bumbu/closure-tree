# closure-tree

A closure tree is a read efficient way to define tree dependencies of items, e.g. tags.

Features:
* store any basic struct information in a tree structure (Uses GORM under the hood)
* multi-user/tenant: allows to perform all the operations on a specific user or tenant
* retrieve all the nested descendants of a node
* control display order of siblings via float64 fractional indexing (`sort_order`)


## DBs
while all the Gorm supported databases should work, since closure-tree relies  heavily on Raw queries, 
it might not be the case; currently it's tested with:
* mysql (8.0.17 or later; with binary logging on, `binlog_format` must be `ROW`, the default, or `MIXED`)
* postgres
* sqlite ("gorm.io/driver/sqlite")
* sqlite without CGO ("github.com/glebarez/sqlite")
 

## Usage

Add the module  
```
go get github.com/go-bumbu/closure-tree
```

Usage: 

Define your tree items, we will crete Tags in this example; make sure to embedd the Node struct, this is required.
```GO
type Tag struct {
  ct.Node
  Name    string
}
```
Now we can create a tree instance:
```GO
tree, err := ct.New(db, Tag{})
// handle error
err = tree.Migrate() // creates or updates the tables; call it once at application start up
// handle error
```
`New` only parses and validates the model, it does not touch the schema. `Migrate` creates the
tables and should be called once at application start up.

A model must not declare its own field mapped to the column `parent_id` (the embedded `Node` already
owns it as `ParentId`); `New` returns `ErrParentIDColumn` in that case.

Tables created by v0.8.0 or earlier may carry a stale `parent_id` column. Reads ignore it, and it is
safe to drop.

To populate the tree we do as follows:
```GO
ctx := context.Background()

colorTag := Tag{Name: "colors"}
err := tree.Add(ctx, &colorTag, nil, nil, "user1") // add a node at the root, first among its siblings
// handle error

warmColor := Tag{Name: "warm"}
parent := colorTag.Id()
err = tree.Add(ctx, &warmColor, &parent, nil, "user1") // add a node below "colors"
// handle error

```
Note that the `Id()` method returns the node ID once the struct has been passed as a pointer to `Add`.
`Add` takes `*uint` for `parentID` and `afterNodeID`: a `nil` `parentID` means "at the root" and a
`nil` `afterNodeID` means "first among the siblings". Pass a sibling's ID as `afterNodeID` to insert
immediately after it.

Once populated we can query the tree (get all descendants)

```GO
parentId := uint(0)
descendantsIds, err := tree.DescendantIds(ctx, parentId, 0, "user1")
// handle error
```
This will return a flat list of all children to the passed parent id, in this case 0 is all.


### Querying leaves

With the basic implementation you have an efficient way to store and manage items in a tree
structure. Closure tree can also read many-to-many relations between _leaves_ and _nodes_.

```GO
// your own model, e.g. Book, with a many2many relation to the node type
type Book struct {
    ID     uint `gorm:"primarykey"`
    Name   string
    Tags []Tag `gorm:"many2many:books_tags;"` // Tag is the item stored in the tree
}

parentId := uint(2)
maxDepth := 0
tenant := "user1"

var books []Book // will be populated
err = tree.GetLeaves(ctx, &books, parentId, maxDepth, tenant)
if err != nil {
    fmt.Print(err)
}
```

* The target is a pointer to `[]T` or `[]*T`, where `T` is any model with one primary key column
  and a many2many relation to the node model. Rows come ordered by the primary key, each once, also
  when it is attached to several nodes of the subtree.
* `maxDepth` limits how far below the parent a node may be; the parent itself is included, and 0 or
  less means no limit. Parent 0 is the whole tenant.
* The relation is loaded into the result unless you pass `ct.NoPreload()`. The loaded nodes are not
  checked against the tenant or the filter. It binds one value per returned row, so use
  `NoPreload` for results of many thousands of rows.
* When the model has several relations to the node model, name one with `ct.Relation("Tags")`;
  without it `GetLeaves` returns `ErrAmbiguousRelation`. A model with no relation to the node
  table, or a `Relation` that is not one, returns `ErrLeafMissingM2M`.
* `ct.DepthRange(min, max)` replaces `maxDepth` (see "Subtree subqueries").
* The filter and the Tree's default filter apply to the node side, with the meaning they have in
  `SubtreeIDs`: a row is returned when it is attached to a node that counts.
* Only the node side is scoped to the tenant. A model that embeds `ct.Leaf` (by value or by pointer)
  also gets the tenant condition on its own table. `Leaf` is optional and deprecated; a caller who
  needs conditions on the model's own table writes the query with `SubtreeIDs` instead.
* `GetLeaves` returns an error before it queries for a relation whose node side does not reference
  `node_id`, for a relation with more than one join column on a side, and for a model that embeds
  `Leaf` but has no tenant column.
* For ordering, paging or counting, use `SubtreeIDs`.


### Get-or-create for derived data

When a tree is built from *derived* data — domain/subdomain hierarchies, filesystem-like paths,
imported categories — the same node is seen repeatedly and must attach to the existing node
instead of minting a duplicate. `GetOrAdd` is an idempotent get-or-create for a direct child; it
uses query-by-example to find an existing match and chains cleanly to build a path:

```GO
// Build a derived path a -> b -> c; re-deriving a shared prefix reuses existing nodes.
parent := uint(0)
for _, name := range []string{"a", "b", "c"} {
    node := &Tag{Name: name}
    created, err := tree.GetOrAdd(ctx, node, parent, "user1", []string{"Name"})
    // handle error; `created` reports whether a new node was inserted
    parent = node.NodeId // the id becomes the parent of the next level down
}
```

`matchFields` names the fields of the item (Go field names or column names) that identify the
child; each is matched by its actual value, including zero values, and the embedded `Node` fields
cannot be used. A name is resolved as in the read options: a column name wins, and a Go field name
that fields of several columns share, such as the model's own `Name` and the `Name` of a struct
embedded with a column prefix, returns `ErrUnknownMatchField`: use the column name. Two
differences from the read options remain: the `Node` columns do not count when a Go field name is
resolved, so a model's own `Tenant` field on another column resolves to that column, where the read
options find the name ambiguous; and a read-only field excluded from migration
(`gorm:"->;-:migration"`) is accepted, so the query fails when the table has no such column. A nil
pointer field and a `driver.Valuer` field whose `Value` is nil (such as an invalid
`sql.NullString`) match NULL. A byte-slice field that is not a `driver.Valuer` and that `Add`
stores as NULL matches NULL too: a nil one, also behind a pointer, and an empty one of a named type
such as `json.RawMessage` that is not behind a pointer, which gorm writes as NULL; an empty
`[]byte` matches empty bytes. On MySQL, a pointer to a nil `json.RawMessage` matches what the
driver stores for it: empty bytes with the driver's `interpolateParams=true`, NULL without. A
`driver.Valuer` is matched by what its `Value` returns, and matches NULL only when that is nil, not
when it is a nil byte slice. `Eq` binds the value it is given, so use `IsNull` there. Every other value is bound as one value, as with `Eq`,
and every value is checked as `Eq` checks it: a field whose value is a slice or an array (other
than a byte slice or a `driver.Valuer`), or whose type is a `gorm.Valuer` (such as a JSON column
type, also as a nil pointer), returns an error before any query. Such a field cannot be a match
field, but an item with a `gorm.Valuer` field can still be added. A field with a column default
(a `default:` tag, or `autoIncrement`) whose value is the zero value, also a nil pointer, returns
an error before any query too, because gorm stores the default instead of the zero value; any other
value, also a pointer to the zero value, is matched as it is. gorm expands a list into several
values on insert as well, so an item whose list field holds more than one element cannot be added;
give such a field a serializer. A field with a gorm serializer (`gorm:"serializer:json"`) is
matched by its serialized value. The match is scoped to the tenant; `parentID = 0` operates on the
root level. Use `FindChild` for the lookup alone:

```GO
var out Tag
found, err := tree.FindChild(ctx, parentID, "user1", Tag{Name: "b"}, []string{"Name"}, &out)
```

`GetOrAdd` runs the find and the add in a single transaction under the tenant lock, so a node is
never left half created and two callers creating the same brand-new child are serialized: the
first creates it, the second finds it and gets `created = false`, so no duplicate is produced.
A new node is placed first among its siblings; pass `closuretree.PlaceLast()` to append it last.

### Narrowing reads

The reads in the table below take trailing options that choose which nodes they return and which
columns they fill. `IsDescendant` and `IsChildOf` take no read options and are not filtered.

| Call | `Filter` / `Unfiltered` | `PerNode` | `Columns` | `DepthRange` | Others |
|---|:-:|:-:|:-:|:-:|---|
| `GetNode`, `FindChild` | yes | - | yes | - | |
| `Descendants` | yes | yes | yes | - | |
| `DescendantIds` | yes | yes | - | - | |
| `TreeDescendants` | yes | - | yes | - | |
| `TreeDescendantsIds` | yes | - | - | - | |
| `Ancestors` | yes | - | yes | - | |
| `AncestorIDs`, `ChildCounts` | yes | - | - | - | |
| `SubtreeIDs`, `SubtreeIDsOf` | yes | yes | - | yes | |
| `ClosurePairs` | yes | - | - | yes | `ForAncestors`, `IncludeVirtualRoot` |
| `GetLeaves` | yes | yes | - | yes | `Relation`, `NoPreload` |

An option that a call does not honour has no effect there. Column names and the depth range of
every option are still validated by every call.

```GO
// Tag also has the field `Hidden bool`
visible, err := tree.WithFilter(ct.Eq("Hidden", false)) // a Tree whose reads skip hidden tags
// handle error

var names []*Tag
err = visible.Descendants(ctx, 0, 1, "user1", &names, ct.Columns("Name")) // one level, only Name filled
// handle error

var all []*Tag // flat reads append to the slice, so use a new one for each read
err = visible.Descendants(ctx, 0, 1, "user1", &all, ct.Unfiltered()) // hidden tags too
```

* `Filter(conds ...Cond)` keeps the nodes that match every cond. The conds are `Eq`, `IsNull`,
  `NotNull` and `In`. `Eq` with `nil`, a nil pointer, or a `driver.Valuer` whose `Value` is nil
  (such as `sql.NullString{}`) means `IsNull`, and `Eq` dereferences a non-nil pointer that is
  not a `driver.Valuer`. `In` with no values matches nothing, and `In` never matches NULL:
  `In("note", nil)` matches nothing, use `IsNull`. Values are passed one by one and each is bound
  as one value; a slice or an array (other than a byte slice or a `driver.Valuer`), a gorm
  expression, a `gorm.Valuer` or a `sql.NamedArg` is rejected with an error before any query
  runs; the check is by type, so a nil pointer of a `gorm.Valuer` type is rejected too. A tree
  read binds the values of `In` up to three times, so a very long list reaches the database's
  limit of bound values sooner there (SQLite 32766, PostgreSQL and MySQL 65535 per statement).
* A column is named by its column name or its Go field name; a column name wins. A column can be
  named when the field gorm maps it to has a column name and is not both read-only and excluded
  from migration; the computed parent cannot be named. A Go field name that fields of several
  columns that can be named share, such as the model's own `Name` and the `Name` of a struct
  embedded with a column prefix, cannot be named either: use the column name. An unknown name returns `ErrUnknownColumn` before any query
  runs, from every read, also one that ignores the option.
* A node does not match when the predicate is not true, which includes NULL.
* Meaning of the filter per read:

| Read | Meaning |
|---|---|
| `Descendants`, `DescendantIds` | A node is returned only if it and every node between the requested parent and it match. With `PerNode()` a node is returned when it matches itself, whatever its ancestors do. |
| `TreeDescendants`, `TreeDescendantsIds` | A node that does not match is left out with its whole subtree. Its children are never re-parented. |
| `GetNode` | A node that does not match returns `ErrNodeNotFound`. |
| `FindChild` | A child that does not match is not found. The parent's own match is not checked. |

The other reads state their meaning in their own sections: "Ancestors and child counts", "Subtree
subqueries" and "Querying leaves".

* A requested parent that is a real node and does not match gives an empty result in the prune and
  tree reads; parent 0 always matches. With `PerNode()` the parent is not checked.
* Without `Columns` a read selects every column of the node table. `Columns(names ...string)`
  selects `node_id`, `tenant`, `sort_order` and the named columns. Fields that are not selected stay
  zero. A filter may name a column that is not selected. The parent is always computed from the
  closure rows and overrides a leftover physical `parent_id` column.
* Do not pass a struct that was read with `Columns` to `Update`: `Update` writes every field of the
  item, so the columns that were not selected would be overwritten with zero values.
* `WithFilter(conds ...Cond)` returns a copy of the Tree with a default filter for every read in the
  table, `GetLeaves` included.
  It replaces any default the Tree already has; with no conds the copy keeps the same default. A
  `Filter` with at least one cond replaces the default for one call; `Unfiltered()` switches it off
  for one call; an empty `Filter()` has no effect. The conds are resolved when `WithFilter` is
  called, including whether an `Eq` value is NULL; a pointer `driver.Valuer` is bound as it is and
  read again by every query, so it must not change afterwards.
* Writes are never filtered, also on a filtered Tree. `GetOrAdd` therefore returns an existing child
  that `FindChild` on the same Tree does not see. `afterNodeID` may name a sibling the filter hides,
  and the placement of a new or moved node counts hidden siblings.

### Ancestors and child counts

```GO
var path []*Tag
err := tree.Ancestors(ctx, warmColor.Id(), "user1", &path) // [colors]: root first, the node itself left out
// handle error

ids, err := tree.AncestorIDs(ctx, warmColor.Id(), "user1") // the same, ids only

counts, err := tree.ChildCounts(ctx, []uint{0, colorTag.Id()}, "user1")
// counts[0] is the number of root nodes, counts[colorTag.Id()] the number of children of "colors"
```

* `Ancestors` and `AncestorIDs` return the ancestors root first and the direct parent last. The node
  itself and the virtual root are left out, so a root node gives an empty result. An unknown node,
  or a node of another tenant, is `ErrNodeNotFound`. `Ancestors` fills the parent of each ancestor
  and appends to the slice like `Descendants`.
* With a filter, an ancestor that does not match is left out and the path continues above it. A
  start node that does not match is `ErrNodeNotFound`.
* `ChildCounts` returns the number of direct children for each id. An id without counted children
  is absent from the map: a leaf, an unknown id and an id of another tenant. Id 0 counts the root
  nodes. Duplicate ids count once, and any number of ids is fine.
* With a filter, `ChildCounts` counts only the matching children and does not check the parent.

### Subtree subqueries

`SubtreeIDs`, `SubtreeIDsOf` and `ClosurePairs` build a `*gorm.DB` subquery. They execute nothing:
you embed the result in your own query, which runs on your connection and in your transaction, so
it also sees the uncommitted rows of that transaction. The output columns are stable:
`descendant_id` for `SubtreeIDs` and `SubtreeIDsOf`; `ancestor_id`, `descendant_id` and `depth`
for `ClosurePairs`. The returned value is a raw statement: conditions chained on it (`Where`,
`Limit`, `Order`) are ignored, so put them in the outer query. You can also run it directly with
`Scan`.

`SubtreeIDs` as a semi-join, with your own ordering, limit and count. A book attached to two nodes
of the subtree comes once:

```GO
sub, err := tree.SubtreeIDs("user1", []uint{colorTag.Id()})
// handle error

under := func() *gorm.DB {
    return db.Model(&Book{}).
        Where("id IN (SELECT book_id FROM books_tags WHERE tag_node_id IN (?))", sub)
}
var total int64
err = under().Count(&total).Error
var page []Book
err = under().Order("name").Limit(20).Offset(40).Find(&page).Error
```

`ClosurePairs` as a derived table, here the number of descendants of each node:

```GO
pairs, err := tree.ClosurePairs("user1", ct.DepthRange(1, -1))
// handle error

var sizes []struct {
    AncestorID uint
    N          int64
}
err = db.Table("(?) AS cp", pairs).
    Select("cp.ancestor_id AS ancestor_id, COUNT(*) AS n").
    Group("cp.ancestor_id").Scan(&sizes).Error
```

Your own rows per subtree are counted the same way, by joining your join table on
`descendant_id`. Index the node column of the join table. A many2many join table created by gorm
has only its composite primary key, led by the owner's column, so on SQLite and PostgreSQL the node
column has no index (MySQL adds one for the foreign key). Without it a count per subtree scans the
join table once per closure row.

```GO
// once: CREATE INDEX idx_books_tags_tag ON books_tags (tag_node_id)
pairs, err := tree.ClosurePairs("user1", ct.ForAncestors(colorTag.Id(), warmColor.Id()))
// handle error

var books []struct {
    AncestorID uint
    N          int64
}
err = db.Table("(?) AS cp", pairs).
    Select("cp.ancestor_id AS ancestor_id, COUNT(DISTINCT bt.book_id) AS n").
    Joins("JOIN books_tags AS bt ON bt.tag_node_id = cp.descendant_id").
    Group("cp.ancestor_id").Order("cp.ancestor_id").Scan(&books).Error
```

* `DepthRange(min, max)` limits the distance from the ancestor: 0 is the node itself, 1 its direct
  children. A `max` below 0 means no upper limit. The default is 0 to unlimited, so the ancestor
  itself is included. `DepthRange(0, 0)` is the node only, `DepthRange(1, -1)` everything strictly
  below it, `DepthRange(1, 1)` the direct children. A negative `min`, or a `max` that is not
  negative and is below `min`, returns `ErrInvalidDepthRange` from every read.
* The filter in `SubtreeIDs` prunes relative to each requested ancestor: a node is left out when it
  or a node between it and the ancestor does not match. The requested ancestor must match too, also
  when the depth range leaves it out. `PerNode()` tests only the node itself.
* The filter in `ClosurePairs` requires the descendant and the ancestor to match. It does not check
  the nodes in between, and `PerNode` does not apply. The virtual root always matches.
* Ancestor 0 in `SubtreeIDs` stands for every node of the tenant. An empty slice is
  `ErrNoAncestors` in `SubtreeIDs`, and a nil subquery is in `SubtreeIDsOf`.
* `SubtreeIDs` binds one value per ancestor (SQLite 32766, PostgreSQL and MySQL 65535 per
  statement). On PostgreSQL gorm also rewrites the placeholders of an embedded subquery one by one,
  which gets slow beyond a few thousand values: about 0.1 s for 10,000 and 1 s for 30,000 per
  statement. For a large or computed set of ancestors use `SubtreeIDsOf`, which takes them as a
  subquery that selects one column of node ids. It must be a subquery that gorm can render on its
  own, built with `Table`, `Model` or `Raw`; the builder renders it once and returns an error when
  that fails. A subquery whose `Error` is set returns that error.
* `ForAncestors(ids...)` limits the ancestors of `ClosurePairs`; an id 0 includes the pairs of the
  virtual root. Without ids it selects nothing, and it binds one value per id, with the limits
  above. Without `ForAncestors`, `ClosurePairs` leaves the virtual root out unless you pass
  `IncludeVirtualRoot()`, and it reads the closure rows of every tenant to find those of yours,
  because no index of the closure table leads with the tenant. `ForAncestors` avoids that.
* MySQL, error 1093: with a filter the subquery reads the node table, and MySQL rejects it inside an
  `UPDATE` or `DELETE` on the node table. Wrap it in a derived table:
  `db.Table("tags").Where("node_id IN (SELECT descendant_id FROM (?) AS sub)", sub).Update("name", "renamed")`.
* MySQL, error 1235: a `LIMIT` is not allowed inside an `IN` subquery. Page in the outer query.

### Sort order

Each node carries a `SortOrder float64` field (stored as `REAL`/`DOUBLE` in the database). The library
manages this value automatically — callers never set it directly.

**Placing nodes:** `Add` and `Update` take an `afterNodeID` parameter (a `*uint`), and `Add` and
`GetOrAdd` accept the `PlaceLast()` option. `GetOrAdd` has no `afterNodeID`: a node it creates is
placed first among its siblings, or last with `PlaceLast()`. The meaning differs per method:

| `afterNodeID` / option | `Add` | `Update` |
|---|---|---|
| `nil` | Place first among siblings (the default) | No reordering; a node moved to a new parent is appended last there |
| `&0` | Place first among siblings | Place first among siblings |
| `&someID` | Place immediately after that sibling | Place immediately after that sibling |
| `closuretree.PlaceLast()` | Place after the current last sibling (also for `GetOrAdd`) | Not available |

```GO
err := tree.Add(ctx, &Tag{Name: "x"}, &parentID, nil, "user1", closuretree.PlaceLast())
```

Combining `PlaceLast()` with a non-zero `afterNodeID` returns `ErrConflictingPlacement`.

**Maintenance:** float64 bisection has finite precision. After many insertions between the same two nodes
the gap eventually exhausts. Use the renormalize API to reset spacing:

```GO
// Check a specific parent
needs, err := tree.NeedsRenormalize(ctx, parentID, "user1", ct.DefaultHalvingsBuffer)

// Check any parent across the whole tenant
needs, err := tree.NeedsRenormalizeAny(ctx, "user1", ct.DefaultHalvingsBuffer)

// Fix a specific parent
err = tree.Renormalize(ctx, parentID, "user1")

// Fix all parents that need it; returns the count of groups renormalized
n, err := tree.RenormalizeAll(ctx, "user1", ct.DefaultHalvingsBuffer)
```

`DefaultHalvingsBuffer = 15` means the check fires when ≤15 bisections remain before a
collision — roughly 36 insertions before true exhaustion at the tightest gap. Call
`NeedsRenormalizeAny` periodically (e.g. after each write batch) and `RenormalizeAll`
when it returns `true`.

### Using closure-tree inside your own transaction

`WithTx` binds the tree to a transaction you opened, so tree writes and your own writes commit or
roll back together:

```GO
err := db.Transaction(func(tx *gorm.DB) error {
    t := tree.WithTx(tx)
    if err := t.LockTenant(ctx, "user1"); err != nil { // first statement of the transaction
        return err
    }
    tag := Tag{Name: "colors"}
    if err := t.Add(ctx, &tag, nil, nil, "user1"); err != nil {
        return err
    }
    return tx.Create(&AuditEntry{Action: "add tag", NodeID: tag.Id()}).Error // your own write
})
```

* `WithTx` returns a cheap copy of the tree bound to the transaction; create one per transaction.
  Reads on the copy see the transaction's uncommitted rows.
* Inside the transaction, use the copy returned by `WithTx` for that tenant: calling the original
  tree for the same tenant from inside the transaction waits on the transaction's own lock, so it
  blocks: forever, or until a lock or busy timeout fails it.
* A failed tree write rolls back only the tree's own work (to a savepoint the tree takes) and leaves
  your transaction usable; you decide whether to commit or roll back. Roll your transaction back
  instead after a MySQL deadlock (below); when `errors.Is(err, closuretree.ErrSavepointRollback)` is
  true: the tree could not roll back to its savepoint, for example because the context ended during
  a statement and the driver dropped the connection, which loses your transaction's earlier work
  too, or because the rollback, which runs even after your context has ended, took more than 30
  seconds; and when the context you passed has ended (`ctx.Err() != nil`), because the driver may have
  dropped the connection and the tree reports that only for statements inside its savepoint.
* Reads on the copy, and the statements a write runs before its savepoint (`SAVEPOINT` itself, the
  first reads of `DeleteTenant` and `RenormalizeAll`), are outside the savepoint: their failure
  affects your transaction like a failed statement of your own, and never returns
  `ErrSavepointRollback`.
* MySQL: a deadlock error (error 1213) means the server has already rolled back your whole
  transaction. Roll back and retry the transaction; continuing would run the following statements
  outside any transaction. The tree's error contains the driver's error, so you can test for it with
  `errors.As` (for `github.com/go-sql-driver/mysql`, a `*mysql.MySQLError` with `Number` 1213).
  Inside a tree write the deadlock error also matches `ErrSavepointRollback`, because the savepoint
  went with the transaction: if you retry, test for the deadlock first.
* MySQL: tree writes of different tenants can deadlock at any isolation level (see "Concurrent
  writes on MySQL" below). At the default isolation level (REPEATABLE READ) your transaction keeps
  the locks of the closure rows its tree writes scanned until it ends, so writes of other tenants
  wait on it more, also writes on a tree not bound to a transaction, which the server may roll back
  instead of yours. Keep such transactions short, or run them at READ COMMITTED. Error 1213 means
  the whole transaction was rolled back: on a tree bound with `WithTx` roll back and retry your
  transaction; on a tree not bound to a transaction retry the call.
* After a successful tree write the tenant lock is held until your transaction ends, so do not hold
  the transaction across slow work. After a failed tree write the lock may already be released;
  `LockTenant` holds it for the whole transaction.
* A transaction that writes to several trees or tenants must take their locks in one fixed order,
  everywhere, to avoid deadlocks. `LockTenant` lets it take them all up front.
* Call `Migrate` on the base tree, not on a copy bound to a transaction: it runs DDL, and MySQL
  commits the transaction implicitly on DDL.

The guards inside tree writes (cycle check, sibling order, duplicate check) are plain reads, so what
they see depends on the database and the isolation level of your transaction:

| Database | Isolation level | Safe? |
|---|---|---|
| PostgreSQL | READ COMMITTED (the default) | Yes. |
| PostgreSQL | REPEATABLE READ | No, and `LockTenant` does not help: the snapshot is fixed at the transaction's first statement, the lock statement itself, before it waits for the lock. |
| MySQL | REPEATABLE READ (the default) | Only when `LockTenant` is the first statement: the snapshot is fixed at the first plain read, so a read before the lock makes it stale. |
| MySQL | READ COMMITTED | Yes. |
| SQLite | any | Yes, with immediate write transactions and a busy timeout. |

On SQLite open write transactions as immediate and set a busy timeout (DSN options `_txlock=immediate`
and `_busy_timeout=5000` for `gorm.io/driver/sqlite`, `_txlock=immediate` and
`_pragma=busy_timeout(5000)` for `github.com/glebarez/sqlite`). A deferred transaction that reads
first and then writes fails with a busy error that the timeout does not retry.

On PostgreSQL and MySQL the transactions the library opens itself (any write on a tree not bound to
a transaction) run at READ COMMITTED, whatever the connection's default level is. A MySQL server
with binary logging on and `binlog_format=STATEMENT` refuses InnoDB writes at READ COMMITTED (error
1665), so that setup is not supported; `ROW`, the default, and `MIXED` work.

#### Concurrent writes on MySQL

No closure-table index leads with the tenant, so on MySQL a write that deletes closure rows, such
as `DeleteRecurse`, `DeleteRecurseMany`, `DeleteTenant`, `BuildFromParents` with `Rebuild()`, or
an `Update` that moves a node, locks the closure rows it scans, also rows of other tenants, and
waits for a row that another transaction holds. Running the library's own transactions at READ
COMMITTED removed the deadlocks between these writes and other tenants' adds, but two of them on
different tenants at once can still deadlock. In the library's tests, on trees not bound to a transaction, each of two or three tenants
repeated one such write at the same time (ten runs of 30 calls per write):

| Writes at the same time, each on its own tenant | MySQL | PostgreSQL |
|---|---|---|
| `DeleteTenant` (after adding nodes), on three tenants | no call failed | no call failed |
| `BuildFromParents(Rebuild())` and `BuildFromParents(Rebuild())` | no call failed | no call failed |
| `DeleteRecurseMany` of 600 ids and `BuildFromParents(Rebuild())` | the rebuild lost calls to deadlocks | no call failed |
| `Update` moving a subtree and `DeleteRecurse` | the pair lost calls to deadlocks | no call failed |

InnoDB locks the rows a delete scans at every isolation level, so the library makes no promise that
writes of different tenants do not deadlock on MySQL. A deadlock (error 1213) can be retried: the
server has rolled back the whole transaction, so repeat a call on a tree not bound to a transaction,
and roll back and retry your transaction on one bound with `WithTx`.

### Adopting an existing table

`BuildFromParents` turns node rows that already exist, each knowing its parent, into a tree. It
writes the closure rows and `sort_order`; it never inserts, deletes or otherwise changes the node
rows.

The node table needs `node_id` as an auto-increment primary key, `tenant` and `sort_order`, which
is what embedding `Node` gives it. No other field of the model may map to the `parent_id` column:
the parent lives in the closure table from now on.

```GO
// your own query: one row per node of the tenant, siblings in the order you want them
var rows []struct{ ID, OldParentID uint }
err := db.Table("categories").Select("node_id AS id, old_parent_id").
    Where("tenant = ?", "user1").Order("old_parent_id, position").Scan(&rows).Error
if err != nil {
    return err
}
links := make([]closuretree.ParentLink, len(rows))
for i, r := range rows {
    links[i] = closuretree.ParentLink{NodeID: r.ID, ParentID: r.OldParentID} // ParentID 0: a root
}
err = tree.BuildFromParents(ctx, "user1", links)
```

* The links must name every node of the tenant exactly once. Siblings are ordered as their links
  appear in the slice; their `sort_order` becomes 10, 20, 30, … The sort-order metadata of the
  tenant is cleared, as `Renormalize` does.
* The four errors, each returned before anything is written:
  * `ErrLinksMismatch`: a node of the tenant has no link, a link names a node that does not exist,
    a node is linked twice, or a link has node id 0. The message lists the ids.
  * `ErrOrphanLink`: a parent is not a node of the tenant. With the `OrphansToRoot()` option such a
    node becomes a root instead.
  * `ErrLinkCycle`: the links form a cycle, including a node that is its own parent. The message
    names one node of each cycle.
  * `ErrClosureExists`: the tenant already has closure rows. With the `Rebuild()` option they are
    deleted first, and the tree is built again.
* Until it has run, a node row is not part of the tree: `Add`, `GetOrAdd` and a move with `Update`
  under it return `ErrParentNotFound`, with a message that names `BuildFromParents`, and write
  nothing.
* It runs under the tenant lock. Through `WithTx` it runs inside your transaction, so a migration
  can build the tree and drop its old parent column atomically, except on MySQL, where DDL commits
  the transaction implicitly: drop the column there after the commit.
* MySQL: the node table has rows before the first `Migrate`, so `Migrate` does not give the tenant
  columns the exact collation (see "Tenants"): its own collation step never changes the tenant
  collation of a tree with rows. gorm's `AutoMigrate`, which `Migrate` runs first, can: it
  modifies the tenant column of the node table or the closure table when its type, size, default
  or comment differs from the model (your table's own type, a changed `DefaultStringSize`), and
  does so without a collation, which gives it the default collation of its table. It leaves the
  tenant columns of the meta and lock tables alone, as they are part of the primary key, so `Migrate` then returns
  `ErrTenantCollation` unless that default is the collation of the other tenant columns. To compare
  tenants exactly, run the four statements of "Tenants" after `Migrate` and before
  `BuildFromParents`.
* PostgreSQL: rows copied with explicit ids leave the id sequence behind, and the next `Add` fails
  with a duplicate key. Advance the sequence after copying:
  `SELECT setval(pg_get_serial_sequence('categories', 'node_id'), (SELECT MAX(node_id) FROM categories))`.
* After a bulk build or load, update the table statistics: `ANALYZE` on PostgreSQL
  (`ANALYZE categories; ANALYZE closure_tree_rel_categories;`) and `ANALYZE TABLE` on MySQL
  (`ANALYZE TABLE categories, closure_tree_rel_categories;`), on the tree's tables and on your own
  tables that join them. Freshly loaded tables have no statistics, or stale ones, and the planner
  then chooses slow plans; on MySQL `GetLeaves` and joins on the closure table can be planned very
  badly.

---

For detailed usage check out the examples in [example_test.go]

##  Closure Tree methods

this is a quick overview of the exposed methods, check the actual signature/doc for details.

**Tenants**
Tenants are compared exactly on every database: "Acme", "acme", "acmé" and "acme " are four
tenants. On MySQL, `Migrate` sets the collation `utf8mb4_0900_bin` on the tenant column of the
tree's four tables while the tree holds no rows. A tree with rows keeps its collation unless gorm's
`AutoMigrate` modifies its tenant column (see "Adopting an existing table"); under
MySQL's default, `utf8mb4_0900_ai_ci`, tenants that differ only in case or accents are one tenant,
and `DeleteTenant(ctx, "acme")` deletes all of them. To change it, stop writes to the tree, store
one spelling per tenant, then run the statement below on each of the four tables, with the column's
current type in place of `varchar(191)`. The four tables are the node table (`NodeTableName()`,
say `categories`) and, named after it, `closure_tree_rel_categories` (`ClosureTableName()`),
`closure_tree_meta_categories` and `closure_tree_lock_categories`:

```sql
ALTER TABLE <table> MODIFY tenant varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL;
```

Each rebuilds its table. Resume writes only after all four: until then the tables compare tenants
differently. `Migrate` returns `ErrTenantCollation` until all four agree. The tenant column of a
model that embeds `Leaf` keeps the collation of your table; it is nullable, so for it leave out
`NOT NULL`.

**Tree management**
* `New(db *gorm.DB, item any) (*Tree, error)` — Return a new tree instance; it validates the model and table names but does not create tables
* `Migrate() error` — Create or update the tree's four tables; call it on the base tree, not on a copy bound to a transaction
* `WithTx(tx *gorm.DB) *Tree` — Return a copy of the tree bound to your own transaction (see "Using closure-tree inside your own transaction"); panics if `tx` is nil
* `LockTenant(ctx context.Context, tenant string) error` — Take the tenant's write lock in the transaction the tree is bound to; returns `ErrNotInTransaction` otherwise
* `NodeTableName() string` — Return the table name of the nodes you store
* `ClosureTableName() string` — Return the table name of the closure tree relationship

**Write operations**
On MySQL, writes of different tenants can deadlock, on a tree bound to a transaction or not. After error 1213 repeat a call on a tree not bound to a transaction; for a write through `WithTx` the server has rolled back your whole transaction: roll it back and retry it (see "Concurrent writes on MySQL").
* `Add(ctx, item, parentID, afterNodeID, tenant, opts ...AddOption)` — Add a new node; it is placed first among its siblings unless `afterNodeID` names a sibling or the `PlaceLast()` option is given
* `GetOrAdd(ctx, item, parentID, tenant, matchFields, opts ...AddOption) (created bool, err error)` — Idempotent get-or-create of a direct child: return the existing match or add it; sets the pointer `item`'s `NodeId` on both paths; takes the same placement options as `Add`
* `Update(ctx, id, item, newParentID, afterNodeID, tenant)` — Update payload, move to a new parent, reorder, or any combination; pass `nil` pointers to skip that aspect; a node that does not exist returns `ErrNodeNotFound` and nothing is written
* `DeleteRecurse(ctx, nodeId, tenant)` — Delete a node and all its descendants; id 0 is not rejected and deletes the tenant's whole tree (every node that has closure rows, not node rows waiting for `BuildFromParents`): use `DeleteTenant`
* `DeleteRecurseMany(ctx, ids, tenant) (int64, error)` — Delete several nodes and their subtrees in one transaction; set semantics (duplicates and nested ids count once), returns the number of nodes deleted, never `ErrNodeNotFound` (unknown ids are ignored), id 0 returns `ErrVirtualRoot`
* `DeleteTenant(ctx, tenant) error` — The supported way to drop a tenant's whole tree: deletes its nodes, closure rows and metadata in one transaction; the tenant's lock row is kept, and an unknown tenant is not an error
* `BuildFromParents(ctx, tenant, links, opts ...BuildOption) error` — Build the closure rows and `sort_order` of node rows that already exist from one `ParentLink` per node (see "Adopting an existing table"); options `Rebuild()` and `OrphansToRoot()`

**Read operations**
Every read below except `IsDescendant` and `IsChildOf` takes trailing `opts ...ReadOption` (see "Narrowing reads").
`Descendants` and `TreeDescendants` scan each row into the tree's model and copy it into the element type: as a whole when the element type is the model or embeds it directly; otherwise each field goes to the field at the same path of Go names (`Name`, `Addr.Name`), or, when there is none, to the field with its Go name if no other model field has that name. A slice whose element is not a struct or a pointer to a struct returns an error before any query runs. `TreeDescendants` needs a `Children []*T` field it can link into: a tree element without one, with one in each of two structs embedded at the same depth (Go promotes neither), with one of another type, or with one promoted through an embedded pointer to an unexported type (it cannot be allocated) returns an error before any query runs and before the tenant is validated, for every `maxDepth`, also 1. Add the field, or read with `Descendants`, which takes the same parameters: without `PerNode`, `Descendants` with `maxDepth` 1 returns the same nodes in the same order as `TreeDescendants` with `maxDepth` 1. A `Children` field promoted through an embedded pointer struct is reached on every element, leaves included, by allocating the nil pointer.
* `GetNode(ctx, nodeID, tenant, item, opts ...ReadOption)` — Load a single node into `item`
* `FindChild(ctx, parentID, tenant, match, matchFields, out, opts ...ReadOption) (found bool, err error)` — Find a direct child whose fields named in `matchFields` equal those of `match` (zero values included), tenant-scoped; populates `out` like `GetNode`
* `IsDescendant(ctx, ancestorID, descendantID, tenant) (bool, error)` — Check ancestry
* `IsChildOf(ctx, nodeID, parentID, tenant) (bool, error)` — Check direct parent relationship
* `Descendants(ctx, parent, maxDepth, tenant, items, opts ...ReadOption)` — Flat list of all nested children (ordered by `sort_order ASC, node_id ASC`)
* `DescendantIds(ctx, parent, maxDepth, tenant, opts ...ReadOption) ([]uint, error)` — Same, IDs only
* `TreeDescendants(ctx, parent, maxDepth, tenant, items, opts ...ReadOption)` — Nested tree via `Children []*T` field
* `TreeDescendantsIds(ctx, parent, maxDepth, tenant, opts ...ReadOption) ([]*TreeNode, error)` — Nested `TreeNode` structs
* `Ancestors(ctx, nodeID, tenant, items, opts ...ReadOption) error` — Ancestors of a node, root first, into `[]T` or `[]*T`
* `AncestorIDs(ctx, nodeID, tenant, opts ...ReadOption) ([]uint, error)` — Same, IDs only
* `ChildCounts(ctx, ids, tenant, opts ...ReadOption) (map[uint]int64, error)` — Number of direct children per id; ids without counted children are absent
* `SubtreeIDs(tenant, ancestors []uint, opts ...ReadOption) (*gorm.DB, error)` — Subquery of the node ids in the subtrees of the ancestors, to embed in your own query
* `SubtreeIDsOf(tenant, ancestors *gorm.DB, opts ...ReadOption) (*gorm.DB, error)` — Same, with the ancestors given as a subquery
* `ClosurePairs(tenant, opts ...ReadOption) (*gorm.DB, error)` — Subquery of `ancestor_id`, `descendant_id`, `depth`, for per-ancestor aggregates
* `GetLeaves(ctx, items, parentId, maxDepth, tenant, opts ...ReadOption)` — Rows of any model with a many2many relation to the nodes (see "Querying leaves")
* `WithFilter(conds ...Cond) (*Tree, error)` — Return a copy of the tree with a default read filter

**Sort-order maintenance**
* `Renormalize(ctx, parentID, tenant)` — Rewrite children of `parentID` with evenly spaced `sort_order` values (10, 20, 30, …)
* `NeedsRenormalize(ctx, parentID, tenant, halvingsBuffer) (bool, error)` — O(1) check; returns `true` when ≤`halvingsBuffer` bisections remain
* `NeedsRenormalizeAny(ctx, tenant, halvingsBuffer) (bool, error)` — Same check across all parents in a tenant
* `RenormalizeAll(ctx, tenant, halvingsBuffer) (int, error)` — Renormalize every group that needs it; returns count renormalized
* `const DefaultHalvingsBuffer = 15` — Recommended threshold for the above methods

**Utility**
* `SortTree(nodes []*TreeNode)` — Sort a `[]*TreeNode` slice recursively by `(sort_order ASC, node_id ASC)`

## Development

for testing and development there are several make targets available:

* _test_ : performs all tests on sqlite only.
* _test-full_ : performs all tests on all supported databases, this takes longer as the DBs start in a docker container.
* _test-race_ : performs the tests with the race detector, on sqlite only, without the large-tree tests.
* _verify_ : runs the linter, _test-full_, _test-race_, the license check, the benchmarks and the coverage check.


Notes for local development 

* to generate the sqlite database in the working dir instead of on a tmp dir:
```
export LOCAL_SQLITE=true
```
then run the tests
```
go test --short -v 
```
now the sqlite files will be placed in ./ and can be inspected


## TODO
* improve example_test.go
* example of getting items between 2 trees, e.g. tag or tag b AND stars >= 5