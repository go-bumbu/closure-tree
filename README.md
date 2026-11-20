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
* mysql
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
with the basic implementation you have now an efficient way to store and manage items in a tree structure,

For convenience Closure tree also allows to manage many 2 many relationships between _leaves_ and _nodes_

```GO
// define your leave data structs, e.g. Book
// important to add the many2many annotation
type Book struct {
    ID     uint `gorm:"primarykey"`
    Name   string
    Tags []Tag `gorm:"many2many:books_tags;"` // Tag is the item stored in the tree
}

parentId := 2
maxDepth := 0
tennant := "user1"

var books []Book // will be populated 
err = tree.GetLeaves(&books, parentId, maxDepth, tenant)
if err != nil {
fmt.Print(err)
}


```

Alternative if you need more advanced queries here is an example on how to use GORM with a many2many relationships table
```GO
// define your leave data structs, e.g. Book
// important to add the many2many annotation
type Book struct {
    ID     uint `gorm:"primarykey"`
    Name   string
    Tags []Tag `gorm:"many2many:books_tags;"` // Tag is the item stored in the tree
}

// get all the ids of a specific type
tags, _ := tree.DescendantIds(ctx, 2, 0, tenant)

// use Gorm pn your leaves 
var gotBooks []Book
db.Model(&Book{}).InnerJoins("INNER JOIN books_tags ON books.id = books_tags.book_id").
  Preload("Tags").
  Where("books_tags.tag_node_id IN ?", spaceOperaIds).
  Distinct().
  Find(&gotBooks)
```


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
cannot be used. The match is scoped to the tenant; `parentID = 0` operates on the root level. Use
`FindChild` for the lookup alone:

```GO
var out Tag
found, err := tree.FindChild(ctx, parentID, "user1", Tag{Name: "b"}, []string{"Name"}, &out)
```

`GetOrAdd` runs the find and the add in a single transaction under the tenant lock, so a node is
never left half created and two callers creating the same brand-new child are serialized: the
first creates it, the second finds it and gets `created = false`, so no duplicate is produced.
A new node is placed first among its siblings; pass `closuretree.PlaceLast()` to append it last.

### Sort order

Each node carries a `SortOrder float64` field (stored as `REAL`/`DOUBLE` in the database). The library
manages this value automatically — callers never set it directly.

**Placing nodes:** `Add`, `GetOrAdd` and `Update` take an `afterNodeID` parameter (a `*uint`), and
`Add` and `GetOrAdd` also accept the `PlaceLast()` option. The meaning differs per method:

| `afterNodeID` / option | `Add` and `GetOrAdd` | `Update` |
|---|---|---|
| `nil` | Place first among siblings (the default) | No reordering; a node moved to a new parent is appended last there |
| `0` (`&0` for `Update`) | Place first among siblings | Place first among siblings |
| `someID` | Place immediately after that sibling | Place immediately after that sibling |
| `closuretree.PlaceLast()` | Place after the current last sibling | Not available |

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
* A tree error rolls back only the tree's own work (to a savepoint the tree takes) and leaves your
  transaction usable; you decide whether to commit or roll back.
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

On PostgreSQL the transactions the library opens itself (any write on a tree not bound to a
transaction) run at READ COMMITTED, whatever the connection's default level is.

---

For detailed usage check out the examples in [example_test.go]

##  Closure Tree methods

this is a quick overview of the exposed methods, check the actual signature/doc for details.

**Tree management**
* `New(db *gorm.DB, item any) (*Tree, error)` — Return a new tree instance (runs AutoMigrate)
* `GetNodeTableName() string` — Return the table name of the nodes you store
* `GetClosureTableName() string` — Return the table name of the closure tree relationship

**Write operations**
* `Add(ctx, item, parentID, afterNodeID, tenant)` — Add a new node; `afterNodeID=0` places it first among siblings
* `GetOrAdd(ctx, item, parentID, tenant, match) (created bool, err error)` — Idempotent get-or-create of a direct child: return the existing match or add it; sets the pointer `item`'s `NodeId` on both paths
* `Update(ctx, id, item, newParentID, afterNodeID, tenant)` — Update payload, move to a new parent, reorder, or any combination; pass `nil` pointers to skip that aspect
* `DeleteRecurse(ctx, nodeId, tenant)` — Delete a node and all its descendants

**Read operations**
* `GetNode(ctx, nodeID, tenant, item)` — Load a single node into `item`
* `FindChild(ctx, parentID, tenant, match, out) (found bool, err error)` — Find a direct child by query-by-example (the non-zero fields of `match`), tenant-scoped; populates `out` like `GetNode`
* `IsDescendant(ctx, ancestorID, descendantID, tenant) (bool, error)` — Check ancestry
* `IsChildOf(ctx, nodeID, parentID, tenant) (bool, error)` — Check direct parent relationship
* `Descendants(ctx, parent, maxDepth, tenant, items)` — Flat list of all nested children (ordered by `sort_order ASC, node_id ASC`)
* `DescendantIds(ctx, parent, maxDepth, tenant) ([]uint, error)` — Same, IDs only
* `TreeDescendants(ctx, parent, maxDepth, tenant, items)` — Nested tree via `Children []*T` field
* `TreeDescendantsIds(ctx, parent, maxDepth, tenant) ([]*TreeNode, error)` — Nested `TreeNode` structs
* `GetLeaves(items, parentId, maxDepth, tenant)` — Many-to-many leaves via GORM `many2many:` tag

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
* _verify_ : on top of running tests, it also runs the linter, license check.


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