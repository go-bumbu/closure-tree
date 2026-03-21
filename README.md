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
```
instantiating a tree will run the gorm automigration under the hood and create the needed tables,
this should only be done at application start up.

To populate the tree we do as follows:
```GO
ctx := context.Background()

colorTag := Tag{Name: "colors"}
err := tree.Add(ctx, &colorTag, 0, 0, "user1") // add a node to the root (afterNodeID=0 → place first)
// handle error

warmColor := Tag{Name: "warm"}
err = tree.Add(ctx, &warmColor, colorTag.Id(), 0, "user1") // add a node below "colors"
// handle error

```
Note that the `Id()` method returns the node ID once the struct has been passed as a pointer to `Add`.
Id `0` is the root node. The `afterNodeID` parameter controls sort order among siblings — pass `0` to
place the node first, or pass a sibling's ID to insert immediately after it.

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


### Sort order

Each node carries a `SortOrder float64` field (stored as `REAL`/`DOUBLE` in the database). The library
manages this value automatically — callers never set it directly.

**Placing nodes:** both `Add` and `Update` accept an `afterNodeID` parameter:

| `afterNodeID` | Behaviour |
|---|---|
| `0` | Place first among siblings |
| `someID` | Place immediately after that sibling |

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
* `Update(ctx, id, item, newParentID, afterNodeID, tenant)` — Update payload, move to a new parent, reorder, or any combination; pass `nil` pointers to skip that aspect
* `DeleteRecurse(ctx, nodeId, tenant)` — Delete a node and all its descendants

**Read operations**
* `GetNode(ctx, nodeID, tenant, item)` — Load a single node into `item`
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