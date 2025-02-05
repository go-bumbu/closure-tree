# closure-tree

A closure tree is a read efficient way to define tree dependencies of items, e.g. tags.

Features:
* store any basic struct information in a tree structure (Uses GORM under the hood)
* multi-user/tenant: allows to perform all the operations on a specific user or tenant
* retrieve all the nested descendants of a node


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
colorTag := Tag{Name: "colors"}
err := tree.Add(&colorTag, 0, "user1") // add a node to the root
// handle error

warmColor := Tag{Name: "warm"}
err = tree.Add(&warmColor, colorTag.Id(), "user1") // add a node bellow "colors"
// handle error
	
```
Note that the Id field is populated if you pase a node pointer, this allows to use it to reference new children, 
also, id 0 is the root node.

Once populated we can query the tree (get all descendants)

```GO
parentId := uint(0)
descendantsIds, err:= tree.DescendantIds(parentId, 0, "user1")
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
tags, _ := tree.DescendantIds(2, 0, tenant)

// use Gorm pn your leaves 
var gotBooks []Book
db.Model(&Book{}).InnerJoins("INNER JOIN books_tags ON books.id = books_tags.book_id").
  Preload("Tags").
  Where("books_tags.tag_node_id IN ?", spaceOperaIds).
  Distinct().
  Find(&gotBooks)
```


---

For detailed usage check out the examples in [example_test.go]

##  Closure Tree methods

this is a quick overview of the exposed methods, check the actual signature/doc for details.

* `New(db *gorm.DB, item any) (*Tree, error)` Return a new tree instance
* `GetNodeTableName` Return the table name of the nodes you store
* `Add` Adds a new node to the tree under the specified parent.
* `Update` Updates a node with specified ID and payload
* `Move` Move a node from pne parent to another one.
* `DeleteRecurse` Deletes all of the children for a given ID.
* `GetNode` Load a single item.
* `Descendants` Loads a pointer of a slice a flat list of all nested children of specific parent
* `DescendantIds` Return a flat list of nesterd children of specific parent.
* `TreeDescendants` Loads a part of the tree into a nested slice of pointers with Children field
* `TreeDescendantsIds` Return a nested struct that contains all nesterd children of specific parent.
* `func SortTree(nodes []*TreeNode)` Helper function to sort a TreeNode by ID

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
* add sort column and functions to change the sort order