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


### querying leaver
with the basic implementation you have now an efficient way to store and manage items in a tree structure, you could
use any other storage mechanism and simple reference nodeIDs.

For convenience Closure tree also allows to manage many 2 many relationships between _leaves_ and _nodes_

```GO
// TODO
```


---

For detailed usage check out the examples in [example_test.go]

## Development

for testing and development there are several make targets available:

* _test_ : performs all tests on sqlite only.
* _test-full_ : performs all tests on all supported databases, this takes longer as the DBs start in a docker container.
* _verify_ : on top of running tests, it also runs the linter, license check.


Notes for local development 

* to generate the sqlite database in the working dir instead of on a tmp dir:
```
export SQLITE_LOCAL_DIR=true
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