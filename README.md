# closure-tree

A closure tree is an read efficient way to define tree dependencies of items, e.g. tags.

This implementation relies on Gorm to add a tree structure to any struct and allows to manage and retrieve
tree information.

Tt uses Gorm as DB abstraction with some Raw queries.
It is currently tested wiht:
* mysql
* postgres
* sqlite ("gorm.io/driver/sqlite")
* sqlite without CGO ("github.com/glebarez/sqlite")
 


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
* add walker function to recurse the tree
* solve: how to have one tree per user/ org wihtout initializing the closure tree on every request
  * maybe: limit access by user
  * alternative: make the table name selection and initialization separated
  * add another column to the closure table with user
* add sort column and functions to change the sort order