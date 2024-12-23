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
 

## TODO
* add example_test.go
* add max depth to get descendants, e.g. only immediate children
* Add delete node function
* Add get descendants for N amount of nodes
* example of getting items between 2 trees, e.g. tag or tag b AND stars >= 5

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