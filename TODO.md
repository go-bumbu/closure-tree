
* Add recursive SQL function to get nodes in a tree structure
* add node order and sort functions


# For the future

this library uses so much custom sql, that a migration libray would
make more sense and not depend on gorm
* cases:
  * for descendants i need to specify parent_id this is ocupying the table with 0 that are not used