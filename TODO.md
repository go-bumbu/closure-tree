
* explore how we can expose a method that takes a function to populate a struct on the recursive tre
  * usecase: now we have TreeDescendants, that a) loads all the items into a map b) creates a tree
  then the caller get a tree and needs to transform the tree again with a recursive function.
  2 options: return a map with descendant/parent info to allow the caller efficently build a tree
  make a recursive function with a populate fn to generate the item on the fly
  
  thought: as a callback pass a function that takes signature (id, parent, order, paload map[string]any)
  and give an example on how to compose the tree as a user
* add node order and sort functions