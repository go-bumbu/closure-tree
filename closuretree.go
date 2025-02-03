package closuretree

import (
	"errors"
	"fmt"
	"gorm.io/gorm"
	"reflect"
	"sort"
	"strings"
)

const closureTblName = "closure_tree_rel"

var ItemIsNotTreeNode = errors.New("the item does not embed Node")
var ParentNotFoundErr = errors.New("wrong parent ID")
var NodeNotFoundErr = errors.New("node not found")

// Tree represents the access to the closure tree allowing to CRUD nodes on the tree of items
type Tree struct {
	db *gorm.DB
	// table names, allows multiple trees
	nodesTbl     string
	relationsTbl string
}

// New returns a Tree for the given item on the specific gorm Database
func New(db *gorm.DB, item any) (*Tree, error) {

	stmt := &gorm.Statement{DB: db}
	err := stmt.Parse(item)
	if err != nil {
		return nil, fmt.Errorf("error parsing schema: %w", err)
	}
	name := stmt.Schema.Table
	relTbl := strings.ToLower(fmt.Sprintf("%s_%s", closureTblName, name))

	ct := Tree{
		db:           db,
		nodesTbl:     name,
		relationsTbl: relTbl,
	}

	if !hasNode(item) {
		return nil, ItemIsNotTreeNode
	}

	err = db.AutoMigrate(item)
	if err != nil {
		return nil, fmt.Errorf("unable to migreate node table: %v", err)
	}

	err = db.Table(ct.relationsTbl).AutoMigrate(closureTree{})
	if err != nil {
		return nil, fmt.Errorf("unable to migrate closure table: %v", err)
	}
	return &ct, nil
}

// GetNodeTableName returns the table name of the stored Nodes, used if you need to interact directly
// with the database
func (ct *Tree) GetNodeTableName() string {
	return ct.nodesTbl
}

// represents the table that store the relationships
type closureTree struct {
	AncestorID   uint   `gorm:"not null,primaryKey,uniqueIndex"`
	DescendantID uint   `gorm:"not null,primaryKey,uniqueIndex"`
	Tenant       string `gorm:"index"`
	Depth        int
}

// DefaultTenant is used in the database as a stub if not tenant was passed
const DefaultTenant = "DefaultTenant"

func defaultTenant(in string) string {
	if in == "" {
		return DefaultTenant
	}
	return in
}

// Add will add a new entry into the node Database under a specific parent and owned to a specific tenant
// Note: the passed item has to embed a Node struct, but any value added to the Node will be ignored
func (ct *Tree) Add(item any, parentID uint, tenant string) error {
	if !hasNode(item) {
		return ItemIsNotTreeNode
	}
	tenant = defaultTenant(tenant)

	t := reflect.TypeOf(item)
	itemIsPointer := false
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		itemIsPointer = true
	}
	reflectItem := reflect.New(t).Interface()
	if itemIsPointer {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item).Elem())
	} else {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item))
	}

	// modify the embedded Node struct
	v := reflect.ValueOf(reflectItem).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		if fieldType.Anonymous && field.Type() == reflect.TypeOf(Node{}) {
			if field.CanSet() {
				nodeValue := Node{
					NodeId: 0,
					Tenant: tenant,
				}
				field.Set(reflect.ValueOf(nodeValue))
			}
		}
	}

	// Check if the parent node exists and the tenant is the same
	if parentID != 0 {
		var parent Node
		err := ct.db.Table(ct.nodesTbl).
			Where("node_id = ? AND tenant = ?", parentID, tenant).
			First(&parent).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ParentNotFoundErr
			}
			return fmt.Errorf("unable to check parent node: %v", err)
		}
	}

	err := ct.db.Transaction(func(tx *gorm.DB) error {

		// create the Node item
		err := tx.Table(ct.nodesTbl).Create(reflectItem).Error
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("unable to add node: %v", err)
		}

		id, gotTennant, err := getNodeData(reflectItem)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("unable to get Item ID: %v", err)
		}

		// Add reflexive relationship
		err = tx.Table(ct.relationsTbl).Create(&closureTree{AncestorID: id, DescendantID: id, Tenant: gotTennant, Depth: 0}).Error
		if err != nil {
			tx.Rollback()
			return err
		}

		if parentID == 0 {
			// Create a root note relationship
			sqlstr := fmt.Sprintf(addRootRelQuery, ct.relationsTbl)
			ex := tx.Exec(sqlstr, id, gotTennant)
			if ex.Error != nil {
				tx.Rollback()
				return err
			}
		} else {
			// Copy all ancestors of the parent to include the new tag
			sqlstr := fmt.Sprintf(addRelsQuery, ct.relationsTbl, ct.relationsTbl)
			ex := tx.Exec(sqlstr, id, gotTennant, parentID)
			if ex.Error != nil {
				tx.Rollback()
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// if topItem is a pointer copy the ID back into it
	if itemIsPointer {
		itemValue := reflect.ValueOf(item).Elem()
		reflectItemValue := reflect.ValueOf(reflectItem).Elem()

		idField := reflectItemValue.FieldByName(nodeIDField)
		if idField.IsValid() && idField.CanSet() {
			itemValue.FieldByName(nodeIDField).Set(idField)
		} else {
			return fmt.Errorf("field: %s is not accessible or settable", nodeIDField)
		}
		tenantFieldVal := reflectItemValue.FieldByName(tenantIdField)
		if tenantFieldVal.IsValid() && tenantFieldVal.CanSet() {
			itemValue.FieldByName(tenantIdField).SetString(tenant)
		} else {
			return fmt.Errorf("field: %s is not accessible or settable", tenantIdField)
		}
	}

	return nil
}

const addRelsQuery = `INSERT INTO %s (ancestor_id, descendant_id, Tenant, depth)
				SELECT ancestor_id, ?, ?, depth + 1
				FROM %s
				WHERE descendant_id = ?;`

const addRootRelQuery = `INSERT INTO %s (ancestor_id, descendant_id, Tenant, depth)	VALUES (0, ?,?,1);`

// Update  will update the entry with given ID and owned to a specific tenant
// Note: the passed item has to embed a Node struct, but any value added to the Node will be ignored
func (ct *Tree) Update(id uint, item any, tenant string) error {
	if !hasNode(item) {
		return ItemIsNotTreeNode
	}
	tenant = defaultTenant(tenant)

	t := reflect.TypeOf(item)
	itemIsPointer := false
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		itemIsPointer = true
	}
	reflectItem := reflect.New(t).Interface()
	if itemIsPointer {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item).Elem())
	} else {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item))
	}

	// modify the embedded Node struct
	v := reflect.ValueOf(reflectItem).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		if fieldType.Anonymous && field.Type() == reflect.TypeOf(Node{}) {
			if field.CanSet() {
				nodeValue := Node{
					NodeId: id,
					Tenant: tenant,
				}
				field.Set(reflect.ValueOf(nodeValue))
			}
		}
	}

	err := ct.db.Transaction(func(tx *gorm.DB) error {
		res := tx.Table(ct.nodesTbl).Where("node_id = ? AND tenant = ?", id, tenant).Updates(reflectItem)

		if res.Error != nil {
			tx.Rollback()
			return fmt.Errorf("unable to update node: %v", res.Error)
		}
		if res.RowsAffected == 0 {
			return NodeNotFoundErr
		}

		return nil
	})
	return err
}

// Descendants allows to load a part of the tree into a slice of node pointers
// parent determines the root node id of to load.
// maxDepth determines the depth of the relationship o load: 0 means all children, 1 only direct children and so on.
// tenant determines the tenant to be used
func (ct *Tree) Descendants(parent uint, maxDepth int, tenant string, items interface{}) error {
	if items == nil {
		return errors.New("items cannot be nil")
	}

	// Check if items is a pointer to a slice using reflection
	itemsValue := reflect.ValueOf(items)
	if itemsValue.Kind() != reflect.Ptr {
		return errors.New("items must be a pointer to a slice")
	}

	// Get the underlying slice
	slice := itemsValue.Elem()
	if slice.Kind() != reflect.Slice {
		return errors.New("items must be a pointer to a slice")
	}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}
	sqlstr := fmt.Sprintf(descendantsQuery, ct.nodesTbl, ct.relationsTbl)
	err := ct.db.Raw(sqlstr, parent, maxDepth, tenant).Scan(slice.Addr().Interface()).Error
	if err != nil {
		return fmt.Errorf("failed to fetch descendants: %w", err)
	}
	return nil
}

const descendantsQuery = `SELECT nodes.*
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.Tenant = ?
ORDER BY ct.depth;`

// DescendantIds behaves the same as Descendants but only returns the node IDs for the search query.
func (ct *Tree) DescendantIds(parent uint, maxDepth int, tenant string) ([]uint, error) {
	tenant = defaultTenant(tenant)
	ids := []uint{}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}
	sqlstr := fmt.Sprintf(descendantsIDQuery, ct.nodesTbl, ct.relationsTbl)
	err := ct.db.Raw(sqlstr, parent, maxDepth, tenant).Scan(&ids).Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch descendants: %w", err)
	}
	return ids, nil
}

const descendantsIDQuery = `SELECT nodes.node_id
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.Tenant = ?
ORDER BY ct.depth;`

type TreeNode struct {
	NodeId     uint `json:"id"`
	AncestorID uint
	Children   []*TreeNode `json:"children"`
}

const absMaxDepth = 2147483647 // limited by the max value of postgres bigint
// NOTE should you ever need this deep level of nesting in a production environment, please reach out to me
// directly I'm really really really interested into knowing why and how!!!

// TreeDescendantsIds returns the tree structure of the descendants to the passed item
func (ct *Tree) TreeDescendantsIds(parent uint, maxDepth int, tenant string) ([]*TreeNode, error) {
	tenant = defaultTenant(tenant)
	nodeMap := make(map[uint]*TreeNode)

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	} else {
		// this is needed because the query will list first level as depth =0, children are depth = 1
		maxDepth = maxDepth - 1
	}

	sqlstr := fmt.Sprintf(treeDescendantsIDQueryAll, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl, ct.nodesTbl)
	rows, err := ct.db.Raw(sqlstr, parent, tenant, tenant, maxDepth).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tree descendants: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var node TreeNode
		err := rows.Scan(&node.NodeId, &node.AncestorID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch tree descendants: %w", err)
		}
		nodeMap[node.NodeId] = &node
	}

	// Now, iterate over the node map and compose the tree
	var trees []*TreeNode
	for _, node := range nodeMap {
		if par, exists := nodeMap[node.AncestorID]; exists {
			par.Children = append(par.Children, node)
		} else {
			trees = append(trees, node)
		}
	}
	return trees, nil
}

func SortTree(nodes []*TreeNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeId < nodes[j].NodeId
	})
	for _, node := range nodes {
		SortTree(node.Children)
	}
}

const treeDescendantsIDQueryAll = `WITH RECURSIVE Tree AS (
	-- Base case: Start with the parent node
	SELECT 
		nodes.node_id,
   		ct.ancestor_id AS  ancestor_id,    
		0 AS depth  
	FROM %s AS nodes
  	JOIN %s AS ct ON ct.descendant_id = nodes.node_id
    WHERE ct.ancestor_id = ? AND ct.depth = 1 AND nodes.Tenant = ?

	UNION ALL

  -- Recursive case: get immediate children (depth = 1 in closure table) of nodes in Tree,

	SELECT 
		nodes.node_id, 
		t.node_id AS ancestor_id, 
    	t.depth + 1 AS depth
	FROM Tree AS t
  	JOIN %s AS ct ON ct.ancestor_id = t.node_id AND ct.depth = 1  -- use only immediate children relationships
  	JOIN %s AS nodes ON nodes.node_id = ct.descendant_id
  	WHERE nodes.Tenant = ? AND t.depth < ?
	)
	SELECT  Tree.node_id, Tree.ancestor_id FROM Tree ORDER BY depth;`

func (ct *Tree) Move(nodeId, newParentID uint, tenant string) error {
	tenant = defaultTenant(tenant)
	return ct.db.Transaction(func(tx *gorm.DB) error {
		var err error
		insertSql := fmt.Sprintf(moveQueryInsertNew, ct.relationsTbl, ct.relationsTbl, ct.relationsTbl)
		exec1 := tx.Exec(insertSql, nodeId, newParentID, tenant, tenant)
		err = exec1.Error
		if err != nil {
			tx.Rollback()
			return err
		}
		// make sure we don't delete items if nothing was moved, e.g. if we try to move cross Tenant unsuccessful
		if exec1.RowsAffected == 0 {
			return nil
		}

		// Delete old closure relationships
		delSql := fmt.Sprintf(moveQueryDeleteOld, ct.relationsTbl, ct.relationsTbl, ct.relationsTbl)
		exec2 := tx.Exec(delSql, nodeId, tenant, newParentID, tenant, tenant)
		err = exec2.Error
		if err != nil {
			tx.Rollback()
			return err
		}

		return nil
	})
}

const moveQueryInsertNew = `
INSERT INTO %s (ancestor_id, descendant_id, depth, Tenant)
SELECT p.ancestor_id, c.descendant_id, p.depth + c.depth + 1, p.Tenant
FROM %s p
JOIN %s c ON c.ancestor_id = ?
WHERE p.descendant_id = ? AND p.Tenant = ? AND c.Tenant = ?;
`

const moveQueryDeleteOld = `WITH descendants AS (
    SELECT descendant_id
    FROM %s
    WHERE ancestor_id = ? AND Tenant = ?
),
excluded_ancestors AS (
    SELECT ancestor_id
    FROM %s
    WHERE descendant_id = ? AND Tenant = ?
)
DELETE FROM %s
WHERE descendant_id IN (SELECT descendant_id FROM descendants)
  AND ancestor_id NOT IN (SELECT ancestor_id FROM excluded_ancestors)
  AND Tenant = ?
  AND depth != 0;
`

func (ct *Tree) DeleteRecurse(nodeId uint, tenant string) error {
	return ct.db.Transaction(func(tx *gorm.DB) error {

		// delete the nodes
		var err error
		delNodesSql := fmt.Sprintf(deleteNodesRec, ct.nodesTbl, ct.relationsTbl, ct.nodesTbl)
		exec1 := tx.Exec(delNodesSql, nodeId, tenant)
		err = exec1.Error
		if err != nil {
			tx.Rollback()
			return err
		}

		// make sure we don't delete relations if no node was deleted
		if exec1.RowsAffected == 0 {
			return nil
		}

		// Delete old closure relationships
		delRelSql := fmt.Sprintf(deleteRelationsQuery, ct.relationsTbl, ct.relationsTbl)
		exec2 := tx.Exec(delRelSql, nodeId)
		err = exec2.Error
		if err != nil {
			tx.Rollback()
			return err
		}

		return nil
	})
}

const deleteNodesRec = `WITH nodes_to_delete AS (
    SELECT nodes.node_id
    FROM %s AS nodes
    JOIN %s AS ct ON ct.descendant_id = nodes.node_id
    WHERE ct.ancestor_id = ? AND nodes.Tenant = ?
)
DELETE FROM %s
WHERE node_id IN (SELECT node_id FROM nodes_to_delete);`

const deleteRelationsQuery = `WITH descendants AS 
	(
		SELECT descendant_id FROM %s WHERE ancestor_id = ? 
	)
	DELETE FROM %s
	WHERE descendant_id IN (SELECT descendant_id FROM descendants);`

// GetNode loads a single item into the passed pointer
func (ct *Tree) GetNode(nodeID uint, tenant string, item any) error {

	if !hasNode(item) {
		return ItemIsNotTreeNode
	}
	tenant = defaultTenant(tenant)
	t := reflect.TypeOf(item)

	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("item needs to be a pointer to a struct")
	}

	err := ct.db.Table(ct.nodesTbl).
		Where("node_id = ? AND tenant = ?", nodeID, tenant).
		First(item).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return NodeNotFoundErr
		}
		return fmt.Errorf("unable to check parent node: %v", err)
	}
	return nil
}
