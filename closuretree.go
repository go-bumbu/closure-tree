package closuretree

import (
	"errors"
	"fmt"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

const branchTblName = "_ct_nodes"
const closureTblName = "_ct_relations"

var ItemIsNotTreeNode = errors.New("the item does not embed Node")
var ParentNotFoundErr = errors.New("wrong parent ID")

// todo add with** options factory
func New(db *gorm.DB, item any, name string) (*Tree, error) {

	ln := branchTblName
	cn := closureTblName
	if name != "" {
		ln = strings.ToLower(fmt.Sprintf("%s_%s", branchTblName, name))
		cn = strings.ToLower(fmt.Sprintf("%s_%s", closureTblName, name))
	}

	ct := Tree{
		db:           db,
		nodesTbl:     ln,
		relationsTbl: cn,
	}

	if !hasNode(item) {
		return nil, ItemIsNotTreeNode
	}

	err := db.Table(ct.nodesTbl).AutoMigrate(item)
	if err != nil {
		return nil, fmt.Errorf("unable to migreate leave: %v", err)
	}

	err = db.Table(ct.relationsTbl).AutoMigrate(closureTree{})
	if err != nil {
		return nil, fmt.Errorf("unable to migrate closure: %v", err)
	}

	//About: this piece of code will add a root node with ID 1 this way we dont need a specific fucntion to get
	// the root nodes,
	//
	//t := reflect.TypeOf(item)
	//itemIsPointer := false
	//if t.Kind() == reflect.Ptr {
	//	t = t.Elem()
	//	itemIsPointer = true
	//}
	//reflectItem := reflect.New(t).Interface()
	//if itemIsPointer {
	//	reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item).Elem())
	//} else {
	//	reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item))
	//}
	//
	//spew.Dump(reflectItem)
	//
	//err = ct.Add(reflectItem, 0)
	//if err != nil {
	//	return nil, fmt.Errorf("unable to add root node with ID 1: %v", err)
	//}

	return &ct, nil
}

const DefaultTenant = "DefaultTenant"

func defaultTenant(in string) string {
	if in == "" {
		return DefaultTenant
	}
	return in
}

type Tree struct {
	db *gorm.DB

	// table names, allows multiple trees
	nodesTbl     string
	relationsTbl string
}

// represents the table that store the relationships
type closureTree struct {
	AncestorID   uint   `gorm:"not null,primaryKey,uniqueIndex"`
	DescendantID uint   `gorm:"not null,primaryKey,uniqueIndex"`
	Tenant       string `gorm:"index"`
	Depth        int
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

		id, tenant, err := getNodeData(reflectItem)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("unable to get Item ID: %v", err)
		}

		// Add reflexive relationship
		err = tx.Table(ct.relationsTbl).Create(&closureTree{AncestorID: id, DescendantID: id, Tenant: tenant, Depth: 0}).Error
		if err != nil {
			tx.Rollback()
			return err
		}

		if parentID != 0 {
			// Copy all ancestors of the parent to include the new tag
			sqlstr := fmt.Sprintf(addQuery, ct.relationsTbl, ct.relationsTbl)
			exec := tx.Exec(sqlstr, id, tenant, parentID)
			if exec.Error != nil {
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

const addQuery = `INSERT INTO %s (ancestor_id, descendant_id, Tenant, depth)
				SELECT ancestor_id, ?, ?, depth + 1
				FROM %s
				WHERE descendant_id = ?;`

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

	if maxDepth > 0 {
		// return aup to max depth
		sqlstr := fmt.Sprintf(descendantsQuery, ct.nodesTbl, ct.relationsTbl)
		err := ct.db.Raw(sqlstr, parent, maxDepth, tenant).Scan(slice.Addr().Interface()).Error
		if err != nil {
			return fmt.Errorf("failed to fetch descendants: %w", err)
		}
	} else {
		// return all children
		sqlstr := fmt.Sprintf(descendantsQueryAll, ct.nodesTbl, ct.relationsTbl)
		err := ct.db.Raw(sqlstr, parent, tenant).Scan(slice.Addr().Interface()).Error
		if err != nil {
			return fmt.Errorf("failed to fetch descendants: %w", err)
		}
	}

	return nil
}

const descendantsQuery = `SELECT nodes.*
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.Tenant = ?
ORDER BY ct.depth;`

const descendantsQueryAll = `SELECT nodes.*
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND nodes.Tenant = ?
ORDER BY ct.depth;`

func (ct *Tree) DescendantIds(parent uint, maxDepth int, tenant string) ([]uint, error) {
	tenant = defaultTenant(tenant)
	ids := []uint{}
	if maxDepth > 0 {
		sqlstr := fmt.Sprintf(descendantsIDQuery, ct.nodesTbl, ct.relationsTbl)
		err := ct.db.Raw(sqlstr, parent, maxDepth, tenant).Scan(&ids).Error
		if err != nil {
			return nil, fmt.Errorf("failed to fetch descendants: %w", err)
		}
		return ids, nil
	} else {
		sqlstr := fmt.Sprintf(descendantsIDQueryAll, ct.nodesTbl, ct.relationsTbl)
		err := ct.db.Raw(sqlstr, parent, tenant).Scan(&ids).Error
		if err != nil {
			return nil, fmt.Errorf("failed to fetch descendants: %w", err)
		}
		return ids, nil
	}
}

const descendantsIDQuery = `SELECT nodes.node_id
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.Tenant = ?
ORDER BY ct.depth;`

const descendantsIDQueryAll = `SELECT nodes.node_id
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND nodes.Tenant = ?
ORDER BY ct.depth;`

func (ct *Tree) Roots(items interface{}, tenant string) error {
	if items == nil {
		return errors.New("items cannot be nil")
	}
	tenant = defaultTenant(tenant)

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

	sqlstr := fmt.Sprintf(rootsQuery, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl)
	err := ct.db.Raw(sqlstr, tenant).Scan(slice.Addr().Interface()).Error
	if err != nil {
		return fmt.Errorf("failed to fetch descendants: %w", err)
	}

	return nil
}

const rootsQuery = `SELECT DISTINCT nodes.*
FROM %s as nodes
JOIN %s AS ct1 ON nodes.node_id = ct1.ancestor_id
LEFT JOIN %s AS ct2 ON ct1.ancestor_id = ct2.descendant_id AND ct2.depth = 1
WHERE ct2.descendant_id IS NULL AND nodes.Tenant = ?;`

func (ct *Tree) RootIds(tenant string) ([]uint, error) {
	tenant = defaultTenant(tenant)
	ids := []uint{}
	sqlstr := fmt.Sprintf(rootIdsQuery, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl)
	err := ct.db.Raw(sqlstr, tenant).Scan(&ids).Error
	if err != nil {
		return ids, fmt.Errorf("failed to fetch descendants: %w", err)
	}
	return ids, nil
}

const rootIdsQuery = `SELECT DISTINCT nodes.node_id
FROM %s as nodes
JOIN %s AS ct1 ON nodes.node_id = ct1.ancestor_id
LEFT JOIN %s AS ct2 ON ct1.ancestor_id = ct2.descendant_id AND ct2.depth = 1
WHERE ct2.descendant_id IS NULL AND nodes.Tenant = ?;`

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
	// leaving in for now in case i enforce one single root node with ID 1
	//if nodeId == 1 {
	//	return fmt.Errorf("the root node cannot be deleted")
	//}

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

//type Tree struct {
//	db *gorm.DB
//	closureTableName string
//}
//
//type Node struct {
//	ID       uint   `gorm:"primaryKey"`
//	Name     string
//	ParentID *uint // NULL for root
//}
//
//// Callback type for custom operations
//type NodeCallback func(node Node) error
//func (t *Tree) WalkTree(rootID uint, callback NodeCallback) error {
//	// Recursive helper function
//	var walk func(nodeID uint) error
//
//	walk = func(nodeID uint) error {
//		// Fetch the current node
//		var node Node
//		if err := t.db.First(&node, nodeID).Error; err != nil {
//			return fmt.Errorf("failed to fetch node %d: %w", nodeID, err)
//		}
//
//		// Invoke the callback for the current node
//		if err := callback(node); err != nil {
//			return fmt.Errorf("callback failed for node %d: %w", node.ID, err)
//		}
//
//		// Fetch children of the current node
//		var children []Node
//		if err := t.db.Table(t.closureTableName).
//			Select("nodes.*").
//			Joins("JOIN nodes ON nodes.id = closure_tree.descendant_id").
//			Where("closure_tree.ancestor_id = ? AND closure_tree.depth = 1", node.ID).
//			Scan(&children).Error; err != nil {
//			return fmt.Errorf("failed to fetch children for node %d: %w", node.ID, err)
//		}
//
//		// Recur for each child
//		for _, child := range children {
//			if err := walk(child.ID); err != nil {
//				return err
//			}
//		}
//
//		return nil
//	}
//
//	// Start traversal from the root
//	return walk(rootID)
//}
