package closuretree

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"reflect"
	"sort"
	"strings"
)

const closureTblName = "closure_tree_rel"

var ErrItemIsNotTreeNode = errors.New("the item does not embed Node")
var ErrParentNotFound = errors.New("wrong parent ID")
var ErrNodeNotFound = errors.New("node not found")

// Tree represents the access to the closure tree allowing to CRUD nodes on the tree of items
type Tree struct {
	db *gorm.DB
	// table names, allows multiple trees
	nodesTbl     string
	relationsTbl string
	col2FieldMap map[string]string
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

	// Generate a map of column names to field names
	columnFieldMap := make(map[string]string)
	for _, field := range stmt.Schema.Fields {
		columnFieldMap[field.DBName] = field.Name
	}
	columnFieldMap["ancestor_id"] = "ancestorId"

	ct := Tree{
		db:           db,
		nodesTbl:     name,
		col2FieldMap: columnFieldMap,
		relationsTbl: relTbl,
	}

	if !hasNode(item) {
		return nil, ErrItemIsNotTreeNode
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

// GetClosureTableName returns the table name of the node closure tree relationship, used if you need to interact directly
// with the database
func (ct *Tree) GetClosureTableName() string {
	return ct.relationsTbl
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
//
//nolint:gocyclo // excluding from linter since implementation was done before we enabled the linter
func (ct *Tree) Add(ctx context.Context, item any, parentID uint, tenant string) error {
	if !hasNode(item) {
		return ErrItemIsNotTreeNode
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
		err := ct.db.WithContext(ctx).Table(ct.nodesTbl).
			Where("node_id = ? AND tenant = ?", parentID, tenant).
			First(&parent).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParentNotFound
			}
			return fmt.Errorf("unable to check parent node: %v", err)
		}
	}

	err := ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
				return ex.Error
			}
		} else {
			// Copy all ancestors of the parent to include the new tag
			sqlstr := fmt.Sprintf(addRelsQuery, ct.relationsTbl, ct.relationsTbl)
			ex := tx.Exec(sqlstr, id, gotTennant, parentID)
			if ex.Error != nil {
				tx.Rollback()
				return ex.Error
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
func (ct *Tree) Update(ctx context.Context, id uint, item any, tenant string) error {
	if !hasNode(item) {
		return ErrItemIsNotTreeNode
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

	err := ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Table(ct.nodesTbl).Where("node_id = ? AND tenant = ?", id, tenant).Updates(reflectItem)

		if res.Error != nil {
			tx.Rollback()
			return fmt.Errorf("unable to update node: %v", res.Error)
		}
		if res.RowsAffected == 0 {
			return ErrNodeNotFound
		}

		return nil
	})
	return err
}

var ErrInvalidMove = errors.New("invalid move")

func (ct *Tree) Move(ctx context.Context, nodeId, newParentID uint, tenant string) error {
	tenant = defaultTenant(tenant)
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {

		// Prevent duplicate move to same parent
		hasSameParent, err := ct.IsChildOf(ctx, nodeId, newParentID, tenant)
		if err != nil {
			return err
		}
		if hasSameParent {
			return ErrInvalidMove
		}

		var exec1 *gorm.DB
		// insert nodes on the new position
		if newParentID == 0 {
			// Special case: move to root
			insertSql := fmt.Sprintf(moveQueryInsertNewToRoot, ct.relationsTbl, ct.relationsTbl)
			exec1 = tx.Exec(insertSql, nodeId, tenant)
		} else {
			// Normal move
			// make sure that move is allowed
			isDesc, err := ct.IsDescendant(ctx, nodeId, newParentID, tenant)
			if err != nil {
				return err
			}
			if isDesc {
				return ErrInvalidMove
			}

			insertSql := fmt.Sprintf(moveQueryInsertNew, ct.relationsTbl, ct.relationsTbl, ct.relationsTbl)
			exec1 = tx.Exec(insertSql, nodeId, newParentID, tenant, tenant)
		}

		if exec1.Error != nil {
			return exec1.Error
		}
		// make sure we don't delete items if nothing was moved, e.g. if we try to move cross Tenant unsuccessful
		if exec1.RowsAffected == 0 {
			// note: for now we assume that if no row were affected we could not find either the node to move
			// or the new parent, either because they don't exist or because they belong to another tenant
			return ErrNodeNotFound
		}

		// carefully delete old closure relationships
		var exec2 *gorm.DB
		if newParentID == 0 {
			// Special case: move to root
			delSql := fmt.Sprintf(
				moveQueryToRootDeleteOld, // your SQL with placeholders
				ct.relationsTbl,          // %s: subtree CTE table name
				ct.relationsTbl,          // %s: old_paths CTE table name
				ct.relationsTbl,          // %s: new_paths alias p table name
				ct.relationsTbl,          // %s: new_paths alias c table name
				ct.relationsTbl,          // %s: DELETE FROM target table name
			)
			exec2 = tx.Exec(delSql,
				nodeId,      // ?1: subtree -> ancestor_id = moved_node_id (e.g., 2)
				tenant,      // ?2: subtree -> tenant string (e.g., "t1")
				tenant,      // ?3: old_paths -> tenant string
				nodeId,      // ?4: new_paths c.ancestor_id = moved_node_id
				newParentID, // ?5: new_paths p.descendant_id = new_parent_node_id (e.g., 5)
				tenant,      // ?6: new_paths p.tenant
				tenant,      // ?7: new_paths c.tenant
				nodeId,      // ?8: SELECT 0 AS ancestor_id, ? AS descendant_id, 1 AS depth
			)
		} else {
			// Normal move

			delSql := fmt.Sprintf(
				moveQueryDeleteOld, // your SQL with placeholders
				ct.relationsTbl,    // %s: subtree CTE table name
				ct.relationsTbl,    // %s: old_paths CTE table name
				ct.relationsTbl,    // %s: new_paths alias p table name
				ct.relationsTbl,    // %s: new_paths alias c table name
				ct.relationsTbl,    // %s: DELETE FROM target table name
			)

			exec2 = tx.Exec(delSql,
				nodeId,      // ?1: subtree -> ancestor_id = moved_node_id (e.g., 2)
				tenant,      // ?2: subtree -> tenant string (e.g., "t1")
				tenant,      // ?3: old_paths -> tenant string
				nodeId,      // ?4: new_paths c.ancestor_id = moved_node_id
				newParentID, // ?5: new_paths p.descendant_id = new_parent_node_id (e.g., 5)
				tenant,      // ?6: new_paths p.tenant
				tenant,      // ?7: new_paths c.tenant
			)
		}
		return exec2.Error
	})
}

const moveQueryInsertNewToRoot = `
INSERT INTO  %s (ancestor_id, descendant_id, depth, tenant)
SELECT 0, c.descendant_id, c.depth + 1, c.tenant
FROM  %s c
WHERE c.ancestor_id = ? AND c.tenant = ?;
`

const moveQueryInsertNew = `
INSERT INTO %s (ancestor_id, descendant_id, depth, Tenant)
SELECT p.ancestor_id, c.descendant_id, p.depth + c.depth + 1, p.Tenant
FROM %s p
JOIN %s c ON c.ancestor_id = ?
WHERE p.descendant_id = ? AND p.Tenant = ? AND c.Tenant = ?;
`

// descendants contains the list of nodes moved
// take this quey and replace closure_tree_rel_test_payloads by %s also replace the node ids 2, 5 and the tenant string with a placeholder ?, write a comment on every placeholder replacement stating what it is used for

const moveQueryDeleteOld = `
WITH subtree AS (
  SELECT descendant_id
  FROM %s
  WHERE ancestor_id = ? AND tenant = ? -- the node to be moved
),
old_paths AS (
  SELECT *
  FROM %s
  WHERE descendant_id IN (SELECT descendant_id FROM subtree)
    AND tenant = ?
    AND ancestor_id != descendant_id  -- skip self-links
    AND ancestor_id NOT IN (SELECT descendant_id FROM subtree) -- skip internal subtree links
),
new_paths AS (
  SELECT p.ancestor_id, c.descendant_id, p.depth + c.depth + 1 AS depth
  FROM %s p
  JOIN %s c ON c.ancestor_id = ?   -- the node to be moved
  WHERE p.descendant_id = ?     -- the new parent
    AND p.tenant = ?   AND c.tenant = ?
)
DELETE FROM %s
WHERE (ancestor_id, descendant_id, tenant, depth) IN (
  SELECT o.ancestor_id, o.descendant_id, o.tenant, o.depth
  FROM old_paths o
  LEFT JOIN new_paths n
    ON o.ancestor_id = n.ancestor_id AND o.descendant_id = n.descendant_id
  WHERE n.descendant_id IS NULL OR o.depth != n.depth
);
`

const moveQueryToRootDeleteOld = `
WITH subtree AS (
  SELECT descendant_id
  FROM %s
  WHERE ancestor_id = ? AND tenant = ?
),
old_paths AS (
  SELECT *
  FROM %s
  WHERE descendant_id IN (SELECT descendant_id FROM subtree)
	AND tenant = ?
	AND ancestor_id != descendant_id
	AND ancestor_id NOT IN (SELECT descendant_id FROM subtree)
),
new_paths AS (
  SELECT p.ancestor_id, c.descendant_id, p.depth + c.depth + 1 AS depth
  FROM %s p
  JOIN %s c ON c.ancestor_id = ?
  WHERE p.descendant_id = ?
	AND p.tenant = ? AND c.tenant = ?
  UNION ALL
  SELECT 0 AS ancestor_id, ? AS descendant_id, 1 AS depth
)
DELETE FROM %s
WHERE (ancestor_id, descendant_id, tenant, depth) IN (
  SELECT o.ancestor_id, o.descendant_id, o.tenant, o.depth
  FROM old_paths o
  LEFT JOIN new_paths n
	ON o.ancestor_id = n.ancestor_id AND o.descendant_id = n.descendant_id
  WHERE n.descendant_id IS NULL OR o.depth != n.depth
);
`

func (ct *Tree) DeleteRecurse(ctx context.Context, nodeId uint, tenant string) error {
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {

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
			// note: for now we assume that if no row were affected we could not find either the node to move
			// or the new parent, either because they don't exist or because they belong to another tenant
			return ErrNodeNotFound
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
func (ct *Tree) GetNode(ctx context.Context, nodeID uint, tenant string, item any) error {

	if !hasNode(item) {
		return ErrItemIsNotTreeNode
	}
	tenant = defaultTenant(tenant)
	t := reflect.TypeOf(item)

	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("item needs to be a pointer to a struct")
	}

	err := ct.db.WithContext(ctx).Table(ct.nodesTbl).
		Where("node_id = ? AND tenant = ?", nodeID, tenant).
		First(item).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNodeNotFound
		}
		return fmt.Errorf("unable to check parent node: %v", err)
	}
	return nil
}

// TODO write unit tests
// IsDescendant returns true if targetID is a descendant of nodeID in the given tenant.
func (ct *Tree) IsDescendant(ctx context.Context, nodeID, parentId uint, tenant string) (bool, error) {
	var count int64
	err := ct.db.WithContext(ctx).
		Table(ct.relationsTbl).
		Where("ancestor_id = ? AND descendant_id = ? AND tenant = ?", nodeID, parentId, tenant).
		Limit(1).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// IsChildOf checks if nodeID already has newParentID as its parent in the closure table.
func (ct *Tree) IsChildOf(ctx context.Context, nodeID, parentID uint, tenant string) (bool, error) {
	var count int64
	err := ct.db.WithContext(ctx).
		Table(ct.relationsTbl).
		Where("ancestor_id = ? AND descendant_id = ? AND depth = 1 AND tenant = ?", parentID, nodeID, tenant).
		Limit(1).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Descendants allows to load a part of the tree into a flat slice of node pointers
// parent determines the root node id of to load.
// maxDepth determines the depth of the relationship o load: 0 means all children, 1 only direct children and so on.
// tenant determines the tenant to be used
func (ct *Tree) Descendants(ctx context.Context, parent uint, maxDepth int, tenant string, items interface{}) (err error) {
	if items == nil {
		return errors.New("items cannot be nil")
	}

	itemsVal := reflect.ValueOf(items)
	if itemsVal.Kind() != reflect.Ptr {
		return errors.New("items must be a pointer to a slice")
	}
	sliceVal := itemsVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return errors.New("items must be a pointer to a slice")
	}

	elemType := sliceVal.Type().Elem()

	// Prepare temp struct type with overridden ParentId tag
	var fields []reflect.StructField
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		if field.Name == "ParentId" {
			field.Tag = `json:"parentId"`
		}
		fields = append(fields, field)
	}
	tempStructType := reflect.StructOf(fields)

	if tempStructType.Kind() != reflect.Struct {
		return errors.New("tempStructType is not a struct")
	}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}
	sqlstr := fmt.Sprintf(descendantsQuery, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl)

	rows, err := ct.db.WithContext(ctx).Raw(sqlstr, parent, maxDepth, tenant).Rows()
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer func() {
		e := rows.Close()
		if err == nil { // don't overwrite the original error
			err = e
		}
	}()

	for rows.Next() {
		tempItem := reflect.New(tempStructType).Interface()
		if err := ct.db.ScanRows(rows, tempItem); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		tempVal := reflect.ValueOf(tempItem).Elem()
		origItem := reflect.New(elemType).Elem()

		for i := 0; i < elemType.NumField(); i++ {
			origField := origItem.Field(i)
			tempField := tempVal.Field(i)
			if origField.CanSet() {
				origField.Set(tempField)
			}
		}

		sliceVal.Set(reflect.Append(sliceVal, origItem))
	}

	return nil
}

const descendantsQuery = `SELECT nodes.*, parent_rel.ancestor_id AS parent_id
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id
LEFT JOIN %s AS parent_rel ON parent_rel.descendant_id = nodes.node_id AND parent_rel.depth = 1
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.tenant = ?
ORDER BY ct.depth;`

// DescendantIds behaves the same as Descendants but only returns the node IDs for the search query.
func (ct *Tree) DescendantIds(ctx context.Context, parent uint, maxDepth int, tenant string) ([]uint, error) {
	tenant = defaultTenant(tenant)
	ids := []uint{}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}
	sqlstr := fmt.Sprintf(descendantsIDQuery, ct.nodesTbl, ct.relationsTbl)
	err := ct.db.WithContext(ctx).Raw(sqlstr, parent, maxDepth, tenant).Scan(&ids).Error
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

const absMaxDepth = 2147483647 // limited by the max value of postgres bigint
// NOTE should you ever need this deep level of nesting in a production environment, please reach out to me directly
// I'm really really really interested into knowing why and how!!! :)

// TreeDescendants  allows to load a part of the tree into a slice of node pointers keeping the tree structure of the DB
// note that the item passed needs to be a []*MyCustomType, and it needs to contain a field Children of type []*MyCustomType
// e.g.
//
//	type Custom struct {
//		ct.Node
//		Name string
//		Children []*Custom
//	}
//
// var items = []*Custom{}
// parent determines the root node id of to load.
// maxDepth determines the depth of the relationship o load: 0 means all children, 1 only direct children and so on.
// tenant determines the tenant to be used
func (ct *Tree) TreeDescendants(ctx context.Context, parent uint, maxDepth int, tenant string, items any) (err error) {
	if err := validateItems(items); err != nil {
		return err
	}

	itemsVal := reflect.ValueOf(items)
	sliceVal := itemsVal.Elem()
	elemType := sliceVal.Type().Elem()

	tenant = defaultTenant(tenant)

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	} else {
		// this is needed because the query will list first level as depth =0, children are depth = 1
		maxDepth = maxDepth - 1
	}

	sqlQuery := fmt.Sprintf(treeDescendantsQuery, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl, ct.nodesTbl)
	rows, err := ct.db.WithContext(ctx).Raw(sqlQuery, parent, tenant, tenant, maxDepth).Rows()
	if err != nil {
		return fmt.Errorf("failed to fetch tree descendants: %w", err)
	}
	defer func() {
		e := rows.Close()
		if err == nil { // don't overwrite the original error
			err = e
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to read column names: %w", err)
	}

	nodes, ancestorMap, err := scanRowsToNodes(rows, columns, ct.col2FieldMap, elemType)
	if err != nil {
		return err
	}

	rootNodes := buildTreeHierarchy(nodes, ancestorMap)
	for _, node := range rootNodes {
		sliceVal.Set(reflect.Append(sliceVal, node))
	}

	return nil
}

// --- Helper Functions ---

func validateItems(items any) error {
	if items == nil {
		return errors.New("items cannot be nil")
	}
	itemsVal := reflect.ValueOf(items)
	if itemsVal.Kind() != reflect.Ptr {
		return errors.New("items must be a pointer to a slice")
	}
	sliceVal := itemsVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return errors.New("items must point to a slice")
	}
	elemType := sliceVal.Type().Elem()
	if elemType.Kind() != reflect.Ptr || elemType.Elem().Kind() != reflect.Struct {
		return errors.New("slice element type must be a pointer to a struct")
	}
	return nil
}

func scanRowsToNodes(rows *sql.Rows, columns []string, col2FieldMap map[string]string, elemType reflect.Type) (
	map[int64]reflect.Value, map[int64]int64, error,
) {
	nodes := make(map[int64]reflect.Value)
	ancestorMap := make(map[int64]int64)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		node, nodeID, ancestorID, err := mapRowToStruct(values, columns, col2FieldMap, elemType)
		if err != nil {
			return nil, nil, err
		}
		nodes[nodeID] = node
		ancestorMap[nodeID] = ancestorID
	}

	return nodes, ancestorMap, nil
}

func mapRowToStruct(values []interface{}, columns []string, col2FieldMap map[string]string, elemType reflect.Type) (
	reflect.Value, int64, int64, error,
) {
	newElem := reflect.New(elemType.Elem())
	var nodeID, ancestorID int64

	for i, col := range columns {
		fieldName, ok := col2FieldMap[col]
		if !ok {
			continue
		}

		var value any
		if b, ok := values[i].([]byte); ok {
			value = string(b)
		} else {
			value = values[i]
		}

		if fieldName == nodeIDField {
			nodeID = value.(int64)
		}
		if fieldName == "ancestorId" {
			ancestorID = value.(int64)
		}

		fieldVal := newElem.Elem().FieldByName(fieldName)
		if !fieldVal.IsValid() || !fieldVal.CanSet() {
			continue
		}

		val := reflect.ValueOf(value)
		if val.Type().AssignableTo(fieldVal.Type()) {
			fieldVal.Set(val)
		} else if val.Type().ConvertibleTo(fieldVal.Type()) {
			fieldVal.Set(val.Convert(fieldVal.Type()))
		} else {
			return reflect.Value{}, 0, 0, fmt.Errorf("cannot assign type %s to field %s", val.Type(), fieldName)
		}
	}

	return newElem, nodeID, ancestorID, nil
}

func buildTreeHierarchy(nodes map[int64]reflect.Value, ancestorMap map[int64]int64) []reflect.Value {
	var roots []reflect.Value

	for nodeID, node := range nodes {
		ancestorID, hasAncestor := ancestorMap[nodeID]
		if !hasAncestor {
			roots = append(roots, node)
			continue
		}

		parent, found := nodes[ancestorID]
		if !found {
			roots = append(roots, node)
			continue
		}

		childrenField := parent.Elem().FieldByName("Children")
		if childrenField.IsValid() {
			childrenField.Set(reflect.Append(childrenField, node))
		}
	}
	return roots
}

const treeDescendantsQuery = `WITH RECURSIVE Tree AS (
	-- Base case: Start with the parent node
	SELECT 
		nodes.*,
   		ct.ancestor_id AS  ancestor_id,    
		0 AS depth  
	FROM %s AS nodes
  	JOIN %s AS ct ON ct.descendant_id = nodes.node_id
    WHERE ct.ancestor_id = ? AND ct.depth = 1 AND nodes.Tenant = ?

	UNION ALL

  -- Recursive case: get immediate children (depth = 1 in closure table) of nodes in Tree,

	SELECT 
		nodes.*,
		t.node_id AS ancestor_id, 
    	t.depth + 1 AS depth
	FROM Tree AS t
  	JOIN %s AS ct ON ct.ancestor_id = t.node_id AND ct.depth = 1  -- use only immediate children relationships
  	JOIN %s AS nodes ON nodes.node_id = ct.descendant_id
  	WHERE nodes.Tenant = ? AND t.depth < ?
	)
	SELECT  * FROM Tree ORDER BY depth;`

// TreeDescendantsIds returns the tree structure of the descendants to the passed item
func (ct *Tree) TreeDescendantsIds(ctx context.Context, parent uint, maxDepth int, tenant string) (tree []*TreeNode, err error) {
	tenant = defaultTenant(tenant)
	nodeMap := make(map[uint]*TreeNode)

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	} else {
		// this is needed because the query will list first level as depth =0, children are depth = 1
		maxDepth = maxDepth - 1
	}

	sqlstr := fmt.Sprintf(treeDescendantsIDQuery, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl, ct.nodesTbl)
	rows, err := ct.db.WithContext(ctx).Raw(sqlstr, parent, tenant, tenant, maxDepth).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tree descendants: %w", err)
	}
	defer func() {
		e := rows.Close()
		if err == nil { // don't overwrite the original error
			err = e
		}
	}()

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

type TreeNode struct {
	NodeId     uint `json:"id"`
	AncestorID uint
	Children   []*TreeNode `json:"children"`
}

const treeDescendantsIDQuery = `WITH RECURSIVE Tree AS (
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

func SortTree(nodes []*TreeNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeId < nodes[j].NodeId
	})
	for _, node := range nodes {
		SortTree(node.Children)
	}
}
