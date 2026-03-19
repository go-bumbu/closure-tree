package closuretree

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

var validTableName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func validateTableName(name string) error {
	if !validTableName.MatchString(name) {
		return fmt.Errorf("invalid table name %q: must match [a-z][a-z0-9_]*", name)
	}
	return nil
}

const closureTblName = "closure_tree_rel"
const ancestorIDMapKey = "ancestorId"

var (
	ErrItemIsNotTreeNode      = errors.New("the item does not embed Node")
	ErrParentNotFound         = errors.New("wrong parent ID")
	ErrNodeNotFound           = errors.New("node not found")
	ErrInvalidMove            = errors.New("invalid move")
	ErrItemNotPointerToStruct = errors.New("item needs to be a pointer to a struct")
)

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
	ct, err := newTree(db, item)
	if err != nil {
		return nil, err
	}
	if err := ct.migrate(item); err != nil {
		return nil, err
	}
	if isMySQLDialect(db) {
		if err := checkMySQLVersion(db); err != nil {
			return nil, err
		}
	}
	return ct, nil
}

// newTree parses the schema and validates the item but does not run migrations.
func newTree(db *gorm.DB, item any) (*Tree, error) {
	if !hasNode(item) {
		return nil, ErrItemIsNotTreeNode
	}

	stmt := &gorm.Statement{DB: db}
	err := stmt.Parse(item)
	if err != nil {
		return nil, fmt.Errorf("error parsing schema: %w", err)
	}
	name := stmt.Schema.Table
	relTbl := strings.ToLower(fmt.Sprintf("%s_%s", closureTblName, name))

	if err := validateTableName(name); err != nil {
		return nil, err
	}
	if err := validateTableName(relTbl); err != nil {
		return nil, err
	}

	// Generate a map of column names to field names
	columnFieldMap := make(map[string]string)
	for _, field := range stmt.Schema.Fields {
		columnFieldMap[field.DBName] = field.Name
	}
	columnFieldMap["ancestor_id"] = ancestorIDMapKey

	ct := &Tree{
		db:           db,
		nodesTbl:     name,
		col2FieldMap: columnFieldMap,
		relationsTbl: relTbl,
	}

	return ct, nil
}

func (ct *Tree) migrate(item any) error {
	err := ct.db.AutoMigrate(item)
	if err != nil {
		return fmt.Errorf("unable to migrate node table: %w", err)
	}
	err = ct.db.Table(ct.relationsTbl).AutoMigrate(closureTree{})
	if err != nil {
		return fmt.Errorf("unable to migrate closure table: %w", err)
	}
	return nil
}

func isMySQLDialect(db *gorm.DB) bool {
	return db.Name() == "mysql"
}

func checkMySQLVersion(db *gorm.DB) error {
	var version string
	if err := db.Raw("SELECT VERSION()").Scan(&version).Error; err != nil {
		return fmt.Errorf("unable to check MySQL version: %w", err)
	}
	parts := strings.SplitN(version, ".", 2)
	if len(parts) < 1 {
		return fmt.Errorf("unable to parse MySQL version: %s", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 8 {
		return fmt.Errorf("MySQL 8.0+ required; got %s", version)
	}
	return nil
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
// Note: if upgrading from a previous version, manually run: DROP INDEX idx_desc_ten
type closureTree struct {
	AncestorID   uint   `gorm:"not null;index:idx_anc_ten_dep,composite:1;uniqueIndex:idx_closure_uniq,composite:a"`
	DescendantID uint   `gorm:"not null;index:idx_desc_ten_dep,composite:1;uniqueIndex:idx_closure_uniq,composite:b"`
	Tenant       string `gorm:"not null;index:idx_anc_ten_dep,composite:2;index:idx_desc_ten_dep,composite:2;uniqueIndex:idx_closure_uniq,composite:c"`
	Depth        int    `gorm:"not null;default:0;check:chk_depth,depth >= 0;index:idx_anc_ten_dep,composite:3;index:idx_desc_ten_dep,composite:3;uniqueIndex:idx_closure_uniq,composite:d"`
}

// DefaultTenant is used in the database as a stub if not tenant was passed
const DefaultTenant = "DefaultTenant"

// ErrEmptyTenant is returned when an empty tenant string is passed to any tree operation.
var ErrEmptyTenant = errors.New("tenant must not be empty; pass closuretree.DefaultTenant to use the default")

func validateTenant(in string) (string, error) {
	if in == "" {
		return "", ErrEmptyTenant
	}
	return in, nil
}

// Add will add a new entry into the node Database under a specific parent and owned to a specific tenant
// Note: the passed item has to embed a Node struct, but any value added to the Node will be ignored
//
//nolint:gocyclo // excluding from linter since implementation was done before we enabled the linter
func (ct *Tree) Add(ctx context.Context, item any, parentID uint, tenant string) error {
	if !hasNode(item) {
		return ErrItemIsNotTreeNode
	}
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}

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
	if nodeField, ok := findNodeValue(t, v); ok && nodeField.CanSet() {
		nodeField.Set(reflect.ValueOf(Node{NodeId: 0, Tenant: tenant}))
	}

	err = ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Check if the parent node exists and the tenant is the same (inside tx to avoid TOCTOU)
		if parentID != 0 {
			var parent Node
			err := tx.Table(ct.nodesTbl).
				Where("node_id = ? AND tenant = ?", parentID, tenant).
				First(&parent).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrParentNotFound
				}
				return fmt.Errorf("unable to check parent node: %w", err)
			}
		}

		// create the Node item
		err := tx.Table(ct.nodesTbl).Create(reflectItem).Error
		if err != nil {
			return fmt.Errorf("unable to add node: %w", err)
		}

		id, gotTennant, err := getNodeData(reflectItem)
		if err != nil {
			return fmt.Errorf("unable to get Item ID: %w", err)
		}

		// Add reflexive relationship
		err = tx.Table(ct.relationsTbl).Create(&closureTree{AncestorID: id, DescendantID: id, Tenant: gotTennant, Depth: 0}).Error
		if err != nil {
			return err
		}

		if parentID == 0 {
			// Create a root note relationship
			sqlstr := fmt.Sprintf(addRootRelQuery, ct.relationsTbl)
			ex := tx.Exec(sqlstr, id, gotTennant)
			if ex.Error != nil {
				return ex.Error
			}
		} else {
			// Copy all ancestors of the parent to include the new tag
			sqlstr := fmt.Sprintf(addRelsQuery, ct.relationsTbl, ct.relationsTbl)
			ex := tx.Exec(sqlstr, id, gotTennant, parentID, gotTennant)
			if ex.Error != nil {
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
		srcT := reflect.TypeOf(reflectItem).Elem()
		srcV := reflect.ValueOf(reflectItem).Elem()
		dstT := reflect.TypeOf(item).Elem()
		dstV := reflect.ValueOf(item).Elem()

		if srcNode, ok := findNodeValue(srcT, srcV); ok {
			if dstNode, ok := findNodeValue(dstT, dstV); ok && dstNode.CanSet() {
				dstNode.Set(srcNode)
			}
		}
	}

	return nil
}

const addRelsQuery = `INSERT INTO %s (ancestor_id, descendant_id, tenant, depth)
			SELECT ancestor_id, ?, ?, depth + 1
			FROM %s
			WHERE descendant_id = ? AND tenant = ?;`

const addRootRelQuery = `INSERT INTO %s (ancestor_id, descendant_id, tenant, depth) VALUES (0, ?, ?, 1);`

// Update  will update the entry with given ID and owned to a specific tenant
// Note: the passed item has to embed a Node struct, but any value added to the Node will be ignored
func (ct *Tree) Update(ctx context.Context, id uint, item any, tenant string) error {
	if !hasNode(item) {
		return ErrItemIsNotTreeNode
	}
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}

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
	if nodeField, ok := findNodeValue(t, v); ok && nodeField.CanSet() {
		nodeField.Set(reflect.ValueOf(Node{NodeId: id, Tenant: tenant}))
	}

	// Build a map of non-Node fields to update (preserves zero values)
	updateStmt := &gorm.Statement{DB: ct.db}
	_ = updateStmt.Parse(reflectItem)
	updateMap := make(map[string]any)
	for _, f := range updateStmt.Schema.Fields {
		if f.DBName == "" || !f.Updatable {
			continue
		}
		// Skip Node fields (node_id, tenant)
		if f.OwnerSchema != nil && f.OwnerSchema.ModelType == reflect.TypeOf(Node{}) {
			continue
		}
		fieldVal := reflect.ValueOf(reflectItem).Elem().FieldByName(f.Name)
		if fieldVal.IsValid() {
			updateMap[f.DBName] = fieldVal.Interface()
		}
	}

	err = ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Table(ct.nodesTbl).Where("node_id = ? AND tenant = ?", id, tenant).Updates(updateMap)

		if res.Error != nil {
			return fmt.Errorf("unable to update node: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return ErrNodeNotFound
		}

		return nil
	})
	return err
}

func (ct *Tree) Move(ctx context.Context, nodeId, newParentID uint, tenant string) error {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {

		// Prevent duplicate move to same parent (uses tx to avoid TOCTOU)
		var sameParentCount int64
		if err := tx.Table(ct.relationsTbl).
			Where("ancestor_id = ? AND descendant_id = ? AND depth = 1 AND tenant = ?", newParentID, nodeId, tenant).
			Count(&sameParentCount).Error; err != nil {
			return err
		}
		if sameParentCount > 0 {
			return ErrInvalidMove
		}

		if newParentID != 0 {
			// Normal move — make sure we're not moving a node under its own descendant (uses tx)
			var descCount int64
			if err := tx.Table(ct.relationsTbl).
				Where("ancestor_id = ? AND descendant_id = ? AND tenant = ?", nodeId, newParentID, tenant).
				Limit(1).Count(&descCount).Error; err != nil {
				return err
			}
			if descCount > 0 {
				return ErrInvalidMove
			}
		}

		// STEP 1: Delete all connections coming from outside the subtree into the subtree.
		// This must happen before the insert to avoid unique-constraint conflicts when
		// an (ancestor_id, descendant_id, tenant) triple needs a new depth value.
		delSql := fmt.Sprintf(moveDeleteExternalPaths,
			ct.relationsTbl, // %s: CTE SELECT FROM
			ct.relationsTbl, // %s: DELETE FROM
		)
		delExec := tx.Exec(delSql, nodeId, tenant, tenant)
		if delExec.Error != nil {
			return delExec.Error
		}
		// If nothing was deleted the node doesn't exist (or belongs to another tenant)
		if delExec.RowsAffected == 0 {
			return ErrNodeNotFound
		}

		// STEP 2: Insert the new connections from the destination's ancestors to the subtree.
		var insExec *gorm.DB
		if newParentID == 0 {
			// Move to root: connect root sentinel (ancestor_id=0) to every subtree node
			insertSql := fmt.Sprintf(moveQueryInsertNewToRoot, ct.relationsTbl, ct.relationsTbl)
			insExec = tx.Exec(insertSql, nodeId, tenant)
		} else {
			// Normal move: copy ancestor paths from new parent to all subtree nodes
			insertSql := fmt.Sprintf(moveQueryInsertNew, ct.relationsTbl, ct.relationsTbl, ct.relationsTbl)
			insExec = tx.Exec(insertSql, nodeId, newParentID, tenant, tenant)
			if insExec.Error == nil && insExec.RowsAffected == 0 {
				// New parent not found in this tenant
				return ErrParentNotFound
			}
		}
		return insExec.Error
	})
}

const moveQueryInsertNewToRoot = `
INSERT INTO  %s (ancestor_id, descendant_id, depth, tenant)
SELECT 0, c.descendant_id, c.depth + 1, c.tenant
FROM  %s c
WHERE c.ancestor_id = ? AND c.tenant = ?;
`

const moveQueryInsertNew = `
INSERT INTO %s (ancestor_id, descendant_id, depth, tenant)
SELECT p.ancestor_id, c.descendant_id, p.depth + c.depth + 1, p.tenant
FROM %s p
JOIN %s c ON c.ancestor_id = ?
WHERE p.descendant_id = ? AND p.tenant = ? AND c.tenant = ?;
`

// moveDeleteExternalPaths removes all closure rows that come from ancestors OUTSIDE the
// moved subtree to nodes INSIDE it. This clears the old parent-chain connections while
// preserving all internal subtree links (self-links and intra-subtree edges).
// Uses a CTE so MySQL 8.0+ can materialise the subtree before the DELETE, avoiding
// Error 1093 ("can't specify target table in FROM clause").
const moveDeleteExternalPaths = `
WITH subtree AS (
    SELECT descendant_id FROM %s WHERE ancestor_id = ? AND tenant = ?
)
DELETE FROM %s
WHERE descendant_id IN (SELECT descendant_id FROM subtree)
AND ancestor_id NOT IN (SELECT descendant_id FROM subtree)
AND tenant = ?`

func (ct *Tree) DeleteRecurse(ctx context.Context, nodeId uint, tenant string) error {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {

		// delete the nodes
		delNodesSql := fmt.Sprintf(deleteNodesRec, ct.nodesTbl, ct.relationsTbl, ct.nodesTbl)
		exec1 := tx.Exec(delNodesSql, nodeId, tenant, tenant)
		if exec1.Error != nil {
			return exec1.Error
		}

		// make sure we don't delete relations if no node was deleted
		if exec1.RowsAffected == 0 {
			// note: for now we assume that if no row were affected we could not find either the node to move
			// or the new parent, either because they don't exist or because they belong to another tenant
			return ErrNodeNotFound
		}

		// Delete old closure relationships
		delRelSql := fmt.Sprintf(deleteRelationsQuery, ct.relationsTbl, ct.relationsTbl)
		exec2 := tx.Exec(delRelSql, nodeId, tenant, tenant)
		return exec2.Error
	})
}

const deleteNodesRec = `WITH nodes_to_delete AS (
    SELECT nodes.node_id
    FROM %s AS nodes
    JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
    WHERE ct.ancestor_id = ? AND nodes.tenant = ?
)
DELETE FROM %s
WHERE node_id IN (SELECT node_id FROM nodes_to_delete)
  AND tenant = ?;`

const deleteRelationsQuery = `WITH descendants AS (
	SELECT descendant_id FROM %s WHERE ancestor_id = ? AND tenant = ?
)
DELETE FROM %s
WHERE tenant = ?
  AND (
      descendant_id IN (SELECT descendant_id FROM descendants)
   OR ancestor_id  IN (SELECT descendant_id FROM descendants)
  );`

// GetNode loads a single item into the passed pointer
func (ct *Tree) GetNode(ctx context.Context, nodeID uint, tenant string, item any) error {

	if !hasNode(item) {
		return ErrItemIsNotTreeNode
	}
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	t := reflect.TypeOf(item)

	if t.Kind() != reflect.Ptr {
		return ErrItemNotPointerToStruct
	}

	sqlstr := fmt.Sprintf(getNodeQuery, ct.nodesTbl, ct.relationsTbl)
	result := ct.db.WithContext(ctx).Raw(sqlstr, nodeID, tenant).Scan(item)
	if result.Error != nil {
		return fmt.Errorf("failed to get node: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNodeNotFound
	}
	return nil
}

const getNodeQuery = `SELECT nodes.*, parent_rel.ancestor_id AS parent_id
FROM %s AS nodes
LEFT JOIN %s AS parent_rel
  ON parent_rel.descendant_id = nodes.node_id
  AND parent_rel.depth = 1
  AND parent_rel.tenant = nodes.tenant
WHERE nodes.node_id = ? AND nodes.tenant = ?
LIMIT 1`

// IsDescendant returns true if descendantID is a descendant of ancestorID in the given tenant.
func (ct *Tree) IsDescendant(ctx context.Context, ancestorID, descendantID uint, tenant string) (bool, error) {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}
	var count int64
	err = ct.db.WithContext(ctx).
		Table(ct.relationsTbl).
		Where("ancestor_id = ? AND descendant_id = ? AND depth > 0 AND tenant = ?", ancestorID, descendantID, tenant).
		Limit(1).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// IsChildOf checks if nodeID already has newParentID as its parent in the closure table.
func (ct *Tree) IsChildOf(ctx context.Context, nodeID, parentID uint, tenant string) (bool, error) {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}
	var count int64
	err = ct.db.WithContext(ctx).
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
// maxDepth determines the depth of the relationship to load: 0 means all children, 1 only direct children and so on.
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
	var tenantErr error
	tenant, tenantErr = validateTenant(tenant)
	if tenantErr != nil {
		return tenantErr
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
		newItem := reflect.New(elemType).Interface()
		if err := ct.db.ScanRows(rows, newItem); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		sliceVal.Set(reflect.Append(sliceVal, reflect.ValueOf(newItem).Elem()))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	return nil
}

const descendantsQuery = `SELECT nodes.*, parent_rel.ancestor_id AS parent_id
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
LEFT JOIN %s AS parent_rel
  ON parent_rel.descendant_id = nodes.node_id
  AND parent_rel.depth = 1
  AND parent_rel.tenant = nodes.tenant
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.tenant = ?
ORDER BY ct.depth;`

// DescendantIds behaves the same as Descendants but only returns the node IDs for the search query.
func (ct *Tree) DescendantIds(ctx context.Context, parent uint, maxDepth int, tenant string) ([]uint, error) {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	ids := []uint{}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}
	sqlstr := fmt.Sprintf(descendantsIDQuery, ct.nodesTbl, ct.relationsTbl)
	err = ct.db.WithContext(ctx).Raw(sqlstr, parent, maxDepth, tenant).Scan(&ids).Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch descendants: %w", err)
	}
	return ids, nil
}

const descendantsIDQuery = `SELECT nodes.node_id
FROM %s AS nodes
JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.tenant = ?
ORDER BY ct.depth;`

// absMaxDepth is limited by the max value of a 32-bit signed integer (matches the Depth column type)
const absMaxDepth = 2147483647

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
// maxDepth determines the depth of the relationship to load: 0 means all children, 1 only direct children and so on.
// tenant determines the tenant to be used
func (ct *Tree) TreeDescendants(ctx context.Context, parent uint, maxDepth int, tenant string, items any) (err error) {
	if err := validateItems(items); err != nil {
		return err
	}

	itemsVal := reflect.ValueOf(items)
	sliceVal := itemsVal.Elem()
	elemType := sliceVal.Type().Elem()

	var tenantErr error
	tenant, tenantErr = validateTenant(tenant)
	if tenantErr != nil {
		return tenantErr
	}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}

	sqlQuery := fmt.Sprintf(treeDescendantsQuery, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl, ct.nodesTbl)
	rows, err := ct.db.WithContext(ctx).Raw(sqlQuery, parent, tenant, tenant, tenant, tenant, maxDepth).Rows()
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
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row iteration error: %w", err)
	}

	return nodes, ancestorMap, nil
}

// toInt64 safely converts database integer values to int64, handling different driver types.
func toInt64(v any) (int64, bool) {
	if v == nil {
		return 0, true
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true //nolint:gosec // overflow guarded by check above
	case uint32:
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case float64:
		return int64(n), true
	case string:
		p, err := strconv.ParseInt(n, 10, 64)
		return p, err == nil
	case []byte:
		p, err := strconv.ParseInt(string(n), 10, 64)
		return p, err == nil
	}
	return 0, false
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
			n, ok := toInt64(value)
			if !ok {
				return reflect.Value{}, 0, 0, fmt.Errorf("cannot convert nodeID column value to int64: %T", value)
			}
			nodeID = n
		}
		if fieldName == ancestorIDMapKey {
			n, ok := toInt64(value)
			if !ok {
				return reflect.Value{}, 0, 0, fmt.Errorf("cannot convert ancestorID column value to int64: %T", value)
			}
			ancestorID = n
			// Also populate ParentId on the struct so TreeDescendants is
			// consistent with GetNode and Descendants.
			if pf := newElem.Elem().FieldByName("ParentId"); pf.IsValid() && pf.CanSet() && n >= 0 {
				pf.SetUint(uint64(n))
			}
		}

		fieldVal := newElem.Elem().FieldByName(fieldName)
		if !fieldVal.IsValid() || !fieldVal.CanSet() {
			continue
		}

		if value == nil {
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

	// Process in sorted key order to ensure deterministic children ordering
	keys := make([]int64, 0, len(nodes))
	for k := range nodes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, nodeID := range keys {
		node := nodes[nodeID]
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
	-- Base case: Start with direct children of the parent node
	SELECT
		nodes.*,
		ct.ancestor_id AS ancestor_id,
		1 AS cte_depth
	FROM %s AS nodes
	JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
	WHERE ct.ancestor_id = ? AND ct.depth = 1 AND nodes.tenant = ? AND ct.tenant = ?

	UNION ALL

	-- Recursive case: get immediate children (depth = 1 in closure table) of nodes in Tree
	SELECT
		nodes.*,
		t.node_id AS ancestor_id,
		t.cte_depth + 1 AS cte_depth
	FROM Tree AS t
	JOIN %s AS ct ON ct.ancestor_id = t.node_id AND ct.depth = 1 AND ct.tenant = ?
	JOIN %s AS nodes ON nodes.node_id = ct.descendant_id
	WHERE nodes.tenant = ? AND t.cte_depth < ?
	)
	SELECT  * FROM Tree ORDER BY cte_depth;`

// TreeDescendantsIds returns the tree structure of the descendants to the passed item
func (ct *Tree) TreeDescendantsIds(ctx context.Context, parent uint, maxDepth int, tenant string) (tree []*TreeNode, err error) {
	tenant, err = validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	nodeMap := make(map[uint]*TreeNode)

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}

	sqlstr := fmt.Sprintf(treeDescendantsIDQuery, ct.nodesTbl, ct.relationsTbl, ct.relationsTbl, ct.nodesTbl)
	rows, err := ct.db.WithContext(ctx).Raw(sqlstr, parent, tenant, tenant, tenant, tenant, maxDepth).Rows()
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
		err := rows.Scan(&node.NodeId, &node.ParentID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch tree descendants: %w", err)
		}
		nodeMap[node.NodeId] = &node
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// Sort keys for deterministic ordering (consistent with buildTreeHierarchy)
	keys := make([]uint, 0, len(nodeMap))
	for k := range nodeMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// Now, iterate over the node map and compose the tree
	var trees []*TreeNode
	for _, id := range keys {
		node := nodeMap[id]
		if par, exists := nodeMap[node.ParentID]; exists {
			par.Children = append(par.Children, node)
		} else {
			trees = append(trees, node)
		}
	}
	return trees, nil
}

type TreeNode struct {
	NodeId   uint        `json:"id"`
	ParentID uint        `json:"parentId"`
	Children []*TreeNode `json:"children"`
}

const treeDescendantsIDQuery = `WITH RECURSIVE Tree AS (
	-- Base case: Start with direct children of the parent node
	SELECT
		nodes.node_id,
		ct.ancestor_id AS ancestor_id,
		1 AS cte_depth
	FROM %s AS nodes
	JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
	WHERE ct.ancestor_id = ? AND ct.depth = 1 AND nodes.tenant = ? AND ct.tenant = ?

	UNION ALL

	-- Recursive case: get immediate children (depth = 1 in closure table) of nodes in Tree
	SELECT
		nodes.node_id,
		t.node_id AS ancestor_id,
		t.cte_depth + 1 AS cte_depth
	FROM Tree AS t
	JOIN %s AS ct ON ct.ancestor_id = t.node_id AND ct.depth = 1 AND ct.tenant = ?
	JOIN %s AS nodes ON nodes.node_id = ct.descendant_id
	WHERE nodes.tenant = ? AND t.cte_depth < ?
	)
	SELECT  Tree.node_id, Tree.ancestor_id FROM Tree ORDER BY cte_depth;`

func SortTree(nodes []*TreeNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeId < nodes[j].NodeId
	})
	for _, node := range nodes {
		SortTree(node.Children)
	}
}
