package closuretree

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

// GetNode loads a single item into the passed pointer
func (ct *Tree) GetNode(ctx context.Context, nodeID uint, tenant string, item any) error {

	if err := checkItem(item); err != nil {
		return err
	}
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	t := reflect.TypeOf(item)

	if t.Kind() != reflect.Pointer {
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
func (ct *Tree) Descendants(ctx context.Context, parent uint, maxDepth int, tenant string, items any) (err error) {
	sliceVal, err := pointerToSlice(items)
	if err != nil {
		return err
	}
	// Descendants intentionally accepts both value ([]T) and pointer ([]*T) element slices;
	// TreeDescendants, by contrast, requires []*T because it links child pointers into a tree.
	// structPtrType is always *T so a single mapRowToStruct call serves both cases.
	elemType := sliceVal.Type().Elem()
	elemIsPtr := elemType.Kind() == reflect.Pointer
	structPtrType := elemType
	if !elemIsPtr {
		structPtrType = reflect.PointerTo(elemType)
	}

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

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to read column names: %w", err)
	}

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		// Route through the same mapRowToStruct coercion TreeDescendants uses, so both read
		// paths agree on driver type conversions (e.g. PostgreSQL []byte numerics).
		elem, _, _, mapErr := mapRowToStruct(values, columns, ct.col2FieldMap, structPtrType)
		if mapErr != nil {
			return mapErr
		}
		if elemIsPtr {
			sliceVal.Set(reflect.Append(sliceVal, elem))
		} else {
			sliceVal.Set(reflect.Append(sliceVal, elem.Elem()))
		}
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
ORDER BY ct.depth, nodes.sort_order ASC, nodes.node_id ASC;`

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
ORDER BY ct.depth, nodes.sort_order ASC, nodes.node_id ASC;`

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
//
// Note: this uses a recursive CTE. On MySQL 8 the recursion is bounded by cte_max_recursion_depth
// (default 1000), so loading a subtree deeper than that many levels fails with error 3636; SQLite
// and PostgreSQL have no such default ceiling. Set maxDepth, or raise cte_max_recursion_depth for
// the session, if you need deeper trees on MySQL.
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

// TreeDescendantsIds returns the tree structure of the descendants to the passed item.
//
// Note: like TreeDescendants this uses a recursive CTE, so on MySQL 8 it is bounded by
// cte_max_recursion_depth (default 1000, error 3636 beyond it); SQLite and PostgreSQL are not.
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
		err := rows.Scan(&node.NodeId, &node.ParentID, &node.SortOrder)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch tree descendants: %w", err)
		}
		nodeMap[node.NodeId] = &node
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// Sort keys by (SortOrder ASC, NodeId ASC) so children are appended in order
	// during assembly — consistent with the strategy in buildTreeHierarchy.
	keys := make([]uint, 0, len(nodeMap))
	for k := range nodeMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ni := nodeMap[keys[i]]
		nj := nodeMap[keys[j]]
		if ni.SortOrder != nj.SortOrder {
			return ni.SortOrder < nj.SortOrder
		}
		return keys[i] < keys[j]
	})

	// Compose the tree; because keys are pre-sorted, each parent's Children slice
	// ends up in (SortOrder ASC, NodeId ASC) order without a second pass.
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
	NodeId    uint        `json:"id"`
	ParentID  uint        `json:"parentId"`
	SortOrder float64     `json:"sortOrder"`
	Children  []*TreeNode `json:"children"`
}

const treeDescendantsIDQuery = `WITH RECURSIVE Tree AS (
	-- Base case: Start with direct children of the parent node
	SELECT
		nodes.node_id,
		nodes.sort_order,
		ct.ancestor_id AS ancestor_id,
		1 AS cte_depth
	FROM %s AS nodes
	JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
	WHERE ct.ancestor_id = ? AND ct.depth = 1 AND nodes.tenant = ? AND ct.tenant = ?

	UNION ALL

	-- Recursive case: get immediate children (depth = 1 in closure table) of nodes in Tree
	SELECT
		nodes.node_id,
		nodes.sort_order,
		t.node_id AS ancestor_id,
		t.cte_depth + 1 AS cte_depth
	FROM Tree AS t
	JOIN %s AS ct ON ct.ancestor_id = t.node_id AND ct.depth = 1 AND ct.tenant = ?
	JOIN %s AS nodes ON nodes.node_id = ct.descendant_id
	WHERE nodes.tenant = ? AND t.cte_depth < ?
	)
	SELECT  Tree.node_id, Tree.ancestor_id, Tree.sort_order FROM Tree ORDER BY cte_depth;`

func SortTree(nodes []*TreeNode) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].SortOrder != nodes[j].SortOrder {
			return nodes[i].SortOrder < nodes[j].SortOrder
		}
		return nodes[i].NodeId < nodes[j].NodeId
	})
	for _, node := range nodes {
		SortTree(node.Children)
	}
}
