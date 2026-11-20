package closuretree

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

// GetNode loads a single item into the passed pointer, scanning straight into it.
//
// GetNode honours the read options Filter, Unfiltered and Columns; PerNode is ignored. A node that
// does not match the filter returns ErrNodeNotFound. Column names of every option are validated,
// so an unknown one returns ErrUnknownColumn.
func (ct *Tree) GetNode(ctx context.Context, nodeID uint, tenant string, item any, opts ...ReadOption) error {

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

	p, err := ct.planRead(opts)
	if err != nil {
		return err
	}
	sqlstr, args := ct.getNodeSQL(p, nodeID, tenant)
	result := ct.db.WithContext(ctx).Raw(sqlstr, args...).Scan(item)
	if result.Error != nil {
		return fmt.Errorf("failed to get node: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNodeNotFound
	}
	return nil
}

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
//
// Each row is scanned with gorm's scanner into the Tree's model and then copied into the element
// type by Go field name. The element type may be the model, a smaller struct, or a struct that
// wraps the model. A same-named field of another type is assigned, converted or dereferenced; if
// that is not possible the read returns "cannot assign type ... to field ..." before querying.
//
// Descendants honours the read options Filter, Unfiltered, PerNode and Columns. By default a node
// is returned only if it and every node between parent and it match the filter; with PerNode it
// is returned when it matches. In the default mode a parent that is a real node and does not
// match gives an empty result; parent 0 always matches. Column names of every option are
// validated, so an unknown one returns ErrUnknownColumn.
func (ct *Tree) Descendants(ctx context.Context, parent uint, maxDepth int, tenant string, items any, opts ...ReadOption) (err error) {
	sliceVal, err := pointerToSlice(items)
	if err != nil {
		return err
	}
	// Descendants intentionally accepts both value ([]T) and pointer ([]*T) element slices;
	// TreeDescendants, by contrast, requires []*T because it links child pointers into a tree.
	// Every row becomes a new T (see scanEach).
	elemType := sliceVal.Type().Elem()
	elemIsPtr := elemType.Kind() == reflect.Pointer
	structType := elemType
	if elemIsPtr {
		structType = elemType.Elem()
	}

	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return err
	}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}
	sqlstr, args := ct.descendantsSQL(p, false, parent, maxDepth, tenant)
	return ct.scanEach(ctx, sqlstr, args, structType, "failed to execute query", func(elem, _ reflect.Value) {
		if !elemIsPtr {
			elem = elem.Elem()
		}
		sliceVal.Set(reflect.Append(sliceVal, elem))
	})
}

// DescendantIds behaves the same as Descendants but only returns the node IDs for the search query.
//
// DescendantIds honours the read options Filter, Unfiltered and PerNode, with the meaning they
// have in Descendants; Columns is ignored. Column names of every option are validated, so an
// unknown one returns ErrUnknownColumn.
func (ct *Tree) DescendantIds(ctx context.Context, parent uint, maxDepth int, tenant string, opts ...ReadOption) ([]uint, error) {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return nil, err
	}
	ids := []uint{}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}
	sqlstr, args := ct.descendantsSQL(p, true, parent, maxDepth, tenant)
	err = ct.db.WithContext(ctx).Raw(sqlstr, args...).Scan(&ids).Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch descendants: %w", err)
	}
	return ids, nil
}

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
// Each row is scanned with gorm's scanner into the Tree's model and then copied into the element
// type by Go field name, as in Descendants. Children are linked through the node id and parent id
// of the scanned row, so the element type does not have to embed Node.
//
// TreeDescendants honours the read options Filter, Unfiltered and Columns; PerNode is ignored. A
// node that does not match the filter is left out with its whole subtree; its children are never
// re-parented. A parent that is a real node and does not match gives an empty result; parent 0
// always matches. Column names of every option are validated, so an unknown one returns
// ErrUnknownColumn.
//
// Note: this uses a recursive CTE. On MySQL 8 the recursion is bounded by cte_max_recursion_depth
// (default 1000), so loading a subtree deeper than that many levels fails with error 3636; SQLite
// and PostgreSQL have no such default ceiling. Set maxDepth, or raise cte_max_recursion_depth for
// the session, if you need deeper trees on MySQL.
func (ct *Tree) TreeDescendants(ctx context.Context, parent uint, maxDepth int, tenant string, items any, opts ...ReadOption) (err error) {
	if err := validateItems(items); err != nil {
		return err
	}

	sliceVal := reflect.ValueOf(items).Elem()
	elemType := sliceVal.Type().Elem()
	if f, ok := elemType.Elem().FieldByName("Children"); ok && f.Type != reflect.SliceOf(elemType) {
		return fmt.Errorf("cannot read a tree into %s: its field Children is %s, not %s", elemType.Elem(), f.Type, reflect.SliceOf(elemType))
	}

	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return err
	}

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}

	sqlstr, args := ct.treeDescendantsSQL(p, false, parent, maxDepth, tenant)
	var entries []treeEntry
	err = ct.scanEach(ctx, sqlstr, args, elemType.Elem(), "failed to fetch tree descendants", func(elem, row reflect.Value) {
		entries = append(entries, treeEntry{elem: elem, node: nodeOf(row)})
	})
	if err != nil {
		return err
	}

	for _, node := range linkTree(entries) {
		sliceVal.Set(reflect.Append(sliceVal, node))
	}
	return nil
}

// TreeDescendantsIds returns the tree structure of the descendants to the passed item.
//
// TreeDescendantsIds honours the read options Filter and Unfiltered, with the meaning they have in
// TreeDescendants; PerNode and Columns are ignored. Column names of every option are validated, so
// an unknown one returns ErrUnknownColumn.
//
// Note: like TreeDescendants this uses a recursive CTE, so on MySQL 8 it is bounded by
// cte_max_recursion_depth (default 1000, error 3636 beyond it); SQLite and PostgreSQL are not.
func (ct *Tree) TreeDescendantsIds(ctx context.Context, parent uint, maxDepth int, tenant string, opts ...ReadOption) (tree []*TreeNode, err error) {
	tenant, err = validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return nil, err
	}
	nodeMap := make(map[uint]*TreeNode)

	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}

	sqlstr, args := ct.treeDescendantsSQL(p, true, parent, maxDepth, tenant)
	rows, err := ct.db.WithContext(ctx).Raw(sqlstr, args...).Rows()
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
	// during assembly — consistent with the strategy in linkTree.
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
