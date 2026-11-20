package closuretree

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

// ParentLink is one node's place in the tree. ParentID 0 means a root node.
type ParentLink struct {
	NodeID   uint
	ParentID uint
}

type buildConfig struct {
	rebuild       bool
	orphansToRoot bool
}

// BuildOption changes how BuildFromParents works.
type BuildOption func(*buildConfig)

// Rebuild deletes the tenant's closure and meta rows before building.
func Rebuild() BuildOption { return func(c *buildConfig) { c.rebuild = true } }

// OrphansToRoot makes a node a root when its parent is not in the input.
func OrphansToRoot() BuildOption { return func(c *buildConfig) { c.orphansToRoot = true } }

var (
	// ErrLinksMismatch is returned by BuildFromParents when a node of the tenant has no link, a link
	// names a node that does not exist, a node is linked twice, or a link has node id 0. The message
	// lists the ids.
	ErrLinksMismatch = errors.New("the links do not match the tenant's nodes")
	// ErrOrphanLink is returned by BuildFromParents when a link's parent is not a node of the
	// tenant, unless the OrphansToRoot option is given.
	ErrOrphanLink = errors.New("a link names a parent that is not a node of the tenant")
	// ErrLinkCycle is returned by BuildFromParents when the links form a cycle, including a node
	// that is its own parent. The message names one node of each cycle.
	ErrLinkCycle = errors.New("the links contain a cycle")
	// ErrClosureExists is returned by BuildFromParents when the tenant already has closure rows,
	// unless the Rebuild option is given.
	ErrClosureExists = errors.New("the tenant already has closure rows; use Rebuild")
)

// maxReportedIDs is the most ids an error message lists.
const maxReportedIDs = 10

// buildPlan is the validated input. children[0] holds the roots. Every slice keeps input order.
type buildPlan struct {
	parent   map[uint]uint
	children map[uint][]uint
}

// idsError wraps sentinel with a message that lists at most maxReportedIDs ids.
func idsError(sentinel error, what string, ids []uint) error {
	shown := ids
	if len(shown) > maxReportedIDs {
		shown = shown[:maxReportedIDs]
	}
	parts := make([]string, len(shown))
	for i, id := range shown {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	list := strings.Join(parts, ", ")
	if more := len(ids) - len(shown); more > 0 {
		list += fmt.Sprintf(" and %d more", more)
	}
	return fmt.Errorf("%w: %s: %s", sentinel, what, list)
}

// planLinks checks links against nodeIDs (the tenant's node ids) and returns the plan.
func planLinks(nodeIDs []uint, links []ParentLink, orphansToRoot bool) (*buildPlan, error) {
	known := make(map[uint]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		known[id] = true
	}

	var zero, duplicate, unknown []uint
	seen := make(map[uint]bool, len(links))
	for _, l := range links {
		switch {
		case l.NodeID == 0:
			zero = append(zero, l.NodeID)
		case seen[l.NodeID]:
			duplicate = append(duplicate, l.NodeID)
		case !known[l.NodeID]:
			unknown = append(unknown, l.NodeID)
		}
		seen[l.NodeID] = true
	}
	if len(zero) > 0 {
		return nil, idsError(ErrLinksMismatch, "link with node id", zero)
	}
	if len(duplicate) > 0 {
		return nil, idsError(ErrLinksMismatch, "nodes linked more than once", duplicate)
	}
	if len(unknown) > 0 {
		return nil, idsError(ErrLinksMismatch, "linked nodes that do not exist", unknown)
	}
	var missing []uint
	for _, id := range nodeIDs {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return nil, idsError(ErrLinksMismatch, "nodes without a link", missing)
	}

	plan := &buildPlan{
		parent:   make(map[uint]uint, len(links)),
		children: make(map[uint][]uint),
	}
	var orphans []uint
	for _, l := range links {
		parent := l.ParentID
		if parent != 0 && !known[parent] {
			if !orphansToRoot {
				orphans = append(orphans, l.NodeID)
				continue
			}
			parent = 0
		}
		plan.parent[l.NodeID] = parent
		plan.children[parent] = append(plan.children[parent], l.NodeID)
	}
	if len(orphans) > 0 {
		return nil, idsError(ErrOrphanLink, "nodes whose parent does not exist", orphans)
	}

	if cyclic := findCycleNodes(nodeIDs, plan.parent); len(cyclic) > 0 {
		return nil, idsError(ErrLinkCycle, "one node of each cycle", cyclic)
	}
	return plan, nil
}

// findCycleNodes walks up from each node with a three-state mark (unseen, in progress, done).
// Meeting an in-progress node means that node is on a cycle. A cycle is met only by the first walk
// that enters it, so one node of each cycle is returned.
func findCycleNodes(nodeIDs []uint, parent map[uint]uint) []uint {
	const (
		inProgress = 1
		done       = 2
	)
	state := make(map[uint]int, len(nodeIDs))
	var cyclic []uint
	for _, start := range nodeIDs {
		var path []uint
		cur := start
		for cur != 0 && state[cur] == 0 {
			state[cur] = inProgress
			path = append(path, cur)
			cur = parent[cur]
		}
		if cur != 0 && state[cur] == inProgress {
			cyclic = append(cyclic, cur)
		}
		for _, id := range path {
			state[id] = done
		}
	}
	return cyclic
}

// closureBatchRows is the number of closure rows BuildFromParents inserts per statement, with four
// bound values each.
const closureBatchRows = 1000

// sortBatchRows is the number of nodes whose sort_order one statement of BuildFromParents sets,
// with three bound values each: two in the CASE and one in the IN list.
const sortBatchRows = 500

// BuildFromParents writes the closure rows of the tenant's existing node rows from one ParentLink
// per node, and sets their sort_order. It is meant for adopting a table whose rows were written
// without closure-tree; it never inserts or deletes node rows.
//
// links must hold exactly one link per node of the tenant; otherwise it returns ErrLinksMismatch
// naming the offending ids. Siblings are ordered as their links appear in links: their sort_order
// becomes 10, 20, 30, ... A parent that is not a node of the tenant returns ErrOrphanLink, unless
// OrphansToRoot makes such a node a root. A cycle, including a node that is its own parent, returns
// ErrLinkCycle. If the tenant already has closure rows it returns ErrClosureExists, unless Rebuild
// is given, which deletes them first. Every check runs before the first write, so an error leaves
// the tables as they were. The tenant's sort-order meta rows are deleted, the state Renormalize
// leaves; the next insertion recreates them. A nil option is ignored.
//
// It runs under the tenant lock in one transaction, or inside the caller's through WithTx. It
// returns ErrEmptyTenant if tenant is empty.
func (ct *Tree) BuildFromParents(ctx context.Context, tenant string, links []ParentLink, opts ...BuildOption) error {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return err
	}
	var cfg buildConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {
		var nodeIDs []uint
		if err := tx.Table(ct.nodesTbl).Where("tenant = ?", tenant).Order("node_id").
			Pluck("node_id", &nodeIDs).Error; err != nil {
			return fmt.Errorf("buildFromParents: failed to read the node ids: %w", err)
		}
		plan, err := planLinks(nodeIDs, links, cfg.orphansToRoot)
		if err != nil {
			return err
		}

		var found []int
		if err := tx.Raw(fmt.Sprintf("SELECT 1 FROM %s WHERE tenant = ? LIMIT 1", ct.relationsTbl), tenant).
			Scan(&found).Error; err != nil {
			return fmt.Errorf("buildFromParents: failed to look for closure rows: %w", err)
		}
		if len(found) > 0 && !cfg.rebuild {
			return ErrClosureExists
		}
		for _, tbl := range []string{ct.relationsTbl, ct.metaTbl} {
			if err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE tenant = ?", tbl), tenant).Error; err != nil {
				return fmt.Errorf("buildFromParents: failed to delete from %s: %w", tbl, err)
			}
		}
		if err := ct.writeClosure(tx, tenant, plan); err != nil {
			return err
		}
		return ct.writeSortOrders(tx, tenant, plan)
	})
}

// writeClosure walks plan from the roots, depth first and without recursion, and inserts each
// node's closure rows: its depth-0 self row, one row per ancestor, and the virtual-root row. The
// rows are inserted closureBatchRows per statement, so they are never all in memory. path holds the
// ids from the current node's root down to the node itself, so path[i] is its ancestor at distance
// len(path)-1-i.
func (ct *Tree) writeClosure(tx *gorm.DB, tenant string, plan *buildPlan) error {
	type step struct {
		id    uint
		level int // 1 for a root
	}
	push := func(stack []step, children []uint, level int) []step {
		// pushed last to first, so the nodes are popped in input order
		for i := len(children) - 1; i >= 0; i-- {
			stack = append(stack, step{id: children[i], level: level})
		}
		return stack
	}

	rels := make([]closureTree, 0, closureBatchRows)
	flush := func() error {
		if len(rels) == 0 {
			return nil
		}
		if err := tx.Table(ct.relationsTbl).Create(&rels).Error; err != nil {
			return fmt.Errorf("buildFromParents: failed to insert closure rows: %w", err)
		}
		rels = rels[:0]
		return nil
	}
	add := func(ancestor, descendant uint, depth int) error {
		rels = append(rels, closureTree{AncestorID: ancestor, DescendantID: descendant, Tenant: tenant, Depth: depth})
		if len(rels) < closureBatchRows {
			return nil
		}
		return flush()
	}

	stack := push(nil, plan.children[0], 1)
	var path []uint
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		path = append(path[:cur.level-1], cur.id)
		for i, ancestor := range path {
			if err := add(ancestor, cur.id, len(path)-1-i); err != nil {
				return err
			}
		}
		if err := add(0, cur.id, cur.level); err != nil {
			return err
		}
		stack = push(stack, plan.children[cur.id], cur.level+1)
	}
	return flush()
}

// writeSortOrders sets sort_order to 10, 20, 30, ... in each sibling group of plan, sortBatchRows
// nodes per statement:
//
//	UPDATE nodes SET sort_order = CASE node_id WHEN ? THEN ? + 0.0 ... END
//	WHERE tenant = ? AND node_id BETWEEN ? AND ? AND node_id IN ?
//
// Each result is written "? + 0.0": PostgreSQL gives a bare bound CASE result the type text, which
// it does not assign to the numeric column; the addition makes it numeric. SQLite and MySQL accept
// the same form.
//
// The nodes are written in id order and the BETWEEN bound, implied by the IN list, is the batch's
// id range. Right after a bulk load PostgreSQL has no statistics for the node table, estimates
// "tenant = ?" as a few rows, and scans all the tenant's rows testing each against the IN list,
// which makes the whole build quadratic. The bound lets it scan only the batch's range of the
// (tenant, node_id) index.
func (ct *Tree) writeSortOrders(tx *gorm.DB, tenant string, plan *buildPlan) error {
	type sortOrder struct {
		id    uint
		order float64
	}
	orders := make([]sortOrder, 0, len(plan.parent))
	for _, children := range plan.children {
		for i, id := range children {
			orders = append(orders, sortOrder{id: id, order: float64((i + 1) * 10)})
		}
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].id < orders[j].id })

	for start := 0; start < len(orders); start += sortBatchRows {
		batch := orders[start:min(start+sortBatchRows, len(orders))]
		var b strings.Builder
		args := make([]any, 0, 2*len(batch)+4)
		ids := make([]uint, len(batch))
		fmt.Fprintf(&b, "UPDATE %s SET sort_order = CASE node_id", ct.nodesTbl)
		for i, o := range batch {
			b.WriteString(" WHEN ? THEN ? + 0.0")
			args = append(args, o.id, o.order)
			ids[i] = o.id
		}
		b.WriteString(" END WHERE tenant = ? AND node_id BETWEEN ? AND ? AND node_id IN ?")
		args = append(args, tenant, ids[0], ids[len(ids)-1], ids)
		if err := tx.Exec(b.String(), args...).Error; err != nil {
			return fmt.Errorf("buildFromParents: failed to set sort_order: %w", err)
		}
	}
	return nil
}
