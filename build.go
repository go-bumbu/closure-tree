package closuretree

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	ErrLinksMismatch = errors.New("the links do not match the tenant's nodes")
	ErrOrphanLink    = errors.New("a link names a parent that is not a node of the tenant")
	ErrLinkCycle     = errors.New("the links contain a cycle")
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
		return nil, idsError(ErrLinkCycle, "nodes on a cycle", cyclic)
	}
	return plan, nil
}

// findCycleNodes walks up from each node with a three-state mark (unseen, in progress, done).
// Meeting an in-progress node means that node is on a cycle; every such node is returned once.
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
