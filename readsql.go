package closuretree

import "strings"

// sqlBuf collects SQL text and its bound arguments in the order they appear.
type sqlBuf struct {
	b    strings.Builder
	args []any
}

func (s *sqlBuf) add(sql string, args ...any) {
	s.b.WriteString(sql)
	s.args = append(s.args, args...)
}

// quoteCol returns alias.column quoted with the dialect's quoting.
func (ct *Tree) quoteCol(alias, column string) string {
	var b strings.Builder
	ct.db.QuoteTo(&b, alias+"."+column)
	return b.String()
}

// selectList returns the quoted, comma-separated columns qualified by alias,
// e.g. `"nodes"."node_id", "nodes"."name"`.
func (ct *Tree) selectList(alias string, columns []string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = ct.quoteCol(alias, c)
	}
	return strings.Join(parts, ", ")
}

// matchSQL renders conds as one parenthesised predicate over alias, joined with AND.
// It returns "" and nil when conds is empty.
func (ct *Tree) matchSQL(alias string, conds []resolvedCond) (string, []any) {
	if len(conds) == 0 {
		return "", nil
	}
	var s sqlBuf
	s.b.WriteString("(")
	for i, c := range conds {
		if i > 0 {
			s.b.WriteString(" AND ")
		}
		col := ct.quoteCol(alias, c.column)
		switch c.op {
		case opEq:
			s.add(col+" = ?", c.values[0])
		case opIsNull:
			s.add(col + " IS NULL")
		case opNotNull:
			s.add(col + " IS NOT NULL")
		case opIn:
			if len(c.values) == 0 {
				s.add("1 = 0")
				continue
			}
			s.add(col+" IN ?", c.values)
		}
	}
	s.b.WriteString(")")
	return s.b.String(), s.args
}

// notMatchSQL renders "the predicate is not true", which includes NULL:
// CASE WHEN <matchSQL> THEN 0 ELSE 1 END = 1. Only called with at least one cond.
func (ct *Tree) notMatchSQL(alias string, conds []resolvedCond) (string, []any) {
	sql, args := ct.matchSQL(alias, conds)
	return "CASE WHEN " + sql + " THEN 0 ELSE 1 END = 1", args
}

// getNodeSQL selects one node by id, with its parent computed from the closure rows.
func (ct *Tree) getNodeSQL(p *readPlan, nodeID uint, tenant string) (string, []any) {
	var s sqlBuf
	s.add("SELECT " + ct.selectList("nodes", p.columns) + ", parent_rel.ancestor_id AS parent_id\n")
	s.add("FROM " + ct.nodesTbl + " AS nodes\n")
	s.add("LEFT JOIN " + ct.relationsTbl + " AS parent_rel\n")
	s.add("  ON parent_rel.descendant_id = nodes.node_id\n")
	s.add("  AND parent_rel.depth = 1\n")
	s.add("  AND parent_rel.tenant = nodes.tenant\n")
	s.add("WHERE nodes.node_id = ? AND nodes.tenant = ?", nodeID, tenant)
	if filter, fargs := ct.matchSQL("nodes", p.conds); filter != "" {
		s.add(" AND "+filter, fargs...)
	}
	s.add("\nLIMIT 1")
	return s.b.String(), s.args
}

// findChildSQL selects the first direct child (depth 1) of parentID that satisfies matchSQL, the
// caller's match conditions over the alias nodes. child_rel.ancestor_id is the direct parent id (0
// for a root node), named parent_id so the read-only Node.ParentId is filled as in GetNode.
func (ct *Tree) findChildSQL(p *readPlan, parentID uint, tenant, matchSQL string, matchArgs []any) (string, []any) {
	var s sqlBuf
	s.add("SELECT " + ct.selectList("nodes", p.columns) + ", child_rel.ancestor_id AS parent_id\n")
	s.add("FROM " + ct.nodesTbl + " AS nodes\n")
	s.add("JOIN " + ct.relationsTbl + " AS child_rel\n")
	s.add("  ON child_rel.descendant_id = nodes.node_id\n")
	s.add("  AND child_rel.depth = 1\n")
	s.add("  AND child_rel.tenant = nodes.tenant\n")
	s.add("WHERE child_rel.ancestor_id = ? AND nodes.tenant = ?", parentID, tenant)
	s.add(" AND "+matchSQL, matchArgs...)
	if filter, fargs := ct.matchSQL("nodes", p.conds); filter != "" {
		s.add(" AND "+filter, fargs...)
	}
	s.add("\nORDER BY nodes.sort_order ASC, nodes.node_id ASC\nLIMIT 1")
	return s.b.String(), s.args
}

// descendantsSQL selects the descendants of parent down to maxDepth levels, ordered by depth, then
// sort_order and node id. With idsOnly it selects only node_id; otherwise the plan's columns and
// the parent computed from the closure rows.
func (ct *Tree) descendantsSQL(p *readPlan, idsOnly bool, parent uint, maxDepth int, tenant string) (string, []any) {
	var s sqlBuf
	if idsOnly {
		s.add("SELECT nodes.node_id\n")
	} else {
		s.add("SELECT " + ct.selectList("nodes", p.columns) + ", parent_rel.ancestor_id AS parent_id\n")
	}
	s.add("FROM " + ct.nodesTbl + " AS nodes\n")
	s.add("JOIN " + ct.relationsTbl + " AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant\n")
	if !idsOnly {
		s.add("LEFT JOIN " + ct.relationsTbl + " AS parent_rel\n")
		s.add("  ON parent_rel.descendant_id = nodes.node_id\n")
		s.add("  AND parent_rel.depth = 1\n")
		s.add("  AND parent_rel.tenant = nodes.tenant\n")
	}
	s.add("WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ? AND nodes.tenant = ?", parent, maxDepth, tenant)
	if len(p.conds) > 0 {
		if p.perNode {
			filter, fargs := ct.matchSQL("nodes", p.conds)
			s.add("\n  AND "+filter, fargs...)
		} else {
			// prune: reject a node when any node on the path from the requested parent down to it,
			// both ends included, does not match. The virtual root has no node row and always passes.
			notMatch, nargs := ct.notMatchSQL("mid", p.conds)
			s.add("\n  AND NOT EXISTS (\n")
			s.add("    SELECT 1\n")
			s.add("    FROM " + ct.relationsTbl + " AS up\n")
			s.add("    JOIN " + ct.relationsTbl + " AS down ON down.ancestor_id = up.descendant_id AND down.tenant = up.tenant\n")
			s.add("    JOIN " + ct.nodesTbl + " AS mid ON mid.node_id = up.descendant_id AND mid.tenant = up.tenant\n")
			s.add("    WHERE up.ancestor_id = ? AND up.tenant = ?\n", parent, tenant)
			s.add("      AND down.descendant_id = nodes.node_id\n")
			s.add("      AND "+notMatch+"\n  )", nargs...)
		}
	}
	s.add("\nORDER BY ct.depth, nodes.sort_order ASC, nodes.node_id ASC;")
	return s.b.String(), s.args
}

// treeDescendantsSQL walks the subtree under parent with a recursive query, one level per step,
// down to maxDepth levels. Each row carries its direct parent: as parent_id, so gorm fills
// Node.ParentId, or as ancestor_id with idsOnly, which selects only node_id and sort_order. The
// level counter is cte_depth, not depth, so it cannot shadow a depth column of the model.
func (ct *Tree) treeDescendantsSQL(p *readPlan, idsOnly bool, parent uint, maxDepth int, tenant string) (string, []any) {
	cols, parentAlias := "nodes.node_id,\n\t\tnodes.sort_order", "ancestor_id"
	if !idsOnly {
		cols, parentAlias = ct.selectList("nodes", p.columns), colParentID
	}
	var s sqlBuf
	s.add("WITH RECURSIVE Tree AS (\n")
	s.add("\t-- Base case: Start with direct children of the parent node\n")
	s.add("\tSELECT\n\t\t" + cols + ",\n")
	s.add("\t\tct.ancestor_id AS " + parentAlias + ",\n")
	s.add("\t\t1 AS cte_depth\n")
	s.add("\tFROM " + ct.nodesTbl + " AS nodes\n")
	s.add("\tJOIN " + ct.relationsTbl + " AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant\n")
	s.add("\tWHERE ct.ancestor_id = ? AND ct.depth = 1 AND nodes.tenant = ? AND ct.tenant = ?", parent, tenant, tenant)
	s.add("\n\n\tUNION ALL\n\n")
	s.add("\t-- Recursive case: get immediate children (depth = 1 in closure table) of nodes in Tree\n")
	s.add("\tSELECT\n\t\t" + cols + ",\n")
	s.add("\t\tt.node_id AS " + parentAlias + ",\n")
	s.add("\t\tt.cte_depth + 1 AS cte_depth\n")
	s.add("\tFROM Tree AS t\n")
	s.add("\tJOIN "+ct.relationsTbl+" AS ct ON ct.ancestor_id = t.node_id AND ct.depth = 1 AND ct.tenant = ?\n", tenant)
	s.add("\tJOIN " + ct.nodesTbl + " AS nodes ON nodes.node_id = ct.descendant_id\n")
	s.add("\tWHERE nodes.tenant = ? AND t.cte_depth < ?", tenant, maxDepth)
	s.add("\n\t)\n")
	if idsOnly {
		s.add("\tSELECT Tree.node_id, Tree.ancestor_id, Tree.sort_order FROM Tree ORDER BY cte_depth;")
	} else {
		s.add("\tSELECT * FROM Tree ORDER BY cte_depth;")
	}
	return s.b.String(), s.args
}
