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
