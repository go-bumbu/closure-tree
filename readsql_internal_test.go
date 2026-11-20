package closuretree

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReadSQLFragments(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	q := func(s string) string { // the dialect's quoting, as the existing internal test does it
		var b strings.Builder
		ct.db.QuoteTo(&b, s)
		return b.String()
	}

	if got, want := ct.selectList("nodes", []string{"node_id", "name"}), q("nodes.node_id")+", "+q("nodes.name"); got != want {
		t.Errorf("selectList: want %q, got %q", want, got)
	}
	if got := ct.selectList("nodes", nil); got != "nodes.*" {
		t.Errorf("selectList without columns: want every column, got %q", got)
	}

	conds := []resolvedCond{
		{"kind", opEq, []any{"doc"}},
		{"archived_at", opIsNull, nil},
		{"note", opNotNull, nil},
		{"name", opIn, []any{"a", "b"}},
	}
	wantSQL := "(" + q("n.kind") + " = ? AND " + q("n.archived_at") + " IS NULL AND " +
		q("n.note") + " IS NOT NULL AND " + q("n.name") + " IN ?)"
	wantArgs := []any{"doc", []any{"a", "b"}}

	sql, args := ct.matchSQL("n", conds)
	if sql != wantSQL || !cmp.Equal(wantArgs, args) {
		t.Errorf("matchSQL:\nwant %q %v\ngot  %q %v", wantSQL, wantArgs, sql, args)
	}
	sql, args = ct.notMatchSQL("n", conds)
	if sql != "CASE WHEN "+wantSQL+" THEN 0 ELSE 1 END = 1" || !cmp.Equal(wantArgs, args) {
		t.Errorf("notMatchSQL: got %q %v", sql, args)
	}
	if sql, args := ct.matchSQL("n", nil); sql != "" || args != nil {
		t.Errorf("no conds: got %q %v", sql, args)
	}
	if sql, args := ct.matchSQL("n", []resolvedCond{{"name", opIn, nil}}); sql != "(1 = 0)" || args != nil {
		t.Errorf("In without values must match nothing: got %q %v", sql, args)
	}

	var buf sqlBuf
	buf.add("a = ?", 1)
	buf.add(" AND b IN ?", []any{2, 3})
	if buf.b.String() != "a = ? AND b IN ?" || !cmp.Equal([]any{1, []any{2, 3}}, buf.args) {
		t.Errorf("sqlBuf: %q %v", buf.b.String(), buf.args)
	}
}
