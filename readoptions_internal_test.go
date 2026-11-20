package closuretree

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type optNode struct {
	Node
	Name       string
	Kind       string
	Hidden     bool
	Note       *string
	ArchivedAt *time.Time
	Computed   string `gorm:"->;-:migration"` // no column in the table
	Skipped    string `gorm:"-"`
}

var baseColumns = []string{"node_id", "tenant", "sort_order"}

func TestPlanReadDefaults(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	p, err := ct.planRead(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := &readPlan{columns: []string{"node_id", "tenant", "sort_order", "name", "kind", "hidden", "note", "archived_at"}}
	if diff := cmp.Diff(want, p, cmp.AllowUnexported(readPlan{}, resolvedCond{})); diff != "" {
		t.Errorf("default plan (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(want, ct.plainPlan(), cmp.AllowUnexported(readPlan{}, resolvedCond{})); diff != "" {
		t.Errorf("plain plan (-want +got):\n%s", diff)
	}
	// review focus 5: a nil option is ignored
	if _, err := ct.planRead([]ReadOption{nil, PerNode(), nil}); err != nil {
		t.Errorf("nil options: %v", err)
	}
}

func TestPlanReadColumns(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	good := []struct {
		names []string
		want  []string
	}{
		{[]string{"Name"}, append(append([]string{}, baseColumns...), "name")},                         // Go field name
		{[]string{"kind", "name", "kind"}, append(append([]string{}, baseColumns...), "kind", "name")}, // given order, no duplicates
		{[]string{"node_id", "SortOrder"}, baseColumns},                                                // the base columns are always there
	}
	for _, tc := range good {
		p, err := ct.planRead([]ReadOption{Columns(tc.names...)})
		if err != nil || !cmp.Equal(tc.want, p.columns) {
			t.Errorf("Columns(%v): got %v, err %v", tc.names, p, err)
		}
	}
	for _, bad := range []string{"nope", "parent_id", "ParentId", "Skipped", "computed"} {
		if _, err := ct.planRead([]ReadOption{Columns(bad)}); !errors.Is(err, ErrUnknownColumn) || !strings.Contains(err.Error(), bad) {
			t.Errorf("Columns(%q): want ErrUnknownColumn naming it, got %v", bad, err)
		}
	}
}

func TestPlanReadFilter(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	conds := func(opts ...ReadOption) []resolvedCond {
		t.Helper()
		p, err := ct.planRead(opts)
		if err != nil {
			t.Fatal(err)
		}
		return p.conds
	}
	eq := func(want, got []resolvedCond) {
		t.Helper()
		if diff := cmp.Diff(want, got, cmp.AllowUnexported(resolvedCond{})); diff != "" {
			t.Errorf("conds (-want +got):\n%s", diff)
		}
	}
	s := "x"
	eq([]resolvedCond{{"kind", opEq, []any{"doc"}}, {"archived_at", opIsNull, nil}},
		conds(Filter(Eq("Kind", "doc"), IsNull("archived_at"))))
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", nil))))            // nil is IS NULL
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", (*string)(nil))))) // so is a nil pointer
	eq([]resolvedCond{{"note", opEq, []any{"x"}}}, conds(Filter(Eq("note", &s))))          // a pointer is dereferenced
	eq([]resolvedCond{{"note", opNotNull, nil}, {"name", opIn, []any{"a", "b"}}}, conds(Filter(NotNull("note"), In("name", "a", "b"))))
	eq([]resolvedCond{{"name", opIn, nil}}, conds(Filter(In("name"))))
	eq([]resolvedCond{{"kind", opEq, []any{"doc"}}, {"hidden", opEq, []any{false}}}, // two Filter options are joined
		conds(Filter(Eq("kind", "doc")), Filter(Eq("hidden", false))))
	eq(nil, conds(Filter())) // review focus 4: an empty Filter is no filter
	if p, _ := ct.planRead([]ReadOption{PerNode()}); !p.perNode {
		t.Error("PerNode not recorded")
	}
	for _, bad := range []ReadOption{Filter(Eq("nope", 1)), Filter(IsNull("parent_id")), Filter(In("Skipped", 1))} {
		if _, err := ct.planRead([]ReadOption{bad}); !errors.Is(err, ErrUnknownColumn) {
			t.Errorf("want ErrUnknownColumn, got %v", err)
		}
	}
}

func TestWithFilterPrecedence(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	ft, err := ct.WithFilter(IsNull("archived_at"))
	if err != nil {
		t.Fatal(err)
	}
	conds := func(tr *Tree, opts ...ReadOption) []resolvedCond {
		t.Helper()
		p, err := tr.planRead(opts)
		if err != nil {
			t.Fatal(err)
		}
		return p.conds
	}
	def := []resolvedCond{{"archived_at", opIsNull, nil}}
	own := []resolvedCond{{"hidden", opEq, []any{true}}}
	opt := cmp.AllowUnexported(resolvedCond{})
	if !cmp.Equal(def, conds(ft), opt) {
		t.Error("the default filter is not applied")
	}
	if conds(ft, Unfiltered()) != nil {
		t.Error("Unfiltered must switch the default off")
	}
	if !cmp.Equal(own, conds(ft, Filter(Eq("hidden", true))), opt) {
		t.Error("a per-call Filter must replace the default, not add to it")
	}
	if !cmp.Equal(own, conds(ft, Unfiltered(), Filter(Eq("hidden", true))), opt) {
		t.Error("a per-call Filter still applies next to Unfiltered")
	}
	if conds(ct) != nil {
		t.Error("WithFilter changed the original Tree")
	}
	if ft.NodeTableName() != ct.NodeTableName() || ft.ClosureTableName() != ct.ClosureTableName() {
		t.Error("the copy lost its table names")
	}
	if bad, err := ct.WithFilter(Eq("nope", 1)); bad != nil || !errors.Is(err, ErrUnknownColumn) {
		t.Errorf("want nil and ErrUnknownColumn, got %v, %v", bad, err)
	}
}
