package closuretree

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
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
	want := &readPlan{} // nil columns: every column of the node table
	if diff := cmp.Diff(want, p, cmp.AllowUnexported(readPlan{}, resolvedCond{})); diff != "" {
		t.Errorf("default plan (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(want, ct.plainPlan(), cmp.AllowUnexported(readPlan{}, resolvedCond{})); diff != "" {
		t.Errorf("plain plan (-want +got):\n%s", diff)
	}
	// a nil option is ignored
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
	eq(nil, conds(Filter())) // an empty Filter is no filter
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
	if !cmp.Equal(def, conds(ft, Filter()), opt) {
		t.Error("an empty Filter must not switch the default off")
	}
	if conds(ft, Filter(), Unfiltered()) != nil {
		t.Error("an empty Filter next to Unfiltered must give no filter")
	}
	if !cmp.Equal(own, conds(ft, Filter(), Filter(Eq("hidden", true))), opt) {
		t.Error("an empty Filter next to a Filter with conds must not matter")
	}
	if conds(ct) != nil {
		t.Error("WithFilter changed the original Tree")
	}
	withFilterReplaces(t, ft, own, opt)
	if ft.NodeTableName() != ct.NodeTableName() || ft.ClosureTableName() != ct.ClosureTableName() {
		t.Error("the copy lost its table names")
	}
	if bad, err := ct.WithFilter(Eq("nope", 1)); bad != nil || !errors.Is(err, ErrUnknownColumn) {
		t.Errorf("want nil and ErrUnknownColumn, got %v, %v", bad, err)
	}
}

// withFilterReplaces checks that WithFilter on a Tree that has a default replaces it, and that
// without conds it keeps it.
func withFilterReplaces(t *testing.T, ft *Tree, own []resolvedCond, opt cmp.Option) {
	t.Helper()
	wf, err := ft.WithFilter(Eq("hidden", true))
	if err != nil {
		t.Fatal(err)
	}
	if p, err := wf.planRead(nil); err != nil || !cmp.Equal(own, p.conds, opt) {
		t.Errorf("WithFilter must replace the default filter: %v", err)
	}
	wf, err = ft.WithFilter()
	if err != nil {
		t.Fatal(err)
	}
	if p, err := wf.planRead(nil); err != nil || !cmp.Equal([]resolvedCond{{"archived_at", opIsNull, nil}}, p.conds, opt) {
		t.Errorf("WithFilter() must keep the default filter: %v", err)
	}
}

func TestInCopiesValues(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	vals := []any{"a", "b"}
	vt, err := ct.WithFilter(In("name", vals...))
	if err != nil {
		t.Fatal(err)
	}
	vals[0] = "changed"
	p, err := vt.planRead(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.conds) != 1 || p.conds[0].values[0] != "a" {
		t.Errorf("In must copy its values, got %v", p.conds)
	}
}

// valuerList is a slice type that binds as one value through driver.Valuer.
type valuerList []string

func (v valuerList) Value() (driver.Value, error) { return strings.Join(v, ","), nil }

// A filter value that gorm would expand into several placeholders, or render as SQL, is rejected
// before querying; a byte slice and a driver.Valuer bind as one value and are accepted.
func TestFilterRejectsValuesThatAreNotOneValue(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	words := []string{"a"}
	bad := map[string]Cond{
		"Eq with a slice":               Eq("name", []string{"a"}),
		"Eq with an array":              Eq("name", [2]string{"a", "b"}),
		"Eq with a pointer to a slice":  Eq("name", &words),
		"In with a slice":               In("name", "a", []any{"b"}),
		"In with a pointer to a slice":  In("name", &words),
		"Eq with an expression":         Eq("name", clause.Expr{SQL: "name"}),
		"In with an expression":         In("name", clause.Expr{SQL: "name"}),
		"Eq with a column":              Eq("name", clause.Column{Name: "kind"}),
		"Eq with a *gorm.DB":            Eq("name", ct.db),
		"In with a *gorm.DB":            In("name", ct.db.Table("x")),
		"Eq with a gorm expression ptr": Eq("name", &clause.Expr{SQL: "name"}),
	}
	for what, c := range bad {
		if _, err := ct.planRead([]ReadOption{Filter(c)}); err == nil || !strings.Contains(err.Error(), "name") {
			t.Errorf("%s: want an error naming the column, got %v", what, err)
		}
		if _, err := ct.WithFilter(c); err == nil {
			t.Errorf("%s: WithFilter must reject it too", what)
		}
	}
	for what, c := range map[string]Cond{
		"Eq with []byte":         Eq("name", []byte("a")),
		"In with []byte":         In("name", []byte("a"), []byte("b")),
		"Eq with a Valuer slice": Eq("name", valuerList{"a"}),
	} {
		if _, err := ct.planRead([]ReadOption{Filter(c)}); err != nil {
			t.Errorf("%s: %v", what, err)
		}
	}
}

// The copy plan never converts a slice to an array or to a pointer to an array: both conversions
// panic at run time when the slice is shorter than the array.
func TestPlanFieldCopyRejectsSliceToArray(t *testing.T) {
	src := func(v any) *schema.Field {
		return &schema.Field{Name: "Raw", StructField: reflect.StructField{Name: "Raw", Type: reflect.TypeOf(v)}}
	}
	dst := func(v any) reflect.StructField {
		return reflect.StructField{Name: "Raw", Type: reflect.TypeOf(v), Index: []int{0}}
	}
	cases := map[string]struct {
		from *schema.Field
		to   reflect.StructField
	}{
		"[]byte to [4]byte":   {src([]byte(nil)), dst([4]byte{})},
		"*[]byte to [4]byte":  {src((*[]byte)(nil)), dst([4]byte{})},
		"[]byte to *[4]byte":  {src([]byte(nil)), dst((*[4]byte)(nil))},
		"*[]byte to *[4]byte": {src((*[]byte)(nil)), dst((*[4]byte)(nil))},
	}
	for what, c := range cases {
		if _, err := planFieldCopy(c.from, c.to); err == nil || !strings.Contains(err.Error(), "cannot assign type") {
			t.Errorf("%s: want a cannot assign type error, got %v", what, err)
		}
	}
}

// matchSQL panics on an operator it does not know, which only a bug in the package can produce.
func TestMatchSQLPanicsOnUnknownOperator(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	defer func() {
		if msg, _ := recover().(string); !strings.Contains(msg, "99") {
			t.Errorf("want a panic naming the operator, got %q", msg)
		}
	}()
	ct.matchSQL("n", []resolvedCond{{"name", condOp(99), nil}})
}

// ExportedModel is exported so that a wrapper can set the embedded model as a whole.
type ExportedModel struct {
	Node
	Name string
}

type exportedWrapper struct {
	ExportedModel
	Children []*exportedWrapper
}

// An element type that embeds an exported model gets the scanned row as a whole.
func TestReadIntoWrapperOfExportedModel(t *testing.T) {
	ct := newInternalTree(t, ExportedModel{})
	ctx := t.Context()
	root := &ExportedModel{Name: "root"}
	if err := ct.Add(ctx, root, nil, nil, "t"); err != nil {
		t.Fatal(err)
	}
	if err := ct.Add(ctx, &ExportedModel{Name: "child"}, &root.NodeId, nil, "t"); err != nil {
		t.Fatal(err)
	}
	var tree []*exportedWrapper
	if err := ct.TreeDescendants(ctx, 0, 0, "t", &tree); err != nil || len(tree) != 1 {
		t.Fatalf("TreeDescendants: %d roots, %v", len(tree), err)
	}
	if r := tree[0]; r.Name != "root" || r.NodeId != root.NodeId || len(r.Children) != 1 || r.Children[0].Name != "child" || r.Children[0].ParentId != root.NodeId {
		t.Errorf("TreeDescendants into a wrapper of an exported model: %+v", *r)
	}
}
