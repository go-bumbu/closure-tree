package closuretree

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
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
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", nil))))              // nil is IS NULL
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", (*string)(nil)))))   // so is a nil pointer
	eq([]resolvedCond{{"note", opEq, []any{"x"}}}, conds(Filter(Eq("note", &s))))            // a pointer is dereferenced
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", sql.NullString{})))) // a Valuer holding NULL is IS NULL
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", &sql.NullString{}))))
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", (*sql.NullString)(nil)))))
	valid := sql.NullString{String: "x", Valid: true} // a Valuer holding a value is bound as it is
	eq([]resolvedCond{{"note", opEq, []any{valid}}}, conds(Filter(Eq("note", valid))))
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

// sqlValuer is a gorm.Valuer: gorm renders the SQL it returns instead of binding it.
type sqlValuer struct{ sql string }

func (v sqlValuer) GormValue(context.Context, *gorm.DB) clause.Expr { return clause.Expr{SQL: v.sql} }

// sqlDriverValuer is a gorm.Valuer and a driver.Valuer; gorm uses GormValue.
type sqlDriverValuer struct{ sqlValuer }

func (v sqlDriverValuer) Value() (driver.Value, error) { return v.sql, nil }

// ptrValuer is a driver.Valuer through its pointer only.
type ptrValuer struct{ s string }

func (v *ptrValuer) Value() (driver.Value, error) { return v.s, nil }

// A filter value that gorm would expand into several placeholders, or render as SQL, is rejected
// before querying; a byte slice and a driver.Valuer bind as one value and are accepted.
func TestFilterRejectsValuesThatAreNotOneValue(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	words := []string{"a"}
	named := sql.Named("x", "a")
	bad := map[string]Cond{
		"Eq with a gorm.Valuer":                  Eq("name", sqlValuer{"'a' OR 1=1"}),
		"In with a gorm.Valuer":                  In("name", "a", sqlValuer{"'a' OR 1=1"}),
		"Eq with a pointer to a gorm.Valuer":     Eq("name", &sqlValuer{"'a' OR 1=1"}),
		"Eq with a gorm.Valuer that is driver's": Eq("name", sqlDriverValuer{sqlValuer{"'a' OR 1=1"}}),
		"Eq with a sql.NamedArg":                 Eq("name", named),
		"In with a sql.NamedArg":                 In("name", named),
		"Eq with a pointer to a sql.NamedArg":    Eq("name", &named),
		"Eq with a slice":                        Eq("name", []string{"a"}),
		"Eq with an array":                       Eq("name", [2]string{"a", "b"}),
		"Eq with a pointer to a slice":           Eq("name", &words),
		"In with a slice":                        In("name", "a", []any{"b"}),
		"In with a pointer to a slice":           In("name", &words),
		"Eq with an expression":                  Eq("name", clause.Expr{SQL: "name"}),
		"In with an expression":                  In("name", clause.Expr{SQL: "name"}),
		"Eq with a column":                       Eq("name", clause.Column{Name: "kind"}),
		"Eq with a *gorm.DB":                     Eq("name", ct.db),
		"In with a *gorm.DB":                     In("name", ct.db.Table("x")),
		"Eq with a gorm expression ptr":          Eq("name", &clause.Expr{SQL: "name"}),
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
		"Eq with []byte":           Eq("name", []byte("a")),
		"In with []byte":           In("name", []byte("a"), []byte("b")),
		"Eq with a Valuer slice":   Eq("name", valuerList{"a"}),
		"Eq with a pointer Valuer": Eq("name", &ptrValuer{"a"}),
		"In with a pointer Valuer": In("name", &ptrValuer{"a"}),
	} {
		if _, err := ct.planRead([]ReadOption{Filter(c)}); err != nil {
			t.Errorf("%s: %v", what, err)
		}
	}
	// a pointer that is a driver.Valuer is bound as it is: its element is not one
	pv := &ptrValuer{"a"}
	if p, err := ct.planRead([]ReadOption{Filter(Eq("name", pv))}); err != nil || len(p.conds) != 1 || p.conds[0].values[0] != any(pv) {
		t.Errorf("Eq with a pointer Valuer must bind the pointer: %+v, %v", p, err)
	}
}

var errBadValue = errors.New("bad value")

// badValuer is a driver.Valuer whose Value fails.
type badValuer string

func (badValuer) Value() (driver.Value, error) { return nil, errBadValue }

type badValuerNode struct {
	Node
	Code badValuer
}

// Eq and the match of FindChild and GetOrAdd ask a driver.Valuer for its value, to match NULL as
// IS NULL; an error from Value is returned.
func TestEqReturnsValuerError(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	if _, err := ct.planRead([]ReadOption{Filter(Eq("name", badValuer("x")))}); !errors.Is(err, errBadValue) {
		t.Errorf("Eq: want the Value error, got %v", err)
	}
	if _, err := ct.WithFilter(Eq("name", badValuer("x"))); !errors.Is(err, errBadValue) {
		t.Errorf("WithFilter: want the Value error, got %v", err)
	}
	if _, _, err := ct.buildMatchConditions(t.Context(), &badValuerNode{Code: "x"}, []string{"Code"}); !errors.Is(err, errBadValue) {
		t.Errorf("match: want the Value error, got %v", err)
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

func TestPlanReadNewOptions(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	plan := func(opts ...ReadOption) *readPlan {
		t.Helper()
		p, err := ct.planRead(opts)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if p := plan(); p.depth != nil || p.hasAncestors || p.virtualRoot || p.relation != "" || p.noPreload {
		t.Errorf("defaults: %+v", p)
	}
	for _, r := range []depthRange{{0, 0}, {1, 1}, {1, -1}, {0, -1}, {2, 5}} {
		if p := plan(DepthRange(r.min, r.max)); p.depth == nil || *p.depth != r {
			t.Errorf("DepthRange(%d, %d): %+v", r.min, r.max, p.depth)
		}
	}
	if p := plan(DepthRange(0, 0), DepthRange(1, 3)); *p.depth != (depthRange{1, 3}) {
		t.Errorf("the last DepthRange must win: %+v", p.depth)
	}
	for _, bad := range [][2]int{{-1, 3}, {3, 1}, {-2, -1}} {
		if _, err := ct.planRead([]ReadOption{DepthRange(bad[0], bad[1])}); !errors.Is(err, ErrInvalidDepthRange) {
			t.Errorf("DepthRange(%d, %d): want ErrInvalidDepthRange, got %v", bad[0], bad[1], err)
		}
	}
	ids := []uint{1, 2}
	opt := ForAncestors(ids...)
	ids[0] = 99 // the option keeps its own copy
	p := plan(opt, ForAncestors(3))
	if !p.hasAncestors || !cmp.Equal([]uint{1, 2, 3}, p.ancestors) {
		t.Errorf("ForAncestors: %+v", p)
	}
	if p := plan(ForAncestors()); !p.hasAncestors || len(p.ancestors) != 0 {
		t.Errorf("ForAncestors without ids must be recorded as an empty set: %+v", p)
	}
	if p := plan(IncludeVirtualRoot(), Relation("Tags"), NoPreload()); !p.virtualRoot || p.relation != "Tags" || !p.noPreload {
		t.Errorf("flags: %+v", p)
	}
}
