package closuretree

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
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
	var none any
	eq([]resolvedCond{{"kind", opEq, []any{"doc"}}, {"archived_at", opIsNull, nil}},
		conds(Filter(Eq("Kind", "doc"), IsNull("archived_at"))))
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", nil))))              // nil is IS NULL
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", (*string)(nil)))))   // so is a nil pointer
	eq([]resolvedCond{{"note", opEq, []any{"x"}}}, conds(Filter(Eq("note", &s))))            // a pointer is dereferenced
	eq([]resolvedCond{{"note", opIsNull, nil}}, conds(Filter(Eq("note", &none))))            // to a nil interface, it is IS NULL
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

// A filter value that gorm would expand into several placeholders, render as SQL, or bind without
// a placeholder (a sql.NamedArg) is rejected before querying, also a nil pointer of a gorm.Valuer
// type; a byte slice and a driver.Valuer bind as one value and are accepted.
func TestFilterRejectsValuesThatAreNotOneValue(t *testing.T) {
	ct := newInternalTree(t, optNode{})
	words := []string{"a"}
	named := sql.Named("x", "a")
	bad := map[string]Cond{
		"Eq with a gorm.Valuer":                  Eq("name", sqlValuer{"'a' OR 1=1"}),
		"In with a gorm.Valuer":                  In("name", "a", sqlValuer{"'a' OR 1=1"}),
		"Eq with a pointer to a gorm.Valuer":     Eq("name", &sqlValuer{"'a' OR 1=1"}),
		"Eq with a nil pointer to a gorm.Valuer": Eq("name", (*sqlValuer)(nil)),
		"In with a nil pointer to a gorm.Valuer": In("name", (*sqlValuer)(nil)),
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

type person struct{ Name string }

// ownerLastDoc and ownerFirstDoc share the Go name Name between their own column name and the
// column owner_name of the embedded Owner, declared after and before their own field.
type ownerLastDoc struct {
	Node
	Name  string
	Owner person `gorm:"embedded;embeddedPrefix:owner_"`
}

type ownerFirstDoc struct {
	Node
	Owner person `gorm:"embedded;embeddedPrefix:owner_"`
	Name  string
}

// A Go name that several columns share is rejected in either declaration order; the column names
// resolve.
func TestColumnRejectsSharedGoName(t *testing.T) {
	for what, model := range map[string]any{"own field first": ownerLastDoc{}, "own field last": ownerFirstDoc{}} {
		ct := newInternalTree(t, model)
		for _, opt := range []ReadOption{Filter(Eq("Name", "a")), Columns("Name")} {
			_, err := ct.planRead([]ReadOption{opt})
			if !errors.Is(err, ErrUnknownColumn) || !strings.Contains(err.Error(), "owner_name") {
				t.Errorf("%s: want ErrUnknownColumn naming the columns, got %v", what, err)
			}
		}
		if _, err := ct.WithFilter(Eq("Name", "a")); !errors.Is(err, ErrUnknownColumn) {
			t.Errorf("%s: WithFilter: want ErrUnknownColumn, got %v", what, err)
		}
		for _, col := range []string{"name", "owner_name"} {
			p, err := ct.planRead([]ReadOption{Filter(Eq(col, "a")), Columns(col)})
			if err != nil || p.conds[0].column != col || !cmp.Equal(append(append([]string{}, baseColumns...), col), p.columns) {
				t.Errorf("%s: %q: %+v, %v", what, col, p, err)
			}
		}
	}
}

// resolvesTo checks that Eq and Columns with name resolve to the column col.
func resolvesTo(t *testing.T, ct *Tree, what, name, col string) {
	t.Helper()
	p, err := ct.planRead([]ReadOption{Filter(Eq(name, 1)), Columns(name)})
	if err != nil || p.conds[0].column != col || !cmp.Equal(append(append([]string{}, baseColumns...), col), p.columns) {
		t.Errorf("%s: %q: want the column %s, got %+v, %v", what, name, col, p, err)
	}
}

// nameBase is embedded without a column prefix, so its Name has the column name.
type nameBase struct{ Name string }

type baseFirstDoc struct {
	Node
	Base nameBase `gorm:"embedded"`
	Name string
}

type baseLastDoc struct {
	Node
	Name string
	Base nameBase `gorm:"embedded"`
}

// A Go name that several fields of one column share is not ambiguous, in either declaration order.
func TestColumnGoNameOfOneColumn(t *testing.T) {
	for what, model := range map[string]any{"embedded first": baseFirstDoc{}, "embedded last": baseLastDoc{}} {
		resolvesTo(t, newInternalTree(t, model), what, "Name", "name")
	}
}

// shadowedNameDoc embeds a writable Name, declared first, and has its own read-only Name excluded
// from migration on the same column, which gorm maps the column to.
type shadowedNameDoc struct {
	Node
	Base nameBase `gorm:"embedded"`
	Name string   `gorm:"->;-:migration"`
}

// A Go name is judged by the field gorm maps its column to, not by the first field declared, so it
// resolves exactly when the column name does.
func TestColumnGoNameUsesMappedField(t *testing.T) {
	ct := newInternalTree(t, shadowedNameDoc{})
	for _, name := range []string{"name", "Name"} {
		for _, opt := range []ReadOption{Filter(Eq(name, "a")), Columns(name)} {
			if _, err := ct.planRead([]ReadOption{opt}); !errors.Is(err, ErrUnknownColumn) {
				t.Errorf("%q: want ErrUnknownColumn, got %v", name, err)
			}
		}
	}
}

// legacyParentAfter and legacyParentBefore have their own ParentId on another column, declared
// after and before Node.
type legacyParentAfter struct {
	Node
	ParentId uint `gorm:"column:legacy_parent"`
}

type legacyParentBefore struct {
	ParentId uint `gorm:"column:legacy_parent"`
	Node
}

// A Go name counts only the columns that can be named, so the model's own ParentId resolves next
// to Node.ParentId.
func TestColumnGoNameSkipsUnnameableColumns(t *testing.T) {
	for what, model := range map[string]any{"after Node": legacyParentAfter{}, "before Node": legacyParentBefore{}} {
		resolvesTo(t, newInternalTree(t, model), what, "ParentId", "legacy_parent")
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

type codeText string

// The copy plan never converts an integer to a string: Go makes the integer a code point, so 65
// would be copied as "A".
func TestPlanFieldCopyRejectsIntegerToString(t *testing.T) {
	src := func(v any) *schema.Field {
		return &schema.Field{Name: "Code", StructField: reflect.StructField{Name: "Code", Type: reflect.TypeOf(v)}}
	}
	dst := func(v any) reflect.StructField {
		return reflect.StructField{Name: "Code", Type: reflect.TypeOf(v), Index: []int{0}}
	}
	rejected := map[string]struct {
		from *schema.Field
		to   reflect.StructField
	}{
		"int to string":        {src(0), dst("")},
		"int64 to codeText":    {src(int64(0)), dst(codeText(""))},
		"uint8 to string":      {src(uint8(0)), dst("")},
		"*int32 to string":     {src((*int32)(nil)), dst("")},
		"*uintptr to codeText": {src((*uintptr)(nil)), dst(codeText(""))},
	}
	for what, c := range rejected {
		if _, err := planFieldCopy(c.from, c.to); err == nil || !strings.Contains(err.Error(), "cannot assign type") {
			t.Errorf("%s: want a cannot assign type error, got %v", what, err)
		}
	}
	// the conversions that keep the value stay allowed
	for what, c := range map[string]struct {
		from *schema.Field
		to   reflect.StructField
	}{
		"string to codeText": {src(""), dst(codeText(""))},
		"[]byte to string":   {src([]byte(nil)), dst("")},
		"int64 to int8":      {src(int64(0)), dst(int8(0))},
		"float64 to int":     {src(float64(0)), dst(0)},
	} {
		if c, err := planFieldCopy(c.from, c.to); err != nil || !c.conv {
			t.Errorf("%s: want a conversion, got %+v, %v", what, c, err)
		}
	}
}

// A narrowing conversion is allowed, but a value outside the range of the element field's type
// overflows: converting it would wrap, or, from a float to an integer, give a value Go does not
// define.
func TestOverflows(t *testing.T) {
	cases := []struct {
		v    any
		to   any
		want bool
	}{
		{int64(127), int8(0), false},
		{int64(128), int8(0), true},
		{int64(-128), int8(0), false},
		{int64(-129), int8(0), true},
		{int64(-1), uint64(0), true},
		{int64(255), uint8(0), false},
		{int64(256), uint8(0), true},
		{uint64(math.MaxInt64), int64(0), false},
		{uint64(math.MaxInt64 + 1), int64(0), true},
		{uint64(65535), uint16(0), false},
		{uint64(65536), uint16(0), true},
		{uint(300), int8(0), true},
		{2.9, int8(0), false}, // truncated to 2
		{127.9, int8(0), false},
		{128.0, int8(0), true},
		{-128.9, int8(0), false},
		{-129.0, int8(0), true},
		{-0.5, uint(0), false},
		{-1.0, uint(0), true},
		{math.Ldexp(1, 63), int64(0), true},
		{math.Ldexp(1, 64), uint64(0), true},
		{math.NaN(), 0, true},
		{math.Inf(1), 0, true},
		{1e300, float32(0), true},
		{math.MaxFloat32, float32(0), false},
		{math.Inf(-1), float32(0), false}, // infinity stays infinity
		{float32(1), 0.0, false},
		{int64(math.MaxInt64), float32(0), false}, // loses precision, not range
		{uint64(math.MaxUint64), float32(0), false},
		{"text", codeText(""), false},
	}
	for _, c := range cases {
		if got := overflows(reflect.ValueOf(c.v), reflect.TypeOf(c.to)); got != c.want {
			t.Errorf("%T %v to %T: overflows %v, want %v", c.v, c.v, c.to, got, c.want)
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

// The element types below exercise childrenIndex on embedding the public tests do not cover.
// CyclicChildren embeds a pointer to itself and has no Children field.
type CyclicChildren struct {
	*CyclicChildren
	Name string
}

type shallowLinks struct{ Children []*shallowWins }
type deepLinks struct{ Children []*shallowWins }
type deepHolder struct{ deepLinks }

// shallowWins has a Children field at depth 1 and one at depth 2; Go promotes the shallower.
type shallowWins struct {
	shallowLinks
	deepHolder
}

type ExportedPtrLinks struct{ Children []*exportedPtrInValue }
type valueHolder struct{ *ExportedPtrLinks }

// exportedPtrInValue reaches Children through an unexported struct value, then an exported pointer.
type exportedPtrInValue struct{ valueHolder }

type hiddenPtrLinks struct{ Children []*hiddenPtrInValue }
type ExportedHolder struct{ *hiddenPtrLinks }

// hiddenPtrInValue reaches Children through an exported struct value, then an unexported pointer.
type hiddenPtrInValue struct{ ExportedHolder }

func TestChildrenIndex(t *testing.T) {
	cases := []struct {
		elem    any
		want    []int
		wantErr string
	}{
		{(*CyclicChildren)(nil), nil, "no field Children"},
		{(*shallowWins)(nil), []int{0, 0}, ""},
		{(*exportedPtrInValue)(nil), []int{0, 0, 0}, ""},
		{(*hiddenPtrInValue)(nil), nil, "cannot be reached"},
	}
	for _, c := range cases {
		elemType := reflect.TypeOf(c.elem)
		got, err := childrenIndex(elemType)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), elemType.Elem().Name()) {
				t.Errorf("%s: want an error with %q, got %v", elemType, c.wantErr, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", elemType, err)
		}
		if diff := cmp.Diff(c.want, got); diff != "" {
			t.Errorf("%s: index (-want +got):\n%s", elemType, diff)
		}
	}
}
