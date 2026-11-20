package closuretree_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
)

type filterNode struct {
	closuretree.Node
	Name       string
	Kind       string
	Hidden     bool
	Note       *string
	ArchivedAt *time.Time
	Children   []*filterNode `gorm:"-"`
}

const tenantF = "f"

// buildFilterTree creates this tree in tenantF and returns name -> node id.
//
//	a
//	├── a1
//	└── x        archived_at set, note NULL      <- the node that does not match
//	    ├── x1
//	    └── x2
//	        └── x21
//	b            kind "folder"
//	└── b1       hidden
//
// Every node has kind "doc", note "keep", hidden false and archived_at NULL unless noted. Nodes
// are added in the order a, b, a1, x, x1, x2, x21, b1 with the default placement, so x has
// sort_order -10.
func buildFilterTree(t *testing.T, ct *closuretree.Tree) map[string]uint {
	t.Helper()
	ctx := context.Background()
	archived := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	id := map[string]uint{}
	add := func(name, parent string, edit func(n *filterNode)) {
		t.Helper()
		note := "keep"
		n := &filterNode{Name: name, Kind: "doc", Note: &note}
		if edit != nil {
			edit(n)
		}
		var parentID *uint
		if parent != "" {
			parentID = up(id[parent])
		}
		if err := ct.Add(ctx, n, parentID, nil, tenantF); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		id[name] = n.NodeId
	}
	add("a", "", nil)
	add("b", "", func(n *filterNode) { n.Kind = "folder" })
	add("a1", "a", nil)
	add("x", "a", func(n *filterNode) { n.Note = nil; n.ArchivedAt = &archived })
	add("x1", "x", nil)
	add("x2", "x", nil)
	add("x21", "x2", nil)
	add("b1", "b", func(n *filterNode) { n.Hidden = true })
	return id
}

// Descendants and TreeDescendants must read booleans, nullable strings and nullable timestamps,
// as GetNode already does.
//
//nolint:gocyclo // test function covers many assertion branches
func TestReadsScanEveryColumnType(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			id := buildFilterTree(t, ct)
			ctx := context.Background()

			check := func(what string, byName map[string]filterNode) {
				t.Helper()
				if len(byName) != 8 {
					t.Fatalf("%s: want 8 nodes, got %d", what, len(byName))
				}
				if !byName["b1"].Hidden || byName["a"].Hidden {
					t.Errorf("%s: boolean column: b1=%v a=%v", what, byName["b1"].Hidden, byName["a"].Hidden)
				}
				if n := byName["a"].Note; n == nil || *n != "keep" {
					t.Errorf("%s: nullable string: %v", what, n)
				}
				if byName["x"].Note != nil || byName["x"].ArchivedAt == nil || byName["a"].ArchivedAt != nil {
					t.Errorf("%s: NULL handling: x=%+v a=%+v", what, byName["x"], byName["a"])
				}
				if byName["x1"].ParentId != id["x"] || byName["b"].Kind != "folder" {
					t.Errorf("%s: parent or plain column: %+v %+v", what, byName["x1"], byName["b"])
				}
			}

			// values and pointers; results are appended to what the slice already holds, as today
			values := []filterNode{{Name: "kept"}}
			if err := ct.Descendants(ctx, 0, 0, tenantF, &values); err != nil {
				t.Fatal(err)
			}
			if len(values) != 9 || values[0].Name != "kept" {
				t.Fatalf("Descendants must append: %d items, first %q", len(values), values[0].Name)
			}
			flat := map[string]filterNode{}
			for _, n := range values[1:] {
				flat[n.Name] = n
			}
			check("Descendants into []T", flat)

			var pointers []*filterNode
			if err := ct.Descendants(ctx, 0, 0, tenantF, &pointers); err != nil {
				t.Fatal(err)
			}
			flat = map[string]filterNode{}
			for _, n := range pointers {
				flat[n.Name] = *n
			}
			check("Descendants into []*T", flat)

			var roots []*filterNode
			if err := ct.TreeDescendants(ctx, 0, 0, tenantF, &roots); err != nil {
				t.Fatal(err)
			}
			nested := map[string]filterNode{}
			var walk func(nodes []*filterNode)
			walk = func(nodes []*filterNode) {
				for _, n := range nodes {
					nested[n.Name] = *n
					walk(n.Children)
				}
			}
			walk(roots)
			check("TreeDescendants", nested)
			if len(roots) != 2 || len(nested["x"].Children) != 2 {
				t.Errorf("TreeDescendants links: %d roots, x has %d children", len(roots), len(nested["x"].Children))
			}
		})
	}
}

//nolint:gocyclo // test function covers many assertion branches
func TestColumnsProjection(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			id := buildFilterTree(t, ct)
			ctx := context.Background()

			cols := closuretree.Columns("name")

			var x filterNode
			if err := ct.GetNode(ctx, id["x"], tenantF, &x, cols); err != nil {
				t.Fatal(err)
			}
			want := filterNode{
				Node: closuretree.Node{NodeId: id["x"], ParentId: id["a"], Tenant: tenantF, SortOrder: -10},
				Name: "x", // kind, hidden, note and archived_at were not selected and stay zero
			}
			if diff := cmp.Diff(want, x); diff != "" {
				t.Errorf("GetNode with Columns (-want +got):\n%s", diff)
			}

			var all []filterNode
			if err := ct.Descendants(ctx, 0, 0, tenantF, &all, cols); err != nil || len(all) != 8 {
				t.Fatalf("Descendants: %d nodes, %v", len(all), err)
			}
			for _, n := range all {
				if n.Name == "" || n.Kind != "" || n.Note != nil {
					t.Errorf("Descendants with Columns: %+v", n)
				}
				if n.Name == "x1" && n.ParentId != id["x"] {
					t.Errorf("x1: want parent %d, got %d", id["x"], n.ParentId)
				}
			}

			var roots []*filterNode
			if err := ct.TreeDescendants(ctx, 0, 0, tenantF, &roots, cols); err != nil || len(roots) != 2 {
				t.Fatalf("TreeDescendants: %d roots, %v", len(roots), err)
			}
			for _, r := range roots {
				if r.Kind != "" || (r.Name == "a" && len(r.Children) != 2) {
					t.Errorf("TreeDescendants with Columns: %+v", r)
				}
			}

			var out filterNode
			found, err := ct.FindChild(ctx, id["a"], tenantF, &filterNode{Name: "x"}, []string{"Name"}, &out, closuretree.Columns("kind"))
			if err != nil || !found || out.NodeId != id["x"] || out.Kind != "doc" || out.Name != "" {
				t.Errorf("FindChild with Columns: found=%v err=%v out=%+v", found, err, out)
			}

			// a Go field name works, and naming a base column does no harm
			var b filterNode
			if err := ct.GetNode(ctx, id["b"], tenantF, &b, closuretree.Columns("Kind", "node_id")); err != nil || b.Kind != "folder" || b.Name != "" {
				t.Errorf("Columns by field name: %+v, %v", b, err)
			}
		})
	}
}

func TestUnknownColumnFailsBeforeQuerying(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			id := buildFilterTree(t, ct)
			ctx := context.Background()

			cancelled, cancel := context.WithCancel(ctx)
			cancel() // a query would fail with context.Canceled, so validation must come first
			bad := map[string]closuretree.ReadOption{
				"unknown column":   closuretree.Columns("nope"),
				"computed parent":  closuretree.Columns("parent_id"),
				"filter unknown":   closuretree.Filter(closuretree.Eq("nope", 1)),
				"filter on parent": closuretree.Filter(closuretree.IsNull("parent_id")),
			}
			for name, opt := range bad {
				var one filterNode
				var many []filterNode
				var tree []*filterNode
				errs := map[string]error{
					"GetNode":         ct.GetNode(cancelled, id["a"], tenantF, &one, opt),
					"Descendants":     ct.Descendants(cancelled, 0, 0, tenantF, &many, opt),
					"TreeDescendants": ct.TreeDescendants(cancelled, 0, 0, tenantF, &tree, opt),
				}
				_, errs["DescendantIds"] = ct.DescendantIds(cancelled, 0, 0, tenantF, opt)
				_, errs["TreeDescendantsIds"] = ct.TreeDescendantsIds(cancelled, 0, 0, tenantF, opt)
				_, errs["FindChild"] = ct.FindChild(cancelled, 0, tenantF, &filterNode{Name: "a"}, []string{"Name"}, &one, opt)
				for read, err := range errs {
					if !errors.Is(err, closuretree.ErrUnknownColumn) {
						t.Errorf("%s, %s: want ErrUnknownColumn, got %v", name, read, err)
					}
				}
			}
		})
	}
}

// A table created by v0.8.0 or earlier still has a physical parent_id column. The reads must
// return the parent computed from the closure rows. This test may pass before the change (today
// the computed column wins by column order); it pins the behaviour.
func TestStaleParentIDColumnIsIgnored(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			id := buildFilterTree(t, ct)
			ctx := context.Background()

			tbl := ct.NodeTableName()
			for _, stmt := range []string{"ALTER TABLE " + tbl + " ADD COLUMN parent_id BIGINT", "UPDATE " + tbl + " SET parent_id = 424242"} {
				if err := gdb.Exec(stmt).Error; err != nil {
					t.Fatal(err)
				}
			}
			var x1 filterNode
			if err := ct.GetNode(ctx, id["x1"], tenantF, &x1); err != nil || x1.ParentId != id["x"] {
				t.Errorf("GetNode: parent %d, %v", x1.ParentId, err)
			}
			var all []filterNode
			if err := ct.Descendants(ctx, 0, 0, tenantF, &all); err != nil || len(all) != 8 {
				t.Fatalf("Descendants: %d, %v", len(all), err)
			}
			for _, n := range all {
				if n.ParentId == 424242 {
					t.Errorf("Descendants returned the stale parent for %s", n.Name)
				}
			}
			var under []*filterNode
			if err := ct.TreeDescendants(ctx, id["x"], 1, tenantF, &under); err != nil || len(under) != 2 || under[0].ParentId != id["x"] {
				t.Errorf("TreeDescendants: %+v, %v", under, err)
			}
			var out filterNode
			if found, err := ct.FindChild(ctx, id["x"], tenantF, &filterNode{Name: "x1"}, []string{"Name"}, &out); err != nil || !found || out.ParentId != id["x"] {
				t.Errorf("FindChild: found=%v parent=%d err=%v", found, out.ParentId, err)
			}
		})
	}
}

// Review focus 1 and 2.
type reservedNode struct {
	closuretree.Node
	Order    int
	Group    string
	Computed string          `gorm:"->;-:migration"` // no column in the table
	Children []*reservedNode `gorm:"-"`
}

func TestReadsQuoteReservedColumnNames(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, reservedNode{})
			ct, err := newTestTree(gdb, reservedNode{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			p := &reservedNode{Order: 1, Group: "g"}
			if err := ct.Add(ctx, p, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			c := &reservedNode{Order: 2, Group: "g"}
			if err := ct.Add(ctx, c, up(p.NodeId), nil, tenant1); err != nil {
				t.Fatal(err)
			}
			var got reservedNode
			if err := ct.GetNode(ctx, c.NodeId, tenant1, &got); err != nil || got.Order != 2 || got.Group != "g" {
				t.Errorf("GetNode: %+v, %v", got, err)
			}
			var only reservedNode
			if err := ct.GetNode(ctx, c.NodeId, tenant1, &only, closuretree.Columns("order")); err != nil || only.Order != 2 || only.Group != "" {
				t.Errorf("GetNode with Columns: %+v, %v", only, err)
			}
			var flat []reservedNode
			if err := ct.Descendants(ctx, 0, 0, tenant1, &flat); err != nil || len(flat) != 2 {
				t.Errorf("Descendants: %d, %v", len(flat), err)
			}
			var tree []*reservedNode
			if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &tree); err != nil || len(tree) != 1 || len(tree[0].Children) != 1 {
				t.Errorf("TreeDescendants: %+v, %v", tree, err)
			}
			if found, err := ct.FindChild(ctx, p.NodeId, tenant1, &reservedNode{Group: "g"}, []string{"Group"}, &got); err != nil || !found {
				t.Errorf("FindChild: %v, %v", found, err)
			}
		})
	}
}

// Review focus 3.
type slimNode struct {
	closuretree.Node
	Name string
}

func TestReadIntoSmallerStruct(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			id := buildFilterTree(t, ct)
			ctx := context.Background()

			var slim []slimNode
			if err := ct.Descendants(ctx, 0, 0, tenantF, &slim); err != nil || len(slim) != 8 || slim[0].Name == "" {
				t.Errorf("Descendants into a smaller struct: %+v, %v", slim, err)
			}
			var one slimNode
			if err := ct.GetNode(ctx, id["a"], tenantF, &one); err != nil || one.Name != "a" {
				t.Errorf("GetNode into a smaller struct: %+v, %v", one, err)
			}
		})
	}
}

type bareNode struct {
	Name     string
	Children []*bareNode
}

// An element type that does not embed Node is still linked through the scanned rows.
func TestTreeDescendantsLinksElementsWithoutNode(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			buildFilterTree(t, ct)
			ctx := context.Background()

			var roots []*bareNode
			if err := ct.TreeDescendants(ctx, 0, 0, tenantF, &roots); err != nil {
				t.Fatal(err)
			}
			if len(roots) != 2 {
				t.Fatalf("want 2 roots, got %d", len(roots))
			}
			for _, r := range roots {
				if r.Name == "a" && len(r.Children) != 2 {
					t.Errorf("a: want 2 children, got %d", len(r.Children))
				}
			}
		})
	}
}

type kindName string

type convertedNode struct {
	closuretree.Node
	Name string
	Kind kindName
	Note string
}

type noteText string

// noteTextNode takes the model's Note *string through dereference and conversion.
type noteTextNode struct {
	closuretree.Node
	Name string
	Note noteText
}

func checkNoteText(t *testing.T, ct *closuretree.Tree) {
	t.Helper()
	var texts []noteTextNode
	if err := ct.Descendants(context.Background(), 0, 0, tenantF, &texts); err != nil || len(texts) != 8 {
		t.Fatalf("Descendants into noteText: %d, %v", len(texts), err)
	}
	for _, n := range texts {
		if (n.Name == "a" && n.Note != "keep") || (n.Name == "x" && n.Note != "") {
			t.Errorf("noteText conversion: %s has note %q", n.Name, n.Note)
		}
	}
}

type wrongTypeNode struct {
	closuretree.Node
	Name     int
	Children []*wrongTypeNode
}

// A field whose type differs from the model's is converted when it can be, and fails the read
// before the query when it cannot.
func TestReadIntoDifferentFieldTypes(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			buildFilterTree(t, ct)
			ctx := context.Background()

			var converted []convertedNode
			if err := ct.Descendants(ctx, 0, 0, tenantF, &converted); err != nil || len(converted) != 8 {
				t.Fatalf("Descendants: %d, %v", len(converted), err)
			}
			byName := map[string]convertedNode{}
			for _, n := range converted {
				byName[n.Name] = n
			}
			if byName["b"].Kind != "folder" || byName["a"].Note != "keep" || byName["x"].Note != "" {
				t.Errorf("converted fields: b=%+v a=%+v x=%+v", byName["b"], byName["a"], byName["x"])
			}

			checkNoteText(t, ct)

			cancelled, cancel := context.WithCancel(ctx)
			cancel() // a query would fail with context.Canceled, so the type check must come first
			for _, c := range []context.Context{ctx, cancelled} {
				var flat []wrongTypeNode
				var tree []*wrongTypeNode
				errs := map[string]error{
					"Descendants":     ct.Descendants(c, 0, 0, tenantF, &flat),
					"TreeDescendants": ct.TreeDescendants(c, 0, 0, tenantF, &tree),
				}
				for read, err := range errs {
					if err == nil || !strings.Contains(err.Error(), "cannot assign") || !strings.Contains(err.Error(), "Name") {
						t.Errorf("%s: want a cannot assign error on Name, got %v", read, err)
					}
				}
			}
		})
	}
}
