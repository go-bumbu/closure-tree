package closuretree_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
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
			staleParentWithColumns(t, ct, id)
		})
	}
}

// staleParentWithColumns checks that GetNode and TreeDescendants with Columns also return the
// computed parent, not the leftover physical parent_id column.
func staleParentWithColumns(t *testing.T, ct *closuretree.Tree, id map[string]uint) {
	t.Helper()
	ctx := context.Background()
	cols := closuretree.Columns("name")
	var x1 filterNode
	if err := ct.GetNode(ctx, id["x1"], tenantF, &x1, cols); err != nil || x1.ParentId != id["x"] {
		t.Errorf("GetNode with Columns: parent %d, %v", x1.ParentId, err)
	}
	var under []*filterNode
	if err := ct.TreeDescendants(ctx, id["x"], 1, tenantF, &under, cols); err != nil || len(under) != 2 || under[0].ParentId != id["x"] {
		t.Errorf("TreeDescendants with Columns: %+v, %v", under, err)
	}
}

// unmigratedNode has a writable field that is excluded from migration; the application adds its
// column itself.
type unmigratedNode struct {
	closuretree.Node
	Name     string
	Extra    string            `gorm:"-:migration"`
	Children []*unmigratedNode `gorm:"-"`
}

// A writable field excluded from migration whose column exists is read by every read, so a read
// followed by Update keeps its value, and it can be named in Filter and Columns.
//
//nolint:gocyclo // test function covers many assertion branches
func TestReadsReturnUnmigratedColumns(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, unmigratedNode{})
			ct, err := newTestTree(gdb, unmigratedNode{})
			if err != nil {
				t.Fatal(err)
			}
			if err := gdb.Exec("ALTER TABLE " + ct.NodeTableName() + " ADD COLUMN extra VARCHAR(50)").Error; err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			root := &unmigratedNode{Name: "r", Extra: "written"}
			if err := ct.Add(ctx, root, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			child := &unmigratedNode{Name: "c", Extra: "written"}
			if err := ct.Add(ctx, child, up(root.NodeId), nil, tenant1); err != nil {
				t.Fatal(err)
			}

			var one unmigratedNode
			if err := ct.GetNode(ctx, child.NodeId, tenant1, &one); err != nil || one.Extra != "written" {
				t.Errorf("GetNode: %+v, %v", one, err)
			}
			var out unmigratedNode
			if found, err := ct.FindChild(ctx, root.NodeId, tenant1, &unmigratedNode{Name: "c"}, []string{"Name"}, &out); err != nil || !found || out.Extra != "written" {
				t.Errorf("FindChild: found=%v %+v, %v", found, out, err)
			}
			var flat []unmigratedNode
			if err := ct.Descendants(ctx, 0, 0, tenant1, &flat); err != nil || len(flat) != 2 || flat[0].Extra != "written" || flat[1].Extra != "written" {
				t.Errorf("Descendants: %+v, %v", flat, err)
			}
			var tree []*unmigratedNode
			if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &tree); err != nil || len(tree) != 1 || tree[0].Extra != "written" ||
				len(tree[0].Children) != 1 || tree[0].Children[0].Extra != "written" {
				t.Errorf("TreeDescendants: %+v, %v", tree, err)
			}

			// a read followed by Update keeps the value
			one.Name = "c2"
			if err := ct.Update(ctx, child.NodeId, &one, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			var again unmigratedNode
			if err := ct.GetNode(ctx, child.NodeId, tenant1, &again); err != nil || again.Extra != "written" || again.Name != "c2" {
				t.Errorf("GetNode after Update: %+v, %v", again, err)
			}

			var named []unmigratedNode
			err = ct.Descendants(ctx, 0, 0, tenant1, &named, closuretree.Filter(closuretree.Eq("extra", "written")), closuretree.Columns("extra"))
			if err != nil || len(named) != 2 || named[0].Extra != "written" || named[0].Name != "" {
				t.Errorf("Filter and Columns on the column: %+v, %v", named, err)
			}
		})
	}
}

// WideBase is the Tree's model; WideNode embeds it and adds a column the application created. Both
// are exported because gorm skips an embedded struct of an unexported type.
type WideBase struct {
	closuretree.Node
	Name string
}

type WideNode struct {
	WideBase
	Color string
}

// GetNode and FindChild into a struct with more columns than the Tree's model fill the extra
// columns, so a read followed by Update keeps them.
func TestSingleReadsFillWiderStruct(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, WideBase{})
			ct, err := newTestTree(gdb, WideBase{})
			if err != nil {
				t.Fatal(err)
			}
			if err := gdb.Exec("ALTER TABLE " + ct.NodeTableName() + " ADD COLUMN color VARCHAR(50)").Error; err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			n := &WideNode{WideBase: WideBase{Name: "a"}, Color: "red"}
			if err := ct.Add(ctx, n, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}

			var one WideNode
			if err := ct.GetNode(ctx, n.NodeId, tenant1, &one); err != nil || one.Color != "red" {
				t.Errorf("GetNode: %+v, %v", one, err)
			}
			var out WideNode
			if found, err := ct.FindChild(ctx, 0, tenant1, &WideBase{Name: "a"}, []string{"Name"}, &out); err != nil || !found || out.Color != "red" {
				t.Errorf("FindChild: found=%v %+v, %v", found, out, err)
			}

			one.Name = "b"
			if err := ct.Update(ctx, n.NodeId, &one, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			var again WideNode
			if err := ct.GetNode(ctx, n.NodeId, tenant1, &again); err != nil || again.Color != "red" || again.Name != "b" {
				t.Errorf("GetNode after Update: %+v, %v", again, err)
			}
		})
	}
}

// reservedNode has columns named with reserved words, which every read must quote, and a
// read-only field that has no column in the table.
type reservedNode struct {
	closuretree.Node
	Order    int
	Group    string
	Computed string          `gorm:"->;-:migration"` // no column in the table
	Children []*reservedNode `gorm:"-"`
}

//nolint:gocyclo // test function covers many assertion branches
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
			var second []reservedNode
			byOrder := closuretree.Filter(closuretree.Eq("order", 2))
			if err := ct.Descendants(ctx, 0, 0, tenant1, &second, byOrder, closuretree.PerNode()); err != nil || len(second) != 1 {
				t.Errorf("filter on a reserved-word column: %d, %v", len(second), err)
			}
			var pruned []reservedNode
			if err := ct.Descendants(ctx, 0, 0, tenant1, &pruned, byOrder); err != nil || len(pruned) != 0 { // prune: the root has order 1
				t.Errorf("prune on a reserved-word column: %d, %v", len(pruned), err)
			}
		})
	}
}

// slimNode is smaller than filterNode; reads into it fill only its fields.
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

type childlessNode struct {
	closuretree.Node
	Name string
}

// leftLinks and rightLinks both hold a Children field of sharedLinksNode, which embeds them at the
// same depth, so Go promotes neither.
type leftLinks struct {
	Children []*sharedLinksNode
}

type rightLinks struct {
	Children []*sharedLinksNode
}

type sharedLinksNode struct {
	closuretree.Node
	Name string
	leftLinks
	rightLinks
}

// treeLinks holds the Children of hiddenLinksNode, which embeds it by pointer. Reflection cannot
// allocate a pointer to an unexported embedded type.
type treeLinks struct {
	Children []*hiddenLinksNode
}

type hiddenLinksNode struct {
	closuretree.Node
	Name string
	*treeLinks
}

// newChainTree returns a tree of TestPayload on db holding the chain a > b > c in tenant1.
func newChainTree(t *testing.T, db testdbs.TargetDb) *closuretree.Tree {
	t.Helper()
	gdb := connAndClose(t, db)
	dropTreeTables(gdb, TestPayload{})
	ct, err := newTestTree(gdb, TestPayload{})
	if err != nil {
		t.Fatal(err)
	}
	var parent *uint
	for _, name := range []string{"a", "b", "c"} {
		n := &TestPayload{Name: name}
		if err := ct.Add(context.Background(), n, parent, nil, tenant1); err != nil {
			t.Fatal(err)
		}
		parent = up(n.NodeId)
	}
	return ct
}

// A tree read into an element type whose Children field cannot be reached returns an error naming
// the type and the case, for every maxDepth, before it validates the tenant and the read options
// and before any query: with the context cancelled a query would fail with context.Canceled.
func TestTreeDescendantsRejectsUnreachableChildren(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newChainTree(t, db)
			ctx := context.Background()
			cancelled, cancel := context.WithCancel(ctx)
			cancel()

			cases := []struct {
				name     string
				read     func(ctx context.Context, maxDepth int, tenant string, opts ...closuretree.ReadOption) error
				elemType string
				want     string
			}{
				{"no Children field", func(ctx context.Context, maxDepth int, tenant string, opts ...closuretree.ReadOption) error {
					var items []*childlessNode
					return ct.TreeDescendants(ctx, 0, maxDepth, tenant, &items, opts...)
				}, "childlessNode", "no field Children"},
				{"Children shared by two embedded structs", func(ctx context.Context, maxDepth int, tenant string, opts ...closuretree.ReadOption) error {
					var items []*sharedLinksNode
					return ct.TreeDescendants(ctx, 0, maxDepth, tenant, &items, opts...)
				}, "sharedLinksNode", "ambiguous"},
				{"Children behind an unexported embedded pointer", func(ctx context.Context, maxDepth int, tenant string, opts ...closuretree.ReadOption) error {
					var items []*hiddenLinksNode
					return ct.TreeDescendants(ctx, 0, maxDepth, tenant, &items, opts...)
				}, "hiddenLinksNode", "cannot be reached"},
			}
			for _, c := range cases {
				for _, maxDepth := range []int{0, 1} {
					errs := map[string]error{
						"read":           c.read(ctx, maxDepth, tenant1),
						"cancelled":      c.read(cancelled, maxDepth, tenant1),
						"empty tenant":   c.read(ctx, maxDepth, ""),
						"unknown column": c.read(ctx, maxDepth, tenant1, closuretree.Columns("nope")),
					}
					for what, err := range errs {
						if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, closuretree.ErrEmptyTenant) ||
							errors.Is(err, closuretree.ErrUnknownColumn) ||
							!strings.Contains(err.Error(), c.elemType) || !strings.Contains(err.Error(), c.want) {
							t.Errorf("%s, maxDepth %d, %s: want an error naming %s and %q, got %v", c.name, maxDepth, what, c.elemType, c.want, err)
						}
					}
				}
			}
		})
	}
}

// Without PerNode, Descendants with maxDepth 1 returns the nodes of TreeDescendants with maxDepth
// 1 in the same order, so it reads one level into an element type without a Children field.
func TestDescendantsMatchesTreeDescendantsOneLevel(t *testing.T) {
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
			ids := func(nodes []*filterNode) []uint {
				out := make([]uint, len(nodes))
				for i, n := range nodes {
					out[i] = n.NodeId
				}
				return out
			}

			reads := map[string][]closuretree.ReadOption{
				"no options": nil,
				"filter":     {closuretree.Filter(closuretree.IsNull("archived_at"))},
				"columns":    {closuretree.Columns("name")},
			}
			for _, parent := range []string{"", "a", "x", "x2", "b", "x21"} {
				for what, opts := range reads {
					var flat, tree []*filterNode
					if err := ct.Descendants(ctx, id[parent], 1, tenantF, &flat, opts...); err != nil {
						t.Fatal(err)
					}
					if err := ct.TreeDescendants(ctx, id[parent], 1, tenantF, &tree, opts...); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(ids(tree), ids(flat)); diff != "" {
						t.Errorf("parent %q, %s (-TreeDescendants +Descendants):\n%s", parent, what, diff)
					}
				}
			}
		})
	}
}

// TreeLinks holds the Children of linkedNode, which embeds it by pointer.
type TreeLinks struct {
	Children []*linkedNode
}

type linkedNode struct {
	closuretree.Node
	Name string
	*TreeLinks
}

// plainLinks holds the Children of plainLinksNode, which embeds it by value. The exported Children
// promoted through an unexported embedded struct value can be set.
type plainLinks struct {
	Children []*plainLinksNode
}

type plainLinksNode struct {
	closuretree.Node
	Name string
	plainLinks
}

// A Children field promoted through an embedded pointer struct is reached by allocating the
// pointer on every element, so the chain a > b > c comes back linked and the Children of a leaf can
// be read. When the pointer is unexported and cannot be allocated, the read returns an error
// before querying.
func TestTreeDescendantsChildrenThroughEmbeddedPointer(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newChainTree(t, db)
			ctx := context.Background()

			var linked []*linkedNode
			if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &linked); err != nil {
				t.Fatal(err)
			}
			if len(linked) != 1 || linked[0].Name != "a" || len(linked[0].Children) != 1 {
				t.Fatalf("want root a with one child, got %d roots", len(linked))
			}
			b := linked[0].Children[0]
			if b.Name != "b" || len(b.Children) != 1 || b.Children[0].Name != "c" {
				t.Fatalf("want b with child c, got %q with %d children", b.Name, len(b.Children))
			}
			if leaf := b.Children[0]; len(leaf.Children) != 0 {
				t.Errorf("leaf c: want no children, got %d", len(leaf.Children))
			}

			var top []*linkedNode
			if err := ct.TreeDescendants(ctx, 0, 1, tenant1, &top); err != nil || len(top) != 1 || len(top[0].Children) != 0 {
				t.Errorf("maxDepth 1: want a without children, got %d roots, %v", len(top), err)
			}

			var hidden []*hiddenLinksNode
			if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &hidden); err == nil || !strings.Contains(err.Error(), "hiddenLinksNode") ||
				!strings.Contains(err.Error(), "cannot be reached") || len(hidden) != 0 {
				t.Errorf("unexported embedded pointer: want an error naming hiddenLinksNode, got %d roots, %v", len(hidden), err)
			}
		})
	}
}

// A Children field promoted through an unexported embedded struct value can be set, so the chain
// a > b > c comes back linked.
func TestTreeDescendantsChildrenThroughEmbeddedValue(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newChainTree(t, db)
			var plain []*plainLinksNode
			if err := ct.TreeDescendants(context.Background(), 0, 0, tenant1, &plain); err != nil {
				t.Fatal(err)
			}
			if len(plain) != 1 || len(plain[0].Children) != 1 || len(plain[0].Children[0].Children) != 1 ||
				plain[0].Children[0].Children[0].Name != "c" {
				t.Errorf("want the chain a > b > c, got %d roots", len(plain))
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

type countedNode struct {
	closuretree.Node
	Name  string
	Count int64
	Ratio float64
}

type narrowCountNode struct {
	closuretree.Node
	Name     string
	Count    int8
	Ratio    int
	Children []*narrowCountNode
}

type countTextNode struct {
	closuretree.Node
	Name     string
	Count    string
	Children []*countTextNode
}

// An integer is never copied into a string field: the read fails before the query. A narrowing
// conversion copies a value that fits; a value that does not fails the read.
func TestReadNarrowingAndIntegerToString(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, countedNode{})
			ct, err := newTestTree(gdb, countedNode{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			small := &countedNode{Name: "small", Count: 5, Ratio: 2.9}
			if err := ct.Add(ctx, small, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			big := &countedNode{Name: "big", Count: 300, Ratio: 1}
			if err := ct.Add(ctx, big, up(small.NodeId), nil, tenant1); err != nil {
				t.Fatal(err)
			}

			cancelled, cancel := context.WithCancel(ctx)
			cancel() // a query would fail with context.Canceled, so the type check must come first
			var flatText []countTextNode
			var treeText []*countTextNode
			for read, err := range map[string]error{
				"Descendants":     ct.Descendants(cancelled, 0, 0, tenant1, &flatText),
				"TreeDescendants": ct.TreeDescendants(cancelled, 0, 0, tenant1, &treeText),
				"Ancestors":       ct.Ancestors(cancelled, big.NodeId, tenant1, &flatText),
			} {
				if err == nil || !strings.Contains(err.Error(), "cannot assign type int64 to field Count") {
					t.Errorf("%s into a string: want a cannot assign error on Count, got %v", read, err)
				}
			}

			var fits []narrowCountNode
			if err := ct.Descendants(ctx, 0, 1, tenant1, &fits); err != nil || len(fits) != 1 ||
				fits[0].Count != 5 || fits[0].Ratio != 2 {
				t.Errorf("a value that fits: want count 5 and ratio 2, got %+v, %v", fits, err)
			}
			var ancestors []narrowCountNode
			if err := ct.Ancestors(ctx, big.NodeId, tenant1, &ancestors); err != nil || len(ancestors) != 1 || ancestors[0].Count != 5 {
				t.Errorf("Ancestors of big: want small with count 5, got %+v, %v", ancestors, err)
			}

			var flat []narrowCountNode
			var tree []*narrowCountNode
			for read, err := range map[string]error{
				"Descendants":     ct.Descendants(ctx, 0, 0, tenant1, &flat),
				"TreeDescendants": ct.TreeDescendants(ctx, 0, 0, tenant1, &tree),
			} {
				if err == nil || !strings.Contains(err.Error(), "field Count") || !strings.Contains(err.Error(), "300 overflows int8") {
					t.Errorf("%s with 300 into an int8: want an overflow error, got %v", read, err)
				}
			}
			if len(tree) != 0 {
				t.Errorf("TreeDescendants must return no nodes on an overflow, got %d", len(tree))
			}
		})
	}
}

func TestFilterOnSingleNodeReads(t *testing.T) {
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

			active := closuretree.Filter(closuretree.IsNull("archived_at"))
			kept := closuretree.Filter(closuretree.Eq("note", "keep")) // note is NULL on x
			var n filterNode
			child := func(parent, name string, opts ...closuretree.ReadOption) bool {
				t.Helper()
				found, err := ct.FindChild(ctx, id[parent], tenantF, &filterNode{Name: name}, []string{"Name"}, &n, opts...)
				if err != nil {
					t.Fatal(err)
				}
				return found
			}
			for name, f := range map[string]closuretree.ReadOption{"IsNull": active, "Eq with NULL": kept} {
				if err := ct.GetNode(ctx, id["x"], tenantF, &n, f); !errors.Is(err, closuretree.ErrNodeNotFound) {
					t.Errorf("%s: GetNode of the non-matching node: want ErrNodeNotFound, got %v", name, err)
				}
				// only the node itself is checked, not the nodes above it
				if err := ct.GetNode(ctx, id["x1"], tenantF, &n, f); err != nil || n.Name != "x1" {
					t.Errorf("%s: GetNode below the non-matching node: %v", name, err)
				}
				if child("a", "x", f) {
					t.Errorf("%s: FindChild found the non-matching child", name)
				}
				if !child("a", "a1", f) {
					t.Errorf("%s: FindChild lost a matching child", name)
				}
				if !child("x", "x1", f) { // the parent's own match is not checked
					t.Errorf("%s: FindChild under the non-matching parent", name)
				}
			}
			if !child("a", "x") {
				t.Error("FindChild without options must find x")
			}

			// GetOrAdd is a write: its lookup is never filtered
			again := &filterNode{Name: "x"}
			created, err := ct.GetOrAdd(ctx, again, id["a"], tenantF, []string{"Name"})
			if err != nil || created || again.NodeId != id["x"] {
				t.Errorf("GetOrAdd: created=%v id=%d err=%v", created, again.NodeId, err)
			}
		})
	}
}

func TestFilterOnFlatReads(t *testing.T) {
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

			nameOf := map[uint]string{}
			for name, nodeID := range id {
				nameOf[nodeID] = name
			}
			// both runs Descendants and DescendantIds and compares each, sorted, with want.
			both := func(what string, want []string, parent string, maxDepth int, opts ...closuretree.ReadOption) {
				t.Helper()
				var items []filterNode
				if err := ct.Descendants(ctx, id[parent], maxDepth, tenantF, &items, opts...); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				ids, err := ct.DescendantIds(ctx, id[parent], maxDepth, tenantF, opts...)
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				fromItems, fromIDs := []string{}, []string{}
				for _, n := range items {
					fromItems = append(fromItems, n.Name)
				}
				for _, nodeID := range ids {
					fromIDs = append(fromIDs, nameOf[nodeID])
				}
				sort.Strings(fromItems)
				sort.Strings(fromIDs)
				if diff := cmp.Diff(want, fromItems); diff != "" {
					t.Errorf("%s, Descendants (-want +got):\n%s", what, diff)
				}
				if diff := cmp.Diff(want, fromIDs); diff != "" {
					t.Errorf("%s, DescendantIds (-want +got):\n%s", what, diff)
				}
			}
			// id[""] is 0: the virtual root
			everything := []string{"a", "a1", "b", "b1", "x", "x1", "x2", "x21"}
			per := closuretree.PerNode()

			filters := map[string]closuretree.ReadOption{
				"IsNull":       closuretree.Filter(closuretree.IsNull("archived_at")),
				"Eq with NULL": closuretree.Filter(closuretree.Eq("note", "keep")), // note is NULL on x: prune must treat NULL as "does not match"
			}
			for name, f := range filters {
				both(name+": prune from the root", []string{"a", "a1", "b", "b1"}, "", 0, f)
				both(name+": per node from the root", []string{"a", "a1", "b", "b1", "x1", "x2", "x21"}, "", 0, f, per)
				both(name+": prune under a", []string{"a1"}, "a", 0, f)
				both(name+": per node under a", []string{"a1", "x1", "x2", "x21"}, "a", 0, f, per)
				both(name+": prune under the non-matching node", []string{}, "x", 0, f)
				both(name+": per node under the non-matching node", []string{"x1", "x2", "x21"}, "x", 0, f, per)
				both(name+": prune under x2", []string{"x21"}, "x2", 0, f) // only the path from the requested parent counts
				both(name+": prune with maxDepth 1", []string{"a", "b"}, "", 1, f)
			}

			operators := []struct {
				name string
				cond closuretree.Cond
				want []string
			}{
				{"Eq false", closuretree.Eq("hidden", false), []string{"a", "a1", "b", "x", "x1", "x2", "x21"}},
				{"Eq true", closuretree.Eq("hidden", true), []string{"b1"}},
				{"Eq string", closuretree.Eq("kind", "folder"), []string{"b"}},
				{"Eq nil", closuretree.Eq("note", nil), []string{"x"}},
				{"NotNull", closuretree.NotNull("archived_at"), []string{"x"}},
				{"In", closuretree.In("name", "a", "b"), []string{"a", "b"}},
				{"In without values", closuretree.In("name"), []string{}},
				{"Go field name", closuretree.IsNull("ArchivedAt"), []string{"a", "a1", "b", "b1", "x1", "x2", "x21"}},
			}
			for _, op := range operators {
				both("per node, "+op.name, op.want, "", 0, closuretree.Filter(op.cond), per)
			}

			docsNotHidden := []string{"a", "a1", "x", "x1", "x2", "x21"}
			both("two conds are joined with AND", docsNotHidden, "", 0,
				closuretree.Filter(closuretree.Eq("kind", "doc"), closuretree.Eq("hidden", false)), per)
			both("two Filter options are joined with AND", docsNotHidden, "", 0,
				closuretree.Filter(closuretree.Eq("kind", "doc")), closuretree.Filter(closuretree.Eq("hidden", false)), per)

			// a filter may name a column that Columns did not select
			var named []filterNode
			err = ct.Descendants(ctx, 0, 0, tenantF, &named, closuretree.Columns("name"),
				closuretree.Filter(closuretree.Eq("kind", "folder")), per)
			if err != nil || len(named) != 1 || named[0].Name != "b" || named[0].Kind != "" {
				t.Errorf("a filter on a column that is not selected: %+v, %v", named, err)
			}

			// no filter, an empty Filter and PerNode without a filter return every node
			both("no options", everything, "", 0)
			both("an empty Filter", everything, "", 0, closuretree.Filter())
			both("PerNode without a filter", everything, "", 0, per)
		})
	}
}

// namePtr is a driver.Valuer through its pointer only.
type namePtr struct{ s string }

func (v *namePtr) Value() (driver.Value, error) { return v.s, nil }

// Eq and In bind a driver.Valuer as one value; a pointer that is one is not dereferenced. In Eq a
// Valuer holding NULL matches NULL.
func TestFilterBindsValuers(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, _ := filterTree(t, db)
			ctx := context.Background()
			cases := []struct {
				name string
				cond closuretree.Cond
				want []string
			}{
				{"Eq with a pointer Valuer", closuretree.Eq("name", &namePtr{"a1"}), []string{"a1"}},
				{"In with pointer Valuers", closuretree.In("name", &namePtr{"a1"}, &namePtr{"b"}), []string{"a1", "b"}},
				// a Valuer holding NULL is IS NULL in Eq; note is NULL on x only
				{"Eq with a NULL Valuer", closuretree.Eq("note", sql.NullString{}), []string{"x"}},
				{"Eq with a pointer to a NULL Valuer", closuretree.Eq("note", &sql.NullString{}), []string{"x"}},
				{"Eq with a Valuer", closuretree.Eq("note", sql.NullString{String: "keep", Valid: true}),
					[]string{"a", "a1", "b", "b1", "x1", "x2", "x21"}},
			}
			for _, c := range cases {
				var items []filterNode
				err := ct.Descendants(ctx, 0, 0, tenantF, &items, closuretree.Filter(c.cond), closuretree.PerNode())
				got := []string{}
				for _, n := range items {
					got = append(got, n.Name)
				}
				sort.Strings(got)
				if diff := cmp.Diff(c.want, got); err != nil || diff != "" {
					t.Errorf("%s: err %v (-want +got):\n%s", c.name, err, diff)
				}
			}
		})
	}
}

// render writes a tree as text with the children of every node sorted by name, e.g.
// "a(a1 x(x1 x2(x21))) b(b1)".
func render(nodes []*filterNode) string {
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		s := n.Name
		if len(n.Children) > 0 {
			s += "(" + render(n.Children) + ")"
		}
		parts = append(parts, s)
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// renderIDs does the same for TreeNode values, using nameOf for the names.
func renderIDs(nodes []*closuretree.TreeNode, nameOf map[uint]string) string {
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		s := nameOf[n.NodeId]
		if len(n.Children) > 0 {
			s += "(" + renderIDs(n.Children, nameOf) + ")"
		}
		parts = append(parts, s)
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

func TestFilterOnTreeReads(t *testing.T) {
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
			nameOf := map[uint]string{}
			for name, nodeID := range id {
				nameOf[nodeID] = name
			}
			both := func(what, want, parent string, maxDepth int, opts ...closuretree.ReadOption) {
				t.Helper()
				var roots []*filterNode
				if err := ct.TreeDescendants(ctx, id[parent], maxDepth, tenantF, &roots, opts...); err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				ids, err := ct.TreeDescendantsIds(ctx, id[parent], maxDepth, tenantF, opts...)
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if got := render(roots); got != want {
					t.Errorf("%s, TreeDescendants: want %q, got %q", what, want, got)
				}
				if got := renderIDs(ids, nameOf); got != want {
					t.Errorf("%s, TreeDescendantsIds: want %q, got %q", what, want, got)
				}
			}
			both("no options", "a(a1 x(x1 x2(x21))) b(b1)", "", 0)
			for name, f := range map[string]closuretree.ReadOption{
				"IsNull":       closuretree.Filter(closuretree.IsNull("archived_at")),
				"Eq with NULL": closuretree.Filter(closuretree.Eq("note", "keep")),
			} {
				both(name+": from the root", "a(a1) b(b1)", "", 0, f)
				both(name+": under the non-matching node", "", "x", 0, f)
				both(name+": under x2", "x21", "x2", 0, f)
				both(name+": maxDepth 1", "a b", "", 1, f)
				both(name+": PerNode changes nothing here", "a(a1) b(b1)", "", 0, f, closuretree.PerNode())
			}
			both("a leaf is pruned", "a(a1 x(x1 x2(x21))) b", "", 0, closuretree.Filter(closuretree.Eq("hidden", false)))
			// a column filtered on but not selected stays empty in the result
			var folders []*filterNode
			err = ct.TreeDescendants(ctx, 0, 0, tenantF, &folders, closuretree.Columns("name"),
				closuretree.Filter(closuretree.Eq("kind", "folder")))
			if err != nil || render(folders) != "b" || len(folders) != 1 || folders[0].Kind != "" {
				t.Errorf("Columns with a filter on another column: %q, %v", render(folders), err)
			}
		})
	}
}

func TestWithFilterOnEveryRead(t *testing.T) {
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
			ftree, err := ct.WithFilter(closuretree.IsNull("archived_at"))
			if err != nil {
				t.Fatal(err)
			}
			flat := func(tr *closuretree.Tree, opts ...closuretree.ReadOption) []string {
				t.Helper()
				var items []filterNode
				if err := tr.Descendants(ctx, 0, 0, tenantF, &items, opts...); err != nil {
					t.Fatal(err)
				}
				out := []string{}
				for _, n := range items {
					out = append(out, n.Name)
				}
				sort.Strings(out)
				return out
			}
			everything := []string{"a", "a1", "b", "b1", "x", "x1", "x2", "x21"}
			check := func(what string, want, got []string) {
				t.Helper()
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("%s (-want +got):\n%s", what, diff)
				}
			}
			check("the default filter prunes", []string{"a", "a1", "b", "b1"}, flat(ftree))
			check("Unfiltered switches it off", everything, flat(ftree, closuretree.Unfiltered()))
			// a per-call filter replaces the default: with AND this would be empty
			check("a per-call Filter replaces it", []string{"x"},
				flat(ftree, closuretree.Filter(closuretree.NotNull("archived_at")), closuretree.PerNode()))
			check("the original Tree is unfiltered", everything, flat(ct))

			var n filterNode
			if err := ftree.GetNode(ctx, id["x"], tenantF, &n); !errors.Is(err, closuretree.ErrNodeNotFound) {
				t.Errorf("GetNode: want ErrNodeNotFound, got %v", err)
			}
			if err := ftree.GetNode(ctx, id["x"], tenantF, &n, closuretree.Unfiltered()); err != nil {
				t.Errorf("GetNode with Unfiltered: %v", err)
			}
			var roots []*filterNode
			if err := ftree.TreeDescendants(ctx, 0, 0, tenantF, &roots); err != nil || render(roots) != "a(a1) b(b1)" {
				t.Errorf("TreeDescendants: %q, %v", render(roots), err)
			}
			ids, err := ftree.DescendantIds(ctx, 0, 0, tenantF)
			if err != nil || len(ids) != 4 {
				t.Errorf("DescendantIds: %d, %v", len(ids), err)
			}
			nameOf := map[uint]string{}
			for name, nodeID := range id {
				nameOf[nodeID] = name
			}
			treeIDs, err := ftree.TreeDescendantsIds(ctx, 0, 0, tenantF)
			if err != nil || renderIDs(treeIDs, nameOf) != "a(a1) b(b1)" {
				t.Errorf("TreeDescendantsIds: %q, %v", renderIDs(treeIDs, nameOf), err)
			}
			if _, err := ct.WithFilter(closuretree.Eq("nope", 1)); !errors.Is(err, closuretree.ErrUnknownColumn) {
				t.Errorf("WithFilter on an unknown column: %v", err)
			}
		})
	}
}

func TestWritesIgnoreTheFilter(t *testing.T) {
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
			ftree, err := ct.WithFilter(closuretree.IsNull("archived_at"))
			if err != nil {
				t.Fatal(err)
			}
			// GetOrAdd returns the existing non-matching child; FindChild on the same Tree does not see it
			again := &filterNode{Name: "x"}
			created, err := ftree.GetOrAdd(ctx, again, id["a"], tenantF, []string{"Name"})
			if err != nil || created || again.NodeId != id["x"] {
				t.Errorf("GetOrAdd: created=%v id=%d err=%v", created, again.NodeId, err)
			}
			var out filterNode
			if found, err := ftree.FindChild(ctx, id["a"], tenantF, &filterNode{Name: "x"}, []string{"Name"}, &out); err != nil || found {
				t.Errorf("FindChild: found=%v err=%v", found, err)
			}
			// Add under a non-matching parent, and after a non-matching sibling
			if err := ftree.Add(ctx, &filterNode{Name: "x3", Kind: "doc"}, up(id["x"]), nil, tenantF); err != nil {
				t.Errorf("Add under the non-matching node: %v", err)
			}
			if err := ftree.Add(ctx, &filterNode{Name: "a2", Kind: "doc"}, up(id["a"]), up(id["x"]), tenantF); err != nil {
				t.Errorf("Add after the non-matching sibling: %v", err)
			}
			// Update and DeleteRecurse see every node
			if err := ftree.Update(ctx, id["x1"], nil, up(id["b"]), nil, tenantF); err != nil {
				t.Errorf("Update: %v", err)
			}
			if err := ftree.DeleteRecurse(ctx, id["x"], tenantF); err != nil {
				t.Errorf("DeleteRecurse of the non-matching node: %v", err)
			}
			var left []filterNode
			if err := ct.Descendants(ctx, 0, 0, tenantF, &left); err != nil {
				t.Fatal(err)
			}
			names := []string{}
			for _, n := range left {
				names = append(names, n.Name)
			}
			sort.Strings(names)
			if diff := cmp.Diff([]string{"a", "a1", "a2", "b", "b1", "x1"}, names); diff != "" {
				t.Errorf("nodes left (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWithFilterAndWithTxCompose(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			keep := closuretree.Eq("name", "keep")
			ft, err := ct.WithFilter(keep)
			if err != nil {
				t.Fatal(err)
			}
			err = gdb.Transaction(func(tx *gorm.DB) error {
				filterThenTx := ft.WithTx(tx)
				txThenFilter, err := ct.WithTx(tx).WithFilter(keep)
				if err != nil {
					return err
				}
				for _, name := range []string{"keep", "drop"} {
					if err := filterThenTx.Add(ctx, &TestPayload{Name: name}, nil, nil, tenant1); err != nil {
						return err
					}
				}
				// both orders keep the filter and see the uncommitted rows
				for what, tr := range map[string]*closuretree.Tree{"WithFilter then WithTx": filterThenTx, "WithTx then WithFilter": txThenFilter} {
					var got []TestPayload
					if err := tr.Descendants(ctx, 0, 0, tenant1, &got); err != nil {
						return err
					}
					if len(got) != 1 || got[0].Name != "keep" {
						return fmt.Errorf("%s: %+v", what, got)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// largeTreeNodes and the ids below describe the tree TestFilteredReadsOnLargeTree builds: ids
// 1..8000, node 1 is the only root and the parent of node i > 1 is (i-2)/4+1, so every node has
// four children (4p-2 .. 4p+1) until the ids run out. Levels hold 1, 4, 16, 64, 256, 1024, 4096
// and 2539 nodes.
const (
	largeTreeNodes = 8000
	// largeHiddenRoot is the last node of level 4. It does not match the filter; its subtree has
	// 1+4+16+64 = 85 nodes (levels 4 to 7; its level-8 children would have ids above 8000).
	largeHiddenRoot    = 85
	largeHiddenSubtree = 85
	// largeRealParent is a real node above largeHiddenRoot (85 -> 21 -> 5); its subtree has
	// 1+4+16+64+256+1024 = 1365 nodes.
	largeRealParent  = 5
	largeRealSubtree = 1365
	// largeReadBound is the time one read may take. Without table statistics on SQLite, the
	// statements before the fix took seconds for these reads (the pruning DescendantIds about 3 s
	// with the CGO driver and 7 s with the pure-Go one, the filtered TreeDescendantsIds about 20 s
	// with the CGO driver), because the planner scanned far more rows than the subtree. The
	// current statements take at most about 80 ms on every database. One second is missed by the
	// old statements three to twenty times over and met by the new ones more than ten times over.
	largeReadBound = time.Second
)

// buildLargeTree inserts the node rows of the tree described at largeTreeNodes in batches,
// builds the closure rows with BuildFromParents, and analyzes the tables on PostgreSQL and MySQL.
// SQLite is left without statistics, its normal state.
func buildLargeTree(t *testing.T, gdb *gorm.DB, db testdbs.TargetDb, ct *closuretree.Tree) {
	t.Helper()
	rows := make([]filterNode, largeTreeNodes)
	links := make([]closuretree.ParentLink, largeTreeNodes)
	for i := range rows {
		id := uint(i + 1)
		kind := "doc"
		if id == largeHiddenRoot {
			kind = "other"
		}
		rows[i] = filterNode{Node: closuretree.Node{NodeId: id, Tenant: tenantF}, Name: fmt.Sprintf("n%d", id), Kind: kind}
		links[i] = closuretree.ParentLink{NodeID: id}
		if id > 1 {
			links[i].ParentID = (id-2)/4 + 1
		}
	}
	if err := gdb.CreateInBatches(&rows, 500).Error; err != nil {
		t.Fatalf("seed nodes: %v", err)
	}
	if err := ct.BuildFromParents(context.Background(), tenantF, links); err != nil {
		t.Fatal(err)
	}
	analyze := ""
	switch db.DbType() {
	case testdbs.DBTypePostgres:
		analyze = "ANALYZE "
	case testdbs.DBTypeMysql:
		analyze = "ANALYZE TABLE "
	}
	if analyze != "" {
		for _, tbl := range []string{ct.NodeTableName(), ct.ClosureTableName()} {
			if err := gdb.Exec(analyze + tbl).Error; err != nil {
				t.Fatalf("analyze %s: %v", tbl, err)
			}
		}
	}
}

// countTree returns the number of nodes in a tree read.
func countTree(nodes []*closuretree.TreeNode) int {
	n := len(nodes)
	for _, c := range nodes {
		n += countTree(c.Children)
	}
	return n
}

// Filtered reads on a large tree return the right number of nodes within largeReadBound, also on
// SQLite without table statistics.
func TestFilteredReadsOnLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct := largeTree(t, db)
			ctx := context.Background()
			docs := closuretree.Filter(closuretree.Eq("kind", "doc"))

			// read checks the count of nodes a read returns and its time.
			read := func(what string, want int, fn func() (int, error)) {
				t.Helper()
				within(t, what, func() error {
					got, err := fn()
					if err == nil && got != want {
						t.Errorf("%s: want %d nodes, got %d", what, want, got)
					}
					return err
				})
			}
			for _, parent := range []struct {
				name    string
				id      uint
				subtree int // descendants of the parent, the parent itself excluded
			}{
				{"virtual root", 0, largeTreeNodes},
				{"real node", largeRealParent, largeRealSubtree - 1},
			} {
				read(parent.name+": DescendantIds, prune", parent.subtree-largeHiddenSubtree, func() (int, error) {
					ids, err := ct.DescendantIds(ctx, parent.id, 0, tenantF, docs)
					return len(ids), err
				})
				read(parent.name+": DescendantIds, per node", parent.subtree-1, func() (int, error) {
					ids, err := ct.DescendantIds(ctx, parent.id, 0, tenantF, docs, closuretree.PerNode())
					return len(ids), err
				})
				read(parent.name+": TreeDescendantsIds", parent.subtree-largeHiddenSubtree, func() (int, error) {
					tree, err := ct.TreeDescendantsIds(ctx, parent.id, 0, tenantF, docs)
					return countTree(tree), err
				})
			}
		})
	}
}

type prefixAddr struct {
	City string
	Name string // the same Go name as the model's own Name
}

// prefixNode has a struct embedded with a column prefix whose field Name has the same Go name as
// a field of the model itself.
type prefixNode struct {
	closuretree.Node
	Name string
	Addr prefixAddr `gorm:"embedded;embeddedPrefix:addr_"`
}

type prefixWrapper struct {
	prefixNode
	Children []*prefixWrapper
}

type prefixFlat struct {
	NodeId uint
	Name   string
}

// Each model field is copied to the field at the same path in the element type, so the model's
// own Name is never confused with the Name of the embedded Addr.
func TestReadIntoWrapperWithEmbeddedPrefix(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, prefixNode{})
			ct, err := newTestTree(gdb, prefixNode{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			n := &prefixNode{Name: "own", Addr: prefixAddr{City: "city", Name: "addr-name"}}
			if err := ct.Add(ctx, n, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			wantAddr := prefixAddr{City: "city", Name: "addr-name"}

			var models []prefixNode
			if err := ct.Descendants(ctx, 0, 0, tenant1, &models); err != nil || len(models) != 1 ||
				models[0].Name != "own" || models[0].Addr != wantAddr {
				t.Errorf("Descendants into the model: %+v, %v", models, err)
			}
			var tree []*prefixWrapper
			if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &tree); err != nil || len(tree) != 1 {
				t.Errorf("TreeDescendants into a wrapper: %d roots, %v", len(tree), err)
			} else if w := tree[0]; w.Name != "own" || w.Addr != wantAddr || w.NodeId != n.NodeId {
				t.Errorf("TreeDescendants into a wrapper: %+v", *w)
			}
			var flat []prefixFlat
			if err := ct.Descendants(ctx, 0, 0, tenant1, &flat); err != nil || len(flat) != 1 ||
				flat[0].Name != "own" || flat[0].NodeId != n.NodeId {
				t.Errorf("Descendants into a flat struct: %+v, %v", flat, err)
			}
		})
	}
}

type docOwner struct{ Name string }

// docOwnerLast and docOwnerFirst share the Go name Name between their own column name and the
// column owner_name of the embedded Owner, declared after and before their own field.
type docOwnerLast struct {
	closuretree.Node
	Name  string
	Owner docOwner `gorm:"embedded;embeddedPrefix:owner_"`
}

type docOwnerFirst struct {
	closuretree.Node
	Owner docOwner `gorm:"embedded;embeddedPrefix:owner_"`
	Name  string
}

// A read option that names a Go name several columns share fails, whatever the declaration order;
// each column name filters its own column.
func TestReadOptionsRejectSharedGoName(t *testing.T) {
	models := []struct {
		name  string
		model any
		add   func(ct *closuretree.Tree, name, owner string) (uint, error)
	}{
		{"own field first", docOwnerLast{}, func(ct *closuretree.Tree, name, owner string) (uint, error) {
			d := &docOwnerLast{Name: name, Owner: docOwner{owner}}
			err := ct.Add(context.Background(), d, nil, nil, tenant1)
			return d.NodeId, err
		}},
		{"own field last", docOwnerFirst{}, func(ct *closuretree.Tree, name, owner string) (uint, error) {
			d := &docOwnerFirst{Name: name, Owner: docOwner{owner}}
			err := ct.Add(context.Background(), d, nil, nil, tenant1)
			return d.NodeId, err
		}},
	}
	for _, db := range testdbs.DBs() {
		for _, m := range models {
			t.Run(db.DbType()+"/"+m.name, func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, m.model)
				ct, err := newTestTree(gdb, m.model)
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				// want: the doc whose own name is alice, and the doc whose owner is alice
				want := map[string]uint{}
				for col, d := range map[string][2]string{"name": {"alice", "bob"}, "owner_name": {"carol", "alice"}} {
					if want[col], err = m.add(ct, d[0], d[1]); err != nil {
						t.Fatal(err)
					}
				}
				for _, opt := range []closuretree.ReadOption{
					closuretree.Filter(closuretree.Eq("Name", "alice")), closuretree.Columns("Name"),
				} {
					if _, err := ct.DescendantIds(ctx, 0, 0, tenant1, opt); !errors.Is(err, closuretree.ErrUnknownColumn) {
						t.Errorf("want ErrUnknownColumn, got %v", err)
					}
				}
				for col, id := range want {
					got, err := ct.DescendantIds(ctx, 0, 0, tenant1, closuretree.Filter(closuretree.Eq(col, "alice")))
					if err != nil || !cmp.Equal([]uint{id}, got) {
						t.Errorf("Eq(%q, alice): want [%d], got %v, %v", col, id, got, err)
					}
				}
			})
		}
	}
}

type uintChildren struct {
	closuretree.Node
	Name     string
	Children []uint
}

// Descendants into a slice of non-struct elements, and a tree read into an element whose
// Children field has another type than []*T, fail before the query runs.
func TestReadsRejectUnusableElementTypes(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, filterNode{})
			ct, err := newTestTree(gdb, filterNode{})
			if err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel() // a query would fail with context.Canceled, so the check must come first

			var ids []uint
			err = ct.Descendants(cancelled, 0, 0, tenantF, &ids)
			if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "uint") {
				t.Errorf("Descendants into []uint: want an error naming the type, got %v", err)
			}
			var ptrs []**filterNode
			err = ct.Descendants(cancelled, 0, 0, tenantF, &ptrs)
			if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "filterNode") {
				t.Errorf("Descendants into []**T: want an error naming the type, got %v", err)
			}
			var tree []*uintChildren
			err = ct.TreeDescendants(cancelled, 0, 0, tenantF, &tree)
			if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "Children") || !strings.Contains(err.Error(), "uintChildren") {
				t.Errorf("TreeDescendants with Children []uint: want an error naming the field and type, got %v", err)
			}
		})
	}
}

// Audit is embedded by pointer in the models below.
type Audit struct {
	Author string
}

// ptrModel is an unexported model with a pointer-embedded struct. gorm marks the fields of such a
// struct with negative entries in their index.
type ptrModel struct {
	closuretree.Node
	Name   string
	*Audit `gorm:"embedded"`
}

type ptrWrapper struct {
	ptrModel
	Children []*ptrWrapper
}

// ptrRefWrapper embeds the unexported model through a pointer. Reflection cannot allocate that
// pointer, so its fields stay zero; the read must not fail.
type ptrRefWrapper struct {
	*ptrModel
	Children []*ptrRefWrapper
}

// PtrModel is ptrModel with an exported type, so that an element type can embed it by pointer.
type PtrModel struct {
	closuretree.Node
	Name   string
	*Audit `gorm:"embedded"`
}

type exportedPtrRefWrapper struct {
	*PtrModel
	Children []*exportedPtrRefWrapper
}

// The fields of a pointer-embedded struct of the model are copied into the element type, also
// when the element type embeds a model of an unexported type, or embeds the model by pointer.
func TestReadIntoWrapperOfUnexportedModel(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, ptrModel{})
			ct, err := newTestTree(gdb, ptrModel{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			root := &ptrModel{Name: "root", Audit: &Audit{Author: "ann"}}
			if err := ct.Add(ctx, root, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			if err := ct.Add(ctx, &ptrModel{Name: "child", Audit: &Audit{Author: "bob"}}, up(root.NodeId), nil, tenant1); err != nil {
				t.Fatal(err)
			}
			check := func(what, name string, a *Audit, want string) {
				t.Helper()
				if a == nil || a.Author != want {
					t.Errorf("%s, %s: want author %q, got %+v", what, name, want, a)
				}
			}

			var models []ptrModel
			if err := ct.Descendants(ctx, 0, 0, tenant1, &models); err != nil || len(models) != 2 || models[0].Name != "root" {
				t.Fatalf("Descendants into the model: %+v, %v", models, err)
			}
			check("Descendants into the model", "root", models[0].Audit, "ann")
			check("Descendants into the model", "child", models[1].Audit, "bob")

			var wrapped []ptrWrapper
			if err := ct.Descendants(ctx, 0, 0, tenant1, &wrapped); err != nil || len(wrapped) != 2 || wrapped[0].Name != "root" || wrapped[1].Name != "child" {
				t.Fatalf("Descendants into a wrapper: %+v, %v", wrapped, err)
			}
			check("Descendants into a wrapper", "root", wrapped[0].Audit, "ann")
			check("Descendants into a wrapper", "child", wrapped[1].Audit, "bob")

			var tree []*ptrWrapper
			if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &tree); err != nil || len(tree) != 1 || len(tree[0].Children) != 1 {
				t.Fatalf("TreeDescendants into a wrapper: %d roots, %v", len(tree), err)
			}
			if tree[0].Name != "root" || tree[0].Children[0].Name != "child" {
				t.Errorf("TreeDescendants into a wrapper: names %q, %q", tree[0].Name, tree[0].Children[0].Name)
			}
			check("TreeDescendants into a wrapper", "root", tree[0].Audit, "ann")
			check("TreeDescendants into a wrapper", "child", tree[0].Children[0].Audit, "bob")

			var refs []*ptrRefWrapper
			if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &refs); err != nil {
				t.Errorf("TreeDescendants into a wrapper of an unexported model pointer: %v", err)
			}

			readIntoExportedPointerWrapper(t, gdb)
		})
	}
}

// readIntoExportedPointerWrapper checks that an element type that embeds the model by pointer gets
// the model's fields, those of its pointer-embedded struct included.
func readIntoExportedPointerWrapper(t *testing.T, gdb *gorm.DB) {
	t.Helper()
	dropTreeTables(gdb, PtrModel{})
	ct, err := newTestTree(gdb, PtrModel{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	root := &PtrModel{Name: "root", Audit: &Audit{Author: "ann"}}
	if err := ct.Add(ctx, root, nil, nil, tenant1); err != nil {
		t.Fatal(err)
	}
	var tree []*exportedPtrRefWrapper
	if err := ct.TreeDescendants(ctx, 0, 0, tenant1, &tree); err != nil || len(tree) != 1 {
		t.Fatalf("TreeDescendants into a pointer wrapper: %d roots, %v", len(tree), err)
	}
	if r := tree[0]; r.PtrModel == nil || r.Name != "root" || r.NodeId != root.NodeId || r.Audit == nil || r.Author != "ann" {
		t.Errorf("TreeDescendants into a pointer wrapper: %+v", r.PtrModel)
	}
}

// DescendantIds ignores DepthRange, ForAncestors, IncludeVirtualRoot, Relation and NoPreload, but still validates them.
func TestDescendantIdsIgnoresOptionsItDoesNotHonour(t *testing.T) {
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

			plain, err := ct.DescendantIds(ctx, 0, 0, tenantF)
			if err != nil {
				t.Fatal(err)
			}
			with, err := ct.DescendantIds(ctx, 0, 0, tenantF, closuretree.DepthRange(0, 0), closuretree.ForAncestors(),
				closuretree.IncludeVirtualRoot(), closuretree.Relation("Nope"), closuretree.NoPreload())
			if err != nil || !cmp.Equal(plain, with) {
				t.Errorf("DescendantIds must ignore the options it does not honour: %v, %v", with, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel() // a query would fail with context.Canceled, so validation must come first
			if _, err := ct.DescendantIds(cancelled, 0, 0, tenantF, closuretree.DepthRange(3, 1)); !errors.Is(err, closuretree.ErrInvalidDepthRange) {
				t.Errorf("want ErrInvalidDepthRange, got %v", err)
			}
		})
	}
}
