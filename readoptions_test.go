package closuretree_test

import (
	"context"
	"errors"
	"sort"
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
			if err := ct.Descendants(ctx, 0, 0, tenant1, &second, byOrder); err != nil { // prune: the root has order 1
				t.Errorf("prune on a reserved-word column: %v", err)
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

			// review focus 4
			both("no options", everything, "", 0)
			both("an empty Filter", everything, "", 0, closuretree.Filter())
			both("PerNode without a filter", everything, "", 0, per)
		})
	}
}
