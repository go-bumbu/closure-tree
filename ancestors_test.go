package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
)

// ancestorNames reads the ancestors of the named node with Ancestors (into []filterNode) and with
// AncestorIds, fails the test when the two disagree, and returns the names, root first.
func ancestorNames(t *testing.T, ct *closuretree.Tree, id map[string]uint, node string, opts ...closuretree.ReadOption) []string {
	t.Helper()
	ctx := context.Background()
	var items []filterNode
	if err := ct.Ancestors(ctx, id[node], tenantF, &items, opts...); err != nil {
		t.Fatalf("Ancestors of %s: %v", node, err)
	}
	ids, err := ct.AncestorIds(ctx, id[node], tenantF, opts...)
	if err != nil {
		t.Fatalf("AncestorIds of %s: %v", node, err)
	}
	names := make([]string, 0, len(items))
	got := make([]uint, 0, len(items))
	for _, it := range items {
		names = append(names, it.Name)
		got = append(got, it.NodeId)
	}
	if !slices.Equal(ids, got) {
		t.Fatalf("Ancestors and AncestorIds of %s disagree: %v and %v", node, got, ids)
	}
	return names
}

func checkNames(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(want, got) {
		t.Errorf("%s: want %v, got %v", what, want, got)
	}
}

func TestAncestors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, id := filterTree(t, db)
			ctx := context.Background()
			checkNames(t, "depth 3", ancestorNames(t, ct, id, "x21"), "a", "x", "x2")
			checkNames(t, "depth 2", ancestorNames(t, ct, id, "x1"), "a", "x")
			checkNames(t, "depth 1", ancestorNames(t, ct, id, "a1"), "a")
			checkNames(t, "a root node", ancestorNames(t, ct, id, "a"))

			// the parent of every ancestor is filled, results are appended, and []*T works
			path := []*filterNode{{Name: "kept"}}
			if err := ct.Ancestors(ctx, id["x21"], tenantF, &path); err != nil || len(path) != 4 {
				t.Fatalf("Ancestors into []*T: %d, %v", len(path), err)
			}
			if path[0].Name != "kept" || path[1].ParentId != 0 || path[2].ParentId != id["a"] || path[3].ParentId != id["x"] {
				t.Errorf("appending or parents: %+v %+v %+v", path[1], path[2], path[3])
			}

			// after a move the new path is returned
			if err := ct.Update(ctx, id["x2"], nil, up(id["b"]), nil, tenantF); err != nil {
				t.Fatal(err)
			}
			checkNames(t, "after a move", ancestorNames(t, ct, id, "x21"), "b", "x2")
		})
	}
}

// checkItemsShape checks the errors of items that are not a pointer to a slice.
func checkItemsShape(t *testing.T, ct *closuretree.Tree, node uint) {
	t.Helper()
	ctx := context.Background()
	var one filterNode
	if err := ct.Ancestors(ctx, node, tenantF, nil); !errors.Is(err, closuretree.ErrItemsNil) {
		t.Errorf("nil items: %v", err)
	}
	if err := ct.Ancestors(ctx, node, tenantF, []filterNode{}); !errors.Is(err, closuretree.ErrItemsNotPointerToSlice) {
		t.Errorf("a slice by value: %v", err)
	}
	if err := ct.Ancestors(ctx, node, tenantF, &one); !errors.Is(err, closuretree.ErrItemsNotPointerToSlice) {
		t.Errorf("a pointer to a struct: %v", err)
	}
}

// checkHiddenStart checks that a default filter hides a start node that does not match it, and
// that Unfiltered shows it again.
func checkHiddenStart(t *testing.T, ftree *closuretree.Tree, node uint) {
	t.Helper()
	ctx := context.Background()
	var items []filterNode
	if err := ftree.Ancestors(ctx, node, tenantF, &items); !errors.Is(err, closuretree.ErrNodeNotFound) {
		t.Errorf("Ancestors of a node the default filter hides: %v", err)
	}
	if _, err := ftree.AncestorIds(ctx, node, tenantF); !errors.Is(err, closuretree.ErrNodeNotFound) {
		t.Errorf("AncestorIds of a node the default filter hides: %v", err)
	}
	if err := ftree.Ancestors(ctx, node, tenantF, &items, closuretree.Unfiltered()); err != nil {
		t.Errorf("Ancestors with Unfiltered: %v", err)
	}
	if _, err := ftree.AncestorIds(ctx, node, tenantF, closuretree.Unfiltered()); err != nil {
		t.Errorf("AncestorIds with Unfiltered: %v", err)
	}
}

func TestAncestorsOfUnknownNodes(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, id := filterTree(t, db)
			ctx := context.Background()
			var items []filterNode
			checkItemsShape(t, ct, id["a"])
			notFound := map[string]error{
				"an unknown id":            ct.Ancestors(ctx, 99999, tenantF, &items),
				"the virtual root":         ct.Ancestors(ctx, 0, tenantF, &items),
				"a node of another tenant": ct.Ancestors(ctx, id["x21"], "other", &items),
			}
			_, notFound["AncestorIds of an unknown id"] = ct.AncestorIds(ctx, 99999, tenantF)
			_, notFound["AncestorIds in another tenant"] = ct.AncestorIds(ctx, id["x21"], "other")
			for what, err := range notFound {
				if !errors.Is(err, closuretree.ErrNodeNotFound) {
					t.Errorf("%s: want ErrNodeNotFound, got %v", what, err)
				}
			}
			if err := ct.Ancestors(ctx, id["a"], "", &items); !errors.Is(err, closuretree.ErrEmptyTenant) {
				t.Errorf("empty tenant: %v", err)
			}
			if ids, err := ct.AncestorIds(ctx, id["a"], tenantF); err != nil || ids == nil || len(ids) != 0 {
				t.Errorf("a root node: want an empty, non-nil slice, got %v, %v", ids, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := ct.Ancestors(cancelled, id["x21"], tenantF, &items, closuretree.Columns("nope")); !errors.Is(err, closuretree.ErrUnknownColumn) {
				t.Errorf("unknown column: %v", err)
			}
		})
	}
}

func TestAncestorsWithOptions(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, id := filterTree(t, db)
			ctx := context.Background()
			for name, f := range map[string]closuretree.ReadOption{
				"IsNull":       closuretree.Filter(closuretree.IsNull("archived_at")),
				"Eq with NULL": closuretree.Filter(closuretree.Eq("note", "keep")), // note is NULL on x
			} {
				checkNames(t, name+": the non-matching ancestor is skipped and the path continues above it",
					ancestorNames(t, ct, id, "x21", f), "a", "x2")
				checkNames(t, name+": PerNode changes nothing", ancestorNames(t, ct, id, "x21", f, closuretree.PerNode()), "a", "x2")
				var items []filterNode
				if err := ct.Ancestors(ctx, id["x"], tenantF, &items, f); !errors.Is(err, closuretree.ErrNodeNotFound) {
					t.Errorf("%s: a start node that does not match: want ErrNodeNotFound, got %v", name, err)
				}
				if _, err := ct.AncestorIds(ctx, id["x"], tenantF, f); !errors.Is(err, closuretree.ErrNodeNotFound) {
					t.Errorf("%s: AncestorIds of a start node that does not match: %v", name, err)
				}
			}

			ftree, err := ct.WithFilter(closuretree.IsNull("archived_at"))
			if err != nil {
				t.Fatal(err)
			}
			checkNames(t, "the default filter applies", ancestorNames(t, ftree, id, "x21"), "a", "x2")
			checkNames(t, "Unfiltered switches it off", ancestorNames(t, ftree, id, "x21", closuretree.Unfiltered()), "a", "x", "x2")
			checkHiddenStart(t, ftree, id["x"])

			var named []filterNode
			if err := ct.Ancestors(ctx, id["x21"], tenantF, &named, closuretree.Columns("name")); err != nil || len(named) != 3 {
				t.Fatalf("Columns: %d, %v", len(named), err)
			}
			if named[1].Name != "x" || named[1].Kind != "" || named[1].ParentId != id["a"] {
				t.Errorf("Columns must fill the name and the parent only: %+v", named[1])
			}
		})
	}
}

// A chain of 50 nodes: the 49 ancestors of the last one come back root first.
func TestAncestorsOfADeepChain(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			var chain []uint
			var parent *uint
			for i := 0; i < 50; i++ {
				n := &TestPayload{Name: fmt.Sprintf("n%d", i)}
				if err := ct.Add(ctx, n, parent, nil, tenant1); err != nil {
					t.Fatal(err)
				}
				chain = append(chain, n.NodeId)
				parent = up(n.NodeId)
			}
			ids, err := ct.AncestorIds(ctx, chain[49], tenant1)
			if err != nil || !slices.Equal(chain[:49], ids) {
				t.Errorf("want %v, got %v, %v", chain[:49], ids, err)
			}
		})
	}
}

// On the large tree, built with BuildFromParents: node 8000 has the parents 2000, 500, 125, 31, 8,
// 2 and 1; node 5461 has 1365, 341, 85, 21, 5 and 1, and 85 does not match the filter.
func TestAncestorsOnLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct := largeTree(t, db)
			ctx := context.Background()
			docs := closuretree.Filter(closuretree.Eq("kind", "doc"))
			read := func(what string, node uint, want []uint, opts ...closuretree.ReadOption) {
				t.Helper()
				within(t, what, func() error {
					ids, err := ct.AncestorIds(ctx, node, tenantF, opts...)
					if err == nil && !slices.Equal(want, ids) {
						t.Errorf("%s: want %v, got %v", what, want, ids)
					}
					return err
				})
			}
			read("a node of the last level", 8000, []uint{1, 2, 8, 31, 125, 500, 2000})
			read("below the non-matching node, unfiltered", 5461, []uint{1, 5, 21, 85, 341, 1365})
			read("below the non-matching node, filtered", 5461, []uint{1, 5, 21, 341, 1365}, docs)
			within(t, "Ancestors into structs", func() error {
				var items []filterNode
				err := ct.Ancestors(ctx, 8000, tenantF, &items, docs)
				if err == nil && (len(items) != 7 || items[6].ParentId != 500) {
					t.Errorf("want 7 ancestors ending with node 2000 under 500, got %d", len(items))
				}
				return err
			})
		})
	}
}
