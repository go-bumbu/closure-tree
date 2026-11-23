package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
)

// buildShape returns (key, parentKey) pairs in input order: roots 1, 2, 3; a chain 4 under 1,
// 5 under 4, 6 under 5, 7 under 6 (depth 5); the 1,000 siblings 8 to 1007 under 2; 1008 and 1009
// under 3.
func buildShape() [][2]uint {
	s := [][2]uint{{1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 4}, {6, 5}, {7, 6}}
	for k := uint(8); k <= 1007; k++ {
		s = append(s, [2]uint{k, 2})
	}
	return append(s, [2]uint{1008, 3}, [2]uint{1009, 3})
}

// TestBuildFromParentsMatchesAdd builds the same forest once with Add and once with
// BuildFromParents, and checks that the closure rows and the sibling order are the same, that
// sort_order is 10, 20, 30, ... in input order, and that reads and later writes work.
//
//nolint:gocyclo // test function covers many assertion branches
func TestBuildFromParentsMatchesAdd(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			shape := buildShape()

			// reference tenant: built with Add in one transaction, each node placed last
			err = gdb.Transaction(func(tx *gorm.DB) error {
				tt, idOf := ct.WithTx(tx), map[uint]uint{}
				for i, kp := range shape {
					n := &TestPayload{Name: fmt.Sprintf("k%d", kp[0])}
					if err := tt.Add(ctx, n, up(idOf[kp[1]]), nil, "ref", closuretree.PlaceLast()); err != nil {
						return err
					}
					idOf[kp[0]] = n.NodeId
					// A real database has statistics; this test is about row equality, not about
					// planning. On freshly created tables PostgreSQL has none and plans the
					// sibling lookup badly, so gather them once after the first 50 nodes.
					if i+1 == 50 && db.DbType() == testdbs.DBTypePostgres {
						for _, tbl := range []string{ct.NodeTableName(), ct.ClosureTableName()} {
							if err := tx.Exec("ANALYZE " + tbl).Error; err != nil {
								return err
							}
						}
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			// built tenant: the rows exist with explicit ids, then the closure rows are built
			const base = 100000
			id := func(key uint) uint {
				if key == 0 {
					return 0
				}
				return base + key
			}
			var nodes []seed
			var links []closuretree.ParentLink
			for _, kp := range shape {
				nodes = append(nodes, seed{id(kp[0]), fmt.Sprintf("k%d", kp[0])})
				links = append(links, closuretree.ParentLink{NodeID: id(kp[0]), ParentID: id(kp[1])})
			}
			seedNodes(t, gdb, db, "built", nodes)
			if err := ct.BuildFromParents(ctx, "built", links); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(namedClosure(t, gdb, ct, "ref"), namedClosure(t, gdb, ct, "built")); diff != "" {
				t.Errorf("closure rows (-Add +BuildFromParents):\n%s", diff)
			}
			names := func(tenant string) []string {
				var all []TestPayload
				if err := ct.Descendants(ctx, 0, 0, tenant, &all); err != nil {
					t.Fatal(err)
				}
				out := make([]string, len(all))
				for i, n := range all {
					out[i] = n.Name
				}
				return out
			}
			if diff := cmp.Diff(names("ref"), names("built")); diff != "" {
				t.Errorf("sibling order (-Add +BuildFromParents):\n%s", diff)
			}

			// sort_order is 10, 20, 30, ... in input order, and no meta rows are written
			var kids []TestPayload
			if err := ct.Descendants(ctx, id(2), 1, "built", &kids); err != nil || len(kids) != 1000 {
				t.Fatalf("children of k2: %d, %v", len(kids), err)
			}
			for i, k := range kids {
				if k.SortOrder != float64((i+1)*10) || k.Name != fmt.Sprintf("k%d", 8+i) {
					t.Fatalf("child %d: name %s sort %v", i, k.Name, k.SortOrder)
				}
			}
			if got := countTenant(t, gdb, ct, "built"); got.Meta != 0 {
				t.Errorf("want no meta rows, got %d", got.Meta)
			}

			// reads and later writes work on the built tree
			if ok, err := ct.IsDescendant(ctx, id(1), id(7), "built"); err != nil || !ok {
				t.Errorf("k7 under k1: %v %v", ok, err)
			}
			var roots []*TestPayload
			if err := ct.TreeDescendants(ctx, 0, 0, "built", &roots); err != nil || len(roots) != 3 {
				t.Errorf("roots: %d, %v", len(roots), err)
			}
			extra := &TestPayload{Name: "extra"}
			if err := ct.Add(ctx, extra, up(id(7)), nil, "built", closuretree.PlaceLast()); err != nil {
				t.Errorf("Add after build: %v", err)
			}
			if err := ct.Update(ctx, id(7), nil, up(id(3)), nil, "built"); err != nil {
				t.Errorf("Update after build: %v", err)
			}
			if err := ct.DeleteRecurse(ctx, id(3), "built"); err != nil {
				t.Errorf("DeleteRecurse after build: %v", err)
			}
			assertConsistent(t, gdb, ct, "built")
		})
	}
}

// TestBuildFromParentsValidation checks that invalid input writes nothing, that a caller's
// rollback undoes the build, OrphansToRoot, Rebuild, and a tenant without nodes.
//
//nolint:gocyclo // test function covers many assertion branches
func TestBuildFromParentsValidation(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			seedNodes(t, gdb, db, tenant1, []seed{{1, "a"}, {2, "b"}, {3, "c"}})
			ok := []closuretree.ParentLink{{NodeID: 1}, {NodeID: 2, ParentID: 1}, {NodeID: 3, ParentID: 2}}
			nothingWritten := func(when string) {
				t.Helper()
				if got := countTenant(t, gdb, ct, tenant1); got.Closure != 0 {
					t.Fatalf("%s: %d closure rows written", when, got.Closure)
				}
			}

			dangling := []closuretree.ParentLink{{NodeID: 1}, {NodeID: 2, ParentID: 1}, {NodeID: 3, ParentID: 999}}
			if err := ct.BuildFromParents(ctx, tenant1, dangling); !errors.Is(err, closuretree.ErrOrphanLink) {
				t.Errorf("dangling parent: want ErrOrphanLink, got %v", err)
			}
			nothingWritten("dangling parent")

			cycle := []closuretree.ParentLink{{NodeID: 1, ParentID: 3}, {NodeID: 2, ParentID: 1}, {NodeID: 3, ParentID: 2}}
			if err := ct.BuildFromParents(ctx, tenant1, cycle); !errors.Is(err, closuretree.ErrLinkCycle) {
				t.Errorf("cycle: want ErrLinkCycle, got %v", err)
			}
			nothingWritten("cycle")

			if err := ct.BuildFromParents(ctx, tenant1, ok[:2]); !errors.Is(err, closuretree.ErrLinksMismatch) || !strings.Contains(err.Error(), "3") {
				t.Errorf("missing node: want ErrLinksMismatch naming 3, got %v", err)
			}
			nothingWritten("missing node")

			if err := ct.BuildFromParents(ctx, "", ok); !errors.Is(err, closuretree.ErrEmptyTenant) {
				t.Errorf("want ErrEmptyTenant, got %v", err)
			}

			// rolled back inside a caller transaction
			_ = gdb.Transaction(func(tx *gorm.DB) error {
				if err := ct.WithTx(tx).BuildFromParents(ctx, tenant1, ok); err != nil {
					t.Errorf("in tx: %v", err)
				}
				return errRollback
			})
			nothingWritten("rollback")

			// the dangling parent becomes a root with OrphansToRoot; a nil option is ignored
			if err := ct.BuildFromParents(ctx, tenant1, dangling, closuretree.OrphansToRoot(), nil); err != nil {
				t.Fatal(err)
			}
			if isRoot, err := ct.IsChildOf(ctx, 3, 0, tenant1); err != nil || !isRoot {
				t.Errorf("node 3 should be a root: %v %v", isRoot, err)
			}

			// closure rows exist now: an error without Rebuild, identical rows with it
			if err := ct.BuildFromParents(ctx, tenant1, ok); !errors.Is(err, closuretree.ErrClosureExists) {
				t.Errorf("want ErrClosureExists, got %v", err)
			}
			if err := ct.BuildFromParents(ctx, tenant1, ok, closuretree.Rebuild()); err != nil {
				t.Fatal(err)
			}
			first := closureRows(t, gdb, ct, tenant1)
			if err := ct.Add(ctx, &TestPayload{Name: "d"}, up(3), nil, tenant1); err != nil { // creates a meta row
				t.Fatal(err)
			}
			if _, err := ct.DeleteRecurseMany(ctx, []uint{4}, tenant1); err != nil { // "d" got id 4
				t.Fatal(err)
			}
			if err := ct.BuildFromParents(ctx, tenant1, ok, closuretree.Rebuild()); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(first, closureRows(t, gdb, ct, tenant1)); diff != "" {
				t.Errorf("rerun with Rebuild (-first +second):\n%s", diff)
			}
			if got := countTenant(t, gdb, ct, tenant1); got.Meta != 0 {
				t.Errorf("Rebuild must delete the meta rows, %d left", got.Meta)
			}

			// no links on a tenant without nodes succeeds and writes nothing
			if err := ct.BuildFromParents(ctx, "empty", nil); err != nil {
				t.Errorf("empty tenant, no links: %v", err)
			}
			if got := countTenant(t, gdb, ct, "empty"); got.Closure != 0 || got.Nodes != 0 {
				t.Errorf("empty tenant: %+v", got)
			}
		})
	}
}

// TestBuildFromParentsPlainBuildRemovesStaleMeta checks that a build without Rebuild deletes the
// tenant's meta rows too.
func TestBuildFromParentsPlainBuildRemovesStaleMeta(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			seedNodes(t, gdb, db, tenant1, []seed{{1, "a"}, {2, "b"}})
			metaTable := "closure_tree_meta_" + ct.NodeTableName()
			if err := gdb.Exec("INSERT INTO "+metaTable+" (tenant, parent_id, min_halvings) VALUES (?, ?, ?)", tenant1, 1, 0).Error; err != nil {
				t.Fatal(err)
			}
			if got := countTenant(t, gdb, ct, tenant1); got.Meta != 1 {
				t.Fatalf("precondition: want one meta row, got %d", got.Meta)
			}
			links := []closuretree.ParentLink{{NodeID: 1}, {NodeID: 2, ParentID: 1}}
			if err := ct.BuildFromParents(ctx, tenant1, links); err != nil {
				t.Fatal(err)
			}
			if got := countTenant(t, gdb, ct, tenant1); got.Meta != 0 {
				t.Errorf("a plain build must delete stale meta rows, %d left", got.Meta)
			}
		})
	}
}

// TestBuildFromParents50k builds 50,000 nodes: ids 1..50000, the first ten are roots, the parent
// of every other node is id/10.
func TestBuildFromParents50k(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			const n = 50000
			nodes := make([]seed, 0, n)
			links := make([]closuretree.ParentLink, 0, n)
			for i := uint(1); i <= n; i++ {
				parent := uint(0)
				if i > 10 {
					parent = i / 10
				}
				nodes = append(nodes, seed{i, fmt.Sprintf("n%d", i)})
				links = append(links, closuretree.ParentLink{NodeID: i, ParentID: parent})
			}
			seedNodes(t, gdb, db, tenant1, nodes)
			if err := ct.BuildFromParents(ctx, tenant1, links); err != nil {
				t.Fatal(err)
			}
			// 10 nodes at level 1, 99 at level 2, 990 at level 3, 9900 at level 4, 39001 at level 5;
			// a node at level L has L+1 closure rows: 20 + 297 + 3960 + 49500 + 234006
			if got := countTenant(t, gdb, ct, tenant1); got.Closure != 287783 || got.Meta != 0 {
				t.Errorf("want 287783 closure rows and no meta rows, got %+v", got)
			}
			if ok, err := ct.IsDescendant(ctx, 1, 12345, tenant1); err != nil || !ok { // 12345 > 1234 > 123 > 12 > 1
				t.Errorf("12345 under 1: %v %v", ok, err)
			}
			ids, err := ct.DescendantIds(ctx, 4999, 1, tenant1)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(seq(49990, 49999), ids); diff != "" {
				t.Errorf("children of 4999 (-want +got):\n%s", diff)
			}
			var sixth TestPayload
			if err := ct.GetNode(ctx, 49995, tenant1, &sixth); err != nil || sixth.SortOrder != 60 {
				t.Errorf("49995 is the sixth child: sort %v, err %v", sixth.SortOrder, err)
			}
		})
	}
}
