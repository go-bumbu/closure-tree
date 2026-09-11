package closuretree_test

import (
	"context"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
)

// Foo and Bar are two distinct node models with different table names. Migrating both into the
// same database guards against schema-global identifier collisions between trees: the node-table
// and closure-table index names, and (on MySQL) the closure depth check constraint, must all be
// scoped per node table. Before the fix, the second Migrate failed with "index idx_node_tenant
// already exists"; scoping only the node index moved the failure to "index idx_desc_ten_dep
// already exists", and on MySQL a shared "chk_depth" check name would collide next. Runs across
// every configured dialect (sqlite via `make test`; +mysql +postgres via `make test-full`).
type Foo struct {
	closuretree.Node
	Name     string
	Children []*Foo `gorm:"-"`
}

func (Foo) TableName() string { return "foos" }

type Bar struct {
	closuretree.Node
	Name     string
	Children []*Bar `gorm:"-"`
}

func (Bar) TableName() string { return "bars" }

func TestTwoModelsCoexistInOneDB(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, Foo{})
			dropTreeTables(gdb, Bar{})

			ctx := context.Background()
			const tenant = "t1"

			fooTree, err := newTestTree(gdb, Foo{})
			if err != nil {
				t.Fatalf("migrate foos: %v", err)
			}
			// The crux: a second node model in the SAME db must not collide on any
			// schema-global identifier (index names, and the depth check on MySQL).
			barTree, err := newTestTree(gdb, Bar{})
			if err != nil {
				t.Fatalf("migrate bars into same db: %v", err)
			}

			// Both trees must be independently usable: Add, GetOrAdd, TreeDescendants.

			// --- foos ---
			fooRoot := Foo{Name: "root"}
			if err := fooTree.Add(ctx, &fooRoot, nil, nil, tenant); err != nil {
				t.Fatalf("foo add root: %v", err)
			}
			fooChild := Foo{Name: "child"}
			if err := fooTree.Add(ctx, &fooChild, up(fooRoot.NodeId), nil, tenant); err != nil {
				t.Fatalf("foo add child: %v", err)
			}
			// GetOrAdd must find the existing child, not duplicate it.
			created, err := fooTree.GetOrAdd(ctx, &Foo{Name: "child"}, fooRoot.NodeId, tenant, []string{"Name"})
			if err != nil {
				t.Fatalf("foo GetOrAdd: %v", err)
			}
			if created {
				t.Fatal("foo GetOrAdd created a duplicate; expected it to find the existing child")
			}

			// --- bars ---
			barRoot := Bar{Name: "barRoot"}
			if err := barTree.Add(ctx, &barRoot, nil, nil, tenant); err != nil {
				t.Fatalf("bar add root: %v", err)
			}
			created, err = barTree.GetOrAdd(ctx, &Bar{Name: "leaf"}, barRoot.NodeId, tenant, []string{"Name"})
			if err != nil {
				t.Fatalf("bar GetOrAdd: %v", err)
			}
			if !created {
				t.Fatal("bar GetOrAdd should have created a new leaf")
			}

			// TreeDescendants on each tree returns only that tree's own nodes.
			var fooOut []*Foo
			if err := fooTree.TreeDescendants(ctx, fooRoot.NodeId, 0, tenant, &fooOut); err != nil {
				t.Fatalf("foo TreeDescendants: %v", err)
			}
			if len(fooOut) != 1 || fooOut[0].Name != "child" {
				t.Fatalf("foo TreeDescendants: want exactly [child], got %+v", fooOut)
			}

			var barOut []*Bar
			if err := barTree.TreeDescendants(ctx, barRoot.NodeId, 0, tenant, &barOut); err != nil {
				t.Fatalf("bar TreeDescendants: %v", err)
			}
			if len(barOut) != 1 || barOut[0].Name != "leaf" {
				t.Fatalf("bar TreeDescendants: want exactly [leaf], got %+v", barOut)
			}
		})
	}
}
