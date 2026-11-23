package closuretree_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TestFindChild covers the query-by-example direct-child lookup: it must match only
// DIRECT children (closure depth=1) of the given parent, scope by tenant, populate the
// out struct like GetNode does, and return found=false (no error) when nothing matches.
func TestFindChild(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name        string
				parentID    uint
				tenant      string
				match       any
				matchFields []string
				out         any
				wantFound   bool
				wantPayload TestPayload
				wantErr     error
			}{
				{
					// Mobile Phones (id 2) is the first child of Electronics (id 1), so sort_order is 0.
					name:        "direct child by name",
					parentID:    1,
					tenant:      tenant1,
					match:       TestPayload{Name: "Mobile Phones"},
					matchFields: []string{"Name"},
					out:         &TestPayload{},
					wantFound:   true,
					wantPayload: TestPayload{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2, Tenant: tenant1, ParentId: 1}},
				},
				{
					// Electronics (id 1) is the first root, so sort_order is 0.
					name:        "root child by name",
					parentID:    0,
					tenant:      tenant1,
					match:       TestPayload{Name: "Electronics"},
					matchFields: []string{"Name"},
					out:         &TestPayload{},
					wantFound:   true,
					wantPayload: TestPayload{Name: "Electronics", Node: closuretree.Node{NodeId: 1, Tenant: tenant1, ParentId: 0}},
				},
				{
					name:        "grandchild is not a direct child",
					parentID:    1,
					tenant:      tenant1,
					match:       TestPayload{Name: "Touch Screen"},
					matchFields: []string{"Name"},
					out:         &TestPayload{},
					wantFound:   false,
				},
				{
					name:        "child of a different parent is not found",
					parentID:    1,
					tenant:      tenant1,
					match:       TestPayload{Name: "T-Shirt"},
					matchFields: []string{"Name"},
					out:         &TestPayload{},
					wantFound:   false,
				},
				{
					name:        "tenant isolation",
					parentID:    0,
					tenant:      tenant2,
					match:       TestPayload{Name: "Electronics"},
					matchFields: []string{"Name"},
					out:         &TestPayload{},
					wantFound:   false,
				},
				{
					name:     "empty matchFields returns ErrEmptyMatch",
					parentID: 0,
					tenant:   tenant1,
					match:    TestPayload{Name: "Electronics"},
					out:      &TestPayload{},
					wantErr:  closuretree.ErrEmptyMatch,
				},
				{
					name:        "unknown match field returns ErrUnknownMatchField",
					parentID:    1,
					tenant:      tenant1,
					match:       TestPayload{Name: "Mobile Phones"},
					matchFields: []string{"Nope"},
					out:         &TestPayload{},
					wantErr:     closuretree.ErrUnknownMatchField,
				},
				{
					name:        "empty tenant returns error",
					parentID:    1,
					tenant:      "",
					match:       TestPayload{Name: "Mobile Phones"},
					matchFields: []string{"Name"},
					out:         &TestPayload{},
					wantErr:     closuretree.ErrEmptyTenant,
				},
				{
					name:        "out is not a pointer",
					parentID:    1,
					tenant:      tenant1,
					match:       TestPayload{Name: "Mobile Phones"},
					matchFields: []string{"Name"},
					out:         TestPayload{},
					wantErr:     closuretree.ErrItemNotPointerToStruct,
				},
				{
					name:        "out does not embed Node",
					parentID:    1,
					tenant:      tenant1,
					match:       TestPayload{Name: "Mobile Phones"},
					matchFields: []string{"Name"},
					out:         &map[string]string{},
					wantErr:     closuretree.ErrItemIsNotTreeNode,
				},
				{
					name:        "match does not embed Node",
					parentID:    1,
					tenant:      tenant1,
					match:       struct{ Name string }{Name: "Mobile Phones"},
					matchFields: []string{"Name"},
					out:         &TestPayload{},
					wantErr:     closuretree.ErrItemIsNotTreeNode,
				},
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					found, err := ct.FindChild(context.Background(), tc.parentID, tc.tenant, tc.match, tc.matchFields, tc.out)
					if tc.wantErr != nil {
						if err == nil {
							t.Fatalf("expected error %v, but got none", tc.wantErr)
						}
						if !errors.Is(err, tc.wantErr) {
							t.Errorf("expected error %v, but got %v", tc.wantErr, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if found != tc.wantFound {
						t.Errorf("found = %v, want %v", found, tc.wantFound)
					}
					if tc.wantFound {
						if diff := cmp.Diff(tc.out, &tc.wantPayload); diff != "" {
							t.Errorf("unexpected result (-got +want):\n%s", diff)
						}
					}
				})
			}
		})
	}
}

// TestFindChildSameNameDifferentParents asserts that a name shared by children of two
// different parents resolves to the correct sibling depending on parentID.
func TestFindChildSameNameDifferentParents(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a := &TestPayload{Name: "a"}
			mustAdd(t, ct, a, 0, tenant1)
			b := &TestPayload{Name: "b"}
			mustAdd(t, ct, b, 0, tenant1)

			dupA := &TestPayload{Name: "dup"}
			mustAdd(t, ct, dupA, a.NodeId, tenant1)
			dupB := &TestPayload{Name: "dup"}
			mustAdd(t, ct, dupB, b.NodeId, tenant1)

			var out TestPayload
			found, err := ct.FindChild(ctx, a.NodeId, tenant1, TestPayload{Name: "dup"}, []string{"Name"}, &out)
			if err != nil || !found {
				t.Fatalf("expected to find dup under a: found=%v err=%v", found, err)
			}
			if out.NodeId != dupA.NodeId {
				t.Errorf("under a: got node id %d, want %d", out.NodeId, dupA.NodeId)
			}

			found, err = ct.FindChild(ctx, b.NodeId, tenant1, TestPayload{Name: "dup"}, []string{"Name"}, &out)
			if err != nil || !found {
				t.Fatalf("expected to find dup under b: found=%v err=%v", found, err)
			}
			if out.NodeId != dupB.NodeId {
				t.Errorf("under b: got node id %d, want %d", out.NodeId, dupB.NodeId)
			}
		})
	}
}

// TestGetOrAdd covers the core get-or-create contract: create when absent, return the
// existing node when present, and set NodeId on the pointer item on BOTH paths.
func TestGetOrAdd(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// create-when-absent
			a := &TestPayload{Name: "a"}
			created, err := ct.GetOrAdd(ctx, a, 0, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Errorf("first call: want created=true, got false")
			}
			if a.NodeId == 0 {
				t.Errorf("first call: want NodeId set on create path, got 0")
			}
			firstID := a.NodeId

			// return-existing-when-present
			a2 := &TestPayload{Name: "a"}
			created, err = ct.GetOrAdd(ctx, a2, 0, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if created {
				t.Errorf("second call: want created=false, got true")
			}
			if a2.NodeId != firstID {
				t.Errorf("second call: want existing NodeId %d, got %d", firstID, a2.NodeId)
			}

			// exactly one "a" root exists
			var roots []TestPayload
			if err := ct.Descendants(ctx, 0, 1, tenant1, &roots); err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, r := range roots {
				if r.Name == "a" {
					count++
				}
			}
			if count != 1 {
				t.Errorf("want exactly one 'a' node, got %d", count)
			}
		})
	}
}

// TestGetOrAddChaining builds a derived path a -> b -> c using get-or-create, then
// re-adds "b": it must return the existing node (created=false, same id, correct parent)
// and must not mint a duplicate.
func TestGetOrAddChaining(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a := &TestPayload{Name: "a"}
			mustGetOrAdd(t, ct, a, 0, tenant1, []string{"Name"})
			b := &TestPayload{Name: "b"}
			mustGetOrAdd(t, ct, b, a.NodeId, tenant1, []string{"Name"})
			c := &TestPayload{Name: "c"}
			mustGetOrAdd(t, ct, c, b.NodeId, tenant1, []string{"Name"})

			b2 := &TestPayload{Name: "b"}
			created, err := ct.GetOrAdd(ctx, b2, a.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if created {
				t.Errorf("re-adding b: want created=false, got true")
			}
			if b2.NodeId != b.NodeId {
				t.Errorf("re-adding b: want existing id %d, got %d", b.NodeId, b2.NodeId)
			}
			if b2.ParentId != a.NodeId {
				t.Errorf("re-adding b: want ParentId %d, got %d", a.NodeId, b2.ParentId)
			}

			ids, err := ct.DescendantIds(ctx, 0, 0, tenant1)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != 3 {
				t.Errorf("want 3 nodes total (a,b,c), got %d", len(ids))
			}
		})
	}
}

// TestGetOrAddDistinctUnderDifferentParents asserts the same name under two different
// parents yields distinct nodes, and re-adding resolves to the right one.
func TestGetOrAddDistinctUnderDifferentParents(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a := &TestPayload{Name: "a"}
			mustGetOrAdd(t, ct, a, 0, tenant1, []string{"Name"})
			b := &TestPayload{Name: "b"}
			mustGetOrAdd(t, ct, b, 0, tenant1, []string{"Name"})

			dupA := &TestPayload{Name: "dup"}
			cA, err := ct.GetOrAdd(ctx, dupA, a.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			dupB := &TestPayload{Name: "dup"}
			cB, err := ct.GetOrAdd(ctx, dupB, b.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if !cA || !cB {
				t.Errorf("dup under different parents: want both created, got cA=%v cB=%v", cA, cB)
			}
			if dupA.NodeId == dupB.NodeId {
				t.Errorf("dup under different parents must be distinct nodes, both got id %d", dupA.NodeId)
			}

			again := &TestPayload{Name: "dup"}
			c, err := ct.GetOrAdd(ctx, again, a.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if c {
				t.Errorf("re-add dup under a: want created=false, got true")
			}
			if again.NodeId != dupA.NodeId {
				t.Errorf("re-add dup under a: want id %d, got %d", dupA.NodeId, again.NodeId)
			}
		})
	}
}

// TestGetOrAddTenantIsolation asserts identical matches in different tenants create
// separate nodes and never cross over.
func TestGetOrAddTenantIsolation(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a1 := &TestPayload{Name: "a"}
			c1, err := ct.GetOrAdd(ctx, a1, 0, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			a2 := &TestPayload{Name: "a"}
			c2, err := ct.GetOrAdd(ctx, a2, 0, tenant2, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if !c1 || !c2 {
				t.Errorf("distinct tenants: want both created, got c1=%v c2=%v", c1, c2)
			}

			a1b := &TestPayload{Name: "a"}
			c, err := ct.GetOrAdd(ctx, a1b, 0, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if c {
				t.Errorf("re-add in tenant1: want created=false, got true")
			}
			if a1b.NodeId != a1.NodeId {
				t.Errorf("re-add in tenant1: want id %d, got %d", a1.NodeId, a1b.NodeId)
			}

			var r1, r2 []TestPayload
			if err := ct.Descendants(ctx, 0, 1, tenant1, &r1); err != nil {
				t.Fatal(err)
			}
			if err := ct.Descendants(ctx, 0, 1, tenant2, &r2); err != nil {
				t.Fatal(err)
			}
			if len(r1) != 1 || len(r2) != 1 {
				t.Errorf("want one root per tenant, got tenant1=%d tenant2=%d", len(r1), len(r2))
			}
		})
	}
}

// TestGetOrAddValueItem asserts get-or-create works when item is passed by value (not a
// pointer): the node is still created and findable, mirroring Add's behavior where a
// non-pointer item is not populated with the new id.
func TestGetOrAddValueItem(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			created, err := ct.GetOrAdd(ctx, TestPayload{Name: "x"}, 0, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Errorf("value item: want created=true, got false")
			}

			var out TestPayload
			found, err := ct.FindChild(ctx, 0, tenant1, TestPayload{Name: "x"}, []string{"Name"}, &out)
			if err != nil {
				t.Fatal(err)
			}
			if !found || out.Name != "x" {
				t.Errorf("value item: expected to find created node, found=%v name=%q", found, out.Name)
			}

			// second call must dedupe rather than create a duplicate
			created, err = ct.GetOrAdd(ctx, TestPayload{Name: "x"}, 0, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if created {
				t.Errorf("value item second call: want created=false, got true")
			}
		})
	}
}

// TestGetOrAddErrors covers the validation error paths.
func TestGetOrAddErrors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			tcs := []struct {
				name        string
				item        any
				parent      uint
				tenant      string
				matchFields []string
				wantErr     error
			}{
				{
					name:    "empty matchFields",
					item:    &TestPayload{Name: "a"},
					tenant:  tenant1,
					wantErr: closuretree.ErrEmptyMatch,
				},
				{
					name:        "item does not embed Node",
					item:        &struct{ Name string }{Name: "a"},
					tenant:      tenant1,
					matchFields: []string{"Name"},
					wantErr:     closuretree.ErrItemIsNotTreeNode,
				},
				{
					name:        "empty tenant",
					item:        &TestPayload{Name: "a"},
					tenant:      "",
					matchFields: []string{"Name"},
					wantErr:     closuretree.ErrEmptyTenant,
				},
				{
					name:        "parent not found",
					item:        &TestPayload{Name: "a"},
					parent:      9999,
					tenant:      tenant1,
					matchFields: []string{"Name"},
					wantErr:     closuretree.ErrParentNotFound,
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					_, err := ct.GetOrAdd(ctx, tc.item, tc.parent, tc.tenant, tc.matchFields)
					if err == nil {
						t.Fatalf("expected error %v, got none", tc.wantErr)
					}
					if !errors.Is(err, tc.wantErr) {
						t.Errorf("expected error %v, got %v", tc.wantErr, err)
					}
				})
			}
		})
	}
}

// TestGetOrAddConcurrent exercises many goroutines racing to get-or-create the same child.
// The per-tenant write lock serializes the find+add transactions, so exactly one goroutine
// creates the child and every other finds it: exactly one child node exists and exactly one
// created=true result is returned. (SQLite serializes writers on its own; the lock closes the
// race on MariaDB/Postgres.)
func TestGetOrAddConcurrent(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			parent := &TestPayload{Name: "parent"}
			if err := ct.Add(ctx, parent, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}

			const goroutines = 8
			var wg sync.WaitGroup
			createdCh := make(chan bool, goroutines)
			errCh := make(chan error, goroutines)
			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					item := &TestPayload{Name: "child"}
					var created bool
					var e error
					for attempt := 0; attempt < 50; attempt++ {
						created, e = ct.GetOrAdd(ctx, item, parent.NodeId, tenant1, []string{"Name"})
						if e == nil || !isTransientDBErr(e) {
							break
						}
						runtime.Gosched()
						time.Sleep(time.Millisecond)
					}
					if e != nil {
						errCh <- e
						return
					}
					createdCh <- created
				}()
			}
			wg.Wait()
			close(createdCh)
			close(errCh)

			for e := range errCh {
				t.Fatalf("unexpected error from concurrent GetOrAdd: %v", e)
			}
			createdCount := 0
			for c := range createdCh {
				if c {
					createdCount++
				}
			}

			var kids []TestPayload
			if err := ct.Descendants(ctx, parent.NodeId, 1, tenant1, &kids); err != nil {
				t.Fatal(err)
			}
			childNodes := 0
			for _, k := range kids {
				if k.Name == "child" {
					childNodes++
				}
			}
			if childNodes != 1 {
				t.Errorf("expected exactly one child node (no duplicates), got %d", childNodes)
			}
			if createdCount != 1 {
				t.Errorf("expected exactly one goroutine to create the child, got %d", createdCount)
			}
		})
	}
}

// TestGetOrAddIdempotent asserts the get-or-create contract deterministically: the first call
// creates the child, a second identical call finds it (created=false) and adds no duplicate.
func TestGetOrAddIdempotent(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			parent := &TestPayload{Name: "parent"}
			if err := ct.Add(ctx, parent, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}

			first := &TestPayload{Name: "child"}
			created, err := ct.GetOrAdd(ctx, first, parent.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Fatalf("first GetOrAdd should create the child")
			}

			second := &TestPayload{Name: "child"}
			created, err = ct.GetOrAdd(ctx, second, parent.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if created {
				t.Errorf("second GetOrAdd should find the existing child, not create a new one")
			}
			if second.NodeId != first.NodeId {
				t.Errorf("second GetOrAdd returned a different node: first=%d second=%d", first.NodeId, second.NodeId)
			}

			var kids []TestPayload
			if err := ct.Descendants(ctx, parent.NodeId, 1, tenant1, &kids); err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, k := range kids {
				if k.Name == "child" {
					count++
				}
			}
			if count != 1 {
				t.Errorf("expected exactly one child after idempotent GetOrAdd, got %d", count)
			}
		})
	}
}

// TestGetOrAddParentIdBothPaths asserts item.ParentId is populated consistently to parentID on
// both the created and the found path (not left 0 on the created path).
func TestGetOrAddParentIdBothPaths(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			root := &TestPayload{Name: "root"}
			if err := ct.Add(ctx, root, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}

			// created path: ParentId must be set to the parent
			child := &TestPayload{Name: "child"}
			created, err := ct.GetOrAdd(ctx, child, root.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Fatalf("want created=true")
			}
			if child.ParentId != root.NodeId {
				t.Errorf("created path: ParentId = %d, want %d", child.ParentId, root.NodeId)
			}

			// found path: ParentId must match too
			child2 := &TestPayload{Name: "child"}
			created, err = ct.GetOrAdd(ctx, child2, root.NodeId, tenant1, []string{"Name"})
			if err != nil {
				t.Fatal(err)
			}
			if created {
				t.Fatalf("want created=false")
			}
			if child2.ParentId != root.NodeId {
				t.Errorf("found path: ParentId = %d, want %d", child2.ParentId, root.NodeId)
			}
		})
	}
}

type nullNoteNode struct {
	closuretree.Node
	Name string
	Note sql.NullString
}

// A match field that is a driver.Valuer holding NULL matches as IS NULL, so GetOrAdd stays
// idempotent and FindChild finds the child.
func TestGetOrAddMatchesNullValuer(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, nullNoteNode{})
			ct, err := newTestTree(gdb, nullNoteNode{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			match := []string{"Name", "Note"}

			first := &nullNoteNode{Name: "a"}
			if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, match); err != nil || !created {
				t.Fatalf("first GetOrAdd: created=%v err=%v", created, err)
			}
			second := &nullNoteNode{Name: "a"}
			created, err := ct.GetOrAdd(ctx, second, 0, tenant1, match)
			if err != nil || created || second.NodeId != first.NodeId {
				t.Errorf("second GetOrAdd: created=%v id=%d (first %d) err=%v", created, second.NodeId, first.NodeId, err)
			}
			if n := countTenant(t, gdb, ct, tenant1).Nodes; n != 1 {
				t.Errorf("want one node, got %d", n)
			}

			var out nullNoteNode
			found, err := ct.FindChild(ctx, 0, tenant1, &nullNoteNode{Name: "a"}, match, &out)
			if err != nil || !found || out.NodeId != first.NodeId {
				t.Errorf("FindChild with a NULL Valuer: found=%v id=%d err=%v", found, out.NodeId, err)
			}
			valid := &nullNoteNode{Name: "a", Note: sql.NullString{String: "n", Valid: true}}
			if found, err := ct.FindChild(ctx, 0, tenant1, valid, match, &out); err != nil || found {
				t.Errorf("FindChild with a non-NULL Valuer must not find the NULL row: found=%v err=%v", found, err)
			}
		})
	}
}

// ptrCode is a driver.Valuer and a sql.Scanner through its pointer only.
type ptrCode struct{ s string }

func (c *ptrCode) Value() (driver.Value, error) { return c.s, nil }

func (c *ptrCode) Scan(src any) error {
	switch v := src.(type) {
	case string:
		c.s = v
	case []byte:
		c.s = string(v)
	default:
		return fmt.Errorf("ptrCode: cannot scan %T", src)
	}
	return nil
}

type ptrCodeNode struct {
	closuretree.Node
	Name string
	Code *ptrCode
}

// A match field that is a pointer driver.Valuer is matched by its Value, not by the struct it
// points to, which the driver cannot bind.
func TestGetOrAddMatchesPointerValuer(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, ptrCodeNode{})
			ct, err := newTestTree(gdb, ptrCodeNode{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			match := []string{"Name", "Code"}

			first := &ptrCodeNode{Name: "a", Code: &ptrCode{"x"}}
			if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, match); err != nil || !created {
				t.Fatalf("first GetOrAdd: created=%v err=%v", created, err)
			}
			second := &ptrCodeNode{Name: "a", Code: &ptrCode{"x"}}
			created, err := ct.GetOrAdd(ctx, second, 0, tenant1, match)
			if err != nil || created || second.NodeId != first.NodeId {
				t.Errorf("second GetOrAdd: created=%v id=%d (first %d) err=%v", created, second.NodeId, first.NodeId, err)
			}
			if n := countTenant(t, gdb, ct, tenant1).Nodes; n != 1 {
				t.Errorf("want one node, got %d", n)
			}

			var out ptrCodeNode
			found, err := ct.FindChild(ctx, 0, tenant1, &ptrCodeNode{Name: "a", Code: &ptrCode{"x"}}, match, &out)
			if err != nil || !found || out.NodeId != first.NodeId || out.Code == nil || out.Code.s != "x" {
				t.Errorf("FindChild with a pointer Valuer: found=%v out=%+v err=%v", found, out, err)
			}
			other := &ptrCodeNode{Name: "a", Code: &ptrCode{"y"}}
			if found, err := ct.FindChild(ctx, 0, tenant1, other, match, &out); err != nil || found {
				t.Errorf("FindChild with another Value must not find the row: found=%v err=%v", found, err)
			}
		})
	}
}

type rawDocNode struct {
	closuretree.Node
	Name string
	Doc  json.RawMessage
}

type bytesDocNode struct {
	closuretree.Node
	Name string
	Doc  []byte
}

type bytesPtrDocNode struct {
	closuretree.Node
	Name string
	Doc  *[]byte
}

type rawPtrDocNode struct {
	closuretree.Node
	Name string
	Doc  *json.RawMessage
}

// idItem is a pointer to a struct that embeds Node.
type idItem interface{ Id() uint }

// A byte-slice match field matches what Add stores, so GetOrAdd stays idempotent and FindChild
// finds the child: NULL for a nil one, also behind a pointer, and for an empty one of a named type
// that is not behind a pointer, which gorm writes as NULL; empty bytes for an empty []byte and for
// an empty one behind a pointer; the bytes for any other. On MariaDB the cases run also with the
// driver's interpolateParams, which stores a pointer to a nil json.RawMessage as empty bytes.
func TestMatchByteSlice(t *testing.T) {
	cases := []struct {
		name  string
		model any
		item  func() idItem
	}{
		{"RawMessage nil", rawDocNode{}, func() idItem { return &rawDocNode{Name: "a"} }},
		{"RawMessage empty", rawDocNode{}, func() idItem { return &rawDocNode{Name: "a", Doc: json.RawMessage{}} }},
		{"RawMessage non empty", rawDocNode{}, func() idItem { return &rawDocNode{Name: "a", Doc: json.RawMessage(`{"a":1}`)} }},
		{"bytes nil", bytesDocNode{}, func() idItem { return &bytesDocNode{Name: "a"} }},
		{"bytes empty", bytesDocNode{}, func() idItem { return &bytesDocNode{Name: "a", Doc: []byte{}} }},
		{"bytes non empty", bytesDocNode{}, func() idItem { return &bytesDocNode{Name: "a", Doc: []byte("x")} }},
		{"pointer to nil bytes", bytesPtrDocNode{}, func() idItem {
			var b []byte
			return &bytesPtrDocNode{Name: "a", Doc: &b}
		}},
		{"pointer to nil RawMessage", rawPtrDocNode{}, func() idItem {
			var doc json.RawMessage
			return &rawPtrDocNode{Name: "a", Doc: &doc}
		}},
		{"pointer to empty RawMessage", rawPtrDocNode{}, func() idItem {
			doc := json.RawMessage{}
			return &rawPtrDocNode{Name: "a", Doc: &doc}
		}},
	}
	for _, db := range testdbs.DBs() {
		conns := []string{db.DbType()}
		if db.DbType() == testdbs.DBTypeMariadb {
			conns = append(conns, db.DbType()+"_interpolateParams")
		}
		for _, conn := range conns {
			for _, c := range cases {
				t.Run(conn+"/"+c.name, func(t *testing.T) {
					gdb := connAndClose(t, db)
					if conn != db.DbType() {
						gdb = interpolatedMariaDB(t, gdb)
					}
					dropTreeTables(gdb, c.model)
					ct, err := newTestTree(gdb, c.model)
					if err != nil {
						t.Fatal(err)
					}
					ctx := context.Background()
					match := []string{"Name", "Doc"}

					first := c.item()
					if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, match); err != nil || !created {
						t.Fatalf("first GetOrAdd: created=%v err=%v", created, err)
					}
					second := c.item()
					created, err := ct.GetOrAdd(ctx, second, 0, tenant1, match)
					if err != nil || created || second.Id() != first.Id() {
						t.Errorf("second GetOrAdd: created=%v id=%d (first %d) err=%v", created, second.Id(), first.Id(), err)
					}
					if n := countTenant(t, gdb, ct, tenant1).Nodes; n != 1 {
						t.Errorf("want one node, got %d", n)
					}

					out := c.item()
					found, err := ct.FindChild(ctx, 0, tenant1, c.item(), match, out)
					if err != nil || !found || out.Id() != first.Id() {
						t.Errorf("FindChild: found=%v id=%d (first %d) err=%v", found, out.Id(), first.Id(), err)
					}
				})
			}
		}
	}
}

// interpolatedMariaDB opens another connection to the MariaDB database of gdb with the driver's
// interpolateParams, which renders the bound values into the statement text instead of sending
// them apart, and closes it when the test ends.
func interpolatedMariaDB(t *testing.T, gdb *gorm.DB) *gorm.DB {
	t.Helper()
	dsn := gdb.Dialector.(*mysql.Dialector).DSN + "&interpolateParams=true"
	idb, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := idb.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return idb
}

// A nil and an empty json.RawMessage are both stored as NULL, so they match the same node.
func TestMatchNilAndEmptyRawMessage(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			ctx := context.Background()
			match := []string{"Name", "Doc"}

			dropTreeTables(gdb, rawDocNode{})
			raw, err := newTestTree(gdb, rawDocNode{})
			if err != nil {
				t.Fatal(err)
			}
			added := &rawDocNode{Name: "a", Doc: json.RawMessage{}}
			if created, err := raw.GetOrAdd(ctx, added, 0, tenant1, match); err != nil || !created {
				t.Fatalf("GetOrAdd of an empty RawMessage: created=%v err=%v", created, err)
			}
			var out rawDocNode
			found, err := raw.FindChild(ctx, 0, tenant1, &rawDocNode{Name: "a", Doc: json.RawMessage(nil)}, match, &out)
			if err != nil || !found || out.NodeId != added.NodeId {
				t.Errorf("FindChild with a nil RawMessage: found=%v id=%d (added %d) err=%v", found, out.NodeId, added.NodeId, err)
			}
			again := &rawDocNode{Name: "a"}
			if created, err := raw.GetOrAdd(ctx, again, 0, tenant1, match); err != nil || created || again.NodeId != added.NodeId {
				t.Errorf("GetOrAdd of a nil RawMessage: created=%v id=%d (added %d) err=%v", created, again.NodeId, added.NodeId, err)
			}
		})
	}
}

// A nil and an empty []byte are stored as NULL and as empty bytes, so they are two different nodes.
func TestMatchNilAndEmptyBytes(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			ctx := context.Background()
			match := []string{"Name", "Doc"}

			dropTreeTables(gdb, bytesDocNode{})
			bin, err := newTestTree(gdb, bytesDocNode{})
			if err != nil {
				t.Fatal(err)
			}
			nilDoc := &bytesDocNode{Name: "a"}
			if created, err := bin.GetOrAdd(ctx, nilDoc, 0, tenant1, match); err != nil || !created {
				t.Fatalf("GetOrAdd of nil bytes: created=%v err=%v", created, err)
			}
			emptyDoc := &bytesDocNode{Name: "a", Doc: []byte{}}
			if created, err := bin.GetOrAdd(ctx, emptyDoc, 0, tenant1, match); err != nil || !created || emptyDoc.NodeId == nilDoc.NodeId {
				t.Fatalf("GetOrAdd of empty bytes must add another node: created=%v id=%d (nil %d) err=%v", created, emptyDoc.NodeId, nilDoc.NodeId, err)
			}
			for _, want := range []*bytesDocNode{nilDoc, emptyDoc} {
				var got bytesDocNode
				found, err := bin.FindChild(ctx, 0, tenant1, &bytesDocNode{Name: "a", Doc: want.Doc}, match, &got)
				if err != nil || !found || got.NodeId != want.NodeId {
					t.Errorf("FindChild with Doc %#v: found=%v id=%d (want %d) err=%v", want.Doc, found, got.NodeId, want.NodeId, err)
				}
			}
			if n := countTenant(t, gdb, bin, tenant1).Nodes; n != 2 {
				t.Errorf("want two nodes, got %d", n)
			}
		})
	}
}

// A match field that names a Go name several columns share fails in either declaration order,
// before GetOrAdd creates anything; the column name matches its own column.
func TestMatchRejectsSharedGoName(t *testing.T) {
	models := []struct {
		name  string
		model any
		item  func(name, owner string) any
	}{
		{"own field first", docOwnerLast{}, func(name, owner string) any {
			return &docOwnerLast{Name: name, Owner: docOwner{owner}}
		}},
		{"own field last", docOwnerFirst{}, func(name, owner string) any {
			return &docOwnerFirst{Name: name, Owner: docOwner{owner}}
		}},
	}
	id := func(item any) uint { return item.(interface{ Id() uint }).Id() }
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
				shared := func(what string, err error) {
					t.Helper()
					if !errors.Is(err, closuretree.ErrUnknownMatchField) || !strings.Contains(err.Error(), "owner_name") {
						t.Errorf("%s by Name: want ErrUnknownMatchField naming the columns, got %v", what, err)
					}
				}

				// the same own name with two owners
				for _, owner := range []string{"x", "y"} {
					_, err := ct.GetOrAdd(ctx, m.item("a", owner), 0, tenant1, []string{"Name"})
					shared("GetOrAdd", err)
				}
				if n := countTenant(t, gdb, ct, tenant1).Nodes; n != 0 {
					t.Fatalf("GetOrAdd by Name must create nothing, got %d nodes", n)
				}
				_, err = ct.FindChild(ctx, 0, tenant1, m.item("a", "x"), []string{"Name"}, m.item("", ""))
				shared("FindChild", err)

				first := m.item("a", "x")
				if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, []string{"name"}); err != nil || !created {
					t.Fatalf("first GetOrAdd by name: created=%v err=%v", created, err)
				}
				second := m.item("a", "y")
				created, err := ct.GetOrAdd(ctx, second, 0, tenant1, []string{"name"})
				if err != nil || created || id(second) != id(first) {
					t.Errorf("second GetOrAdd by name: created=%v id=%d (first %d) err=%v", created, id(second), id(first), err)
				}
				out := m.item("", "")
				if found, err := ct.FindChild(ctx, 0, tenant1, m.item("b", "x"), []string{"owner_name"}, out); err != nil || !found || id(out) != id(first) {
					t.Errorf("FindChild by owner_name: found=%v id=%d err=%v", found, id(out), err)
				}
			})
		}
	}
}

type matchNameBase struct{ Name string }

// matchBaseFirst and matchBaseLast embed a Name without a column prefix, on the column name of
// their own Name, declared before and after it.
type matchBaseFirst struct {
	closuretree.Node
	Base matchNameBase `gorm:"embedded"`
	Name string
}

type matchBaseLast struct {
	closuretree.Node
	Name string
	Base matchNameBase `gorm:"embedded"`
}

// A Go name that several fields of one column share is not ambiguous as a match field.
func TestMatchGoNameOfOneColumn(t *testing.T) {
	models := []struct {
		name  string
		model any
		item  func(name string) any
	}{
		{"embedded first", matchBaseFirst{}, func(name string) any {
			return &matchBaseFirst{Base: matchNameBase{name}, Name: name}
		}},
		{"embedded last", matchBaseLast{}, func(name string) any {
			return &matchBaseLast{Name: name, Base: matchNameBase{name}}
		}},
	}
	id := func(item any) uint { return item.(interface{ Id() uint }).Id() }
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
				first := m.item("a")
				if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, []string{"Name"}); err != nil || !created {
					t.Fatalf("first GetOrAdd: created=%v err=%v", created, err)
				}
				second := m.item("a")
				created, err := ct.GetOrAdd(ctx, second, 0, tenant1, []string{"Name"})
				if err != nil || created || id(second) != id(first) {
					t.Errorf("second GetOrAdd: created=%v id=%d (first %d) err=%v", created, id(second), id(first), err)
				}
				out := m.item("")
				if found, err := ct.FindChild(ctx, 0, tenant1, m.item("a"), []string{"Name"}, out); err != nil || !found || id(out) != id(first) {
					t.Errorf("FindChild by Name: found=%v id=%d err=%v", found, id(out), err)
				}
			})
		}
	}
}

// gormCode is a gorm.Valuer: gorm renders its GormValue as SQL instead of binding it.
type gormCode string

func (c gormCode) GormValue(context.Context, *gorm.DB) clause.Expr {
	return clause.Expr{SQL: "?", Vars: []any{string(c)}}
}

type gormCodeNode struct {
	closuretree.Node
	Name string
	Code gormCode
}

// gormCodePtrNode has a pointer to a gorm.Valuer type, which gorm renders as SQL when it is not nil.
type gormCodePtrNode struct {
	closuretree.Node
	Name string
	Code *gormCode
}

// tagsNode has a list column, which gorm binds as a list.
type tagsNode struct {
	closuretree.Node
	Name string
	Tags []string `gorm:"type:text"`
}

// A match field whose value gorm would not bind as one value fails before any query, as Eq does:
// a list and a gorm.Valuer, also as a nil pointer, which is checked by its type. GetOrAdd creates
// nothing.
func TestMatchRejectsUnboundValues(t *testing.T) {
	cases := []struct {
		name  string
		model any
		field string
		item  func() any
	}{
		{"list", tagsNode{}, "Tags", func() any { return &tagsNode{Name: "a", Tags: []string{}} }},
		{"gorm.Valuer", gormCodeNode{}, "Code", func() any { return &gormCodeNode{Name: "a", Code: "x"} }},
		{"nil pointer to a gorm.Valuer", gormCodePtrNode{}, "Code", func() any { return &gormCodePtrNode{Name: "a"} }},
	}
	for _, db := range testdbs.DBs() {
		for _, c := range cases {
			t.Run(db.DbType()+"/"+c.name, func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, c.model)
				ct, err := newTestTree(gdb, c.model)
				if err != nil {
					t.Fatal(err)
				}
				fields := []string{"Name", c.field}
				rejected := func(what string, err error) {
					t.Helper()
					if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), c.field) {
						t.Errorf("%s: want an error naming %s before any query, got %v", what, c.field, err)
					}
				}
				cancelled, cancel := context.WithCancel(context.Background())
				cancel() // a query would fail with context.Canceled, so the check must come first
				_, err = ct.GetOrAdd(cancelled, c.item(), 0, tenant1, fields)
				rejected("GetOrAdd", err)
				_, err = ct.FindChild(cancelled, 0, tenant1, c.item(), fields, c.item())
				rejected("FindChild", err)

				for range 2 {
					_, err := ct.GetOrAdd(context.Background(), c.item(), 0, tenant1, fields)
					rejected("GetOrAdd", err)
				}
				if n := countTenant(t, gdb, ct, tenant1).Nodes; n != 0 {
					t.Errorf("GetOrAdd must create nothing, got %d nodes", n)
				}
			})
		}
	}
}

// defaultKindNode and defaultPtrKindNode have a column with a default, which gorm writes instead of
// the field's zero value.
type defaultKindNode struct {
	closuretree.Node
	Name string
	Kind string `gorm:"size:20;default:x"`
}

type defaultPtrKindNode struct {
	closuretree.Node
	Name string
	Kind *string `gorm:"size:20;default:x"`
}

// A match field with a default whose value is the zero value fails before any query, since Add
// would store the default and the next GetOrAdd would not find the node. GetOrAdd creates nothing.
func TestMatchRejectsZeroValueWithDefault(t *testing.T) {
	cases := []struct {
		name  string
		model any
		item  func() any
	}{
		{"zero value", defaultKindNode{}, func() any { return &defaultKindNode{Name: "a"} }},
		{"nil pointer", defaultPtrKindNode{}, func() any { return &defaultPtrKindNode{Name: "a"} }},
	}
	for _, db := range testdbs.DBs() {
		for _, c := range cases {
			t.Run(db.DbType()+"/"+c.name, func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, c.model)
				ct, err := newTestTree(gdb, c.model)
				if err != nil {
					t.Fatal(err)
				}
				fields := []string{"Name", "Kind"}
				rejected := func(what string, err error) {
					t.Helper()
					if err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "Kind") {
						t.Errorf("%s: want an error naming Kind before any query, got %v", what, err)
					}
				}
				cancelled, cancel := context.WithCancel(context.Background())
				cancel() // a query would fail with context.Canceled, so the check must come first
				_, err = ct.GetOrAdd(cancelled, c.item(), 0, tenant1, fields)
				rejected("GetOrAdd", err)
				_, err = ct.FindChild(cancelled, 0, tenant1, c.item(), fields, c.item())
				rejected("FindChild", err)

				for range 2 {
					_, err := ct.GetOrAdd(context.Background(), c.item(), 0, tenant1, fields)
					rejected("GetOrAdd", err)
				}
				if n := countTenant(t, gdb, ct, tenant1).Nodes; n != 0 {
					t.Errorf("GetOrAdd must create nothing, got %d nodes", n)
				}
			})
		}
	}
}

// A match field with a default whose value is not the zero value, also a pointer to the zero
// value, is stored and matched as it is.
func TestMatchValueWithDefault(t *testing.T) {
	empty := ""
	cases := []struct {
		name  string
		model any
		item  func() idItem
	}{
		{"value", defaultKindNode{}, func() idItem { return &defaultKindNode{Name: "a", Kind: "y"} }},
		{"pointer to the zero value", defaultPtrKindNode{}, func() idItem { return &defaultPtrKindNode{Name: "a", Kind: &empty} }},
	}
	for _, db := range testdbs.DBs() {
		for _, c := range cases {
			t.Run(db.DbType()+"/"+c.name, func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, c.model)
				ct, err := newTestTree(gdb, c.model)
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				fields := []string{"Name", "Kind"}
				first := c.item()
				if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, fields); err != nil || !created {
					t.Fatalf("first GetOrAdd: created=%v err=%v", created, err)
				}
				second := c.item()
				created, err := ct.GetOrAdd(ctx, second, 0, tenant1, fields)
				if err != nil || created || second.Id() != first.Id() {
					t.Errorf("second GetOrAdd: created=%v id=%d (first %d) err=%v", created, second.Id(), first.Id(), err)
				}
				out := c.item()
				if found, err := ct.FindChild(ctx, 0, tenant1, c.item(), fields, out); err != nil || !found || out.Id() != first.Id() {
					t.Errorf("FindChild: found=%v id=%d (first %d) err=%v", found, out.Id(), first.Id(), err)
				}
			})
		}
	}
}

// A gorm.Valuer field is checked only when it is a match field: GetOrAdd matching on another field
// adds the item with it, and finds that item the second time.
func TestGetOrAddStoresValuerThatIsNotMatched(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, gormCodeNode{})
			ct, err := newTestTree(gdb, gormCodeNode{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			first := &gormCodeNode{Name: "a", Code: "x"}
			if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, []string{"Name"}); err != nil || !created {
				t.Fatalf("first GetOrAdd: created=%v err=%v", created, err)
			}
			second := &gormCodeNode{Name: "a", Code: "y"}
			if created, err := ct.GetOrAdd(ctx, second, 0, tenant1, []string{"Name"}); err != nil || created || second.NodeId != first.NodeId {
				t.Errorf("second GetOrAdd: created=%v id=%d (first %d) err=%v", created, second.NodeId, first.NodeId, err)
			}
			var got gormCodeNode
			if err := ct.GetNode(ctx, first.NodeId, tenant1, &got); err != nil || got.Code != "x" {
				t.Errorf("stored node: %+v, err=%v; want Code x", got, err)
			}
		})
	}
}

// labelsNode has a field with a gorm serializer, which gorm binds as one value.
type labelsNode struct {
	closuretree.Node
	Name   string
	Labels []string `gorm:"serializer:json"`
}

// A match field with a serializer is matched by its serialized value.
func TestMatchSerializerField(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, labelsNode{})
			ct, err := newTestTree(gdb, labelsNode{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			match := []string{"Name", "Labels"}

			first := &labelsNode{Name: "a", Labels: []string{"x", "y"}}
			if created, err := ct.GetOrAdd(ctx, first, 0, tenant1, match); err != nil || !created {
				t.Fatalf("first GetOrAdd: created=%v err=%v", created, err)
			}
			second := &labelsNode{Name: "a", Labels: []string{"x", "y"}}
			created, err := ct.GetOrAdd(ctx, second, 0, tenant1, match)
			if err != nil || created || second.NodeId != first.NodeId {
				t.Errorf("second GetOrAdd: created=%v id=%d (first %d) err=%v", created, second.NodeId, first.NodeId, err)
			}
			var out labelsNode
			other := &labelsNode{Name: "a", Labels: []string{"x"}}
			if found, err := ct.FindChild(ctx, 0, tenant1, other, match, &out); err != nil || found {
				t.Errorf("FindChild with other labels must not find the row: found=%v err=%v", found, err)
			}
			if n := countTenant(t, gdb, ct, tenant1).Nodes; n != 1 {
				t.Errorf("want one node, got %d", n)
			}
		})
	}
}

func mustAdd(t *testing.T, ct *closuretree.Tree, item any, parentID uint, tenant string) {
	t.Helper()
	if err := ct.Add(context.Background(), item, up(parentID), nil, tenant); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
}

func mustGetOrAdd(t *testing.T, ct *closuretree.Tree, item any, parentID uint, tenant string, matchFields []string) {
	t.Helper()
	if _, err := ct.GetOrAdd(context.Background(), item, parentID, tenant, matchFields); err != nil {
		t.Fatalf("GetOrAdd failed: %v", err)
	}
}

// isTransientDBErr reports whether err is a transient concurrency error (lock/deadlock)
// worth retrying, as opposed to a real failure.
func isTransientDBErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{"locked", "busy", "deadlock", "try restarting transaction", "database table is locked"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
