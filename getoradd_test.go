package closuretree_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
)

// TestFindChild covers the query-by-example direct-child lookup: it must match only
// DIRECT children (closure depth=1) of the given parent, scope by tenant, populate the
// out struct like GetNode does, and return found=false (no error) when nothing matches.
func TestFindChild(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name        string
				parentID    uint
				tenant      string
				match       any
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
					out:         &TestPayload{},
					wantFound:   true,
					wantPayload: TestPayload{Name: "Electronics", Node: closuretree.Node{NodeId: 1, Tenant: tenant1, ParentId: 0}},
				},
				{
					name:      "grandchild is not a direct child",
					parentID:  1,
					tenant:    tenant1,
					match:     TestPayload{Name: "Touch Screen"},
					out:       &TestPayload{},
					wantFound: false,
				},
				{
					name:      "child of a different parent is not found",
					parentID:  1,
					tenant:    tenant1,
					match:     TestPayload{Name: "T-Shirt"},
					out:       &TestPayload{},
					wantFound: false,
				},
				{
					name:      "tenant isolation",
					parentID:  0,
					tenant:    tenant2,
					match:     TestPayload{Name: "Electronics"},
					out:       &TestPayload{},
					wantFound: false,
				},
				{
					name:     "empty match returns ErrEmptyMatch",
					parentID: 0,
					tenant:   tenant1,
					match:    TestPayload{},
					out:      &TestPayload{},
					wantErr:  closuretree.ErrEmptyMatch,
				},
				{
					name:     "empty tenant returns error",
					parentID: 1,
					tenant:   "",
					match:    TestPayload{Name: "Mobile Phones"},
					out:      &TestPayload{},
					wantErr:  closuretree.ErrEmptyTenant,
				},
				{
					name:     "out is not a pointer",
					parentID: 1,
					tenant:   tenant1,
					match:    TestPayload{Name: "Mobile Phones"},
					out:      TestPayload{},
					wantErr:  closuretree.ErrItemNotPointerToStruct,
				},
				{
					name:     "out does not embed Node",
					parentID: 1,
					tenant:   tenant1,
					match:    TestPayload{Name: "Mobile Phones"},
					out:      &map[string]string{},
					wantErr:  closuretree.ErrItemIsNotTreeNode,
				},
				{
					name:     "match does not embed Node",
					parentID: 1,
					tenant:   tenant1,
					match:    struct{ Name string }{Name: "Mobile Phones"},
					out:      &TestPayload{},
					wantErr:  closuretree.ErrItemIsNotTreeNode,
				},
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					found, err := ct.FindChild(context.Background(), tc.parentID, tc.tenant, tc.match, tc.out)
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
			ct, err := closuretree.New(gdb, TestPayload{})
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
			found, err := ct.FindChild(ctx, a.NodeId, tenant1, TestPayload{Name: "dup"}, &out)
			if err != nil || !found {
				t.Fatalf("expected to find dup under a: found=%v err=%v", found, err)
			}
			if out.NodeId != dupA.NodeId {
				t.Errorf("under a: got node id %d, want %d", out.NodeId, dupA.NodeId)
			}

			found, err = ct.FindChild(ctx, b.NodeId, tenant1, TestPayload{Name: "dup"}, &out)
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
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// create-when-absent
			a := &TestPayload{Name: "a"}
			created, err := ct.GetOrAdd(ctx, a, 0, tenant1, TestPayload{Name: "a"})
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
			created, err = ct.GetOrAdd(ctx, a2, 0, tenant1, TestPayload{Name: "a"})
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
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a := &TestPayload{Name: "a"}
			mustGetOrAdd(t, ct, a, 0, tenant1, TestPayload{Name: "a"})
			b := &TestPayload{Name: "b"}
			mustGetOrAdd(t, ct, b, a.NodeId, tenant1, TestPayload{Name: "b"})
			c := &TestPayload{Name: "c"}
			mustGetOrAdd(t, ct, c, b.NodeId, tenant1, TestPayload{Name: "c"})

			b2 := &TestPayload{Name: "b"}
			created, err := ct.GetOrAdd(ctx, b2, a.NodeId, tenant1, TestPayload{Name: "b"})
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
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a := &TestPayload{Name: "a"}
			mustGetOrAdd(t, ct, a, 0, tenant1, TestPayload{Name: "a"})
			b := &TestPayload{Name: "b"}
			mustGetOrAdd(t, ct, b, 0, tenant1, TestPayload{Name: "b"})

			dupA := &TestPayload{Name: "dup"}
			cA, err := ct.GetOrAdd(ctx, dupA, a.NodeId, tenant1, TestPayload{Name: "dup"})
			if err != nil {
				t.Fatal(err)
			}
			dupB := &TestPayload{Name: "dup"}
			cB, err := ct.GetOrAdd(ctx, dupB, b.NodeId, tenant1, TestPayload{Name: "dup"})
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
			c, err := ct.GetOrAdd(ctx, again, a.NodeId, tenant1, TestPayload{Name: "dup"})
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
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a1 := &TestPayload{Name: "a"}
			c1, err := ct.GetOrAdd(ctx, a1, 0, tenant1, TestPayload{Name: "a"})
			if err != nil {
				t.Fatal(err)
			}
			a2 := &TestPayload{Name: "a"}
			c2, err := ct.GetOrAdd(ctx, a2, 0, tenant2, TestPayload{Name: "a"})
			if err != nil {
				t.Fatal(err)
			}
			if !c1 || !c2 {
				t.Errorf("distinct tenants: want both created, got c1=%v c2=%v", c1, c2)
			}

			a1b := &TestPayload{Name: "a"}
			c, err := ct.GetOrAdd(ctx, a1b, 0, tenant1, TestPayload{Name: "a"})
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
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			created, err := ct.GetOrAdd(ctx, TestPayload{Name: "x"}, 0, tenant1, TestPayload{Name: "x"})
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Errorf("value item: want created=true, got false")
			}

			var out TestPayload
			found, err := ct.FindChild(ctx, 0, tenant1, TestPayload{Name: "x"}, &out)
			if err != nil {
				t.Fatal(err)
			}
			if !found || out.Name != "x" {
				t.Errorf("value item: expected to find created node, found=%v name=%q", found, out.Name)
			}

			// second call must dedupe rather than create a duplicate
			created, err = ct.GetOrAdd(ctx, TestPayload{Name: "x"}, 0, tenant1, TestPayload{Name: "x"})
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
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			tcs := []struct {
				name    string
				item    any
				parent  uint
				tenant  string
				match   any
				wantErr error
			}{
				{
					name:    "empty match",
					item:    &TestPayload{Name: "a"},
					tenant:  tenant1,
					match:   TestPayload{},
					wantErr: closuretree.ErrEmptyMatch,
				},
				{
					name:    "item does not embed Node",
					item:    &struct{ Name string }{Name: "a"},
					tenant:  tenant1,
					match:   TestPayload{Name: "a"},
					wantErr: closuretree.ErrItemIsNotTreeNode,
				},
				{
					name:    "empty tenant",
					item:    &TestPayload{Name: "a"},
					tenant:  "",
					match:   TestPayload{Name: "a"},
					wantErr: closuretree.ErrEmptyTenant,
				},
				{
					name:    "parent not found",
					item:    &TestPayload{Name: "a"},
					parent:  9999,
					tenant:  tenant1,
					match:   TestPayload{Name: "a"},
					wantErr: closuretree.ErrParentNotFound,
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					_, err := ct.GetOrAdd(ctx, tc.item, tc.parent, tc.tenant, tc.match)
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
// GetOrAdd wraps find+add in a single transaction but does not fully eliminate the race for
// a brand-new child (documented on the method). The invariant that MUST hold regardless is:
// the number of actual child nodes equals the number of created=true results.
func TestGetOrAddConcurrent(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			parent := &TestPayload{Name: "parent"}
			if err := ct.Add(ctx, parent, 0, 0, tenant1); err != nil {
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
						created, e = ct.GetOrAdd(ctx, item, parent.NodeId, tenant1, TestPayload{Name: "child"})
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
			if childNodes < 1 {
				t.Errorf("expected at least one child node, got %d", childNodes)
			}
			if childNodes != createdCount {
				t.Errorf("consistency violated: created=true count %d but %d child nodes exist", createdCount, childNodes)
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
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			root := &TestPayload{Name: "root"}
			if err := ct.Add(ctx, root, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			// created path: ParentId must be set to the parent
			child := &TestPayload{Name: "child"}
			created, err := ct.GetOrAdd(ctx, child, root.NodeId, tenant1, TestPayload{Name: "child"})
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
			created, err = ct.GetOrAdd(ctx, child2, root.NodeId, tenant1, TestPayload{Name: "child"})
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

func mustAdd(t *testing.T, ct *closuretree.Tree, item any, parentID uint, tenant string) {
	t.Helper()
	if err := ct.Add(context.Background(), item, parentID, 0, tenant); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
}

func mustGetOrAdd(t *testing.T, ct *closuretree.Tree, item any, parentID uint, tenant string, match any) {
	t.Helper()
	if _, err := ct.GetOrAdd(context.Background(), item, parentID, tenant, match); err != nil {
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
