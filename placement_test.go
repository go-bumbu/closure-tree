package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
)

func TestAddPlaceLast(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			last := closuretree.PlaceLast()
			add := func(name string, parent *uint, opts ...closuretree.AddOption) *TestPayload {
				t.Helper()
				n := &TestPayload{Name: name}
				if err := ct.Add(ctx, n, parent, nil, tenant1, opts...); err != nil {
					t.Fatal(err)
				}
				return n
			}
			p := add("p", nil)
			if first := add("first", up(p.NodeId), last); first.SortOrder != 0 { // empty parent
				t.Errorf("empty parent: want sort order 0, got %v", first.SortOrder)
			}
			add("front", up(p.NodeId)) // default: first among the siblings, at -10
			if l1 := add("l1", up(p.NodeId), last); l1.SortOrder != 10 {
				t.Errorf("want 10, got %v", l1.SortOrder)
			}
			if l2 := add("l2", up(p.NodeId), last); l2.SortOrder != 20 {
				t.Errorf("want 20, got %v", l2.SortOrder)
			}
			if diff := cmp.Diff([]string{"front", "first", "l1", "l2"}, childNames(t, ct, p.NodeId, tenant1)); diff != "" {
				t.Errorf("order (-want +got):\n%s", diff)
			}
			// the meta row exists and reports a healthy gap, as after a move without afterNodeID
			if exists, err := ct.NeedsRenormalize(ctx, p.NodeId, tenant1, 9999); err != nil || !exists {
				t.Errorf("meta row missing: %v %v", exists, err)
			}
			if tight, err := ct.NeedsRenormalize(ctx, p.NodeId, tenant1, closuretree.DefaultHalvingsBuffer); err != nil || tight {
				t.Errorf("gap reported as tight: %v %v", tight, err)
			}
			// review focus 2: a nil option is ignored
			if n := add("nilopt", up(p.NodeId), nil); n.SortOrder != -20 {
				t.Errorf("nil option: want the default first place at -20, got %v", n.SortOrder)
			}
		})
	}
}

func TestAddPlaceLastConflict(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			p, s := &TestPayload{Name: "p"}, &TestPayload{Name: "s"}
			if err := ct.Add(ctx, p, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			if err := ct.Add(ctx, s, up(p.NodeId), nil, tenant1); err != nil {
				t.Fatal(err)
			}
			n := &TestPayload{Name: "n"}
			err = ct.Add(ctx, n, up(p.NodeId), up(s.NodeId), tenant1, closuretree.PlaceLast())
			if !errors.Is(err, closuretree.ErrConflictingPlacement) {
				t.Fatalf("want ErrConflictingPlacement, got %v", err)
			}
			if diff := cmp.Diff([]string{"s"}, childNames(t, ct, p.NodeId, tenant1)); diff != "" {
				t.Errorf("something was written (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetOrAddPlaceLast(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			p := &TestPayload{Name: "p"}
			if err := ct.Add(ctx, p, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			if err := ct.Add(ctx, &TestPayload{Name: "a"}, up(p.NodeId), nil, tenant1); err != nil {
				t.Fatal(err)
			}
			z := &TestPayload{Name: "z"}
			created, err := ct.GetOrAdd(ctx, z, p.NodeId, tenant1, []string{"Name"}, closuretree.PlaceLast())
			if err != nil || !created || z.SortOrder != 10 {
				t.Fatalf("create path: created=%v sort=%v err=%v", created, z.SortOrder, err)
			}
			again := &TestPayload{Name: "z"}
			created, err = ct.GetOrAdd(ctx, again, p.NodeId, tenant1, []string{"Name"}, closuretree.PlaceLast())
			if err != nil || created || again.NodeId != z.NodeId || again.SortOrder != 10 {
				t.Fatalf("found path: created=%v id=%d sort=%v err=%v", created, again.NodeId, again.SortOrder, err)
			}
		})
	}
}

func TestAddPlaceLastConcurrent(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			p := &TestPayload{Name: "p"}
			if err := ct.Add(ctx, p, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			const n = 8
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					deadline := time.Now().Add(10 * time.Second)
					for {
						errs[i] = ct.Add(ctx, &TestPayload{Name: fmt.Sprintf("c%d", i)}, up(p.NodeId), nil, tenant1, closuretree.PlaceLast())
						if errs[i] == nil || !isTransientDBErr(errs[i]) || time.Now().After(deadline) {
							return
						}
						time.Sleep(time.Millisecond)
					}
				}(i)
			}
			wg.Wait()
			for i, e := range errs {
				if e != nil {
					t.Fatalf("add %d: %v", i, e)
				}
			}
			var kids []TestPayload
			if err := ct.Descendants(ctx, p.NodeId, 1, tenant1, &kids); err != nil {
				t.Fatal(err)
			}
			got := make([]float64, 0, n)
			for i, k := range kids {
				got = append(got, k.SortOrder)
				// node ids are handed out inside the locked transaction, so they follow commit order
				if i > 0 && k.NodeId < kids[i-1].NodeId {
					t.Errorf("sort order does not follow commit order: id %d comes after id %d", k.NodeId, kids[i-1].NodeId)
				}
			}
			if diff := cmp.Diff([]float64{0, 10, 20, 30, 40, 50, 60, 70}, got); diff != "" {
				t.Errorf("sort orders (-want +got):\n%s", diff)
			}
		})
	}
}
