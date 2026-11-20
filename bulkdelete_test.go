package closuretree_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
)

func TestDeleteRecurseMany(t *testing.T) {
	all := []uint{1, 2, 3, 4, 5, 6}
	cases := []struct {
		name string
		ids  []uint
		want int64
		left []uint
	}{
		{"disjoint roots", []uint{2, 3}, 4, []uint{1, 4}},
		{"nested roots", []uint{1, 2}, 4, []uint{3, 5}},
		{"duplicate ids", []uint{5, 5, 5}, 1, []uint{1, 2, 3, 4, 6}},
		{"one missing id", []uint{4, 9999}, 1, []uint{1, 2, 3, 5, 6}},
		{"only missing ids", []uint{9999}, 0, all},
		{"an id of another tenant", []uint{7}, 0, all},
		{"no ids", nil, 0, all},
		{"5000 ids", seq(1, 5000), 6, []uint{}},
		// a node and its descendant in different chunks: the ancestor (1) is in the first chunk, the descendant (6) in a later one
		{"nested across chunks, ancestor first", append(append([]uint{1}, seq(20000, 20600)...), 6), 4, []uint{3, 5}},
		// the same pair in the other order: the descendant (6) is in the first chunk, the ancestor (1) in a later one
		{"nested across chunks, descendant first", append(append([]uint{6}, seq(20000, 20600)...), 1), 4, []uint{3, 5}},
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					gdb := connAndClose(t, db)
					dropTreeTables(gdb, TestPayload{})
					ct, err := newTestTree(gdb, TestPayload{})
					if err != nil {
						t.Fatal(err)
					}
					populateTree(t, ct)
					ctx := context.Background()

					got, err := ct.DeleteRecurseMany(ctx, tc.ids, tenant1)
					if err != nil || got != tc.want {
						t.Fatalf("want %d deleted, got %d, err %v", tc.want, got, err)
					}
					left, err := ct.DescendantIds(ctx, 0, 0, tenant1)
					if err != nil {
						t.Fatal(err)
					}
					sort.Slice(left, func(i, j int) bool { return left[i] < left[j] })
					if diff := cmp.Diff(tc.left, left); diff != "" {
						t.Errorf("nodes left (-want +got):\n%s", diff)
					}
					assertConsistent(t, gdb, ct, tenant1)
					if other := countTenant(t, gdb, ct, tenant2); other.Nodes != 8 {
						t.Errorf("tenant 2 was touched: %+v", other)
					}
				})
			}
		})
	}
}

func TestDeleteRecurseManyEdges(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			populateTree(t, ct)

			if _, err := ct.DeleteRecurseMany(ctx, []uint{2, 0}, tenant1); !errors.Is(err, closuretree.ErrVirtualRoot) {
				t.Errorf("id 0: want ErrVirtualRoot, got %v", err)
			}
			if got := countTenant(t, gdb, ct, tenant1); got.Nodes != 6 {
				t.Errorf("id 0 must delete nothing, %d nodes left", got.Nodes)
			}
			if _, err := ct.DeleteRecurseMany(ctx, []uint{1}, ""); !errors.Is(err, closuretree.ErrEmptyTenant) {
				t.Errorf("want ErrEmptyTenant, got %v", err)
			}
			if n, err := ct.DeleteRecurseMany(ctx, nil, "never-seen"); n != 0 || err != nil {
				t.Errorf("no ids: got %d, %v", n, err)
			}
			if got := countTenant(t, gdb, ct, "never-seen"); got != (tenantCounts{}) {
				t.Errorf("no ids must write nothing, got %+v", got)
			}
			// inside a caller transaction that is rolled back
			_ = gdb.Transaction(func(tx *gorm.DB) error {
				if n, err := ct.WithTx(tx).DeleteRecurseMany(ctx, []uint{1, 3}, tenant1); n != 6 || err != nil {
					t.Errorf("in tx: got %d, %v", n, err)
				}
				return errRollback
			})
			if got := countTenant(t, gdb, ct, tenant1); got.Nodes != 6 {
				t.Errorf("rollback must restore the tenant, %d nodes left", got.Nodes)
			}
		})
	}
}
