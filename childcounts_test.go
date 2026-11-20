package closuretree_test

import (
	"context"
	"errors"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
)

// countsByName runs ChildCounts for every node of the fixture and the virtual root, and returns
// the counts by node name; the virtual root is named "root". The map is never nil.
func countsByName(t *testing.T, ct *closuretree.Tree, id map[string]uint, opts ...closuretree.ReadOption) map[string]int64 {
	t.Helper()
	ids := []uint{0}
	names := map[uint]string{0: "root"}
	for name, nodeID := range id {
		ids = append(ids, nodeID)
		names[nodeID] = name
	}
	counts, err := ct.ChildCounts(context.Background(), ids, tenantF, opts...)
	if err != nil {
		t.Fatalf("ChildCounts: %v", err)
	}
	byName := make(map[string]int64, len(counts))
	for nodeID, n := range counts {
		byName[names[nodeID]] = n
	}
	return byName
}

func TestChildCounts(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, id := filterTree(t, db)
			check := func(what string, want map[string]int64, opts ...closuretree.ReadOption) {
				t.Helper()
				if diff := cmp.Diff(want, countsByName(t, ct, id, opts...)); diff != "" {
					t.Errorf("%s (-want +got):\n%s", what, diff)
				}
			}
			// leaves (a1, x1, x21, b1) are absent
			check("no options", map[string]int64{"root": 2, "a": 2, "x": 2, "x2": 1, "b": 1})
			// only matching children are counted; whether the parent matches is not checked (x keeps its 2)
			check("a child that does not match is not counted", map[string]int64{"root": 2, "a": 1, "x": 2, "x2": 1, "b": 1},
				closuretree.Filter(closuretree.IsNull("archived_at")))
			check("a NULL comparison counts as no match", map[string]int64{"root": 2, "a": 1, "x": 2, "x2": 1, "b": 1},
				closuretree.Filter(closuretree.Eq("note", "keep")))
			// b's only child is hidden: b is absent with the filter and counted without it
			check("a parent whose only child is filtered out is absent", map[string]int64{"root": 2, "a": 2, "x": 2, "x2": 1},
				closuretree.Filter(closuretree.Eq("hidden", false)))
			check("PerNode changes nothing", map[string]int64{"root": 2, "a": 2, "x": 2, "x2": 1},
				closuretree.Filter(closuretree.Eq("hidden", false)), closuretree.PerNode())

			ftree, err := ct.WithFilter(closuretree.Eq("hidden", false))
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(map[string]int64{"root": 2, "a": 2, "x": 2, "x2": 1}, countsByName(t, ftree, id)); diff != "" {
				t.Errorf("the default filter (-want +got):\n%s", diff)
			}
			if got := countsByName(t, ftree, id, closuretree.Unfiltered()); got["b"] != 1 {
				t.Errorf("Unfiltered: %v", got)
			}
			if diff := cmp.Diff(countsByName(t, ct, id), countsByName(t, ftree, id, closuretree.Unfiltered())); diff != "" {
				t.Errorf("Unfiltered gives the counts of the unfiltered Tree (-want +got):\n%s", diff)
			}
		})
	}
}

func TestChildCountsInputs(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, id := filterTree(t, db)
			ctx := context.Background()
			counts, err := ct.ChildCounts(ctx, []uint{id["a"], id["a"], 99999, id["a1"]}, tenantF)
			if err != nil || !cmp.Equal(map[uint]int64{id["a"]: 2}, counts) {
				t.Errorf("a duplicate, an unknown id and a leaf: %v, %v", counts, err)
			}
			if counts, err := ct.ChildCounts(ctx, []uint{id["a"], 0}, "other"); err != nil || counts == nil || len(counts) != 0 {
				t.Errorf("ids of another tenant must be absent: %v, %v", counts, err)
			}
			if _, err := ct.ChildCounts(ctx, []uint{id["a"]}, ""); !errors.Is(err, closuretree.ErrEmptyTenant) {
				t.Errorf("empty tenant: %v", err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel() // a query would fail with context.Canceled
			if counts, err := ct.ChildCounts(cancelled, nil, tenantF); err != nil || counts == nil || len(counts) != 0 {
				t.Errorf("no ids: want an empty map without a query, got %v, %v", counts, err)
			}
			if _, err := ct.ChildCounts(cancelled, []uint{id["a"]}, tenantF, closuretree.Filter(closuretree.Eq("nope", 1))); !errors.Is(err, closuretree.ErrUnknownColumn) {
				t.Errorf("unknown column: %v", err)
			}
		})
	}
}

// 8,003 ids in one call, more than one chunk, two of them duplicates: the nodes 1 to 2000 have
// children, and the virtual root has one (the single root node).
func TestChildCountsOnLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct := largeTree(t, db)
			ctx := context.Background()
			all := append([]uint{0}, seq(1, largeTreeNodes)...)
			all = append(all, 21, 1) // duplicates that land in another chunk than their first occurrence
			check := func(what string, wantOf21 int64, opts ...closuretree.ReadOption) {
				t.Helper()
				within(t, what, func() error {
					counts, err := ct.ChildCounts(ctx, all, tenantF, opts...)
					if err != nil {
						return err
					}
					if len(counts) != 2001 || counts[0] != 1 || counts[1] != 4 || counts[2000] != 3 || counts[21] != wantOf21 {
						t.Errorf("%s: %d parents, root %d, node 1 %d, node 2000 %d, node 21 %d",
							what, len(counts), counts[0], counts[1], counts[2000], counts[21])
					}
					return nil
				})
			}
			check("unfiltered", 4)
			check("filtered", 3, closuretree.Filter(closuretree.Eq("kind", "doc"))) // node 85, a child of 21, does not match
		})
	}
}
