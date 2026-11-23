package closuretree_test

import (
	"context"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
)

// On MariaDB a recursive query stops after max_recursive_iterations steps with only a warning. The
// tree reads lift that limit for their own statement, so they return the whole subtree even when
// the session's limit is lower than its depth. The test lowers the session's limit inside a
// transaction, which pins one connection, and reads through a Tree bound to it.
func TestTreeReadsIgnoreTheRecursionLimitOnMariaDB(t *testing.T) {
	const levels, limit = 8, 3
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMariadb {
				t.Skip("MariaDB only: PostgreSQL and SQLite have no default limit")
			}
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			var parent *uint
			for range levels {
				n := TestPayload{Name: "n"}
				if err := ct.Add(ctx, &n, parent, nil, tenant1); err != nil {
					t.Fatal(err)
				}
				parent = up(n.NodeId)
			}

			err = gdb.Transaction(func(tx *gorm.DB) error {
				if err := tx.Exec("SET SESSION max_recursive_iterations = ?", limit).Error; err != nil {
					return err
				}
				var cut int
				if err := tx.Raw(`WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < ?)
					SELECT MAX(n) FROM r`, levels).Scan(&cut).Error; err != nil {
					return err
				}
				if cut >= levels {
					t.Fatalf("precondition: a plain recursive query reached %d levels under the limit %d", cut, limit)
				}

				tt := ct.WithTx(tx)
				var items []*TestPayload
				if err := tt.TreeDescendants(ctx, 0, 0, tenant1, &items); err != nil {
					return err
				}
				if got := payloadDepth(items); got != levels {
					t.Errorf("TreeDescendants: want %d levels, got %d", levels, got)
				}
				ids, err := tt.TreeDescendantsIds(ctx, 0, 0, tenant1)
				if err != nil {
					return err
				}
				if got := treeNodeDepth(ids); got != levels {
					t.Errorf("TreeDescendantsIds: want %d levels, got %d", levels, got)
				}
				items = nil
				if err := tt.TreeDescendants(ctx, 0, levels-1, tenant1, &items); err != nil {
					return err
				}
				if got := payloadDepth(items); got != levels-1 {
					t.Errorf("TreeDescendants with maxDepth %d: got %d levels", levels-1, got)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// payloadDepth returns the number of levels of the linked tree nodes.
func payloadDepth(nodes []*TestPayload) int {
	most := 0
	for _, n := range nodes {
		most = max(most, payloadDepth(n.Children))
	}
	if len(nodes) == 0 {
		return 0
	}
	return most + 1
}

// treeNodeDepth returns the number of levels of the linked TreeNodes.
func treeNodeDepth(nodes []*closuretree.TreeNode) int {
	most := 0
	for _, n := range nodes {
		most = max(most, treeNodeDepth(n.Children))
	}
	if len(nodes) == 0 {
		return 0
	}
	return most + 1
}
