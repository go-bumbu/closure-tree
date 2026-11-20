package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
)

func TestWithTxRollbackLeavesNothing(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			err = gdb.Transaction(func(tx *gorm.DB) error {
				if err := txSequence(ctx, ct.WithTx(tx), "tx-rb"); err != nil {
					t.Fatal(err)
				}
				return errRollback
			})
			if !errors.Is(err, errRollback) {
				t.Fatalf("want the rollback error, got %v", err)
			}
			if got := countTenant(t, gdb, ct, "tx-rb"); got != (tenantCounts{}) {
				t.Errorf("rows left after rollback: %+v", got)
			}
			// the original Tree still works after copies were made and discarded
			if err := ct.Add(ctx, &TestPayload{Name: "after"}, nil, nil, "tx-rb"); err != nil {
				t.Errorf("original Tree unusable: %v", err)
			}
		})
	}
}

func TestWithTxCommitEqualsPlainRun(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := gdb.Transaction(func(tx *gorm.DB) error { return txSequence(ctx, ct.WithTx(tx), "tx-a") }); err != nil {
				t.Fatal(err)
			}
			if err := txSequence(ctx, ct, "tx-b"); err != nil {
				t.Fatal(err)
			}
			want := []string{"0>b@2", "0>c@1", "b>b@0", "c>b@1", "c>c@0"}
			if diff := cmp.Diff(want, namedClosure(t, gdb, ct, "tx-a")); diff != "" {
				t.Errorf("closure rows in the outer transaction (-want +got):\n%s", diff)
			}
			if a, b := countTenant(t, gdb, ct, "tx-a"), countTenant(t, gdb, ct, "tx-b"); a != b {
				t.Errorf("row counts differ: outer tx %+v, plain %+v", a, b)
			}
		})
	}
}

type txUnique struct {
	closuretree.Node
	Name string `gorm:"size:64;uniqueIndex"`
}

// Two kinds of tree error inside the outer transaction: one raised by the database (a unique
// violation, which aborts a PostgreSQL transaction unless a savepoint is rolled back) and one
// raised by a guard (a cycle). After each, later writes succeed and the transaction commits.
func TestWithTxFailedWriteKeepsOuterUsable(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, txUnique{})
			ct, err := newTestTree(gdb, txUnique{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			err = gdb.Transaction(func(tx *gorm.DB) error {
				tt := ct.WithTx(tx)
				a, b := &txUnique{Name: "a"}, &txUnique{Name: "b"}
				if err := tt.Add(ctx, a, nil, nil, tenant1); err != nil {
					return err
				}
				if err := tt.Add(ctx, &txUnique{Name: "a"}, nil, nil, tenant1); err == nil {
					return errors.New("the duplicate name must fail")
				}
				if err := tt.Add(ctx, b, nil, nil, tenant1); err != nil {
					return fmt.Errorf("write after the database error: %w", err)
				}
				if err := tt.Update(ctx, a.NodeId, nil, up(b.NodeId), nil, tenant1); err != nil {
					return err
				}
				if err := tt.Update(ctx, b.NodeId, nil, up(a.NodeId), nil, tenant1); !errors.Is(err, closuretree.ErrInvalidMove) {
					return fmt.Errorf("the cycle: want ErrInvalidMove, got %v", err)
				}
				return tt.Add(ctx, &txUnique{Name: "c"}, nil, nil, tenant1)
			})
			if err != nil {
				t.Fatal(err)
			}
			// b and c are roots (2 rows each); a is under b (3 rows)
			if got := countTenant(t, gdb, ct, tenant1); got.Nodes != 3 || got.Closure != 7 {
				t.Errorf("want 3 nodes and 7 closure rows, got %+v", got)
			}
			assertConsistent(t, gdb, ct, tenant1)
		})
	}
}

// With DisableNestedTransaction gorm takes no savepoint of its own. A move that fails after it
// has deleted closure rows must still leave the subtree where it was.
func TestWithTxFailedMoveKeepsSubtree(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			a, b := &TestPayload{Name: "a"}, &TestPayload{Name: "b"}
			if err := ct.Add(ctx, a, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			if err := ct.Add(ctx, b, up(a.NodeId), nil, tenant1); err != nil {
				t.Fatal(err)
			}
			before := closureRows(t, gdb, ct, tenant1)
			sess := gdb.Session(&gorm.Session{DisableNestedTransaction: true})
			err = sess.Transaction(func(tx *gorm.DB) error {
				moveErr := ct.WithTx(tx).Update(ctx, b.NodeId, nil, up(99999), nil, tenant1)
				if !errors.Is(moveErr, closuretree.ErrParentNotFound) {
					return fmt.Errorf("want ErrParentNotFound, got %v", moveErr)
				}
				return nil // commit
			})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, closureRows(t, gdb, ct, tenant1)); diff != "" {
				t.Errorf("the failed move changed the tree (-before +after):\n%s", diff)
			}
		})
	}
}

func TestWithTxReadYourWrites(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			err = gdb.Transaction(func(tx *gorm.DB) error {
				tt := ct.WithTx(tx)
				n := &TestPayload{Name: "n"}
				if err := tt.Add(ctx, n, nil, nil, tenant1); err != nil {
					return err
				}
				var got TestPayload
				if err := tt.GetNode(ctx, n.NodeId, tenant1, &got); err != nil {
					return fmt.Errorf("GetNode before commit: %w", err)
				}
				var all []TestPayload
				if err := tt.Descendants(ctx, 0, 0, tenant1, &all); err != nil {
					return err
				}
				if len(all) != 1 || all[0].Name != "n" {
					return fmt.Errorf("Descendants before commit: %+v", all)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWithTxSingleConnection(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if !strings.HasPrefix(db.DbType(), "Sqlite") {
				t.Skip("the single-connection case is SQLite specific")
			}
			sqlDB, err := gdb.DB()
			if err != nil {
				t.Fatal(err)
			}
			sqlDB.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second) // a blocked pool fails instead of hanging
			defer cancel()
			err = gdb.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return txSequence(ctx, ct.WithTx(tx), tenant1) })
			if err != nil {
				t.Fatalf("sequence on one connection: %v", err)
			}
		})
	}
}

// MySQL refuses savepoint statements in the prepared statement protocol, so the savepoints must
// bypass gorm's prepared-statement connection.
func TestWithTxPreparedStatements(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			prepared := gdb.Session(&gorm.Session{PrepareStmt: true})
			err = prepared.Transaction(func(tx *gorm.DB) error {
				tt := ct.WithTx(tx)
				if err := txSequence(ctx, tt, tenant1); err != nil {
					return err
				}
				// a failed write rolls back to its savepoint
				if err := tt.Update(ctx, 99999, nil, up(0), nil, tenant1); !errors.Is(err, closuretree.ErrNodeNotFound) {
					return fmt.Errorf("want ErrNodeNotFound, got %v", err)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("prepared statements: %v", err)
			}
			assertConsistent(t, gdb, ct, tenant1)
		})
	}
}

// Review focus 1: a handle that is not a transaction.
func TestWithTxPlainHandle(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			tr := ct.WithTx(gdb.Session(&gorm.Session{}))
			if err := txSequence(ctx, tr, tenant1); err != nil {
				t.Fatalf("plain handle: %v", err)
			}
			assertConsistent(t, gdb, ct, tenant1)
		})
	}
}

// Review focus 5.
func TestWithTxNilPanics(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "WithTx") {
					t.Errorf("want a panic naming WithTx, got %v", r)
				}
			}()
			ct.WithTx(nil)
		})
	}
}
