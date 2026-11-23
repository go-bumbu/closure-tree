package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
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
			if !strings.HasPrefix(db.DbType(), "Sqlite") {
				t.Skip("the single-connection case is SQLite specific")
			}
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
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

// A handle that is not a transaction: the copy behaves like the base Tree and LockTenant refuses.
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

// txOrder is a model of the caller's own, with an AfterCreate hook that runs afterCreate.
type txOrder struct {
	ID          uint `gorm:"primaryKey"`
	Name        string
	afterCreate func(tx *gorm.DB) error
}

func (o *txOrder) AfterCreate(tx *gorm.DB) error {
	if o.afterCreate == nil {
		return nil
	}
	return o.afterCreate(tx)
}

// treeWithOrders returns a tree with a (root) and b (under a) in tenant1, and an empty order table.
func treeWithOrders(t *testing.T, db testdbs.TargetDb) (*gorm.DB, *closuretree.Tree, *TestPayload, *TestPayload) {
	t.Helper()
	gdb := connAndClose(t, db) // one database per test name
	dropTreeTables(gdb, TestPayload{})
	if err := gdb.Migrator().DropTable(&txOrder{}); err != nil {
		t.Fatal(err)
	}
	if err := gdb.AutoMigrate(&txOrder{}); err != nil {
		t.Fatal(err)
	}
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
	return gdb, ct, a, b
}

// readAndAdd runs a check, a read and a write on tr, which sees a and b (b under a).
func readAndAdd(ctx context.Context, tr *closuretree.Tree, a, b *TestPayload, add string) error {
	isDesc, err := tr.IsDescendant(ctx, a.NodeId, b.NodeId, tenant1)
	if err != nil {
		return fmt.Errorf("IsDescendant: %w", err)
	}
	if !isDesc {
		return errors.New("IsDescendant: b must be a descendant of a")
	}
	var nodes []TestPayload
	if err := tr.Descendants(ctx, 0, 0, tenant1, &nodes); err != nil {
		return fmt.Errorf("Descendants: %w", err)
	}
	if len(nodes) != 2 {
		return fmt.Errorf("Descendants: want 2 nodes, got %+v", nodes)
	}
	if err := tr.Add(ctx, &TestPayload{Name: add}, up(a.NodeId), nil, tenant1); err != nil {
		return fmt.Errorf("Add: %w", err)
	}
	return nil
}

// A gorm hook gets a handle that carries the statement of the running create. The Tree bound to it
// uses only its transaction: the create runs once, the tree calls work, and a tree write in the
// hook commits or rolls back with the create.
func TestWithTxInsideHook(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, a, b := treeWithOrders(t, db)
			ctx := context.Background()
			commit := &txOrder{Name: "commit", afterCreate: func(tx *gorm.DB) error {
				return readAndAdd(ctx, ct.WithTx(tx), a, b, "c")
			}}
			if err := gdb.Create(commit).Error; err != nil {
				t.Fatalf("create with tree calls in the hook: %v", err)
			}
			rollback := &txOrder{Name: "rollback", afterCreate: func(tx *gorm.DB) error {
				if err := ct.WithTx(tx).Add(ctx, &TestPayload{Name: "d"}, nil, nil, tenant1); err != nil {
					return err
				}
				return errRollback
			}}
			quiet := gdb.Session(&gorm.Session{Logger: logger.Discard}) // the rollback error is checked below
			if err := quiet.Create(rollback).Error; !errors.Is(err, errRollback) {
				t.Fatalf("want the rollback error, got %v", err)
			}
			var orders []string
			if err := gdb.Model(&txOrder{}).Order("id").Pluck("name", &orders).Error; err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff([]string{"commit"}, orders); diff != "" {
				t.Errorf("orders (-want +got):\n%s", diff)
			}
			want := []string{"0>a@1", "0>b@2", "0>c@2", "a>a@0", "a>b@1", "a>c@1", "b>b@0", "c>c@0"}
			if diff := cmp.Diff(want, namedClosure(t, gdb, ct, tenant1)); diff != "" {
				t.Errorf("closure rows (-want +got):\n%s", diff)
			}
			assertConsistent(t, gdb, ct, tenant1)
		})
	}
}

// A handle that carries a model and a condition: the Tree bound to it ignores both and keeps the
// transaction.
func TestWithTxHandleWithStatement(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, a, b := treeWithOrders(t, db)
			ctx := context.Background()
			before := countTenant(t, gdb, ct, tenant1)
			err := gdb.Transaction(func(tx *gorm.DB) error {
				tr := ct.WithTx(tx.Model(&txOrder{}).Where("name = ?", "none"))
				if err := readAndAdd(ctx, tr, a, b, "c"); err != nil {
					return err
				}
				var got TestPayload
				if err := tr.GetNode(ctx, a.NodeId, tenant1, &got); err != nil {
					return fmt.Errorf("GetNode: %w", err)
				}
				if got.Name != "a" {
					return fmt.Errorf("GetNode: want a, got %+v", got)
				}
				return errRollback
			})
			if !errors.Is(err, errRollback) {
				t.Fatalf("want the rollback error, got %v", err)
			}
			if got := countTenant(t, gdb, ct, tenant1); got != before {
				t.Errorf("rows after rollback: want %+v, got %+v", before, got)
			}
		})
	}
}

// WithTx(nil) panics at the call with a message that names WithTx. The check needs no database:
// it comes before anything else, so a zero Tree is enough.
func TestWithTxNilPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "WithTx") {
			t.Errorf("want a panic naming WithTx, got %v", r)
		}
	}()
	new(closuretree.Tree).WithTx(nil)
}

func TestLockTenantNeedsTransaction(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := ct.LockTenant(ctx, tenant1); !errors.Is(err, closuretree.ErrNotInTransaction) {
				t.Errorf("base Tree: want ErrNotInTransaction, got %v", err)
			}
			if err := ct.LockTenant(ctx, ""); !errors.Is(err, closuretree.ErrEmptyTenant) {
				t.Errorf("empty tenant: want ErrEmptyTenant, got %v", err)
			}
			// a handle that is not a transaction cannot hold the lock
			plain := ct.WithTx(gdb.Session(&gorm.Session{}))
			if err := plain.LockTenant(ctx, tenant1); !errors.Is(err, closuretree.ErrNotInTransaction) {
				t.Errorf("plain handle: want ErrNotInTransaction, got %v", err)
			}
		})
	}
}

// A holds the lock from its first statement, reads, then moves x under y. B tries the opposite
// move meanwhile. A must win and B must see A's move.
func TestLockTenantFirstSerializesConflictingMoves(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			x, y := &TestPayload{Name: "x"}, &TestPayload{Name: "y"}
			for _, n := range []*TestPayload{x, y} {
				if err := ct.Add(ctx, n, nil, nil, tenant1); err != nil {
					t.Fatal(err)
				}
			}
			locked := make(chan struct{})
			var errB error
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-locked
				deadline := time.Now().Add(10 * time.Second)
				for {
					errB = ct.Update(ctx, y.NodeId, nil, up(x.NodeId), nil, tenant1)
					if errB == nil || !isTransientDBErr(errB) || time.Now().After(deadline) {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
			}()
			// B starts once A holds the lock. releaseB closes locked once only: it is called again
			// after A returns, so an A that fails before the lock (or cannot begin) fails the test
			// instead of leaving B blocked on <-locked.
			var releaseOnce sync.Once
			releaseB := func() { releaseOnce.Do(func() { close(locked) }) }
			errA := gdb.Transaction(func(tx *gorm.DB) error {
				tt := ct.WithTx(tx)
				if err := tt.LockTenant(ctx, tenant1); err != nil {
					return err
				}
				var probe TestPayload
				if err := tt.GetNode(ctx, x.NodeId, tenant1, &probe); err != nil { // a plain read after the lock
					return err
				}
				releaseB()
				switch db.DbType() {
				case testdbs.DBTypePostgres:
					if err := waitForPostgresLockWaiter(ctx, gdb, 10*time.Second); err != nil {
						return fmt.Errorf("B did not wait on the lock: %w", err)
					}
				case testdbs.DBTypeMysql:
					if err := waitForMySQLLockStatement(ctx, gdb, ct.NodeTableName(), 10*time.Second); err != nil {
						return fmt.Errorf("B did not wait on the lock: %w", err)
					}
				default:
					// SQLite shows no lock waits: B's write fails as busy and B retries (see above).
					// A gives B time to reach the lock.
					time.Sleep(200 * time.Millisecond)
				}
				return tt.Update(ctx, x.NodeId, nil, up(y.NodeId), nil, tenant1)
			})
			releaseB()
			wg.Wait()
			if errA != nil {
				t.Fatalf("A: %v", errA)
			}
			if !errors.Is(errB, closuretree.ErrInvalidMove) {
				t.Fatalf("B: want ErrInvalidMove, got %v", errB)
			}
			rel := ct.ClosureTableName()
			if relCount(t, gdb, rel, y.NodeId, x.NodeId, tenant1) != 1 || relCount(t, gdb, rel, x.NodeId, y.NodeId, tenant1) != 0 {
				t.Fatal("want x under y and no cycle")
			}
		})
	}
}

// waitForPostgresLockWaiter returns once a session on gdb's PostgreSQL database waits on a lock, as
// pg_stat_activity reports it, or an error when none does within timeout.
func waitForPostgresLockWaiter(ctx context.Context, gdb *gorm.DB, timeout time.Duration) error {
	const query = "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'"
	deadline := time.Now().Add(timeout)
	for {
		var n int64
		if err := gdb.WithContext(ctx).Raw(query).Scan(&n).Error; err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no session waited on a lock within %v", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForMySQLLockStatement returns once another session on gdb's MySQL database runs a statement on
// the lock table of the tree whose node table is nodeTbl, or an error when none does within
// timeout. MySQL shows lock waits (INNODB_TRX, performance_schema.data_lock_waits) only to a user
// with privileges the test user lacks, and the processlist has no state for a row-lock wait, but
// each user sees its own sessions there. A session that runs a statement on the lock table while
// another holds the tenant's row is at the lock: B's attempt blocks on the upsert of the lock row
// or on its SELECT ... FOR UPDATE (lockTenantHook). A prepared statement shows command Execute.
func waitForMySQLLockStatement(ctx context.Context, gdb *gorm.DB, nodeTbl string, timeout time.Duration) error {
	const query = "SELECT COUNT(*) FROM information_schema.processlist WHERE db = DATABASE() " +
		"AND id <> CONNECTION_ID() AND command <> 'Sleep' AND LOCATE(?, info) > 0"
	lockTbl := "closure_tree_lock_" + nodeTbl
	deadline := time.Now().Add(timeout)
	for {
		var n int64
		if err := gdb.WithContext(ctx).Raw(query, lockTbl).Scan(&n).Error; err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no session ran a statement on %s within %v", lockTbl, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The transactions the library opens itself must stay safe when the database defaults to
// REPEATABLE READ: there the snapshot is fixed at the first statement, the lock statement, before
// it waits for the lock, so the cycle guard of a move could read a stale tree.
func TestPostgresOwnTransactionsUnderRepeatableReadDefault(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypePostgres {
				t.Skip("PostgreSQL only")
			}
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			// connAndClose keeps no idle connections, so every statement opens a new session and the
			// database default set below applies to it.
			var name string
			if err := gdb.Raw("SELECT current_database()").Scan(&name).Error; err != nil {
				t.Fatal(err)
			}
			setDefault := func(level string) {
				stmt := fmt.Sprintf("ALTER DATABASE %q SET default_transaction_isolation TO '%s'", name, level)
				if err := gdb.Exec(stmt).Error; err != nil {
					t.Fatal(err)
				}
			}
			setDefault("repeatable read")
			t.Cleanup(func() {
				reset := fmt.Sprintf("ALTER DATABASE %q RESET default_transaction_isolation", name)
				if err := gdb.Exec(reset).Error; err != nil {
					t.Errorf("reset default isolation: %v", err)
				}
			})
			var level string
			if err := gdb.Raw("SHOW default_transaction_isolation").Scan(&level).Error; err != nil {
				t.Fatal(err)
			}
			if level != "repeatable read" {
				t.Fatalf("precondition: the default level is %q", level)
			}
			raceConflictingMoves(t, gdb, ct, 40)
		})
	}
}

// On a Tree not bound to a transaction, writes that delete closure rows must not deadlock with
// other tenants' adds. No closure-table index leads with the tenant, so on MySQL at REPEATABLE
// READ their deletes locked closure rows of other tenants, and concurrent adds failed with error
// 1213, until the library's own transactions ran at READ COMMITTED there too. One goroutine repeats
// the write on tenant a while two others repeat Add on tenants b and c; no call may fail.
func TestOwnTransactionsDoNotDeadlockAcrossTenants(t *testing.T) {
	const (
		writes = 40   // calls of the write on tenant a
		adds   = 80   // calls of Add on each of the tenants b and c
		seeded = 1000 // nodes of b and c before the calls
	)
	cases := []struct {
		name string
		// prepare returns the write, one call on tenant a; it may first add nodes to tenant a.
		prepare func(t *testing.T, ctx context.Context, ct *closuretree.Tree) func(ctx context.Context) error
	}{
		{"DeleteTenant", func(_ *testing.T, _ context.Context, ct *closuretree.Tree) func(context.Context) error {
			return addAndDeleteTenant(ct, "a")
		}},
		{"DeleteRecurseMany", func(_ *testing.T, _ context.Context, ct *closuretree.Tree) func(context.Context) error {
			return func(ctx context.Context) error {
				root, err := addRootAndChild(ctx, ct, "a")
				if err != nil {
					return err
				}
				_, err = ct.DeleteRecurseMany(ctx, []uint{root}, "a")
				return err
			}
		}},
		{"DeleteRecurse", func(_ *testing.T, _ context.Context, ct *closuretree.Tree) func(context.Context) error {
			return addAndDeleteRecurse(ct, "a")
		}},
		{"BuildFromParents", func(t *testing.T, ctx context.Context, ct *closuretree.Tree) func(context.Context) error {
			var links []closuretree.ParentLink // a chain of ten nodes
			var parent uint
			for range 10 {
				n := &TestPayload{Name: "n"}
				if err := ct.Add(ctx, n, up(parent), nil, "a"); err != nil {
					t.Fatal(err)
				}
				links = append(links, closuretree.ParentLink{NodeID: n.NodeId, ParentID: parent})
				parent = n.NodeId
			}
			return func(ctx context.Context) error {
				return ct.BuildFromParents(ctx, "a", links, closuretree.Rebuild())
			}
		}},
		{"Update moving a subtree", func(t *testing.T, ctx context.Context, ct *closuretree.Tree) func(context.Context) error {
			return moveSubtree(t, ctx, ct, "a")
		}},
	}
	for _, db := range testdbs.DBs() {
		for _, c := range cases {
			t.Run(db.DbType()+"/"+c.name, func(t *testing.T) {
				if strings.HasPrefix(db.DbType(), "Sqlite") {
					t.Skip("SQLite allows one writing transaction per database, so writes never wait on each other's rows")
				}
				gdb := connAndClose(t, db) // one database per test name
				dropTreeTables(gdb, TestPayload{})
				quiet := gdb.Session(&gorm.Session{Logger: logger.Discard}) // errors are reported by the test
				ct, err := newTestTree(quiet, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // fail instead of hanging
				defer cancel()
				for i, tenant := range []string{"b", "c"} {
					seedRootWithChildren(t, ctx, quiet, db, ct, tenant, seq(uint(i*seeded+1), uint((i+1)*seeded)))
				}
				write := c.prepare(t, ctx, ct)
				failures := raceOtherTenantAdds(ctx, ct, c.name+" on a", writes, write, adds, "b", "c")
				for _, f := range failures {
					t.Error(f)
				}
				for _, tenant := range []string{"a", "b", "c"} {
					assertConsistent(t, gdb, ct, tenant)
				}
			})
		}
	}
}

// seedRootWithChildren inserts the node rows ids for tenant, the first a root and the others its
// children, builds their closure rows with BuildFromParents, and returns the links it built from.
func seedRootWithChildren(t *testing.T, ctx context.Context, gdb *gorm.DB, db testdbs.TargetDb,
	ct *closuretree.Tree, tenant string, ids []uint) []closuretree.ParentLink {
	t.Helper()
	nodes := make([]seed, len(ids))
	links := make([]closuretree.ParentLink, len(ids))
	for i, id := range ids {
		nodes[i] = seed{ID: id, Name: "seed"}
		links[i] = closuretree.ParentLink{NodeID: id, ParentID: ids[0]}
	}
	links[0].ParentID = 0
	seedNodes(t, gdb, db, tenant, nodes)
	if err := ct.BuildFromParents(ctx, tenant, links); err != nil {
		t.Fatal(err)
	}
	return links
}

// raceOtherTenantAdds calls write writes times on one goroutine while, on one goroutine per tenant
// of others, it calls Add on ct adds times, alternating a root and a child of it. It returns one
// line for each of them whose calls failed, with the count and the first error; name names write.
func raceOtherTenantAdds(ctx context.Context, ct *closuretree.Tree, name string, writes int,
	write func(context.Context) error, adds int, others ...string) []string {
	writers := []raceWriter{{name: name, calls: writes, write: write}}
	for _, tenant := range others {
		var root uint
		i := 0
		writers = append(writers, raceWriter{name: "Add on " + tenant, calls: adds, write: func(ctx context.Context) error {
			var parent *uint // a root, then a child of it
			if i%2 == 1 {
				parent = up(root)
			}
			i++
			n := &TestPayload{Name: "n"}
			err := ct.Add(ctx, n, parent, nil, tenant)
			if err == nil && parent == nil {
				root = n.NodeId
			}
			return err
		}})
	}
	return raceWriters(ctx, writers...)
}

// raceWriter is one goroutine of raceWriters: it calls write calls times.
type raceWriter struct {
	name  string
	calls int
	write func(ctx context.Context) error
}

// raceWriters runs every writer on its own goroutine, all at the same time. It returns one line for
// each writer whose calls failed, with the count and the first error.
func raceWriters(ctx context.Context, writers ...raceWriter) []string {
	var mu sync.Mutex
	failed := map[string]int{}
	first := map[string]error{}
	var wg sync.WaitGroup
	for _, w := range writers {
		wg.Go(func() {
			for range w.calls {
				err := w.write(ctx)
				if err == nil {
					continue
				}
				mu.Lock()
				if failed[w.name]++; failed[w.name] == 1 {
					first[w.name] = err
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	var lines []string
	for _, w := range writers {
		if n := failed[w.name]; n > 0 {
			lines = append(lines, fmt.Sprintf("%s: %d of %d calls failed, the first with: %v", w.name, n, w.calls, first[w.name]))
		}
	}
	return lines
}

// addRootAndChild adds a root and a child of it to tenant on ct, and returns the root's id.
func addRootAndChild(ctx context.Context, ct *closuretree.Tree, tenant string) (uint, error) {
	root := &TestPayload{Name: "r"}
	if err := ct.Add(ctx, root, nil, nil, tenant); err != nil {
		return 0, err
	}
	return root.NodeId, ct.Add(ctx, &TestPayload{Name: "c"}, up(root.NodeId), nil, tenant)
}

// addAndDeleteTenant returns a write that adds a root and a child of it to tenant on ct, then
// deletes the tenant.
func addAndDeleteTenant(ct *closuretree.Tree, tenant string) func(context.Context) error {
	return func(ctx context.Context) error {
		if _, err := addRootAndChild(ctx, ct, tenant); err != nil {
			return err
		}
		return ct.DeleteTenant(ctx, tenant)
	}
}

// addAndDeleteRecurse returns a write that adds a root and a child of it to tenant on ct, then
// deletes the root with DeleteRecurse.
func addAndDeleteRecurse(ct *closuretree.Tree, tenant string) func(context.Context) error {
	return func(ctx context.Context) error {
		root, err := addRootAndChild(ctx, ct, tenant)
		if err != nil {
			return err
		}
		return ct.DeleteRecurse(ctx, root, tenant)
	}
}

// moveSubtree adds three roots p, q and x, each with a child, to tenant on ct, moves x under p, and
// returns a write that moves x with its child to the other of p and q on each call.
func moveSubtree(t *testing.T, ctx context.Context, ct *closuretree.Tree, tenant string) func(context.Context) error {
	t.Helper()
	var roots [3]uint
	for i := range roots {
		root, err := addRootAndChild(ctx, ct, tenant)
		if err != nil {
			t.Fatal(err)
		}
		roots[i] = root
	}
	p, q, x := roots[0], roots[1], roots[2]
	if err := ct.Update(ctx, x, nil, up(p), nil, tenant); err != nil {
		t.Fatal(err)
	}
	parents, i := [2]uint{p, q}, 0
	return func(ctx context.Context) error {
		i++
		return ct.Update(ctx, x, nil, up(parents[i%2]), nil, tenant)
	}
}

// On a Tree not bound to a transaction, two or three tenants each repeat a write that deletes
// closure rows, at the same time; no call may fail. At READ COMMITTED InnoDB still locks each
// closure row such a delete scans, and waits for a row that another transaction holds (no
// closure-table index leads with the tenant), so on MySQL some of these pairs deadlock: they run on
// PostgreSQL only (mysqlDeadlocks). The others guard what was measured. Each writer gets its own
// tenant and its own range of node ids; tenant z holds rows that the deletes scan past.
func TestOwnDeletingWritesAcrossTenants(t *testing.T) {
	const (
		calls  = 30   // calls of each writer
		seeded = 1000 // node ids per range
		many   = 600  // nodes of DeleteRecurseMany: two chunks of ids
	)
	// A writer's prepare returns its write, one call on tenant; it may first seed the node rows ids
	// of tenant.
	type writer struct {
		name    string
		prepare func(t *testing.T, ctx context.Context, rt raceTree, tenant string, ids []uint) func(context.Context) error
	}
	deleteTenant := writer{"DeleteTenant", func(_ *testing.T, _ context.Context, rt raceTree, tenant string, _ []uint) func(context.Context) error {
		return addAndDeleteTenant(rt.ct, tenant)
	}}
	deleteRecurse := writer{"DeleteRecurse", func(_ *testing.T, _ context.Context, rt raceTree, tenant string, _ []uint) func(context.Context) error {
		return addAndDeleteRecurse(rt.ct, tenant)
	}}
	move := writer{"Update moving a subtree", func(t *testing.T, ctx context.Context, rt raceTree, tenant string, _ []uint) func(context.Context) error {
		return moveSubtree(t, ctx, rt.ct, tenant)
	}}
	rebuild := writer{"BuildFromParents(Rebuild())", func(t *testing.T, ctx context.Context, rt raceTree, tenant string, ids []uint) func(context.Context) error {
		links := seedRootWithChildren(t, ctx, rt.gdb, rt.db, rt.ct, tenant, ids)
		return func(ctx context.Context) error {
			return rt.ct.BuildFromParents(ctx, tenant, links, closuretree.Rebuild())
		}
	}}
	deleteMany := writer{fmt.Sprintf("DeleteRecurseMany of %d ids", many), func(t *testing.T, ctx context.Context, rt raceTree, tenant string, ids []uint) func(context.Context) error {
		ids = ids[:many]
		roots := func() ([]TestPayload, []closuretree.ParentLink) {
			nodes := make([]TestPayload, len(ids))
			links := make([]closuretree.ParentLink, len(ids))
			for i, id := range ids {
				nodes[i] = TestPayload{Node: closuretree.Node{NodeId: id, Tenant: tenant}, Name: "r"}
				links[i] = closuretree.ParentLink{NodeID: id}
			}
			return nodes, links
		}
		// insert adds the node rows ids to tenant, each a root, the way an import would.
		insert := func(ctx context.Context) error {
			nodes, links := roots()
			if err := rt.gdb.WithContext(ctx).Create(&nodes).Error; err != nil {
				return err
			}
			return rt.ct.BuildFromParents(ctx, tenant, links)
		}
		if err := insert(ctx); err != nil {
			t.Fatal(err)
		}
		if rt.db.DbType() == testdbs.DBTypePostgres { // the adds of other writers must not reuse ids
			if err := rt.gdb.Exec(`SELECT setval(pg_get_serial_sequence('test_payloads', 'node_id'),
				(SELECT MAX(node_id) FROM test_payloads))`).Error; err != nil {
				t.Fatal(err)
			}
		}
		return func(ctx context.Context) error {
			deleted, err := rt.ct.DeleteRecurseMany(ctx, ids, tenant)
			if err != nil {
				return err
			}
			if deleted != many {
				return fmt.Errorf("deleted %d nodes, want %d", deleted, many)
			}
			return insert(ctx)
		}
	}}

	cases := []struct {
		name    string
		writers []writer
		// mysqlDeadlocks marks a pair that deadlocks on MySQL, so it runs on PostgreSQL only. In ten
		// runs of 30 calls per writer, the rebuild lost calls to deadlocks against
		// DeleteRecurseMany, which never failed, and the pair of Update and DeleteRecurse lost calls
		// to deadlocks. raceWriters counts every failed call, not deadlocks alone, so no count is
		// given.
		mysqlDeadlocks bool
	}{
		{"three DeleteTenant", []writer{deleteTenant, deleteTenant, deleteTenant}, false},
		{"DeleteRecurseMany and rebuild", []writer{deleteMany, rebuild}, true},
		{"rebuild and rebuild", []writer{rebuild, rebuild}, false},
		{"Update and DeleteRecurse", []writer{move, deleteRecurse}, true},
	}
	for _, db := range testdbs.DBs() {
		for _, c := range cases {
			t.Run(db.DbType()+"/"+c.name, func(t *testing.T) {
				if strings.HasPrefix(db.DbType(), "Sqlite") {
					t.Skip("SQLite allows one writing transaction per database, so writes never wait on each other's rows")
				}
				if c.mysqlDeadlocks && db.DbType() == testdbs.DBTypeMysql {
					t.Skip("these writes deadlock on MySQL: InnoDB locks the closure rows a delete scans, also at READ COMMITTED")
				}
				gdb := connAndClose(t, db) // one database per test name
				dropTreeTables(gdb, TestPayload{})
				quiet := gdb.Session(&gorm.Session{Logger: logger.Discard}) // errors are reported by the test
				ct, err := newTestTree(quiet, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute) // fail instead of hanging
				defer cancel()
				rt := raceTree{gdb: quiet, db: db, ct: ct}
				seedRootWithChildren(t, ctx, quiet, db, ct, "z", seq(1, seeded))
				tenants := []string{"a", "b", "c"}[:len(c.writers)]
				racers := make([]raceWriter, len(c.writers))
				for i, w := range c.writers {
					ids := seq(uint((i+1)*seeded+1), uint((i+2)*seeded))
					racers[i] = raceWriter{name: w.name + " on " + tenants[i], calls: calls, write: w.prepare(t, ctx, rt, tenants[i], ids)}
				}
				for _, f := range raceWriters(ctx, racers...) {
					t.Error(f)
				}
				for _, tenant := range append([]string{"z"}, tenants...) {
					assertConsistent(t, gdb, ct, tenant)
				}
			})
		}
	}
}

// raceTree is the tree a race runs on: its database handle, the database and the Tree.
type raceTree struct {
	gdb *gorm.DB
	db  testdbs.TargetDb
	ct  *closuretree.Tree
}

// Two caller transactions write to two new tenants whose names sort next to each other, with other
// tenants' rows around them: A adds a root, B's first add starts while A is open, then A adds a
// child of its root. Both must commit without a deadlock and keep their rows (on MySQL an UPDATE of
// the sort-order meta table that matched no row used to leave gap locks that deadlocked here). The
// order of the statements is fixed: A's second add starts only once B's add has finished or is
// waiting on a lock, and B commits only after A.
func TestWithTxAdjacentTenantsDoNotDeadlock(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if strings.HasPrefix(db.DbType(), "Sqlite") {
				t.Skip("SQLite allows one writing transaction per database, so B cannot write while A is open")
			}
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // fail instead of hanging
			defer cancel()
			const tenantA, tenantB = "adj-a", "adj-b"
			for _, other := range []string{"adj-0", "adj-z"} { // sort before A and after B
				if err := ct.Add(ctx, &TestPayload{Name: "other"}, nil, nil, other); err != nil {
					t.Fatal(err)
				}
			}
			quiet := gdb.Session(&gorm.Session{Logger: logger.Discard}) // errors are reported by the test

			txA := quiet.WithContext(ctx).Begin()
			if txA.Error != nil {
				t.Fatal(txA.Error)
			}
			defer txA.Rollback()
			a1 := &TestPayload{Name: "a1"}
			if err := ct.WithTx(txA).Add(ctx, a1, nil, nil, tenantA); err != nil {
				t.Fatalf("A's first add: %v", err)
			}

			written := make(chan error, 1)
			releaseB := make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseB) })
			defer release() // a failing test does not leave B waiting
			idB, doneB := startTxWrite(ctx, db, quiet, written, releaseB, func(tx *gorm.DB) error {
				return ct.WithTx(tx).Add(ctx, &TestPayload{Name: "b1"}, nil, nil, tenantB)
			})
			waitWrittenOrBlocked(t, ctx, gdb, db, <-idB, written)

			errA := ct.WithTx(txA).Add(ctx, &TestPayload{Name: "a2"}, up(a1.NodeId), nil, tenantA)
			if errA == nil {
				errA = txA.Commit().Error
			}
			release()
			var resB txResult
			select {
			case resB = <-doneB:
			case <-time.After(30 * time.Second):
				t.Fatal("B did not finish")
			}
			if errA != nil {
				t.Errorf("A: %v", errA)
			}
			if resB.write != nil || resB.commit != nil {
				t.Errorf("B: add: %v, commit: %v", resB.write, resB.commit)
			}
			if got := countTenant(t, gdb, ct, tenantA).Nodes; got != 2 {
				t.Errorf("tenant A: want 2 nodes, got %d", got)
			}
			if got := countTenant(t, gdb, ct, tenantB).Nodes; got != 1 {
				t.Errorf("tenant B: want 1 node, got %d", got)
			}
			assertConsistent(t, gdb, ct, tenantA)
			assertConsistent(t, gdb, ct, tenantB)
		})
	}
}

// txResult is the outcome of a caller transaction run by startTxWrite.
type txResult struct{ write, commit error }

// startTxWrite runs write in a transaction on gdb, on a goroutine of its own. It sends the
// transaction's database session id on the first returned channel and the write's error on
// written, then waits for release and commits, or rolls back after a failed write; the outcome
// arrives on the second returned channel.
func startTxWrite(ctx context.Context, db testdbs.TargetDb, gdb *gorm.DB, written chan<- error,
	release <-chan struct{}, write func(tx *gorm.DB) error) (<-chan int64, <-chan txResult) {
	session := make(chan int64, 1)
	done := make(chan txResult, 1)
	go func() {
		var res txResult
		defer func() { done <- res }()
		var id int64
		tx := gdb.WithContext(ctx).Begin()
		res.write = tx.Error
		if res.write == nil {
			query := "SELECT CONNECTION_ID()"
			if db.DbType() == testdbs.DBTypePostgres {
				query = "SELECT pg_backend_pid()"
			}
			res.write = tx.Raw(query).Scan(&id).Error
		}
		session <- id
		if res.write == nil {
			res.write = write(tx)
		}
		written <- res.write
		<-release
		if res.write != nil {
			tx.Rollback()
			return
		}
		res.commit = tx.Commit().Error
	}()
	return session, done
}

// waitWrittenOrBlocked returns once the write that startTxWrite runs in session id has finished
// (its error, read from written, is put back) or is waiting on a lock. PostgreSQL reports the lock
// wait; MySQL shows it only with the PROCESS privilege, so there the write counts as waiting once
// one of its statements has run for 150ms, far longer than any of them takes unblocked.
func waitWrittenOrBlocked(t *testing.T, ctx context.Context, gdb *gorm.DB, db testdbs.TargetDb, id int64, written chan error) {
	t.Helper()
	running := "SELECT COUNT(*) FROM pg_stat_activity WHERE pid = ? AND wait_event_type = 'Lock'"
	polls := 1
	if db.DbType() == testdbs.DBTypeMysql {
		running = "SELECT COUNT(*) FROM information_schema.processlist WHERE id = ? AND command <> 'Sleep' AND info IS NOT NULL"
		polls = 15
	}
	for seen := 0; seen < polls; {
		select {
		case err := <-written:
			written <- err
			return
		case <-ctx.Done():
			t.Fatal("the write neither finished nor waited on a lock")
		case <-time.After(10 * time.Millisecond):
		}
		if countRaw(t, gdb, running, id) > 0 {
			seen++
		} else {
			seen = 0
		}
	}
}

// A holds tenant1's lock. B, in a transaction that already has a write of its own, adds to tenant1
// with a short timeout, which ends while B's statement waits on the lock: the driver drops B's
// connection, so the rollback to the savepoint fails too. The error matches ErrSavepointRollback,
// and B's transaction, with its earlier write, is gone.
func TestWithTxTimeoutDuringStatementLosesTransaction(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if strings.HasPrefix(db.DbType(), "Sqlite") {
				t.Skip("SQLite allows one writing transaction per database, so B cannot write while A is open")
			}
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // fail instead of hanging
			defer cancel()
			for _, tenant := range []string{tenant1, tenant2} { // the lock rows exist before A and B
				if err := ct.Add(ctx, &TestPayload{Name: "committed"}, nil, nil, tenant); err != nil {
					t.Fatal(err)
				}
			}
			quiet := gdb.Session(&gorm.Session{Logger: logger.Discard}) // errors are reported by the test

			txA := quiet.WithContext(ctx).Begin()
			if txA.Error != nil {
				t.Fatal(txA.Error)
			}
			defer txA.Rollback()
			if err := ct.WithTx(txA).LockTenant(ctx, tenant1); err != nil {
				t.Fatalf("A: %v", err)
			}

			txB := quiet.WithContext(ctx).Begin()
			if txB.Error != nil {
				t.Fatal(txB.Error)
			}
			defer txB.Rollback() // its error is expected: B's connection is gone
			tb := ct.WithTx(txB)
			if err := tb.Add(ctx, &TestPayload{Name: "early"}, nil, nil, tenant2); err != nil {
				t.Fatalf("B's first add: %v", err)
			}
			short, cancelShort := context.WithTimeout(ctx, 300*time.Millisecond)
			err = tb.Add(short, &TestPayload{Name: "blocked"}, nil, nil, tenant1)
			cancelShort()
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, closuretree.ErrSavepointRollback) {
				t.Fatalf("want context.DeadlineExceeded and ErrSavepointRollback, got %v", err)
			}

			if err := tb.Add(ctx, &TestPayload{Name: "later"}, nil, nil, tenant2); err == nil {
				t.Error("a later write in B's transaction succeeded")
			}
			if err := txB.Commit().Error; err == nil {
				t.Error("B's transaction committed")
			}
			if err := txA.Rollback().Error; err != nil {
				t.Fatalf("roll back A: %v", err)
			}
			if got := countTenant(t, gdb, ct, tenant2).Nodes; got != 1 {
				t.Errorf("tenant2: want only the node committed before B, got %d nodes", got)
			}
		})
	}
}

// A holds tenant1's lock and B tenant2's; then, concurrently, A adds to tenant2 and B to tenant1.
// MySQL detects the deadlock and rolls back one whole transaction (error 1213), so the rollback to
// that write's savepoint fails too: the error reports the deadlock first and matches
// ErrSavepointRollback. The other add gets its lock once the server's rollback has released it,
// and its transaction commits. A statement on the failed transaction's handle is then committed at
// once, and the handle's rollback does not undo it.
func TestWithTxDeadlockMatchesSavepointRollback(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMysql {
				t.Skip("MySQL only: the server rolls back the whole transaction on a deadlock")
			}
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // fail instead of hanging
			defer cancel()
			for _, tenant := range []string{tenant1, tenant2} { // the lock rows exist before A and B
				if err := ct.Add(ctx, &TestPayload{Name: "committed"}, nil, nil, tenant); err != nil {
					t.Fatal(err)
				}
			}
			quiet := gdb.Session(&gorm.Session{Logger: logger.Discard}) // errors are reported by the test

			txA := quiet.WithContext(ctx).Begin()
			if txA.Error != nil {
				t.Fatal(txA.Error)
			}
			defer txA.Rollback()
			txB := quiet.WithContext(ctx).Begin()
			if txB.Error != nil {
				t.Fatal(txB.Error)
			}
			defer txB.Rollback()
			if err := ct.WithTx(txA).LockTenant(ctx, tenant1); err != nil {
				t.Fatalf("A: %v", err)
			}
			if err := ct.WithTx(txB).LockTenant(ctx, tenant2); err != nil {
				t.Fatalf("B: %v", err)
			}

			var errA, errB error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				errA = ct.WithTx(txA).Add(ctx, &TestPayload{Name: "a"}, nil, nil, tenant2)
			}()
			go func() {
				defer wg.Done()
				errB = ct.WithTx(txB).Add(ctx, &TestPayload{Name: "b"}, nil, nil, tenant1)
			}()
			wg.Wait()
			if (errA == nil) == (errB == nil) {
				t.Fatalf("want exactly one add to fail, A: %v, B: %v", errA, errB)
			}
			// The failed add's transaction is gone; the other one wrote to the failed one's tenant.
			failed, failedTx, survivor, grown, unchanged := errA, txA, txB, tenant1, tenant2
			if errA == nil {
				failed, failedTx, survivor, grown, unchanged = errB, txB, txA, tenant2, tenant1
			}

			msg := failed.Error()
			deadlock := strings.Index(msg, "Error 1213")
			rollback := strings.Index(msg, closuretree.ErrSavepointRollback.Error())
			if deadlock < 0 || rollback < deadlock {
				t.Errorf("want the deadlock (Error 1213) before the failed rollback to the savepoint, got %v", failed)
			}
			if !errors.Is(failed, closuretree.ErrSavepointRollback) {
				t.Errorf("want ErrSavepointRollback, got %v", failed)
			}

			if err := survivor.Commit().Error; err != nil {
				t.Fatalf("commit the surviving transaction: %v", err)
			}
			if got := countTenant(t, gdb, ct, grown).Nodes; got != 2 {
				t.Errorf("%s: want the committed node and the surviving add, got %d nodes", grown, got)
			}
			if got := countTenant(t, gdb, ct, unchanged).Nodes; got != 1 {
				t.Errorf("%s: want only the committed node, got %d nodes", unchanged, got)
			}
			assertConsistent(t, gdb, ct, tenant1)
			assertConsistent(t, gdb, ct, tenant2)
			assertStatementsAfterDeadlockCommit(t, gdb, ct, failedTx)
		})
	}
}

// assertStatementsAfterDeadlockCommit checks the handle of a transaction that MySQL rolled back
// on a deadlock: it is no longer in a transaction, so a later statement on it is committed by
// itself, at once, and the final rollback undoes nothing. A caller that kept using the handle
// would commit its following statements one by one.
func assertStatementsAfterDeadlockCommit(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, failedTx *gorm.DB) {
	t.Helper()
	const after = "after the deadlock"
	if err := failedTx.Create(&TestPayload{Node: closuretree.Node{Tenant: after}, Name: after}).Error; err != nil {
		t.Fatalf("insert on the failed transaction's handle: %v", err)
	}
	if got := countTenant(t, gdb, ct, after).Nodes; got != 1 {
		t.Errorf("before the rollback: want the insert committed, got %d nodes", got)
	}
	if err := failedTx.Rollback().Error; err != nil {
		t.Errorf("rollback of the failed transaction's handle: %v", err)
	}
	if got := countTenant(t, gdb, ct, after).Nodes; got != 1 {
		t.Errorf("after the rollback: want the insert kept, got %d nodes", got)
	}
}

// GetOrAdd, Renormalize and RenormalizeAll on a Tree bound to a caller's transaction: their writes
// show inside the transaction, and its rollback undoes them.
func TestWithTxSortOrderWritesRollBack(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db) // one database per test name
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			p := &TestPayload{Name: "p"}
			for _, n := range []*TestPayload{p, {Name: "r2"}} { // r2 is placed first: sort_order -10
				if err := ct.Add(ctx, n, nil, nil, tenant1); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"c1", "c2"} {
				if err := ct.Add(ctx, &TestPayload{Name: name}, up(p.NodeId), nil, tenant1); err != nil {
					t.Fatal(err)
				}
			}
			before := readSortState(t, gdb, ct, tenant1)

			err = gdb.Transaction(func(tx *gorm.DB) error {
				tt := ct.WithTx(tx)
				if created, err := tt.GetOrAdd(ctx, &TestPayload{Name: "c0"}, p.NodeId, tenant1, []string{"Name"}); err != nil || !created {
					t.Errorf("GetOrAdd: created=%v err=%v", created, err)
				}
				if created, err := tt.GetOrAdd(ctx, &TestPayload{Name: "c0"}, p.NodeId, tenant1, []string{"Name"}); err != nil || created {
					t.Errorf("GetOrAdd again: want the child added in the transaction, created=%v err=%v", created, err)
				}
				if err := tt.Renormalize(ctx, p.NodeId, tenant1); err != nil {
					t.Errorf("Renormalize: %v", err)
				}
				// Renormalize removed the meta row of p, so only the root group is left to do.
				if n, err := tt.RenormalizeAll(ctx, tenant1, 9999); err != nil || n != 1 {
					t.Errorf("RenormalizeAll: n=%d err=%v", n, err)
				}
				// each add placed its node first: the roots are r2, p and the children c0, c2, c1
				want := map[string]float64{"r2": 10, "p": 20, "c0": 10, "c2": 20, "c1": 30}
				if diff := cmp.Diff(want, readSortState(t, tx, ct, tenant1).Orders); diff != "" {
					t.Errorf("sort orders inside the transaction (-want +got):\n%s", diff)
				}
				return errRollback
			})
			if !errors.Is(err, errRollback) {
				t.Fatalf("want the rollback error, got %v", err)
			}
			if diff := cmp.Diff(before, readSortState(t, gdb, ct, tenant1)); diff != "" {
				t.Errorf("the rollback left changes (-before +after):\n%s", diff)
			}
			assertConsistent(t, gdb, ct, tenant1)
		})
	}
}

// sortState is a tenant's sort_order per node name and min_halvings per parent id.
type sortState struct {
	Orders map[string]float64
	Meta   map[uint]int
}

// readSortState reads the tenant's sortState through db.
func readSortState(t *testing.T, db *gorm.DB, ct *closuretree.Tree, tenant string) sortState {
	t.Helper()
	var nodes []TestPayload
	if err := db.Table(ct.NodeTableName()).Where("tenant = ?", tenant).Find(&nodes).Error; err != nil {
		t.Fatal(err)
	}
	var meta []struct {
		ParentID    uint
		MinHalvings int
	}
	if err := db.Table("closure_tree_meta_"+ct.NodeTableName()).Select("parent_id, min_halvings").
		Where("tenant = ?", tenant).Scan(&meta).Error; err != nil {
		t.Fatal(err)
	}
	s := sortState{Orders: map[string]float64{}, Meta: map[uint]int{}}
	for _, n := range nodes {
		s.Orders[n.Name] = n.SortOrder
	}
	for _, m := range meta {
		s.Meta[m.ParentID] = m.MinHalvings
	}
	return s
}
