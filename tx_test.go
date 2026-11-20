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
				time.Sleep(200 * time.Millisecond) // let B reach the lock
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
