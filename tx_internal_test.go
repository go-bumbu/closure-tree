package closuretree

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type txSample struct {
	Node
	Name string
}

// countTxSampleName counts the tenant's rows named name in ct's node table, reading through db.
func countTxSampleName(t *testing.T, ct *Tree, db *gorm.DB, tenant, name string) int64 {
	t.Helper()
	var n int64
	if err := db.Table(ct.nodesTbl).Where("tenant = ? AND name = ?", tenant, name).Count(&n).Error; err != nil {
		t.Fatalf("count %q: %v", name, err)
	}
	return n
}

// A panic inside a write on a tx-bound Tree rolls the write back to its savepoint and propagates;
// the outer transaction stays usable and commits its other writes.
func TestWriteTxPanicRollsBackToSavepoint(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			const tenant = "t1"

			err := ct.db.Transaction(func(tx *gorm.DB) error {
				tt := ct.WithTx(tx)
				recovered := func() (r any) {
					defer func() { r = recover() }()
					_ = tt.writeTx(ctx, tenant, func(tx *gorm.DB) error {
						if err := tx.Exec("INSERT INTO "+ct.nodesTbl+" (tenant, name, sort_order) VALUES (?, ?, 0)",
							tenant, "panicked").Error; err != nil {
							t.Fatalf("insert: %v", err)
						}
						panic("boom")
					})
					return nil
				}()
				if recovered != "boom" {
					t.Errorf("want the panic to propagate, got %v", recovered)
				}
				if n := countTxSampleName(t, ct, tx, tenant, "panicked"); n != 0 {
					t.Errorf("the panicked write left %d rows", n)
				}
				return tt.Add(ctx, &txSample{Name: "after"}, nil, nil, tenant)
			})
			if err != nil {
				t.Fatalf("outer transaction after the panic: %v", err)
			}
			if n := countTxSampleName(t, ct, ct.db, tenant, "panicked"); n != 0 {
				t.Errorf("the panicked write was committed: %d rows", n)
			}
			if n := countTxSampleName(t, ct, ct.db, tenant, "after"); n != 1 {
				t.Errorf("want the later write committed, got %d rows", n)
			}
		})
	}
}

// A write whose context is cancelled before it fails is still rolled back to its savepoint: the
// partial write must not stay in the caller's transaction, which remains usable.
func TestWriteTxCancelledContextRollsBackToSavepoint(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			const tenant = "t1"
			errWrite := errors.New("write failed")

			err := ct.db.Transaction(func(tx *gorm.DB) error {
				tt := ct.WithTx(tx)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				err := tt.writeTx(ctx, tenant, func(tx *gorm.DB) error {
					if err := tx.Exec("INSERT INTO "+ct.nodesTbl+" (tenant, name, sort_order) VALUES (?, ?, 0)",
						tenant, "cancelled").Error; err != nil {
						t.Fatalf("insert: %v", err)
					}
					cancel()
					return errWrite
				})
				if !errors.Is(err, errWrite) {
					t.Errorf("want the write's error, got %v", err)
				}
				if n := countTxSampleName(t, ct, tx, tenant, "cancelled"); n != 0 {
					t.Errorf("the cancelled write left %d rows in the transaction", n)
				}
				return tt.Add(context.Background(), &txSample{Name: "after"}, nil, nil, tenant)
			})
			if err != nil {
				t.Fatalf("outer transaction after the cancelled write: %v", err)
			}
			if n := countTxSampleName(t, ct, ct.db, tenant, "cancelled"); n != 0 {
				t.Errorf("the cancelled write was committed: %d rows", n)
			}
			if n := countTxSampleName(t, ct, ct.db, tenant, "after"); n != 1 {
				t.Errorf("want the later write committed, got %d rows", n)
			}
		})
	}
}

// A write whose context is already done fails before it creates its savepoint, and it must not
// touch the caller's transaction: a later write with a live context in the same transaction
// succeeds and the transaction commits.
func TestWriteTxDoneContextKeepsTransactionUsable(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			const tenant = "t1"
			done, cancel := context.WithCancel(context.Background())
			cancel()

			err := ct.db.Transaction(func(tx *gorm.DB) error {
				// The failing write's statements would be logged as errors; keep the output clean.
				tt := ct.WithTx(tx.Session(&gorm.Session{Logger: logger.Discard}))
				err := tt.Add(done, &txSample{Name: "done"}, nil, nil, tenant)
				if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "unable to create savepoint") {
					t.Errorf("want context.Canceled from the savepoint of the write with a done context, got %v", err)
				}
				if errors.Is(err, ErrSavepointRollback) {
					t.Errorf("no savepoint was taken, want no ErrSavepointRollback, got %v", err)
				}
				return tt.Add(context.Background(), &txSample{Name: "after"}, nil, nil, tenant)
			})
			if err != nil {
				t.Fatalf("outer transaction after the write with a done context: %v", err)
			}
			var names []string
			if err := ct.db.Table(ct.nodesTbl).Where("tenant = ?", tenant).Pluck("name", &names).Error; err != nil {
				t.Fatal(err)
			}
			if len(names) != 1 || names[0] != "after" {
				t.Errorf("want only the later write committed, got %v", names)
			}
		})
	}
}

// When the rollback to the savepoint fails (here the outer transaction is already gone), the caller
// still receives the write's own error, joined with the failed rollback.
func TestWriteTxFailedRollbackKeepsOriginalError(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			errSentinel := errors.New("sentinel")

			// The rollback to the savepoint fails on purpose; keep the output clean.
			tx := ct.db.Session(&gorm.Session{Logger: logger.Discard}).Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			err := ct.WithTx(tx).writeTx(ctx, "t1", func(tx *gorm.DB) error {
				if err := tx.Rollback().Error; err != nil {
					t.Fatalf("roll back the outer transaction: %v", err)
				}
				return errSentinel
			})
			if !errors.Is(err, errSentinel) {
				t.Fatalf("want the original error, got %v", err)
			}
			if !errors.Is(err, sql.ErrTxDone) || !strings.Contains(err.Error(), "unable to roll back to savepoint") {
				t.Errorf("want the failed rollback to the savepoint reported too, got %v", err)
			}
			if !errors.Is(err, ErrSavepointRollback) {
				t.Errorf("want ErrSavepointRollback, got %v", err)
			}
		})
	}
}

// A write whose context is cancelled after its last statement fails to release its savepoint. The
// rollback to the savepoint still runs, so the error does not match ErrSavepointRollback: the row
// is gone and the caller's transaction stays usable.
func TestWriteTxFailedReleaseRollsBackToSavepoint(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			const tenant = "t1"

			err := ct.db.Transaction(func(tx *gorm.DB) error {
				// The release fails on purpose; keep the output clean.
				tt := ct.WithTx(tx.Session(&gorm.Session{Logger: logger.Discard}))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				err := tt.writeTx(ctx, tenant, func(tx *gorm.DB) error {
					if err := tx.Exec("INSERT INTO "+ct.nodesTbl+" (tenant, name, sort_order) VALUES (?, ?, 0)",
						tenant, "unreleased").Error; err != nil {
						t.Fatalf("insert: %v", err)
					}
					cancel()
					return nil
				})
				if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "unable to release savepoint") {
					t.Errorf("want the failed release, got %v", err)
				}
				if errors.Is(err, ErrSavepointRollback) {
					t.Errorf("the rollback to the savepoint succeeded, want no ErrSavepointRollback, got %v", err)
				}
				if n := countTxSampleName(t, ct, tx, tenant, "unreleased"); n != 0 {
					t.Errorf("the unreleased write left %d rows in the transaction", n)
				}
				return tt.Add(context.Background(), &txSample{Name: "after"}, nil, nil, tenant)
			})
			if err != nil {
				t.Fatalf("outer transaction after the failed release: %v", err)
			}
			if n := countTxSampleName(t, ct, ct.db, tenant, "unreleased"); n != 0 {
				t.Errorf("the unreleased write was committed: %d rows", n)
			}
			if n := countTxSampleName(t, ct, ct.db, tenant, "after"); n != 1 {
				t.Errorf("want the later write committed, got %d rows", n)
			}
		})
	}
}
