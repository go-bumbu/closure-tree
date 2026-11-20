package closuretree

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gorm.io/gorm"
)

type txSample struct {
	Node
	Name string
}

// A panic inside a write on a tx-bound Tree rolls the write back to its savepoint and propagates;
// the outer transaction stays usable and commits its other writes.
func TestWriteTxPanicRollsBackToSavepoint(t *testing.T) {
	ct := newInternalTree(t, txSample{})
	ctx := context.Background()
	const tenant = "t1"
	countName := func(db *gorm.DB, name string) int64 {
		t.Helper()
		var n int64
		if err := db.Table(ct.nodesTbl).Where("tenant = ? AND name = ?", tenant, name).Count(&n).Error; err != nil {
			t.Fatalf("count %q: %v", name, err)
		}
		return n
	}

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
		if n := countName(tx, "panicked"); n != 0 {
			t.Errorf("the panicked write left %d rows", n)
		}
		return tt.Add(ctx, &txSample{Name: "after"}, nil, nil, tenant)
	})
	if err != nil {
		t.Fatalf("outer transaction after the panic: %v", err)
	}
	if n := countName(ct.db, "panicked"); n != 0 {
		t.Errorf("the panicked write was committed: %d rows", n)
	}
	if n := countName(ct.db, "after"); n != 1 {
		t.Errorf("want the later write committed, got %d rows", n)
	}
}

// When the rollback to the savepoint fails (here the outer transaction is already gone), the caller
// still receives the write's own error.
func TestWriteTxFailedRollbackKeepsOriginalError(t *testing.T) {
	ct := newInternalTree(t, txSample{})
	ctx := context.Background()
	errSentinel := errors.New("sentinel")

	tx := ct.db.Begin()
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
	// The SQLite dialector discards the error of ROLLBACK TO SAVEPOINT, so here the failure
	// surfaces from the release that follows it.
	if !strings.Contains(err.Error(), "savepoint") {
		t.Errorf("want the savepoint failure reported too, got %v", err)
	}
}
