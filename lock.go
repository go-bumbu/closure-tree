package closuretree

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"gorm.io/gorm"
)

// savepointSeq numbers the savepoints writeTx takes, so every call uses a name of its own.
var savepointSeq atomic.Uint64

// writeTx runs fn inside a transaction that first takes the per-tenant lock, so structural writes
// for a tenant serialize instead of racing on the lock-free guards in Add/Update/moveInTx. Every
// mutating operation (Add, Update, DeleteRecurse, GetOrAdd, Renormalize) goes through here. The
// dialect-specific lock acquisition lives in dialect.lockTenant; see the concurrency note on Tree.
//
// When the Tree is bound to a caller's transaction (WithTx), writeTx opens no transaction of its
// own: it runs the lock and fn inside a savepoint, see savepointTx.
func (ct *Tree) writeTx(ctx context.Context, tenant string, fn func(tx *gorm.DB) error) error {
	if ct.inTransaction() {
		return ct.savepointTx(ct.db.WithContext(ctx), tenant, fn)
	}
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ct.dialect.lockTenant(tx, ct.lockTbl, tenant); err != nil {
			return err
		}
		return fn(tx)
	})
}

// savepointTx runs the tenant lock and fn inside a savepoint of the caller's transaction tx. It
// takes the savepoint itself instead of using gorm's nested Transaction, which takes none when
// DisableNestedTransaction is set. On success the savepoint is released, so savepoints do not nest
// deeper with every write; the lock survives the release. On an error, or a panic, the work is
// rolled back to the savepoint, even when the write's context is cancelled, which leaves the
// caller's transaction usable, and the original error is returned (or the panic re-raised).
func (ct *Tree) savepointTx(tx *gorm.DB, tenant string, fn func(tx *gorm.DB) error) (err error) {
	name := fmt.Sprintf("ct_sp_%d", savepointSeq.Add(1))
	if err := tx.SavePoint(name).Error; err != nil {
		return fmt.Errorf("unable to create savepoint: %w", err)
	}
	panicked := true
	defer func() {
		if panicked || err != nil {
			// Detach from the write's cancellation: database/sql refuses a statement on a done
			// context, and a skipped rollback would leave the partial write in the caller's
			// transaction.
			rbTx := tx.WithContext(context.WithoutCancel(tx.Statement.Context))
			if rbErr := rollbackToSavepoint(rbTx, name); rbErr != nil {
				err = errors.Join(err, rbErr)
			}
		}
	}()
	if err = ct.dialect.lockTenant(tx, ct.lockTbl, tenant); err == nil {
		err = fn(tx)
	}
	panicked = false
	if err != nil {
		return err
	}
	if err := releaseSavepoint(tx, name); err != nil {
		return fmt.Errorf("unable to release savepoint: %w", err)
	}
	return nil
}

// rollbackToSavepoint undoes the work done since the savepoint name and then releases it.
func rollbackToSavepoint(tx *gorm.DB, name string) error {
	if err := tx.RollbackTo(name).Error; err != nil {
		return fmt.Errorf("unable to roll back to savepoint: %w", err)
	}
	if err := releaseSavepoint(tx, name); err != nil {
		return fmt.Errorf("unable to release savepoint: %w", err)
	}
	return nil
}

// releaseSavepoint releases the savepoint name. gorm has no call for it, so it is plain SQL that all
// three databases accept. Like gorm's SavePoint and RollbackTo, it bypasses a prepared-statement
// connection (gorm.Session.PrepareStmt): MySQL refuses savepoint statements in the prepared
// statement protocol.
func releaseSavepoint(tx *gorm.DB, name string) error {
	stmt := "RELEASE SAVEPOINT " + name
	if prepared, ok := tx.Statement.ConnPool.(*gorm.PreparedStmtTX); ok {
		_, err := prepared.Tx.ExecContext(tx.Statement.Context, stmt)
		return err
	}
	return tx.Exec(stmt).Error
}
