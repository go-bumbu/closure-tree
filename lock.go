package closuretree

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

// savepointSeq numbers the savepoints writeTx takes, so every call uses a name of its own.
var savepointSeq atomic.Uint64

// savepointRollbackTimeout bounds the rollback to the savepoint after a failed write, which runs
// detached from the write's cancellation. A variable only so that a test can shorten it.
var savepointRollbackTimeout = 30 * time.Second

// writeTx runs fn inside a transaction that first takes the per-tenant lock, so structural writes
// for a tenant serialize instead of racing on the lock-free guards in Add/Update/moveInTx. Every
// mutating operation (Add, GetOrAdd, Update, DeleteRecurse, DeleteRecurseMany, DeleteTenant,
// Renormalize, BuildFromParents) and LockTenant go through here. The dialect-specific lock
// acquisition lives in dialect.lockTenant; see the concurrency note on Tree.
// The transaction it opens itself uses dialect.txOptions (READ COMMITTED on PostgreSQL and MySQL)
// and is repeated by the Tree's retry policy (ct.retry), so fn must start from scratch on every
// call: it may run more than once, each time in a new transaction.
//
// When the Tree is bound to a caller's transaction (WithTx), writeTx opens no transaction of its
// own: it runs the lock and fn inside a savepoint, see savepointTx, and never repeats them.
func (ct *Tree) writeTx(ctx context.Context, tenant string, fn func(tx *gorm.DB) error) error {
	if ct.inTransaction() {
		return ct.savepointTx(ct.db.WithContext(ctx), tenant, fn)
	}
	return ct.retry.run(ctx, func() error {
		return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := ct.dialect.lockTenant(tx, ct.lockTbl, tenant); err != nil {
				return err
			}
			return fn(tx)
		}, ct.dialect.txOptions()...)
	})
}

// savepointTx runs the tenant lock and fn inside a savepoint of the caller's transaction tx. It
// takes the savepoint itself instead of using gorm's nested Transaction, which takes none when
// DisableNestedTransaction is set. On success the savepoint is released, so savepoints do not nest
// deeper with every write; the lock survives the release. On an error, or a panic, the work is
// rolled back to the savepoint, even when the write's context is cancelled, which leaves the
// caller's transaction usable, and the original error is returned (or the panic re-raised). The
// rollback keeps the context's values but not its cancellation, and runs under a timeout of its
// own (savepointRollbackTimeout, 30 seconds). When that rollback fails, for example because the
// context ended during a statement and the driver dropped the connection, or because it timed
// out, the transaction is unusable: the failed rollback, wrapped in
// ErrSavepointRollback, is joined to the original error.
func (ct *Tree) savepointTx(tx *gorm.DB, tenant string, fn func(tx *gorm.DB) error) (err error) {
	name := fmt.Sprintf("ct_sp_%d", savepointSeq.Add(1))
	if err := execSavepointStmt(tx, "SAVEPOINT "+name); err != nil {
		// Nothing to roll back, so no ErrSavepointRollback. A context already done is refused before
		// the statement is sent; one that ends while it runs may cost the connection (see WithTx).
		return fmt.Errorf("unable to create savepoint: %w", err)
	}
	panicked := true
	defer func() {
		if panicked || err != nil {
			// Detach from the write's cancellation: database/sql refuses a statement on a done
			// context, and a skipped rollback would leave the partial write in the caller's
			// transaction. Bound it instead, so a stuck server cannot hold the caller forever.
			rbCtx, cancel := context.WithTimeout(context.WithoutCancel(tx.Statement.Context), savepointRollbackTimeout)
			defer cancel()
			rbTx := tx.WithContext(rbCtx)
			if rbErr := rollbackToSavepoint(rbTx, name); rbErr != nil {
				err = errors.Join(err, fmt.Errorf("%w: %w", ErrSavepointRollback, rbErr))
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
	if err := execSavepointStmt(tx, "ROLLBACK TO SAVEPOINT "+name); err != nil {
		return fmt.Errorf("unable to roll back to savepoint: %w", err)
	}
	if err := releaseSavepoint(tx, name); err != nil {
		return fmt.Errorf("unable to release savepoint: %w", err)
	}
	return nil
}

// releaseSavepoint releases the savepoint name.
func releaseSavepoint(tx *gorm.DB, name string) error {
	return execSavepointStmt(tx, "RELEASE SAVEPOINT "+name)
}

// execSavepointStmt runs one savepoint statement (SAVEPOINT, ROLLBACK TO SAVEPOINT or RELEASE
// SAVEPOINT) as plain SQL that all three databases accept, and returns its error on every database.
// gorm's SavePoint and RollbackTo are not used: the PostgreSQL and SQLite dialectors discard the
// statement's error. Like them, it bypasses a prepared-statement connection
// (gorm.Session.PrepareStmt): MySQL refuses savepoint statements in the prepared statement protocol.
func execSavepointStmt(tx *gorm.DB, stmt string) error {
	if prepared, ok := tx.Statement.ConnPool.(*gorm.PreparedStmtTX); ok {
		_, err := prepared.Tx.ExecContext(tx.Statement.Context, stmt)
		return err
	}
	return tx.Exec(stmt).Error
}
