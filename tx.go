package closuretree

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// ErrNotInTransaction is returned by LockTenant on a Tree that is not bound to a transaction.
var ErrNotInTransaction = errors.New("the Tree is not bound to a transaction; use WithTx")

// ErrSavepointRollback is returned, joined with the write's own error, when a write on a Tree bound
// to a transaction (see WithTx) failed and could not then be rolled back to its savepoint, for
// example because the write's context ended during a statement and the driver dropped the
// connection. The caller's transaction can no longer be used, and its earlier work may be lost:
// roll it back. Its absence does not prove the transaction survived; see WithTx.
var ErrSavepointRollback = errors.New("the transaction can no longer be used; roll it back")

// WithTx returns a copy of the Tree that runs every read and write on tx, usually a transaction
// the caller opened. The copy shares everything else with ct; only the database handle differs, so
// it is cheap and meant to be created per transaction. ct itself is unchanged and stays safe for
// concurrent use.
//
// Only the statement tx carries is dropped: its Model, Table, Where and Set values, for example the
// statement of a running gorm hook, so the tx a hook receives can be passed as it is. The handle's
// connection (its transaction), its context and its session settings (such as Logger, DryRun or
// PrepareStmt) are kept, and so is an error already set on the handle.
//
// Reads on the copy see the transaction's uncommitted rows. Each write on the copy joins the
// caller's transaction inside a savepoint that closure-tree takes itself, whatever gorm's
// DisableNestedTransaction says: a failed write rolls back to its savepoint and returns its error,
// and the outer transaction stays usable, unless errors.Is(err, ErrSavepointRollback): then the
// rollback to the savepoint failed, for example because the write's context ended during a
// statement and the driver dropped the connection, which loses the transaction's earlier work too,
// and the caller must roll the transaction back. ErrSavepointRollback covers only the statements
// inside the savepoint: reads on the copy, and the statements a write runs before its savepoint
// (SAVEPOINT itself, the first reads of DeleteTenant and RenormalizeAll), fail like a statement of
// the caller's own, without it. So the caller should also roll the transaction back after an error
// from a call whose context has ended (ctx.Err() != nil): the driver may have dropped the
// connection. A successful write keeps the tenant lock until the outer transaction ends
// (PostgreSQL and MySQL).
//
// A further exception is a deadlock on MySQL (error 1213): the server has already rolled back the
// whole outer transaction, so the caller must roll it back and retry it; continuing would run the
// following statements outside any transaction. The returned error contains the driver's error,
// so it can be tested with errors.As. On MySQL at its default isolation level (REPEATABLE READ),
// tree writes of different tenants can wait on each other, and deadlock, while a caller's
// transaction is open: keep such transactions short, or run them at READ COMMITTED. DeleteRecurse,
// DeleteRecurseMany, DeleteTenant and BuildFromParents can deadlock with other tenants' writes
// there inside or outside a caller's transaction: no closure-table index leads with the tenant, so
// their deletes lock closure rows of other tenants (see the concurrency note on Tree).
//
// Inside the transaction, use the copy for that tenant: calling ct for the same tenant from inside
// the transaction waits on the transaction's own lock, so it blocks: forever, or until a lock or
// busy timeout fails it. A transaction that writes to several trees or tenants must take their locks in
// one fixed order everywhere, for example with LockTenant up front. Call Migrate on ct, not on the
// copy: it runs DDL, and MySQL commits the transaction implicitly on DDL.
//
// If tx is not a transaction, each write on the copy opens its own transaction, as on ct.
// WithTx panics if tx is nil.
func (ct *Tree) WithTx(tx *gorm.DB) *Tree {
	if tx == nil {
		panic("closuretree: WithTx called with a nil *gorm.DB")
	}
	cp := *ct
	// A new, clean statement on the same connection and context, then a session so that each tree
	// query clones that clean statement. NewDB alone is not enough: WithContext would clone
	// the old statement.
	cp.db = tx.Session(&gorm.Session{NewDB: true, Initialized: true}).Session(&gorm.Session{})
	return &cp
}

// inTransaction reports whether the Tree's handle is inside a transaction, using the same test as
// gorm's own Transaction: the connection pool is a transaction that can be committed.
func (ct *Tree) inTransaction() bool {
	committer, ok := ct.db.Statement.ConnPool.(gorm.TxCommitter)
	return ok && committer != nil
}

// LockTenant takes the tenant's write lock in the transaction the Tree is bound to (see WithTx). It
// is meant to be the first statement of the caller's transaction: the guards inside later tree
// writes (cycle check, sibling order, duplicate check) are plain reads, and taking the lock first
// makes them see every write committed before the lock was granted, where the database's isolation
// level allows it. LockTenant behaves like a tree write with an empty body, with the same savepoint
// handling; the lock is held until the caller's transaction ends (PostgreSQL and MySQL). Later
// tree writes in the same transaction take the lock again, which is harmless.
//
// It returns ErrEmptyTenant for an empty tenant, and ErrNotInTransaction when the Tree is not
// inside a transaction, because the lock would be released at once.
func (ct *Tree) LockTenant(ctx context.Context, tenant string) error {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return err
	}
	if !ct.inTransaction() {
		return ErrNotInTransaction
	}
	return ct.writeTx(ctx, tenant, func(*gorm.DB) error { return nil })
}
