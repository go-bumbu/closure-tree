package closuretree

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// ErrNotInTransaction is returned by LockTenant on a Tree that is not bound to a transaction.
var ErrNotInTransaction = errors.New("the Tree is not bound to a transaction; use WithTx")

// WithTx returns a copy of the Tree that runs every read and write on tx, usually a transaction
// the caller opened. The copy shares everything else with ct; only the database handle differs, so
// it is cheap and meant to be created per transaction. ct itself is unchanged and stays safe for
// concurrent use.
//
// Reads on the copy see the transaction's uncommitted rows. Each write on the copy joins the
// caller's transaction inside a savepoint that closure-tree takes itself, whatever gorm's
// DisableNestedTransaction says: a failed write rolls back to its savepoint and returns its error,
// and the outer transaction stays usable. A successful write keeps the tenant lock until the outer
// transaction ends (PostgreSQL and MySQL).
//
// Inside the transaction, use the copy for that tenant: calling ct for the same tenant from inside
// the transaction waits on the transaction's own lock, so it blocks: forever, or until a lock or
// busy timeout fails it. A transaction that writes to several trees or tenants must take their locks in
// one fixed order everywhere, for example with LockTenant up front. Call Migrate on ct, not on the
// copy: it runs DDL, and MySQL commits the transaction implicitly on DDL.
//
// If tx is not a transaction, the copy behaves like a Tree built on tx: each write opens its own.
// WithTx panics if tx is nil.
func (ct *Tree) WithTx(tx *gorm.DB) *Tree {
	if tx == nil {
		panic("closuretree: WithTx called with a nil *gorm.DB")
	}
	cp := *ct
	cp.db = tx
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
