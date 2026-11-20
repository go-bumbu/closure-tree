package closuretree

import "gorm.io/gorm"

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
