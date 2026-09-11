package closuretree

import (
	"context"

	"gorm.io/gorm"
)

// writeTx runs fn inside a transaction that first takes the per-tenant lock, so structural writes
// for a tenant serialize instead of racing on the lock-free guards in Add/Update/moveInTx. Every
// mutating operation (Add, Update, DeleteRecurse, GetOrAdd, Renormalize) goes through here. The
// dialect-specific lock acquisition lives in dialect.lockTenant; see the concurrency note on Tree.
func (ct *Tree) writeTx(ctx context.Context, tenant string, fn func(tx *gorm.DB) error) error {
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ct.dialect.lockTenant(tx, ct.lockTbl, tenant); err != nil {
			return err
		}
		return fn(tx)
	})
}
