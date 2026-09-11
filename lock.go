package closuretree

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// writeTx runs fn inside a transaction that first takes the per-tenant lock, so structural writes
// for a tenant serialize instead of racing on the lock-free guards in Add/Update/moveInTx. Every
// mutating operation (Add, Update, DeleteRecurse, GetOrAdd, Renormalize) goes through here. See
// lockTenant and the concurrency note on Tree.
func (ct *Tree) writeTx(ctx context.Context, tenant string, fn func(tx *gorm.DB) error) error {
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ct.lockTenant(tx, tenant); err != nil {
			return err
		}
		return fn(tx)
	})
}

// lockTenant takes the tenant's anchor row as the FIRST statement of a write transaction, so that
// conflicting structural writes within a tenant run one at a time. It upserts the anchor row (so it
// always exists) and then locks it:
//
//   - PostgreSQL / MySQL: SELECT ... FOR UPDATE holds the row until the transaction ends, released
//     automatically on commit or rollback.
//   - SQLite: there is no FOR UPDATE and none is needed — SQLite already serializes writers per
//     database, and the anchor upsert takes that writer lock up front.
//
// Taking the lock before the operation's own reads is what makes their guards see committed state;
// this matters under MySQL REPEATABLE READ, where the transaction's read view is fixed at its first
// read. The lock is per (tree, tenant): different tenants never contend, and reads never take it.
func (ct *Tree) lockTenant(tx *gorm.DB, tenant string) error {
	switch tx.Name() {
	case "sqlite":
		// The upsert acquires SQLite's database write lock; that is the serialization.
		if err := tx.Exec(fmt.Sprintf(
			`INSERT INTO %s (tenant) VALUES (?) ON CONFLICT(tenant) DO UPDATE SET tenant = excluded.tenant`,
			ct.lockTbl), tenant).Error; err != nil {
			return fmt.Errorf("unable to acquire tenant lock: %w", err)
		}
		return nil
	case "mysql":
		if err := tx.Exec(fmt.Sprintf(
			`INSERT INTO %s (tenant) VALUES (?) ON DUPLICATE KEY UPDATE tenant = tenant`,
			ct.lockTbl), tenant).Error; err != nil {
			return fmt.Errorf("unable to ensure tenant lock row: %w", err)
		}
	default: // postgres and any other engine supporting SELECT ... FOR UPDATE
		if err := tx.Exec(fmt.Sprintf(
			`INSERT INTO %s (tenant) VALUES (?) ON CONFLICT (tenant) DO NOTHING`,
			ct.lockTbl), tenant).Error; err != nil {
			return fmt.Errorf("unable to ensure tenant lock row: %w", err)
		}
	}
	// Hold the row for the rest of the transaction. Scan the value so the query executes.
	var locked string
	if err := tx.Raw(fmt.Sprintf(
		`SELECT tenant FROM %s WHERE tenant = ? FOR UPDATE`, ct.lockTbl), tenant).
		Scan(&locked).Error; err != nil {
		return fmt.Errorf("unable to acquire tenant lock: %w", err)
	}
	return nil
}
