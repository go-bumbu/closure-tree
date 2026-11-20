package closuretree

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

// dialect identifies the SQL database backing a Tree. It is the single home for the handful of
// places the three supported databases differ (version gate, upsert syntax, row locking): classify
// the database once with dialectOf and read the resulting value instead of scattering db.Name()
// checks. The zero value is dialectPostgres, the standard-SQL / FOR UPDATE-capable default.
type dialect uint8

const (
	dialectPostgres dialect = iota // default: any engine supporting standard SELECT ... FOR UPDATE
	dialectMySQL
	dialectSQLite
)

// dialectOf classifies db by its GORM driver name.
func dialectOf(db *gorm.DB) dialect {
	return dialectFromName(db.Name())
}

// dialectFromName maps a GORM dialector name to a dialect. Unknown names fall back to
// dialectPostgres, matching the previous default branch (standard SQL with SELECT ... FOR UPDATE).
func dialectFromName(name string) dialect {
	switch name {
	case "sqlite":
		return dialectSQLite
	case "mysql":
		return dialectMySQL
	default:
		return dialectPostgres
	}
}

// checkVersion enforces any minimum server version the dialect requires. Only MySQL has one (8.0+,
// for recursive CTEs and the upsert syntax used here); the other dialects return nil.
func (d dialect) checkVersion(db *gorm.DB) error {
	if d != dialectMySQL {
		return nil
	}
	var version string
	if err := db.Raw("SELECT VERSION()").Scan(&version).Error; err != nil {
		return fmt.Errorf("unable to check MySQL version: %w", err)
	}
	// SplitN with a non-empty separator always returns at least one element, so parts[0] is safe.
	parts := strings.SplitN(version, ".", 2)
	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 8 {
		return fmt.Errorf("MySQL 8.0+ required; got %s", version)
	}
	return nil
}

// metaUpsertSQL returns the statement that inserts a sort-order meta row or, on conflict, keeps the
// lower min_halvings. Placeholders are (tenant, parent_id, min_halvings) on every dialect.
func (d dialect) metaUpsertSQL(metaTbl string) string {
	if d == dialectMySQL {
		return fmt.Sprintf(
			`INSERT INTO %s (tenant, parent_id, min_halvings) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE min_halvings = LEAST(min_halvings, VALUES(min_halvings))`,
			metaTbl)
	}
	return fmt.Sprintf(
		`INSERT INTO %s (tenant, parent_id, min_halvings) VALUES (?, ?, ?) ON CONFLICT (tenant, parent_id) DO UPDATE SET min_halvings = EXCLUDED.min_halvings WHERE %s.min_halvings > EXCLUDED.min_halvings`,
		metaTbl, metaTbl)
}

// lockAttempts bounds how often lockTenantHook repeats "ensure the row, then lock it" when the
// lock row disappears between the two statements.
const lockAttempts = 3

// ErrLockNotAcquired is returned when the tenant's lock row was deleted between ensuring it and
// locking it, on every one of lockAttempts attempts.
var ErrLockNotAcquired = errors.New("unable to lock the tenant: the lock row disappeared")

// lockTenant takes the tenant's anchor row as the FIRST statement of a write transaction, so that
// conflicting structural writes within a tenant run one at a time (see writeTx and the concurrency
// note on Tree). It upserts the anchor row (so it always exists) and then locks it:
//
//   - PostgreSQL / MySQL: SELECT ... FOR UPDATE holds the row until the transaction ends, released
//     automatically on commit or rollback. If the select returns no row, because the row was
//     deleted after the upsert, the upsert and the lock are repeated up to lockAttempts times
//     before ErrLockNotAcquired is returned.
//   - SQLite: there is no FOR UPDATE and none is needed — SQLite already serializes writers per
//     database, and the anchor upsert takes that writer lock up front.
//
// Taking the lock before the operation's own reads is what makes their guards see committed state;
// this matters under MySQL REPEATABLE READ, where the read view is fixed at the first read.
func (d dialect) lockTenant(tx *gorm.DB, lockTbl, tenant string) error {
	return d.lockTenantHook(tx, lockTbl, tenant, nil)
}

// lockTenantHook is lockTenant with a test hook: afterEnsure, when not nil, runs between the upsert
// of the lock row and the locking select, on every attempt. SQLite never calls it.
func (d dialect) lockTenantHook(tx *gorm.DB, lockTbl, tenant string, afterEnsure func()) error {
	if d == dialectSQLite {
		// The upsert acquires SQLite's database write lock; that is the serialization.
		if err := tx.Exec(fmt.Sprintf(
			`INSERT INTO %s (tenant) VALUES (?) ON CONFLICT(tenant) DO UPDATE SET tenant = excluded.tenant`,
			lockTbl), tenant).Error; err != nil {
			return fmt.Errorf("unable to acquire tenant lock: %w", err)
		}
		return nil
	}
	for range lockAttempts {
		if err := d.ensureLockRow(tx, lockTbl, tenant); err != nil {
			return err
		}
		if afterEnsure != nil {
			afterEnsure()
		}
		// Hold the row for the rest of the transaction. Scan the value so the query executes, and
		// count the rows to know whether one was locked.
		var locked string
		res := tx.Raw(fmt.Sprintf(
			`SELECT tenant FROM %s WHERE tenant = ? FOR UPDATE`, lockTbl), tenant).Scan(&locked)
		if res.Error != nil {
			return fmt.Errorf("unable to acquire tenant lock: %w", res.Error)
		}
		if res.RowsAffected > 0 {
			return nil
		}
	}
	return ErrLockNotAcquired
}

// ensureLockRow inserts the tenant's lock row unless it already exists (PostgreSQL and MySQL).
func (d dialect) ensureLockRow(tx *gorm.DB, lockTbl, tenant string) error {
	stmt := `INSERT INTO %s (tenant) VALUES (?) ON CONFLICT (tenant) DO NOTHING` // postgres and any other engine supporting SELECT ... FOR UPDATE
	if d == dialectMySQL {
		stmt = `INSERT INTO %s (tenant) VALUES (?) ON DUPLICATE KEY UPDATE tenant = tenant`
	}
	if err := tx.Exec(fmt.Sprintf(stmt, lockTbl), tenant).Error; err != nil {
		return fmt.Errorf("unable to ensure tenant lock row: %w", err)
	}
	return nil
}
