package closuretree

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"

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

// checkVersion enforces any minimum server version the dialect requires. Only MySQL has one
// (8.0.17+, for recursive CTEs, the upsert syntax used here and the collation utf8mb4_0900_bin of
// the tenant columns); the other dialects return nil.
func (d dialect) checkVersion(db *gorm.DB) error {
	if d != dialectMySQL {
		return nil
	}
	var version string
	if err := db.Raw("SELECT VERSION()").Scan(&version).Error; err != nil {
		return fmt.Errorf("unable to check MySQL version: %w", err)
	}
	if !mysqlVersionOK(version) {
		return fmt.Errorf("MySQL 8.0.17 or later required; got %s", version)
	}
	return nil
}

// mysqlVersionPrefix matches the major, minor and patch number at the start of a version string as
// SELECT VERSION() returns it; a suffix such as "-log" may follow.
var mysqlVersionPrefix = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

// mysqlVersionOK reports whether version, as SELECT VERSION() returns it, is 8.0.17 or later.
func mysqlVersionOK(version string) bool {
	m := mysqlVersionPrefix.FindStringSubmatch(version)
	if m == nil {
		return false
	}
	var got [3]int
	for i, s := range m[1:] {
		n, err := strconv.Atoi(s)
		if err != nil {
			return false
		}
		got[i] = n
	}
	return slices.Compare(got[:], []int{8, 0, 17}) >= 0
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

// txOptions returns the options for the transactions the library opens itself (see writeTx). On
// PostgreSQL and MySQL they run at READ COMMITTED whatever the connection's default is. On
// PostgreSQL at REPEATABLE READ the snapshot is fixed at the transaction's first statement, which
// is the lock statement, before it waits for the lock, so the guards after the lock would not see
// the writes committed while it waited. On MySQL at REPEATABLE READ a delete that scans the closure
// table past the tenant's rows (see the concurrency note on Tree) keeps locks on the closure rows of
// other tenants and on the gaps between them, so it deadlocks with other tenants' writes; at READ
// COMMITTED it takes no gap locks and releases the rows it does not delete, which removed its
// deadlocks with other tenants' adds. It still locks the rows it scans, also other tenants' rows,
// so two writes of different tenants that both delete closure rows can still deadlock (see the
// concurrency note on Tree). The guards need no snapshot that lasts across statements: every
// writer of the tenant holds its lock. SQLite uses the connection's default (nil): it serializes
// writers.
func (d dialect) txOptions() []*sql.TxOptions {
	if d == dialectSQLite {
		return nil
	}
	return []*sql.TxOptions{{Isolation: sql.LevelReadCommitted}}
}
