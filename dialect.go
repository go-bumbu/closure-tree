package closuretree

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

// dialect identifies the SQL database backing a Tree. It is the single home for the handful of
// places the supported databases differ (version gate, upsert syntax, row locking, retry): classify
// the database once with dialectOf and read the resulting value instead of scattering db.Name()
// checks. The zero value is dialectPostgres, the standard-SQL / FOR UPDATE-capable default.
type dialect uint8

const (
	dialectPostgres dialect = iota // default: any engine supporting standard SELECT ... FOR UPDATE
	// dialectMariaDB is gorm's "mysql" driver: MariaDB, the supported server, and MySQL, which
	// shares the driver and the protocol and is accepted but not tested (see checkServerVersion).
	dialectMariaDB
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
		return dialectMariaDB
	default:
		return dialectPostgres
	}
}

// checkVersion enforces the minimum server version of the dialect and reports whether the server
// of dialectMariaDB is MySQL rather than MariaDB. Only dialectMariaDB has a minimum (see
// checkServerVersion); the other dialects return false and nil without a query.
func (d dialect) checkVersion(db *gorm.DB) (mysql bool, err error) {
	if d != dialectMariaDB {
		return false, nil
	}
	version, err := serverVersion(db)
	if err != nil {
		return false, err
	}
	return !isMariaDBVersion(version), checkServerVersion(version)
}

// serverVersion returns the server's version as SELECT VERSION() returns it.
func serverVersion(db *gorm.DB) (string, error) {
	var version string
	if err := db.Raw("SELECT VERSION()").Scan(&version).Error; err != nil {
		return "", fmt.Errorf("unable to read the server version: %w", err)
	}
	return version, nil
}

// checkServerVersion returns an error unless version, as SELECT VERSION() returns it on gorm's
// "mysql" driver, is MariaDB 11.8 or later, the version the library is tested on. A version without "MariaDB" is MySQL, which is accepted
// from 8.0.17, the first release with its tenant collation (see tenantCollationOf), but not tested.
func checkServerVersion(version string) error {
	if isMariaDBVersion(version) {
		if !versionAtLeast(version, 11, 8, 0) {
			return fmt.Errorf("MariaDB 11.8 or later required; got %s", version)
		}
		return nil
	}
	if !versionAtLeast(version, 8, 0, 17) {
		return fmt.Errorf("MariaDB 11.8 or later required (MySQL is accepted from 8.0.17, untested); got %s", version)
	}
	return nil
}

// isMariaDBVersion reports whether version, as SELECT VERSION() returns it on gorm's "mysql" driver,
// is MariaDB's rather than MySQL's.
func isMariaDBVersion(version string) bool {
	return strings.Contains(version, "MariaDB")
}

// serverVersionPrefix matches the major, minor and patch number at the start of a version string as
// SELECT VERSION() returns it; a suffix such as "-MariaDB-ubu2404" or "-log" may follow.
var serverVersionPrefix = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)

// versionAtLeast reports whether version, as SELECT VERSION() returns it, is major.minor.patch or
// later.
func versionAtLeast(version string, major, minor, patch int) bool {
	m := serverVersionPrefix.FindStringSubmatch(version)
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
	return slices.Compare(got[:], []int{major, minor, patch}) >= 0
}

// metaUpsertSQL returns the statement that inserts a sort-order meta row or, on conflict, keeps the
// lower min_halvings. Placeholders are (tenant, parent_id, min_halvings) on every dialect.
func (d dialect) metaUpsertSQL(metaTbl string) string {
	if d == dialectMariaDB {
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
//   - PostgreSQL / MariaDB: SELECT ... FOR UPDATE holds the row until the transaction ends, released
//     automatically on commit or rollback. If the select returns no row, because the row was
//     deleted after the upsert, the upsert and the lock are repeated up to lockAttempts times
//     before ErrLockNotAcquired is returned.
//   - SQLite: there is no FOR UPDATE and none is needed — SQLite already serializes writers per
//     database, and the anchor upsert takes that writer lock up front.
//
// Taking the lock before the operation's own reads is what makes their guards see committed state;
// this matters under MariaDB's REPEATABLE READ, where the read view is fixed at the first read.
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

// ensureLockRow inserts the tenant's lock row unless it already exists (PostgreSQL and MariaDB).
func (d dialect) ensureLockRow(tx *gorm.DB, lockTbl, tenant string) error {
	stmt := `INSERT INTO %s (tenant) VALUES (?) ON CONFLICT (tenant) DO NOTHING` // postgres and any other engine supporting SELECT ... FOR UPDATE
	if d == dialectMariaDB {
		stmt = `INSERT INTO %s (tenant) VALUES (?) ON DUPLICATE KEY UPDATE tenant = tenant`
	}
	if err := tx.Exec(fmt.Sprintf(stmt, lockTbl), tenant).Error; err != nil {
		return fmt.Errorf("unable to ensure tenant lock row: %w", err)
	}
	return nil
}

// txOptions returns the options for the transactions the library opens itself (see writeTx). On
// PostgreSQL and MariaDB they run at READ COMMITTED whatever the connection's default is. On
// PostgreSQL at REPEATABLE READ the snapshot is fixed at the transaction's first statement, which
// is the lock statement, before it waits for the lock, so the guards after the lock would not see
// the writes committed while it waited. On MariaDB at REPEATABLE READ a delete that scans the closure
// table past the tenant's rows (see the concurrency note on Tree) keeps locks on the closure rows of
// other tenants and on the gaps between them, so it deadlocks with other tenants' writes; at READ
// COMMITTED it takes no gap locks and releases the rows it does not delete. A statement that scans
// other tenants' rows still locks them while it runs, so two writes of different tenants can still
// deadlock, and writeTx then repeats the transaction (retryPolicy; see the concurrency note on
// Tree); in the library's measurements on MariaDB 11.8, with the forced tenant indexes, none did. The guards need no
// snapshot that lasts across statements: every writer of the tenant holds its lock. SQLite uses the
// connection's default (nil): it serializes writers.
func (d dialect) txOptions() []*sql.TxOptions {
	if d == dialectSQLite {
		return nil
	}
	return []*sql.TxOptions{{Isolation: sql.LevelReadCommitted}}
}

// mariadbDeadlockAttempts is how many times in all writeTx runs a transaction it opened itself on
// MariaDB when every attempt but the last ends in a deadlock.
const mariadbDeadlockAttempts = 5

// retryPolicy returns the policy by which writeTx repeats a transaction it opened itself. On MariaDB
// it makes up to mariadbDeadlockAttempts attempts, the next one only after a deadlock (error 1213):
// InnoDB has then rolled back the whole transaction, so the write can run again from the start. A
// lock wait timeout (error 1205) is not repeated: InnoDB rolls back only the statement that timed
// out, and it is not a deadlock. PostgreSQL and SQLite make one attempt.
func (d dialect) retryPolicy() retryPolicy {
	if d != dialectMariaDB {
		return retryPolicy{}
	}
	return retryPolicy{attempts: mariadbDeadlockAttempts, retryable: isMariaDBDeadlock, pause: retryPause}
}

// mariadbDeadlockText matches the start of the text of the driver's error for MariaDB's deadlock,
// number 1213: "Error 1213 (40001): Deadlock found ...", or "Error 1213: ..." without a SQL state.
var mariadbDeadlockText = regexp.MustCompile(`^Error 1213\b`)

// mariadbNoSuchKeyText matches the start of the text of the driver's error for a FORCE INDEX that
// names an index the table lacks, number 1176: "Error 1176 (42000): Key '...' doesn't exist ...".
var mariadbNoSuchKeyText = regexp.MustCompile(`^Error 1176\b`)

// isMariaDBDeadlock reports whether err or an error in its chain (errors.Unwrap, and each error of an
// Unwrap() []error, as from errors.Join) is MariaDB's deadlock error: an error whose text begins with
// "Error 1213". It reads the text, so the library needs no import of the driver
// (github.com/go-sql-driver/mysql, which MariaDB shares with MySQL). Only the
// start of each text counts, so an error that quotes the deadlock's text later on, like a duplicate
// key error on a value holding "Error 1213" or a hook's "hook failed: Error 1213 ...", is not one.
func isMariaDBDeadlock(err error) bool {
	return errorTextInChain(err, mariadbDeadlockText)
}

// errorTextInChain reports whether the text of err or of an error in its chain (errors.Unwrap, and
// each error of an Unwrap() []error) matches re.
func errorTextInChain(err error, re *regexp.Regexp) bool {
	if err == nil {
		return false
	}
	if re.MatchString(err.Error()) {
		return true
	}
	switch u := err.(type) {
	case interface{ Unwrap() error }:
		return errorTextInChain(u.Unwrap(), re)
	case interface{ Unwrap() []error }:
		for _, e := range u.Unwrap() {
			if errorTextInChain(e, re) {
				return true
			}
		}
	}
	return false
}
