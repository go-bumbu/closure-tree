package closuretree

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-bumbu/testdbs"
	"github.com/go-sql-driver/mysql"
)

// TestDialectFromName pins the mapping from GORM dialector names to the internal dialect enum,
// including the fallback for an unrecognized driver.
func TestDialectFromName(t *testing.T) {
	tests := []struct {
		name string
		want dialect
	}{
		{"sqlite", dialectSQLite},
		{"mysql", dialectMariaDB},
		{"postgres", dialectPostgres},
		{"someOtherDriver", dialectPostgres}, // unknown falls back to the FOR UPDATE-capable default
	}
	for _, tt := range tests {
		if got := dialectFromName(tt.name); got != tt.want {
			t.Errorf("dialectFromName(%q) = %d, want %d", tt.name, got, tt.want)
		}
	}
}

// TestCheckServerVersion pins which server versions New accepts on gorm's "mysql" driver: MariaDB
// 11.8 and later, and MySQL (accepted, untested) 8.0.17 and later, the first release with the
// collation utf8mb4_0900_bin, with any suffix after the patch number.
func TestCheckServerVersion(t *testing.T) {
	tests := []struct {
		version string
		want    bool
	}{
		{"10.11.6-MariaDB", false},
		{"11.3.2-MariaDB", false},
		{"11.4.13-MariaDB-ubu2404", false},
		{"11.7.2-MariaDB-ubu2404", false},
		{"11.8.0-MariaDB", true},
		{"11.8.9-MariaDB-ubu2404", true},
		{"12.0.2-MariaDB-log", true},
		{"5.7.44", false},
		{"8.0.16", false},
		{"8.0.11-TiDB-v7.5.0", false},
		{"8.0.17", true},
		{"8.0.45-0ubuntu0.22.04.1", true},
		{"9.1.0", true},
		{"8.0", false},
		{"", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		if err := checkServerVersion(tt.version); (err == nil) != tt.want {
			t.Errorf("checkServerVersion(%q) = %v, want accepted %v", tt.version, err, tt.want)
		}
	}
}

// TestTxOptions pins the options the library passes to the transactions it opens itself: READ
// COMMITTED on PostgreSQL and MariaDB, the connection's default on SQLite.
func TestTxOptions(t *testing.T) {
	for _, d := range []dialect{dialectPostgres, dialectMariaDB} {
		got := d.txOptions()
		if len(got) != 1 || got[0].Isolation != sql.LevelReadCommitted {
			t.Errorf("dialect %d: want one option at READ COMMITTED, got %+v", d, got)
		}
	}
	if got := dialectSQLite.txOptions(); got != nil {
		t.Errorf("sqlite: want no options, got %+v", got)
	}
}

// TestIsMariaDBDeadlock pins which errors the MariaDB retry repeats: a deadlock (error 1213), also
// wrapped or joined, and no other error: not a lock wait timeout (error 1205), and not an error
// whose text only quotes "Error 1213" after its start.
func TestIsMariaDBDeadlock(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'},
		Message: "Deadlock found when trying to get lock; try restarting transaction"}
	duplicate := &mysql.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'},
		Message: "Duplicate entry 'Error 1213 (40001): x' for key 'nodes.name'"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"driverError", deadlock, true},
		{"withoutSQLState", &mysql.MySQLError{Number: 1213, Message: "Deadlock found"}, true},
		{"wrapped", fmt.Errorf("deleteRecurseMany: failed to delete nodes: %w", deadlock), true},
		{"joined", errors.Join(errors.New("first"), deadlock), true},
		{"wrappedTwice", fmt.Errorf("add: %w", fmt.Errorf("insert: %w", deadlock)), true},
		{"wrappedInJoined", errors.Join(errors.New("first"), fmt.Errorf("delete: %w", deadlock)), true},
		{"multiWrapped", fmt.Errorf("first: %w; second: %w", errors.New("other"), deadlock), true},
		{"duplicateQuotingDeadlock", duplicate, false},
		{"wrappedDuplicateQuotingDeadlock", fmt.Errorf("add: %w", duplicate), false},
		{"hookQuotingDeadlock", errors.New("hook failed: Error 1213 (40001): Deadlock found"), false},
		{"joinedTextQuotingDeadlock", errors.Join(errors.New("first\nError 1213 (40001): Deadlock found")), false},
		{"lockWaitTimeout", &mysql.MySQLError{Number: 1205, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
			Message: "Lock wait timeout exceeded; try restarting transaction"}, false},
		{"longerNumber", errors.New("Error 12130: some other error"), false},
		{"other", errors.New("deadlock"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMariaDBDeadlock(tt.err); got != tt.want {
				t.Errorf("isMariaDBDeadlock(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestRetryPolicyOfDialect pins which transactions writeTx repeats: on MariaDB up to 5 attempts in
// all, after a deadlock only, with the jittered pause; on PostgreSQL and SQLite one attempt.
func TestRetryPolicyOfDialect(t *testing.T) {
	p := dialectMariaDB.retryPolicy()
	if p.attempts != 5 || p.retryable == nil || p.pause == nil {
		t.Fatalf("mariadb: want 5 attempts with a retryable test and a pause, got %+v", p)
	}
	if !p.retryable(&mysql.MySQLError{Number: 1213}) || p.retryable(&mysql.MySQLError{Number: 1205}) {
		t.Error("mariadb: want a deadlock retried and a lock wait timeout not")
	}
	for _, d := range []dialect{dialectPostgres, dialectSQLite} {
		if got := d.retryPolicy(); got.attempts > 1 {
			t.Errorf("dialect %d: want one attempt, got %d", d, got.attempts)
		}
	}
}

// TestUpsertMetaHalvingsKeepsLower pins what the single upsert of upsertMetaHalvings stores: a new
// row for a new (tenant, parent), otherwise the lower of the stored and the new min_halvings.
func TestUpsertMetaHalvingsKeepsLower(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			stored := func(parentID uint) []int {
				t.Helper()
				var got []int
				if err := ct.db.Table(ct.metaTbl).Where("tenant = ? AND parent_id = ?", "t1", parentID).
					Pluck("min_halvings", &got).Error; err != nil {
					t.Fatal(err)
				}
				return got
			}
			steps := []struct {
				parentID uint
				halvings int
				want     int
			}{
				{0, 50, 50}, // new row
				{0, 60, 50}, // higher: kept
				{0, 50, 50}, // equal: kept
				{0, 40, 40}, // lower: stored
				{7, 9999, 9999},
				{7, 0, 0},
			}
			for _, s := range steps {
				if err := ct.upsertMetaHalvings(ct.db, s.parentID, "t1", s.halvings); err != nil {
					t.Fatalf("upsert parent %d halvings %d: %v", s.parentID, s.halvings, err)
				}
				if got := stored(s.parentID); len(got) != 1 || got[0] != s.want {
					t.Errorf("after upserting %d for parent %d: want [%d], got %v", s.halvings, s.parentID, s.want, got)
				}
			}
			if got := stored(0); len(got) != 1 || got[0] != 40 {
				t.Errorf("parent 0 changed by the writes for parent 7: %v", got)
			}
		})
	}
}

// TestTenantDeleteSQL pins the statement that deletes a tenant's rows: on MariaDB the closure
// table's forces the tenant index in the multi-table form of DELETE; the Tree's other tables and
// the other databases get the plain statement.
func TestTenantDeleteSQL(t *testing.T) {
	const plainRel = "DELETE FROM rel WHERE tenant = ?"
	tests := []struct {
		name string
		d    dialect
		tbl  string
		want string
	}{
		{"mariadbClosure", dialectMariaDB, "rel", "DELETE rel FROM rel FORCE INDEX (idx_rel_tenant) WHERE tenant = ?"},
		{"mariadbNodes", dialectMariaDB, "nodes", "DELETE FROM nodes WHERE tenant = ?"},
		{"mariadbMeta", dialectMariaDB, "meta", "DELETE FROM meta WHERE tenant = ?"},
		{"postgresClosure", dialectPostgres, "rel", plainRel},
		{"sqliteClosure", dialectSQLite, "rel", plainRel},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := &Tree{dialect: tt.d, nodesTbl: "nodes", relationsTbl: "rel", metaTbl: "meta", tenantIdx: "idx_rel_tenant"}
			if got := ct.tenantDeleteSQL(tt.tbl); got != tt.want {
				t.Errorf("want %q, got %q", tt.want, got)
			}
		})
	}
}

// TestTenantIndexMissing pins which errors of a tenant delete get the advice to run Migrate:
// MariaDB's error 1176 for a missing index, also wrapped, and no other.
func TestTenantIndexMissing(t *testing.T) {
	ct := &Tree{relationsTbl: "rel", tenantIdx: "idx_rel_tenant"}
	noKey := &mysql.MySQLError{Number: 1176, SQLState: [5]byte{'4', '2', '0', '0', '0'},
		Message: "Key 'idx_rel_tenant' doesn't exist in table 'rel'"}
	for _, err := range []error{noKey, fmt.Errorf("exec: %w", noKey)} {
		got := ct.tenantIndexMissing(err)
		if !errors.Is(got, noKey) || !strings.Contains(got.Error(), "run Migrate") {
			t.Errorf("%v: want the advice to run Migrate around the error, got %v", err, got)
		}
	}
	other := &mysql.MySQLError{Number: 1213, Message: "Deadlock found"}
	if got := ct.tenantIndexMissing(other); got != error(other) {
		t.Errorf("want a deadlock returned as it is, got %v", got)
	}
}
