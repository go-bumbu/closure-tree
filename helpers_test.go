package closuretree_test

import (
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"gorm.io/gorm"
)

// newTestTree constructs a Tree and runs Migrate, restoring the one-call convenience the tests
// relied on before New and Migrate were split. It returns New's or Migrate's error unwrapped so the
// error-path tests keep asserting on the exact construction error.
func newTestTree(db *gorm.DB, model any) (*closuretree.Tree, error) {
	ct, err := closuretree.New(db, model)
	if err != nil {
		return ct, err
	}
	return ct, ct.Migrate()
}

// harnessDefaultDb is the database the PostgreSQL harness (testdbs v0.2.3) connects to for
// administration. It cannot be read from the harness: db.Conn() lowercases the name before the
// pool lookup, misses, and creates and connects to a new database "testdbdefault" instead.
const harnessDefaultDb = "testdbDefault"

// freeLeakedPostgresConns works around a connection leak in testdbs v0.2.3: for every new database
// name, ConnDbName opens an admin pool on the harness's default database to run CREATE DATABASE and
// never closes it, so each test leaks one idle connection until the server's limit (100) is
// reached. It terminates the idle backends on that default database, using gdb (a connection to
// the test's own database). Delete it once testdbs is at v0.2.4 or later, which closes the pool.
func freeLeakedPostgresConns(t *testing.T, gdb *gorm.DB) {
	t.Helper()
	if err := gdb.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = ? AND state = 'idle' AND pid <> pg_backend_pid()`, harnessDefaultDb).Error; err != nil {
		t.Fatalf("terminate leaked harness connections: %v", err)
	}
}
