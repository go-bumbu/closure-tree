package closuretree

import (
	"testing"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
)

// connInternal is the internal package's copy of connAndClose (closuretree_test.go): it opens the
// test's own database, disables idle connections and, on PostgreSQL, frees the connections the
// harness leaks (see freeLeakedPostgresConns in helpers_test.go).
func connInternal(t *testing.T, db testdbs.TargetDb) *gorm.DB {
	t.Helper()
	gdb := db.ConnDbName(t.Name())
	if sqlDB, err := gdb.DB(); err == nil {
		sqlDB.SetMaxIdleConns(0)
	}
	if db.DbType() == testdbs.DBTypePostgres {
		// testdbDefault is the harness's administration database; see harnessDefaultDb.
		if err := gdb.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE datname = ? AND state = 'idle' AND pid <> pg_backend_pid()`, "testdbDefault").Error; err != nil {
			t.Fatalf("terminate leaked harness connections: %v", err)
		}
	}
	return gdb
}

// newDBInternalTree builds and migrates a Tree for model on the test's own database, dropping the
// model's tables first so a rerun (-count > 1) starts empty.
func newDBInternalTree(t *testing.T, db testdbs.TargetDb, model any) *Tree {
	t.Helper()
	gdb := connInternal(t, db)
	ct, err := New(gdb, model)
	if err != nil {
		t.Fatalf("new tree: %v", err)
	}
	for _, tbl := range []string{ct.relationsTbl, ct.metaTbl, ct.lockTbl, ct.nodesTbl} {
		if err := gdb.Exec("DROP TABLE IF EXISTS " + tbl).Error; err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if err := ct.Migrate(); err != nil {
		t.Fatalf("migrate tree: %v", err)
	}
	return ct
}
