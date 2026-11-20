package closuretree

import (
	"errors"
	"testing"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
)

type lockRetryNode struct {
	Node
	Name string
}

func TestLockTenantRetry(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := db.ConnDbName("lockretry")
			ct, err := New(gdb, lockRetryNode{})
			if err != nil {
				t.Fatal(err)
			}
			if ct.dialect == dialectSQLite {
				t.Skip("SQLite takes no row lock")
			}
			if err := ct.Migrate(); err != nil {
				t.Fatal(err)
			}
			del := func(tx *gorm.DB) { tx.Exec("DELETE FROM "+ct.lockTbl+" WHERE tenant = ?", "t") }

			t.Run("retries when the row vanishes once", func(t *testing.T) {
				calls := 0
				err := gdb.Transaction(func(tx *gorm.DB) error {
					return ct.dialect.lockTenantHook(tx, ct.lockTbl, "t", func() {
						if calls == 0 {
							del(tx)
						}
						calls++
					})
				})
				if err != nil || calls != 2 {
					t.Fatalf("want success on the second attempt, got err=%v calls=%d", err, calls)
				}
			})

			t.Run("gives up after lockAttempts", func(t *testing.T) {
				calls := 0
				err := gdb.Transaction(func(tx *gorm.DB) error {
					return ct.dialect.lockTenantHook(tx, ct.lockTbl, "t", func() { del(tx); calls++ })
				})
				if !errors.Is(err, ErrLockNotAcquired) || calls != lockAttempts {
					t.Fatalf("want ErrLockNotAcquired after %d attempts, got err=%v calls=%d", lockAttempts, err, calls)
				}
			})
		})
	}
}
