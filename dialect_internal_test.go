package closuretree

import (
	"database/sql"
	"testing"

	"github.com/go-bumbu/testdbs"
)

// TestDialectFromName pins the mapping from GORM dialector names to the internal dialect enum,
// including the fallback for an unrecognized driver.
func TestDialectFromName(t *testing.T) {
	tests := []struct {
		name string
		want dialect
	}{
		{"sqlite", dialectSQLite},
		{"mysql", dialectMySQL},
		{"postgres", dialectPostgres},
		{"someOtherDriver", dialectPostgres}, // unknown falls back to the FOR UPDATE-capable default
	}
	for _, tt := range tests {
		if got := dialectFromName(tt.name); got != tt.want {
			t.Errorf("dialectFromName(%q) = %d, want %d", tt.name, got, tt.want)
		}
	}
}

// TestMySQLVersionOK pins which server versions New accepts on MySQL: 8.0.17 and later, the first
// release with the collation utf8mb4_0900_bin, with any suffix after the patch number.
func TestMySQLVersionOK(t *testing.T) {
	tests := []struct {
		version string
		want    bool
	}{
		{"5.7.44", false},
		{"8.0.0", false},
		{"8.0.16", false},
		{"8.0.11-TiDB-v7.5.0", false},
		{"8.0.17", true},
		{"8.0.17-log", true},
		{"8.0.45-0ubuntu0.22.04.1", true},
		{"8.1.0", true},
		{"8.4.0", true},
		{"9.1.0", true},
		{"10.11.6-MariaDB", true},
		{"8.0", false},
		{"8", false},
		{"", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		if got := mysqlVersionOK(tt.version); got != tt.want {
			t.Errorf("mysqlVersionOK(%q) = %v, want %v", tt.version, got, tt.want)
		}
	}
}

// TestTxOptions pins the options the library passes to the transactions it opens itself: READ
// COMMITTED on PostgreSQL and MySQL, the connection's default on SQLite.
func TestTxOptions(t *testing.T) {
	for _, d := range []dialect{dialectPostgres, dialectMySQL} {
		got := d.txOptions()
		if len(got) != 1 || got[0].Isolation != sql.LevelReadCommitted {
			t.Errorf("dialect %d: want one option at READ COMMITTED, got %+v", d, got)
		}
	}
	if got := dialectSQLite.txOptions(); got != nil {
		t.Errorf("sqlite: want no options, got %+v", got)
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
