package closuretree

import (
	"database/sql"
	"testing"
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

// TestTxOptions pins the options the library passes to the transactions it opens itself: READ
// COMMITTED on PostgreSQL, the connection's default elsewhere.
func TestTxOptions(t *testing.T) {
	pg := dialectPostgres.txOptions()
	if len(pg) != 1 || pg[0].Isolation != sql.LevelReadCommitted {
		t.Errorf("postgres: want one option at READ COMMITTED, got %+v", pg)
	}
	for _, d := range []dialect{dialectMySQL, dialectSQLite} {
		if got := d.txOptions(); got != nil {
			t.Errorf("dialect %d: want no options, got %+v", d, got)
		}
	}
}
