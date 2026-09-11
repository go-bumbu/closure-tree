package closuretree

import "testing"

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
