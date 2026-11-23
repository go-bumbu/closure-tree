package closuretree

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestAlterTenantCollationSQL pins the statement that gives a tenant column the exact collation and
// that only a plain varchar type reaches its text.
func TestAlterTenantCollationSQL(t *testing.T) {
	tests := []struct {
		columnType string
		want       string // "" when the type is rejected
	}{
		{"varchar(191)", "ALTER TABLE `nodes` MODIFY COLUMN tenant varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin NOT NULL"},
		{"varchar(1024)", "ALTER TABLE `nodes` MODIFY COLUMN tenant varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin NOT NULL"},
		{"longtext", ""},
		{"char(10)", ""},
		{"varchar()", ""},
		{"varchar(191) CHARACTER SET latin1", ""},
		{"varchar(191); DROP TABLE nodes", ""},
		{"VARCHAR(191)", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got, err := alterTenantCollationSQL("`nodes`", tt.columnType, tenantCollationMariaDB)
		if tt.want == "" {
			if err == nil {
				t.Errorf("type %q: want an error, got %q", tt.columnType, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("type %q: %v", tt.columnType, err)
			continue
		}
		if got != tt.want {
			t.Errorf("type %q:\nwant %s\ngot  %s", tt.columnType, tt.want, got)
		}
	}
}

// TestSameTenantCollation pins that tenant columns which do not share one collation give
// ErrTenantCollation, in an error that names each column and its collation.
func TestSameTenantCollation(t *testing.T) {
	exact := tenantColumn{Table: "a", Type: "varchar(191)", Collation: "utf8mb4_0900_bin"}
	ci := tenantColumn{Table: "b", Type: "varchar(191)", Collation: "utf8mb4_0900_ai_ci"}
	ci2 := tenantColumn{Table: "c", Type: "varchar(191)", Collation: "utf8mb4_0900_ai_ci"}
	for _, cols := range [][]tenantColumn{nil, {exact}, {ci, ci2}} {
		if err := sameTenantCollation(cols); err != nil {
			t.Errorf("%v: want nil, got %v", cols, err)
		}
	}
	err := sameTenantCollation([]tenantColumn{exact, ci, ci2})
	if !errors.Is(err, ErrTenantCollation) {
		t.Fatalf("want ErrTenantCollation, got %v", err)
	}
	want := ErrTenantCollation.Error() + ": a.tenant utf8mb4_0900_bin, b.tenant utf8mb4_0900_ai_ci, c.tenant utf8mb4_0900_ai_ci"
	if err.Error() != want {
		t.Errorf("message:\nwant %s\ngot  %s", want, err)
	}
}

// TestInexactTenantColumns pins which tenant columns Migrate alters on a tree without rows: those
// whose collation is not the exact one, utf8mb4_nopad_bin on MariaDB. utf8mb4_0900_bin, an alias of
// it that information_schema reports by its own name, is altered too.
func TestInexactTenantColumns(t *testing.T) {
	exact := tenantColumn{Table: "a", Type: "varchar(191)", Collation: "utf8mb4_nopad_bin"}
	ci := tenantColumn{Table: "b", Type: "varchar(191)", Collation: "utf8mb4_uca1400_ai_ci"}
	bin := tenantColumn{Table: "c", Type: "varchar(191)", Collation: "utf8mb4_bin"}
	alias := tenantColumn{Table: "d", Type: "varchar(191)", Collation: "utf8mb4_0900_bin"}
	tests := []struct {
		name string
		cols []tenantColumn
		want []tenantColumn
	}{
		{"all exact", []tenantColumn{exact, exact}, nil},
		{"mixed", []tenantColumn{exact, ci, bin, alias}, []tenantColumn{ci, bin, alias}},
		{"none exact", []tenantColumn{ci}, []tenantColumn{ci}},
	}
	for _, tt := range tests {
		if diff := cmp.Diff(tt.want, inexactTenantColumns(tt.cols, tenantCollationMariaDB)); diff != "" {
			t.Errorf("%s (-want +got):\n%s", tt.name, diff)
		}
	}
}

// TestTenantCollationOf pins the exact tenant collation per server: utf8mb4_nopad_bin on MariaDB,
// utf8mb4_0900_bin on MySQL (accepted, untested).
func TestTenantCollationOf(t *testing.T) {
	tests := []struct{ version, want string }{
		{"11.8.9-MariaDB-ubu2404", "utf8mb4_nopad_bin"},
		{"8.0.45-0ubuntu0.22.04.1", "utf8mb4_0900_bin"},
		{"9.1.0", "utf8mb4_0900_bin"},
	}
	for _, tt := range tests {
		if got := tenantCollationOf(tt.version); got != tt.want {
			t.Errorf("tenantCollationOf(%q) = %s, want %s", tt.version, got, tt.want)
		}
	}
}
