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
		{"varchar(191)", "ALTER TABLE `nodes` MODIFY COLUMN tenant varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"},
		{"varchar(1024)", "ALTER TABLE `nodes` MODIFY COLUMN tenant varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"},
		{"longtext", ""},
		{"char(10)", ""},
		{"varchar()", ""},
		{"varchar(191) CHARACTER SET latin1", ""},
		{"varchar(191); DROP TABLE nodes", ""},
		{"VARCHAR(191)", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got, err := alterTenantCollationSQL("`nodes`", tt.columnType)
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
// whose collation is not utf8mb4_0900_bin.
func TestInexactTenantColumns(t *testing.T) {
	exact := tenantColumn{Table: "a", Type: "varchar(191)", Collation: "utf8mb4_0900_bin"}
	ci := tenantColumn{Table: "b", Type: "varchar(191)", Collation: "utf8mb4_0900_ai_ci"}
	bin := tenantColumn{Table: "c", Type: "varchar(191)", Collation: "utf8mb4_bin"}
	tests := []struct {
		name string
		cols []tenantColumn
		want []tenantColumn
	}{
		{"all exact", []tenantColumn{exact, exact}, nil},
		{"mixed", []tenantColumn{exact, ci, bin}, []tenantColumn{ci, bin}},
		{"none exact", []tenantColumn{ci}, []tenantColumn{ci}},
	}
	for _, tt := range tests {
		if diff := cmp.Diff(tt.want, inexactTenantColumns(tt.cols)); diff != "" {
			t.Errorf("%s (-want +got):\n%s", tt.name, diff)
		}
	}
}
