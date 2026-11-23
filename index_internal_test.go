package closuretree

import (
	"context"
	"database/sql"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
)

// tenantIndexLongName has a node table name long enough that the name of its closure table's
// tenant index, idx_closure_tree_rel_<node table>_tenant, exceeds the identifier length of every
// database, so the naming strategy shortens it.
type tenantIndexLongName struct {
	Node
	Name string
}

func (tenantIndexLongName) TableName() string { return "tenant_index_naming_with_a_long_table_name" }

// tenantIndexColumns returns the columns of the closure table's index named ct.tenantIdx, or nil
// when the table has no index of that name.
func tenantIndexColumns(t *testing.T, ct *Tree) []string {
	t.Helper()
	idxs, err := ct.db.Migrator().GetIndexes(ct.relationsTbl)
	if err != nil {
		t.Fatalf("indexes of %s: %v", ct.relationsTbl, err)
	}
	for _, idx := range idxs {
		if idx.Name() == ct.tenantIdx {
			return idx.Columns()
		}
	}
	return nil
}

// tenantOnlyIndexes counts the closure table's indexes on the column tenant alone.
func tenantOnlyIndexes(t *testing.T, ct *Tree) int {
	t.Helper()
	idxs, err := ct.db.Migrator().GetIndexes(ct.relationsTbl)
	if err != nil {
		t.Fatalf("indexes of %s: %v", ct.relationsTbl, err)
	}
	n := 0
	for _, idx := range idxs {
		if reflect.DeepEqual(idx.Columns(), []string{"tenant"}) {
			n++
		}
	}
	return n
}

// closureRows returns every row of the closure table in a fixed order.
func closureRows(t *testing.T, ct *Tree) []closureTree {
	t.Helper()
	var rows []closureTree
	if err := ct.db.Table(ct.relationsTbl).Order("tenant, ancestor_id, descendant_id, depth").Find(&rows).Error; err != nil {
		t.Fatalf("read %s: %v", ct.relationsTbl, err)
	}
	return rows
}

// Migrate creates the closure table with an index on the tenant column alone, named
// idx_<closure table>_tenant.
func TestTenantIndexOnNewTree(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			if want := "idx_" + ct.relationsTbl + "_tenant"; ct.tenantIdx != want {
				t.Errorf("want the index name %s, got %s", want, ct.tenantIdx)
			}
			if got := tenantIndexColumns(t, ct); !reflect.DeepEqual(got, []string{"tenant"}) {
				t.Errorf("want the index %s on (tenant), got columns %v", ct.tenantIdx, got)
			}
		})
	}
}

// A tree migrated before the tenant index existed gets it from the next Migrate, which keeps the
// rows.
func TestTenantIndexAddedToExistingTree(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			for _, tenant := range []string{"t1", "t2"} {
				root := txSample{Name: "root"}
				if err := ct.Add(ctx, &root, nil, nil, tenant); err != nil {
					t.Fatal(err)
				}
				if err := ct.Add(ctx, &txSample{Name: "child"}, &root.NodeId, nil, tenant); err != nil {
					t.Fatal(err)
				}
			}
			if err := ct.db.Migrator().DropIndex(ct.relationsTbl, ct.tenantIdx); err != nil {
				t.Fatalf("drop the tenant index: %v", err)
			}
			if got := tenantIndexColumns(t, ct); got != nil {
				t.Fatalf("precondition: the index %s is still there, on %v", ct.tenantIdx, got)
			}
			before := closureRows(t, ct)

			if err := ct.Migrate(); err != nil {
				t.Fatalf("migrate the existing tree: %v", err)
			}
			if got := tenantIndexColumns(t, ct); !reflect.DeepEqual(got, []string{"tenant"}) {
				t.Errorf("want the index %s on (tenant), got columns %v", ct.tenantIdx, got)
			}
			if after := closureRows(t, ct); !reflect.DeepEqual(after, before) {
				t.Errorf("Migrate changed the closure rows:\nbefore %v\nafter  %v", before, after)
			}
			if err := ct.Add(ctx, &txSample{Name: "after"}, nil, nil, "t1"); err != nil {
				t.Errorf("add after Migrate: %v", err)
			}
		})
	}
}

// When idx_<closure table>_tenant is longer than the database allows, the Tree names the index as
// gorm's naming strategy shortens it, so a second Migrate finds it and builds no other.
func TestTenantIndexShortenedName(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, tenantIndexLongName{})
			full := "idx_" + ct.relationsTbl + "_tenant"
			maxLen := 64 // gorm's default; MySQL's limit
			if db.DbType() == testdbs.DBTypePostgres {
				maxLen = 63 // gorm's PostgreSQL driver, after PostgreSQL's limit
			}
			if len(full) <= maxLen {
				t.Fatalf("precondition: %s is not longer than %d characters", full, maxLen)
			}
			if len(ct.tenantIdx) != maxLen || ct.tenantIdx[:20] != full[:20] {
				t.Errorf("want %s shortened to %d characters, got %s", full, maxLen, ct.tenantIdx)
			}
			if got := tenantIndexColumns(t, ct); !reflect.DeepEqual(got, []string{"tenant"}) {
				t.Errorf("want the index %s on (tenant), got columns %v", ct.tenantIdx, got)
			}
			if err := ct.Migrate(); err != nil {
				t.Fatalf("second Migrate: %v", err)
			}
			if n := tenantOnlyIndexes(t, ct); n != 1 {
				t.Errorf("want one index on (tenant) after the second Migrate, got %d", n)
			}
		})
	}
}

// explainKeys returns the key column of each row of EXPLAIN for stmt, "" for a row without a key.
func explainKeys(t *testing.T, ct *Tree, stmt, tenant string) []string {
	t.Helper()
	var plan []struct {
		Key sql.NullString `gorm:"column:key"`
	}
	if err := ct.db.Raw("EXPLAIN "+stmt, tenant).Scan(&plan).Error; err != nil {
		t.Fatalf("explain %s: %v", stmt, err)
	}
	keys := make([]string, len(plan))
	for i, row := range plan {
		keys[i] = row.Key.String
	}
	return keys
}

// On MySQL the delete of a tenant holding most closure rows uses the tenant index because of the
// hint; without it the optimizer scans the table, also the other tenants' rows.
func TestTenantDeleteHintUsesIndexOnMySQL(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMysql {
				t.Skip("MySQL only")
			}
			ct := newDBInternalTree(t, db, txSample{})
			rows := make([]closureTree, 0, 2020)
			for i := range 2000 {
				id := uint(i + 1)
				rows = append(rows, closureTree{AncestorID: id, DescendantID: id, Tenant: "big", Depth: 0})
			}
			for i := range 20 {
				id := uint(i + 1)
				rows = append(rows, closureTree{AncestorID: id, DescendantID: id, Tenant: "small", Depth: 0})
			}
			if err := ct.db.Table(ct.relationsTbl).CreateInBatches(rows, 1000).Error; err != nil {
				t.Fatalf("insert closure rows: %v", err)
			}
			if err := ct.db.Exec("ANALYZE TABLE " + ct.relationsTbl).Error; err != nil {
				t.Fatalf("analyze: %v", err)
			}
			plain := "DELETE FROM " + ct.relationsTbl + " WHERE tenant = ?"
			if got := explainKeys(t, ct, plain, "big"); reflect.DeepEqual(got, []string{ct.tenantIdx}) {
				t.Fatalf("precondition: the plain delete already uses %s", ct.tenantIdx)
			}
			stmt := ct.tenantDeleteSQL(ct.relationsTbl)
			if got := explainKeys(t, ct, stmt, "big"); !reflect.DeepEqual(got, []string{ct.tenantIdx}) {
				t.Errorf("want %s to use the key %s, got %v", stmt, ct.tenantIdx, got)
			}
		})
	}
}

// DeleteTenant and BuildFromParents delete the tenant's closure rows with tenantDeleteSQL, so on
// MySQL with the hint.
func TestTenantDeletesSendTenantDeleteSQL(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			root := txSample{Name: "root"}
			if err := ct.Add(ctx, &root, nil, nil, "t1"); err != nil {
				t.Fatal(err)
			}
			var sent []string
			if err := ct.db.Callback().Raw().After("gorm:raw").Register("test:record", func(db *gorm.DB) {
				sent = append(sent, db.Statement.SQL.String())
			}); err != nil {
				t.Fatal(err)
			}
			// PostgreSQL numbers the placeholder, so compare up to it.
			want := strings.TrimSuffix(ct.tenantDeleteSQL(ct.relationsTbl), "?")
			calls := []struct {
				name string
				call func() error
			}{
				{"BuildFromParents", func() error {
					return ct.BuildFromParents(ctx, "t1", []ParentLink{{NodeID: root.NodeId}}, Rebuild())
				}},
				{"DeleteTenant", func() error { return ct.DeleteTenant(ctx, "t1") }},
			}
			for _, c := range calls {
				sent = nil
				if err := c.call(); err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				if !slices.ContainsFunc(sent, func(s string) bool { return strings.HasPrefix(s, want) }) {
					t.Errorf("%s: want a statement starting with %q, sent %q", c.name, want, sent)
				}
			}
		})
	}
}

// warning is a row of SHOW WARNINGS.
type warning struct {
	Level   string
	Code    int
	Message string
}

// A hint MySQL cannot apply does not fail the delete: one naming an index the table lacks (a tree
// migrated before the index existed), and one the server does not know, which stands in for INDEX on
// MySQL 8.0.17 to 8.0.19 (the test image runs a later 8.0). Each deletes the tenant's rows and leaves
// one warning.
func TestTenantDeleteHintNotAppliedOnMySQL(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMysql {
				t.Skip("MySQL only")
			}
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			tenants := []string{"bound1", "literal1", "bound2", "literal2", "deleteTenant"}
			for _, tenant := range tenants {
				if err := ct.Add(ctx, &txSample{Name: "root"}, nil, nil, tenant); err != nil {
					t.Fatal(err)
				}
			}
			if err := ct.db.Migrator().DropIndex(ct.relationsTbl, ct.tenantIdx); err != nil {
				t.Fatalf("drop the tenant index: %v", err)
			}
			hinted := ct.tenantDeleteSQL(ct.relationsTbl)
			unknown := strings.Replace(hinted, "INDEX(", "NO_SUCH_HINT(", 1)
			if unknown == hinted {
				t.Fatalf("precondition: no INDEX hint in %s", hinted)
			}
			tests := []struct {
				name    string
				bound   string // deleted with the tenant bound, as the library sends it
				literal string // deleted with the tenant in the text, so SHOW WARNINGS sees the parse
				stmt    string
				code    int // of the one warning
			}{
				{"missingIndex", "bound1", "literal1", hinted, 3128}, // ER_UNRESOLVED_HINT_NAME
				{"unknownHint", "bound2", "literal2", unknown, 1064}, // the hint parser's syntax error, as a warning
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					res := ct.db.Exec(tt.stmt, tt.bound)
					if res.Error != nil {
						t.Fatalf("%s with a bound tenant: %v", tt.stmt, res.Error)
					}
					if res.RowsAffected != 2 { // the root's own row and its row under the virtual root
						t.Errorf("bound tenant: want 2 closure rows deleted, got %d", res.RowsAffected)
					}

					// A prepared statement's warnings are gone after it executes, so this one is sent
					// as text, on one connection with the SHOW WARNINGS that follows it.
					literal := strings.Replace(tt.stmt, "?", "'"+tt.literal+"'", 1)
					var deleted int64
					var warnings []warning
					err := ct.db.Connection(func(conn *gorm.DB) error {
						res := conn.Exec(literal)
						if res.Error != nil {
							return res.Error
						}
						deleted = res.RowsAffected
						return conn.Raw("SHOW WARNINGS").Scan(&warnings).Error
					})
					if err != nil {
						t.Fatalf("%s: %v", literal, err)
					}
					if deleted != 2 {
						t.Errorf("literal tenant: want 2 closure rows deleted, got %d", deleted)
					}
					if len(warnings) != 1 || warnings[0].Level != "Warning" || warnings[0].Code != tt.code {
						t.Errorf("want one warning with code %d, got %+v", tt.code, warnings)
					}
				})
			}
			if err := ct.DeleteTenant(ctx, "deleteTenant"); err != nil {
				t.Errorf("DeleteTenant without the tenant index: %v", err)
			}
		})
	}
}

// On MySQL the statement Migrate sends builds the tenant index in place without blocking writes.
// Migrate sends it without ALGORITHM and LOCK, and MySQL's defaults choose INPLACE and LOCK=NONE
// when the operation allows them. The test appends ALGORITHM=INPLACE LOCK=NONE to that statement,
// so MySQL fails it if it cannot build the index that way; it also fails if Migrate sends no
// such statement.
func TestTenantIndexBuildsOnlineOnMySQL(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMysql {
				t.Skip("MySQL only")
			}
			ct := newDBInternalTree(t, db, txSample{})
			if err := ct.Add(context.Background(), &txSample{Name: "root"}, nil, nil, "t1"); err != nil {
				t.Fatal(err)
			}
			if err := ct.db.Migrator().DropIndex(ct.relationsTbl, ct.tenantIdx); err != nil {
				t.Fatalf("drop the tenant index: %v", err)
			}
			const name = "test:online_tenant_index"
			prefix := "CREATE INDEX " + ct.db.Statement.Quote(ct.tenantIdx) + " "
			online := 0
			if err := ct.db.Callback().Raw().Before("gorm:raw").Register(name, func(db *gorm.DB) {
				if strings.HasPrefix(db.Statement.SQL.String(), prefix) {
					online++
					db.Statement.SQL.WriteString(" ALGORITHM=INPLACE LOCK=NONE")
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := ct.db.Callback().Raw().Remove(name); err != nil {
					t.Error(err)
				}
			})
			if err := ct.Migrate(); err != nil {
				t.Fatalf("migrate with the index built online: %v", err)
			}
			if online != 1 {
				t.Errorf("want Migrate to send one statement starting with %q, got %d", prefix, online)
			}
			if got := tenantIndexColumns(t, ct); !reflect.DeepEqual(got, []string{"tenant"}) {
				t.Errorf("want the index %s on (tenant), got columns %v", ct.tenantIdx, got)
			}
		})
	}
}
