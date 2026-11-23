package closuretree

import (
	"context"
	"reflect"
	"testing"

	"github.com/go-bumbu/testdbs"
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

// MySQL builds the tenant index in place without blocking writes: the statement fails if it cannot.
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
			stmt := "CREATE INDEX " + ct.tenantIdx + " ON " + ct.relationsTbl + " (tenant) ALGORITHM=INPLACE LOCK=NONE"
			if err := ct.db.Exec(stmt).Error; err != nil {
				t.Fatalf("build the index online: %v", err)
			}
			if got := tenantIndexColumns(t, ct); !reflect.DeepEqual(got, []string{"tenant"}) {
				t.Errorf("want the index %s on (tenant), got columns %v", ct.tenantIdx, got)
			}
		})
	}
}
