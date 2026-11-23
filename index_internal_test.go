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
			maxLen := 64 // gorm's default; MariaDB's limit
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

// On MariaDB the delete of a tenant holding most closure rows uses the tenant index because it
// forces it; without that the optimizer scans the table, also the other tenants' rows.
func TestTenantDeleteForcesIndexOnMariaDB(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMariadb {
				t.Skip("MariaDB only")
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
// MariaDB with the forced index.
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

// On MariaDB the tenant-wide deletes force the tenant index, which fails on a tree whose closure
// table lacks it (one not migrated since the index was added): DeleteTenant and BuildFromParents
// then return an error that says to run Migrate, and write nothing. After Migrate they work.
func TestTenantDeleteWithoutIndexOnMariaDB(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMariadb {
				t.Skip("MariaDB only")
			}
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			root := txSample{Name: "root"}
			if err := ct.Add(ctx, &root, nil, nil, "t1"); err != nil {
				t.Fatal(err)
			}
			if err := ct.db.Migrator().DropIndex(ct.relationsTbl, ct.tenantIdx); err != nil {
				t.Fatalf("drop the tenant index: %v", err)
			}
			before := closureRows(t, ct)
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
				err := c.call()
				if err == nil || !strings.Contains(err.Error(), "run Migrate") || !strings.Contains(err.Error(), ct.tenantIdx) {
					t.Errorf("%s without the tenant index: want an error naming %s and Migrate, got %v", c.name, ct.tenantIdx, err)
				}
				if after := closureRows(t, ct); !reflect.DeepEqual(after, before) {
					t.Errorf("%s changed the closure rows:\nbefore %v\nafter  %v", c.name, before, after)
				}
			}
			if err := ct.Migrate(); err != nil {
				t.Fatal(err)
			}
			for _, c := range calls {
				if err := c.call(); err != nil {
					t.Errorf("%s after Migrate: %v", c.name, err)
				}
			}
		})
	}
}

// On MariaDB the statement Migrate sends builds the tenant index in place without blocking writes.
// Migrate sends it without ALGORITHM and LOCK, and MariaDB's defaults choose INPLACE and LOCK=NONE
// when the operation allows them. The test appends ALGORITHM=INPLACE LOCK=NONE to that statement,
// so MariaDB fails it if it cannot build the index that way; it also fails if Migrate sends no
// such statement.
func TestTenantIndexBuildsOnlineOnMariaDB(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMariadb {
				t.Skip("MariaDB only")
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

// The Tree names the node table's (tenant, node_id) index as gorm does, so the index the sort_order
// update forces on MariaDB is the one Migrate created.
func TestNodeIndexName(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			idxs, err := ct.db.Migrator().GetIndexes(ct.nodesTbl)
			if err != nil {
				t.Fatal(err)
			}
			for _, idx := range idxs {
				if idx.Name() == ct.nodeIdx {
					// gorm's PostgreSQL driver lists the columns in table order, not index order
					got := slices.Sorted(slices.Values(idx.Columns()))
					if !reflect.DeepEqual(got, []string{"node_id", "tenant"}) {
						t.Errorf("index %s: want the columns tenant and node_id, got %v", ct.nodeIdx, idx.Columns())
					}
					return
				}
			}
			t.Errorf("no index %s on %s", ct.nodeIdx, ct.nodesTbl)
		})
	}
}

// On MariaDB BuildFromParents' sort_order update forces the (tenant, node_id) index: for a batch
// of hundreds of ids MariaDB otherwise scans the whole primary key, also the other tenants' rows.
// The test captures the update BuildFromParents sends and EXPLAINs it.
func TestSortOrderUpdateForcesIndexOnMariaDB(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMariadb {
				t.Skip("MariaDB only")
			}
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			seed := func(tenant string) []ParentLink {
				rows := make([]txSample, 600)
				for i := range rows {
					rows[i] = txSample{Node: Node{Tenant: tenant}, Name: "n"}
				}
				if err := ct.db.Table(ct.nodesTbl).CreateInBatches(rows, 300).Error; err != nil {
					t.Fatal(err)
				}
				links := make([]ParentLink, len(rows))
				for i, r := range rows {
					links[i] = ParentLink{NodeID: r.NodeId}
				}
				return links
			}
			seed("before")
			links := seed("t1")
			seed("after")
			if err := ct.db.Exec("ANALYZE TABLE " + ct.nodesTbl).Error; err != nil {
				t.Fatal(err)
			}

			var updates []struct {
				sql  string
				vars []any
			}
			const name = "test:record_sort_update"
			if err := ct.db.Callback().Raw().Before("gorm:raw").Register(name, func(db *gorm.DB) {
				if s := db.Statement.SQL.String(); strings.HasPrefix(s, "UPDATE "+ct.nodesTbl) {
					updates = append(updates, struct {
						sql  string
						vars []any
					}{s, slices.Clone(db.Statement.Vars)})
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := ct.db.Callback().Raw().Remove(name); err != nil {
					t.Error(err)
				}
			})
			if err := ct.BuildFromParents(ctx, "t1", links); err != nil {
				t.Fatal(err)
			}
			if len(updates) == 0 {
				t.Fatal("BuildFromParents sent no sort_order update")
			}
			u := updates[0]
			if !strings.Contains(u.sql, "FORCE INDEX ("+ct.nodeIdx+")") {
				t.Errorf("want the update to force %s, got %.120s", ct.nodeIdx, u.sql)
			}
			var plan []struct {
				Key  sql.NullString `gorm:"column:key"`
				Rows int64          `gorm:"column:rows"`
			}
			if err := ct.db.Raw("EXPLAIN "+u.sql, u.vars...).Scan(&plan).Error; err != nil {
				t.Fatal(err)
			}
			if len(plan) != 1 || plan[0].Key.String != ct.nodeIdx || plan[0].Rows > int64(sortBatchRows) {
				t.Errorf("want the update to read at most %d rows through %s, got %+v", sortBatchRows, ct.nodeIdx, plan)
			}
		})
	}
}
