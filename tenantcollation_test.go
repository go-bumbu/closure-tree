package closuretree_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
)

// exactCollation is the collation Migrate gives the tenant columns of a MySQL tree without rows.
const exactCollation = "utf8mb4_0900_bin"

// tenantCollations returns the collation of the tenant column of each of the tree's four tables on
// MySQL, by table name, read from information_schema.
func tenantCollations(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree) map[string]string {
	t.Helper()
	tables := []string{
		ct.NodeTableName(),
		ct.ClosureTableName(),
		"closure_tree_meta_" + ct.NodeTableName(),
		"closure_tree_lock_" + ct.NodeTableName(),
	}
	var rows []struct {
		TableName     string
		CollationName string
	}
	if err := gdb.Raw(`SELECT TABLE_NAME AS table_name, COLLATION_NAME AS collation_name
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_NAME = 'tenant' AND TABLE_NAME IN ?`, tables).
		Scan(&rows).Error; err != nil {
		t.Fatalf("read the tenant collations: %v", err)
	}
	got := make(map[string]string, len(rows))
	for _, r := range rows {
		got[r.TableName] = r.CollationName
	}
	if len(got) != len(tables) {
		t.Fatalf("want the tenant columns of %v, got %v", tables, got)
	}
	return got
}

// assertTenantCollation stops the test unless the tenant columns of all four tables of the tree have
// the collation want.
func assertTenantCollation(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, want string) {
	t.Helper()
	for tbl, got := range tenantCollations(t, gdb, ct) {
		if got != want {
			t.Fatalf("tenant column of %s: want collation %s, got %s", tbl, want, got)
		}
	}
}

// TestTenantSpellingsStaySeparate pins that tenants are compared exactly on every database:
// spellings that differ only in case, accents or trailing spaces are four tenants for the reads and
// for DeleteTenant. On MySQL that needs the collation Migrate gives the tenant columns of a tree
// without rows, so the test first checks that the tree got it.
func TestTenantSpellingsStaySeparate(t *testing.T) {
	spellings := []string{"Acme", "acme", "acmé", "acme "}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := newTestTree(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			if db.DbType() == testdbs.DBTypeMysql {
				assertTenantCollation(t, gdb, ct, exactCollation)
			}
			ctx := context.Background()

			ids := map[string][]uint{}
			for _, tenant := range spellings {
				root := &TestPayload{Name: "root"}
				if err := ct.Add(ctx, root, nil, nil, tenant); err != nil {
					t.Fatalf("add the root of %q: %v", tenant, err)
				}
				child := &TestPayload{Name: "child"}
				if err := ct.Add(ctx, child, up(root.NodeId), nil, tenant); err != nil {
					t.Fatalf("add the child of %q: %v", tenant, err)
				}
				ids[tenant] = []uint{root.NodeId, child.NodeId}
			}

			for _, tenant := range spellings {
				got, err := ct.DescendantIds(ctx, 0, 0, tenant)
				if err != nil {
					t.Fatal(err)
				}
				slices.Sort(got)
				if diff := cmp.Diff(ids[tenant], got); diff != "" {
					t.Errorf("DescendantIds of %q (-want +got):\n%s", tenant, diff)
				}
				for _, other := range spellings {
					if other == tenant {
						continue
					}
					var n TestPayload
					if err := ct.GetNode(ctx, ids[other][0], tenant, &n); !errors.Is(err, closuretree.ErrNodeNotFound) {
						t.Errorf("GetNode of the root of %q as %q: want ErrNodeNotFound, got %v", other, tenant, err)
					}
				}
			}

			assertDeleteTenantExact(t, gdb, ct, spellings, "acme")
		})
	}
}

// assertDeleteTenantExact runs DeleteTenant on deleted, one of the spellings, each of which has two
// nodes, and checks that it deletes the node, closure and meta rows of deleted only. DeleteTenant
// deletes them each by its own tenant predicate, so the rows of each table are checked.
func assertDeleteTenantExact(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, spellings []string, deleted string) {
	t.Helper()
	before := map[string][]relRow{}
	beforeCounts := map[string]tenantCounts{}
	for _, tenant := range spellings {
		before[tenant] = closureRows(t, gdb, ct, tenant)
		beforeCounts[tenant] = countTenant(t, gdb, ct, tenant)
		if c := beforeCounts[tenant]; c.Nodes != 2 || c.Meta == 0 || c.Lock != 1 {
			t.Fatalf("precondition: want 2 nodes, meta rows and one lock row of %q, got %+v", tenant, c)
		}
	}
	if err := ct.DeleteTenant(context.Background(), deleted); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range spellings {
		got := closureRows(t, gdb, ct, tenant)
		counts := countTenant(t, gdb, ct, tenant)
		if tenant == deleted {
			if len(got) != 0 {
				t.Errorf("closure rows of %q after DeleteTenant: %v", tenant, got)
			}
			// node, closure and meta rows are gone; the lock row is kept
			if counts != (tenantCounts{Lock: 1}) {
				t.Errorf("rows of %q after DeleteTenant: %+v", tenant, counts)
			}
			continue
		}
		if diff := cmp.Diff(before[tenant], got); diff != "" {
			t.Errorf("closure rows of %q changed by DeleteTenant(%q) (-want +got):\n%s", tenant, deleted, diff)
		}
		if counts != beforeCounts[tenant] {
			t.Errorf("rows of %q changed by DeleteTenant(%q): want %+v, got %+v", tenant, deleted, beforeCounts[tenant], counts)
		}
	}
}

// TestMigrateTenantCollation pins when Migrate gives the tenant columns the exact collation on
// MySQL: while no table of the tree has a row, also when the caller's AutoMigrate created the node
// table first. A tree with rows keeps the collation it has, and Migrate returns ErrTenantCollation
// when its tenant columns disagree.
func TestMigrateTenantCollation(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMysql {
				t.Skip("MySQL only: PostgreSQL and SQLite compare tenants exactly by default")
			}
			ctx := context.Background()

			t.Run("new", func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				ct, err := newTestTree(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				assertTenantCollation(t, gdb, ct, exactCollation)
			})

			t.Run("again", func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				ct, err := newTestTree(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				if err := ct.Add(ctx, &TestPayload{Name: "a"}, nil, nil, tenant1); err != nil {
					t.Fatal(err)
				}
				if err := ct.Migrate(); err != nil {
					t.Fatalf("second Migrate: %v", err)
				}
				assertTenantCollation(t, gdb, ct, exactCollation)
			})

			t.Run("callerfirst", func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				if err := gdb.AutoMigrate(&TestPayload{}); err != nil {
					t.Fatal(err)
				}
				ct, err := newTestTree(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				assertTenantCollation(t, gdb, ct, exactCollation)
			})

			t.Run("populated", func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				if err := gdb.AutoMigrate(&TestPayload{}); err != nil {
					t.Fatal(err)
				}
				row := TestPayload{Node: closuretree.Node{Tenant: tenant1}, Name: "a"}
				if err := gdb.Create(&row).Error; err != nil {
					t.Fatal(err)
				}
				ct, err := newTestTree(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				var dbDefault string
				if err := gdb.Raw("SELECT @@collation_database").Scan(&dbDefault).Error; err != nil {
					t.Fatal(err)
				}
				if dbDefault == exactCollation {
					t.Fatalf("the database default is %s: the test cannot tell it from the exact one", dbDefault)
				}
				assertTenantCollation(t, gdb, ct, dbDefault)
			})

			t.Run("disagree", func(t *testing.T) {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				ct, err := newTestTree(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				if err := ct.Add(ctx, &TestPayload{Name: "a"}, nil, nil, tenant1); err != nil {
					t.Fatal(err)
				}
				if err := gdb.Exec(`ALTER TABLE test_payloads MODIFY COLUMN tenant varchar(191)
					CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL`).Error; err != nil {
					t.Fatal(err)
				}
				err = ct.Migrate()
				if !errors.Is(err, closuretree.ErrTenantCollation) {
					t.Fatalf("want ErrTenantCollation, got %v", err)
				}
				for _, want := range []string{"test_payloads.tenant utf8mb4_0900_ai_ci", "closure_tree_rel_test_payloads.tenant utf8mb4_0900_bin"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not name %q", err, want)
					}
				}
			})
		})
	}
}
