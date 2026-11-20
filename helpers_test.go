package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	closuretree "github.com/go-bumbu/closure-tree"
	"gorm.io/gorm"
)

// newTestTree constructs a Tree and runs Migrate, restoring the one-call convenience the tests
// relied on before New and Migrate were split. It returns New's or Migrate's error unwrapped so the
// error-path tests keep asserting on the exact construction error.
func newTestTree(db *gorm.DB, model any) (*closuretree.Tree, error) {
	ct, err := closuretree.New(db, model)
	if err != nil {
		return ct, err
	}
	return ct, ct.Migrate()
}

// harnessDefaultDb is the database the PostgreSQL harness (testdbs v0.2.3) connects to for
// administration. It cannot be read from the harness: db.Conn() lowercases the name before the
// pool lookup, misses, and creates and connects to a new database "testdbdefault" instead.
const harnessDefaultDb = "testdbDefault"

// freeLeakedPostgresConns works around a connection leak in testdbs v0.2.3: for every new database
// name, ConnDbName opens an admin pool on the harness's default database to run CREATE DATABASE and
// never closes it, so each test leaks one idle connection until the server's limit (100) is
// reached. It terminates the idle backends on that default database, using gdb (a connection to
// the test's own database). Delete it once testdbs is at v0.2.4 or later, which closes the pool.
func freeLeakedPostgresConns(t *testing.T, gdb *gorm.DB) {
	t.Helper()
	if err := gdb.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = ? AND state = 'idle' AND pid <> pg_backend_pid()`, harnessDefaultDb).Error; err != nil {
		t.Fatalf("terminate leaked harness connections: %v", err)
	}
}

var errRollback = errors.New("rollback")

type relRow struct {
	AncestorID   uint
	DescendantID uint
	Depth        int
}

// closureRows returns the tenant's closure rows ordered by (ancestor_id, descendant_id, depth).
func closureRows(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, tenant string) []relRow {
	t.Helper()
	var rows []relRow
	if err := gdb.Table(ct.ClosureTableName()).
		Select("ancestor_id, descendant_id, depth").
		Where("tenant = ?", tenant).
		Order("ancestor_id, descendant_id, depth").
		Scan(&rows).Error; err != nil {
		t.Fatalf("read closure rows: %v", err)
	}
	return rows
}

type tenantCounts struct{ Nodes, Closure, Meta, Lock int64 }

// countTenant counts the tenant's rows in the node, closure, meta and lock tables.
func countTenant(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, tenant string) tenantCounts {
	t.Helper()
	count := func(tbl string) int64 {
		return countRaw(t, gdb, "SELECT COUNT(*) FROM "+tbl+" WHERE tenant = ?", tenant)
	}
	return tenantCounts{
		Nodes:   count(ct.NodeTableName()),
		Closure: count(ct.ClosureTableName()),
		Meta:    count("closure_tree_meta_" + ct.NodeTableName()),
		Lock:    count("closure_tree_lock_" + ct.NodeTableName()),
	}
}

// namedClosure returns the tenant's closure rows as "ancestorName>descendantName@depth", sorted.
// The virtual root is written "0". It reads the "name" column of the node table, so two tenants
// with different node ids can be compared.
func namedClosure(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, tenant string) []string {
	t.Helper()
	var nodes []struct {
		NodeId uint
		Name   string
	}
	if err := gdb.Table(ct.NodeTableName()).Select("node_id, name").
		Where("tenant = ?", tenant).Scan(&nodes).Error; err != nil {
		t.Fatalf("read node names: %v", err)
	}
	names := map[uint]string{0: "0"}
	for _, n := range nodes {
		names[n.NodeId] = n.Name
	}
	name := func(id uint) string {
		if n, ok := names[id]; ok {
			return n
		}
		return "missing#" + strconv.FormatUint(uint64(id), 10)
	}
	rows := closureRows(t, gdb, ct, tenant)
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%s>%s@%d", name(r.AncestorID), name(r.DescendantID), r.Depth))
	}
	sort.Strings(out)
	return out
}

// assertConsistent fails the test when the tenant has a node without its depth-0 self row, a
// closure row naming a node that does not exist, or a meta row whose parent_id is a non-zero id
// of a node that does not exist.
func assertConsistent(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, tenant string) {
	t.Helper()
	nodes, rel := ct.NodeTableName(), ct.ClosureTableName()
	meta := "closure_tree_meta_" + nodes
	if n := countRaw(t, gdb, fmt.Sprintf(`SELECT COUNT(*) FROM %s n WHERE n.tenant = ? AND NOT EXISTS (
		SELECT 1 FROM %s r WHERE r.tenant = n.tenant AND r.ancestor_id = n.node_id
		AND r.descendant_id = n.node_id AND r.depth = 0)`, nodes, rel), tenant); n != 0 {
		t.Errorf("tenant %s: %d nodes without a depth-0 self row", tenant, n)
	}
	if n := countRaw(t, gdb, fmt.Sprintf(`SELECT COUNT(*) FROM %s r WHERE r.tenant = ? AND (
		(r.ancestor_id <> 0 AND NOT EXISTS (SELECT 1 FROM %s n WHERE n.tenant = r.tenant AND n.node_id = r.ancestor_id))
		OR NOT EXISTS (SELECT 1 FROM %s n WHERE n.tenant = r.tenant AND n.node_id = r.descendant_id))`,
		rel, nodes, nodes), tenant); n != 0 {
		t.Errorf("tenant %s: %d closure rows naming a missing node", tenant, n)
	}
	if n := countRaw(t, gdb, fmt.Sprintf(`SELECT COUNT(*) FROM %s m WHERE m.tenant = ? AND m.parent_id <> 0
		AND NOT EXISTS (SELECT 1 FROM %s n WHERE n.tenant = m.tenant AND n.node_id = m.parent_id)`,
		meta, nodes), tenant); n != 0 {
		t.Errorf("tenant %s: %d meta rows naming a missing parent", tenant, n)
	}
}

// txSequence adds a (root), b (under a) and c (root), moves b under c and deletes a, all on tr.
func txSequence(ctx context.Context, tr *closuretree.Tree, tenant string) error {
	a, b, c := &TestPayload{Name: "a"}, &TestPayload{Name: "b"}, &TestPayload{Name: "c"}
	if err := tr.Add(ctx, a, nil, nil, tenant); err != nil {
		return err
	}
	if err := tr.Add(ctx, b, up(a.NodeId), nil, tenant); err != nil {
		return err
	}
	if err := tr.Add(ctx, c, nil, nil, tenant); err != nil {
		return err
	}
	if err := tr.Update(ctx, b.NodeId, nil, up(c.NodeId), nil, tenant); err != nil {
		return err
	}
	return tr.DeleteRecurse(ctx, a.NodeId, tenant)
}

// raceConflictingMoves races two opposing re-parent operations ("move A under B" and "move B
// under A") on ct, iterations times, each pair on two fresh root nodes of tenant1. Transient
// lock errors are retried. It fails the test on any other move error, and when A and B end up
// mutual ancestors (a cycle).
func raceConflictingMoves(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, iterations int) {
	t.Helper()
	ctx := context.Background()
	rel := ct.ClosureTableName()

	move := func(id, under uint) error {
		var e error
		for attempt := 0; attempt < 50; attempt++ {
			e = ct.Update(ctx, id, nil, up(under), nil, tenant1)
			if e == nil || errors.Is(e, closuretree.ErrInvalidMove) || !isTransientDBErr(e) {
				return e
			}
			runtime.Gosched()
			time.Sleep(time.Millisecond)
		}
		return e
	}

	for i := 0; i < iterations; i++ {
		a := &TestPayload{Name: "A"}
		if err := ct.Add(ctx, a, nil, nil, tenant1); err != nil {
			t.Fatal(err)
		}
		b := &TestPayload{Name: "B"}
		if err := ct.Add(ctx, b, nil, nil, tenant1); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() { defer wg.Done(); <-start; errs[0] = move(a.NodeId, b.NodeId) }()
		go func() { defer wg.Done(); <-start; errs[1] = move(b.NodeId, a.NodeId) }()
		close(start)
		wg.Wait()

		for _, e := range errs {
			if e != nil && !errors.Is(e, closuretree.ErrInvalidMove) {
				t.Fatalf("iteration %d: unexpected move error: %v", i, e)
			}
		}

		if relCount(t, gdb, rel, b.NodeId, a.NodeId, tenant1) > 0 &&
			relCount(t, gdb, rel, a.NodeId, b.NodeId, tenant1) > 0 {
			t.Fatalf("iteration %d: cycle formed — A(%d) and B(%d) are mutual ancestors", i, a.NodeId, b.NodeId)
		}
	}
}

// childNames returns the names of the direct children of parent, in the order Descendants returns them.
func childNames(t *testing.T, ct *closuretree.Tree, parent uint, tenant string) []string {
	t.Helper()
	var kids []TestPayload
	if err := ct.Descendants(context.Background(), parent, 1, tenant, &kids); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(kids))
	for _, k := range kids {
		names = append(names, k.Name)
	}
	return names
}

// seq returns the ids from..to, both inclusive.
func seq(from, to uint) []uint {
	ids := make([]uint, 0, to-from+1)
	for id := from; id <= to; id++ {
		ids = append(ids, id)
	}
	return ids
}
