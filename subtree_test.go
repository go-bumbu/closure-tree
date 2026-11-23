package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// namesIn returns, sorted, the names of the nodes whose id the subquery selects, read with a query
// on the node table: WHERE node_id IN (?).
func namesIn(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, sub *gorm.DB) []string {
	t.Helper()
	names := []string{}
	if err := gdb.Table(ct.NodeTableName()).Where("node_id IN (?)", sub).Order("name").Pluck("name", &names).Error; err != nil {
		t.Fatalf("read the nodes of the subquery: %v", err)
	}
	return names
}

// subtreeItem is a model of the caller with a many2many relation to the nodes.
type subtreeItem struct {
	ID    uint `gorm:"primaryKey"`
	Name  string
	Nodes []*filterNode `gorm:"many2many:subtree_item_nodes;"`
}

// seedSubtreeItems migrates subtreeItem and creates five items: i1 on a1 and x1 (two nodes of a's
// subtree), i2 on x21, i3 on b1, i4 on a, i5 on nothing.
func seedSubtreeItems(t *testing.T, gdb *gorm.DB, id map[string]uint) {
	t.Helper()
	if err := gdb.Migrator().DropTable("subtree_item_nodes", &subtreeItem{}); err != nil {
		t.Fatal(err)
	}
	if err := gdb.AutoMigrate(&subtreeItem{}); err != nil {
		t.Fatal(err)
	}
	attach := map[string][]string{"i1": {"a1", "x1"}, "i2": {"x21"}, "i3": {"b1"}, "i4": {"a"}, "i5": nil}
	for _, name := range []string{"i1", "i2", "i3", "i4", "i5"} {
		item := &subtreeItem{Name: name}
		if err := gdb.Create(item).Error; err != nil {
			t.Fatal(err)
		}
		for _, node := range attach[name] {
			if err := gdb.Model(item).Association("Nodes").Append(&filterNode{Node: closuretree.Node{NodeId: id[node], Tenant: tenantF}}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// subtreeChecker builds subtrees of the fixture by node name and compares the names they select.
type subtreeChecker struct {
	t   *testing.T
	gdb *gorm.DB
	ct  *closuretree.Tree
	id  map[string]uint
}

// sub builds the slice form; the name "" is the virtual root.
func (c subtreeChecker) sub(ancestors []string, opts ...closuretree.ReadOption) *gorm.DB {
	c.t.Helper()
	ids := make([]uint, len(ancestors))
	for i, name := range ancestors {
		ids[i] = c.id[name] // id[""] is 0, the virtual root
	}
	s, err := c.ct.SubtreeIDs(tenantF, ids, opts...)
	if err != nil {
		c.t.Fatal(err)
	}
	return s
}

func (c subtreeChecker) check(what string, got *gorm.DB, want ...string) {
	c.t.Helper()
	if names := namesIn(c.t, c.gdb, c.ct, got); !slices.Equal(want, names) {
		c.t.Errorf("%s: want %v, got %v", what, want, names)
	}
}

func TestSubtreeIDs(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			c := subtreeChecker{t, gdb, ct, id}
			a := []string{"a"}
			c.check("one ancestor, itself included", c.sub(a), "a", "a1", "x", "x1", "x2", "x21")
			c.check("several ancestors", c.sub([]string{"x2", "b"}), "b", "b1", "x2", "x21")
			c.check("overlapping and duplicate ancestors", c.sub([]string{"a", "x", "a"}), "a", "a1", "x", "x1", "x2", "x21")
			c.check("the virtual root is the whole tenant", c.sub([]string{""}), "a", "a1", "b", "b1", "x", "x1", "x2", "x21")
			c.check("this node only", c.sub(a, closuretree.DepthRange(0, 0)), "a")
			c.check("direct children", c.sub(a, closuretree.DepthRange(1, 1)), "a1", "x")
			c.check("strictly below", c.sub(a, closuretree.DepthRange(1, -1)), "a1", "x", "x1", "x2", "x21")
			c.check("the default range", c.sub(a, closuretree.DepthRange(0, -1)), "a", "a1", "x", "x1", "x2", "x21")
		})
	}
}

func TestSubtreeIDsFiltered(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			c := subtreeChecker{t, gdb, ct, id}
			a := []string{"a"}
			for name, f := range map[string]closuretree.ReadOption{
				"IsNull":       closuretree.Filter(closuretree.IsNull("archived_at")),
				"Eq with NULL": closuretree.Filter(closuretree.Eq("note", "keep")),
			} {
				c.check(name+": prune", c.sub(a, f), "a", "a1")
				c.check(name+": prune from the virtual root", c.sub([]string{""}, f), "a", "a1", "b", "b1")
				c.check(name+": per node", c.sub(a, f, closuretree.PerNode()), "a", "a1", "x1", "x2", "x21")
				c.check(name+": an ancestor that does not match", c.sub([]string{"x"}, f))
				// pruning is relative to each requested ancestor: x2 is requested itself
				c.check(name+": prune with two ancestors", c.sub([]string{"a", "x2"}, f), "a", "a1", "x2", "x21")
				c.check(name+": prune strictly below", c.sub(a, f, closuretree.DepthRange(1, -1)), "a1")
				// the requested ancestor must match although the range leaves it out
				c.check(name+": an ancestor that does not match, strictly below", c.sub([]string{"x"}, f, closuretree.DepthRange(1, -1)))
			}
		})
	}
}

func TestSubtreeIDsReuse(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			c := subtreeChecker{t, gdb, ct, id}
			if err := ct.Add(context.Background(), &filterNode{Name: "o1", Kind: "doc"}, nil, nil, "other"); err != nil {
				t.Fatal(err)
			}
			other, err := ct.SubtreeIDs("other", []uint{id["a"], 0})
			if err != nil {
				t.Fatal(err)
			}
			c.check("another tenant's nodes never appear", c.sub([]string{""}), "a", "a1", "b", "b1", "x", "x1", "x2", "x21")
			c.check("only the nodes of the tenant", other, "o1")

			// one subquery value used in two statements, and twice in one
			once := c.sub([]string{"a"}, closuretree.DepthRange(1, 1))
			c.check("first use", once, "a1", "x")
			var n int64
			err = gdb.Table(ct.NodeTableName()).Where("node_id IN (?) OR node_id IN (?)", once, once).Count(&n).Error
			if err != nil || n != 2 {
				t.Errorf("the same subquery twice in one statement: %d, %v", n, err)
			}
		})
	}
}

// checkRerun embeds a subquery, runs it directly, embeds it again and runs it directly again. The
// two embeddings must give the same non-empty rows, and so must the two direct runs.
func checkRerun(t *testing.T, what string, embed, direct func() ([]string, error)) {
	t.Helper()
	var got [4][]string
	for i, run := range []func() ([]string, error){embed, direct, embed, direct} {
		rows, err := run()
		if err != nil {
			t.Errorf("%s, run %d: %v", what, i+1, err)
		}
		got[i] = rows
	}
	if len(got[0]) == 0 || !slices.Equal(got[0], got[2]) {
		t.Errorf("%s: embedded before a direct run %v, after it %v", what, got[0], got[2])
	}
	if len(got[1]) == 0 || !slices.Equal(got[1], got[3]) {
		t.Errorf("%s: first direct run %v, second %v", what, got[1], got[3])
	}
}

// A subquery value stays usable after it was executed directly with Scan.
func TestSubqueriesRunDirectly(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			silent := gdb.Session(&gorm.Session{Logger: logger.Discard})
			sub, err := ct.SubtreeIDs(tenantF, []uint{id["x"]})
			if err != nil {
				t.Fatal(err)
			}
			checkRerun(t, "SubtreeIDs", func() ([]string, error) {
				names := []string{}
				err := silent.Table(ct.NodeTableName()).Where("node_id IN (?)", sub).Order("name").Pluck("name", &names).Error
				return names, err
			}, func() ([]string, error) {
				var ids []uint
				err := sub.Scan(&ids).Error
				slices.Sort(ids)
				return strings.Fields(fmt.Sprint(ids)), err
			})
			pairs, err := ct.ClosurePairs(tenantF, closuretree.ForAncestors(id["x"]))
			if err != nil {
				t.Fatal(err)
			}
			checkRerun(t, "ClosurePairs", func() ([]string, error) {
				var n int64
				err := silent.Table("(?) AS cp", pairs).Count(&n).Error
				return []string{fmt.Sprint(n)}, err
			}, func() ([]string, error) {
				var rows []relRow
				err := pairs.Scan(&rows).Error
				out := make([]string, 0, len(rows))
				for _, r := range rows {
					out = append(out, fmt.Sprint(r))
				}
				slices.Sort(out)
				return out, err
			})
		})
	}
}

// blobNode has a byte-slice column, to filter on byte-slice values.
type blobNode struct {
	closuretree.Node
	Name string
	Tag  []byte
}

// blobTree returns a migrated Tree of blobNode with the root nodes p, q and r, whose tags are
// "abc", "zz" and "mm", and the node id of p.
func blobTree(t *testing.T, db testdbs.TargetDb) (*gorm.DB, *closuretree.Tree, uint) {
	t.Helper()
	gdb := connAndClose(t, db)
	dropTreeTables(gdb, blobNode{})
	ct, err := newTestTree(gdb, blobNode{})
	if err != nil {
		t.Fatal(err)
	}
	var p uint
	for _, n := range []*blobNode{{Name: "p", Tag: []byte("abc")}, {Name: "q", Tag: []byte("zz")}, {Name: "r", Tag: []byte("mm")}} {
		if err := ct.Add(context.Background(), n, nil, nil, tenantF); err != nil {
			t.Fatal(err)
		}
		if n.Name == "p" {
			p = n.NodeId
		}
	}
	return gdb, ct, p
}

// blobSelections returns the node ids a filter selects through DescendantIds, through a
// SubtreeIDs value embedded once and embedded twice in one query, and through ClosurePairs.
func blobSelections(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree, f closuretree.ReadOption) map[string][]uint {
	t.Helper()
	silent := gdb.Session(&gorm.Session{Logger: logger.Discard})
	got := map[string][]uint{}
	ids, err := ct.DescendantIds(context.Background(), 0, 0, tenantF, f)
	if err != nil {
		t.Errorf("DescendantIds: %v", err)
	}
	got["DescendantIds"] = ids
	sub, err := ct.SubtreeIDs(tenantF, []uint{0}, f)
	if err != nil {
		t.Fatal(err)
	}
	pluck := func(what string, q *gorm.DB, column string) {
		ids := []uint{}
		if err := q.Order(column).Pluck(column, &ids).Error; err != nil {
			t.Errorf("%s: %v", what, err)
		}
		got[what] = ids
	}
	nodes := ct.NodeTableName()
	pluck("SubtreeIDs", silent.Table(nodes).Where("node_id IN (?)", sub), "node_id")
	pluck("SubtreeIDs twice", silent.Table(nodes).Where("node_id IN (?) AND node_id IN (?)", sub, sub), "node_id")
	pairs, err := ct.ClosurePairs(tenantF, f)
	if err != nil {
		t.Fatal(err)
	}
	pluck("ClosurePairs", silent.Table("(?) AS cp", pairs), "descendant_id")
	return got
}

// A byte-slice value in a filter is bound as one value, also inside an embedded subquery.
func TestFilterOnBytes(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, p := blobTree(t, db)
			for what, f := range map[string]closuretree.ReadOption{
				"In": closuretree.Filter(closuretree.In("tag", []byte("abc"), []byte("nope"))),
				"Eq": closuretree.Filter(closuretree.Eq("tag", []byte("abc"))),
			} {
				for read, ids := range blobSelections(t, gdb, ct, f) {
					if !slices.Equal([]uint{p}, ids) {
						t.Errorf("%s through %s: want [%d], got %v", what, read, p, ids)
					}
				}
			}
		})
	}
}

// errCase is the outcome of a call and the error it must return.
type errCase struct {
	what string
	sub  *gorm.DB
	err  error
	want error
}

func (e errCase) verify(t *testing.T) {
	t.Helper()
	if e.sub != nil {
		t.Errorf("%s: want a nil subquery with the error", e.what)
	}
	if !errors.Is(e.err, e.want) {
		t.Errorf("%s: want %v, got %v", e.what, e.want, e.err)
	}
}

func TestSubtreeIDsErrors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, id := filterTree(t, db)
			a := []uint{id["a"]}
			var cases []errCase
			add := func(what string, want error, sub *gorm.DB, err error) {
				cases = append(cases, errCase{what, sub, err, want})
			}
			s, err := ct.SubtreeIDs("", a)
			add("empty tenant", closuretree.ErrEmptyTenant, s, err)
			s, err = ct.SubtreeIDs(tenantF, nil)
			add("nil ancestors", closuretree.ErrNoAncestors, s, err)
			s, err = ct.SubtreeIDs(tenantF, []uint{})
			add("empty ancestors", closuretree.ErrNoAncestors, s, err)
			s, err = ct.SubtreeIDs(tenantF, a, closuretree.Filter(closuretree.Eq("nope", 1)))
			add("unknown column", closuretree.ErrUnknownColumn, s, err)
			s, err = ct.SubtreeIDs(tenantF, a, closuretree.DepthRange(2, 1))
			add("invalid range", closuretree.ErrInvalidDepthRange, s, err)
			for _, e := range cases {
				e.verify(t)
			}
		})
	}
}

func TestSubtreeIDsOfErrors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, _ := filterTree(t, db)
			ancestors := gdb.Table(ct.NodeTableName()).Select("node_id").Where("name = ?", "a")
			var cases []errCase
			add := func(what string, want error, sub *gorm.DB, err error) {
				cases = append(cases, errCase{what, sub, err, want})
			}
			s, err := ct.SubtreeIDsOf("", ancestors)
			add("SubtreeIDsOf, empty tenant", closuretree.ErrEmptyTenant, s, err)
			s, err = ct.SubtreeIDsOf(tenantF, nil)
			add("SubtreeIDsOf, nil ancestors", closuretree.ErrNoAncestors, s, err)
			// a subquery that already failed must not be rendered: gorm would render it as nothing
			errBroken := errors.New("broken")
			broken := gdb.Session(&gorm.Session{Logger: logger.Discard}).Table(ct.NodeTableName()).Select("node_id")
			_ = broken.AddError(errBroken)
			s, err = ct.SubtreeIDsOf(tenantF, broken)
			add("SubtreeIDsOf, ancestors with an error", errBroken, s, err)
			// no Error is set, but gorm cannot render a model it cannot parse, so it would embed nothing
			unparsable := gdb.Session(&gorm.Session{Logger: logger.Discard}).Model(make(chan int)).Select("node_id")
			if unparsable.Error != nil {
				t.Fatalf("the fixture must not carry an error yet: %v", unparsable.Error)
			}
			s, err = ct.SubtreeIDsOf(tenantF, unparsable)
			want := err // any error will do; a nil one fails the case below
			if want == nil {
				want = errors.New("an error")
			}
			add("SubtreeIDsOf, ancestors that cannot be rendered", want, s, err)
			s, err = ct.SubtreeIDsOf(tenantF, ancestors, closuretree.DepthRange(2, 1))
			add("SubtreeIDsOf, invalid range", closuretree.ErrInvalidDepthRange, s, err)
			for _, e := range cases {
				e.verify(t)
			}
		})
	}
}

// The subquery form gives the same nodes as the slice form.
func TestSubtreeIDsOf(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			active := closuretree.Filter(closuretree.IsNull("archived_at"))
			byName := func() *gorm.DB {
				return gdb.Table(ct.NodeTableName()).Select("node_id").Where("name IN ?", []string{"a", "x2"})
			}
			byModel := func() *gorm.DB {
				return gdb.Model(&filterNode{}).Select("node_id").Where("name IN ?", []string{"a", "x2"})
			}
			byRaw := func() *gorm.DB {
				return gdb.Raw("SELECT node_id FROM "+ct.NodeTableName()+" WHERE name IN ?", []string{"a", "x2"})
			}
			for what, opts := range map[string][]closuretree.ReadOption{
				"no options":     nil,
				"prune":          {active},
				"per node":       {active, closuretree.PerNode()},
				"strictly below": {closuretree.DepthRange(1, -1)},
			} {
				bySlice, err := ct.SubtreeIDs(tenantF, []uint{id["a"], id["x2"]}, opts...)
				if err != nil {
					t.Fatal(err)
				}
				want := namesIn(t, gdb, ct, bySlice)
				for form, ancestors := range map[string]*gorm.DB{"Table": byName(), "Model": byModel(), "Raw": byRaw()} {
					bySub, err := ct.SubtreeIDsOf(tenantF, ancestors, opts...)
					if err != nil {
						t.Fatalf("%s, %s: %v", what, form, err)
					}
					if got := namesIn(t, gdb, ct, bySub); !slices.Equal(want, got) || len(want) == 0 {
						t.Errorf("%s, %s: slice form %v, subquery form %v", what, form, want, got)
					}
				}
			}
		})
	}
}

// An item attached to two nodes of one subtree is returned once through a semi-join, and the
// caller's ordering, limit, offset and count agree.
func TestSubtreeIDsSemiJoin(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			seedSubtreeItems(t, gdb, id)
			sub, err := ct.SubtreeIDs(tenantF, []uint{id["a"]})
			if err != nil {
				t.Fatal(err)
			}
			under := func() *gorm.DB {
				return gdb.Model(&subtreeItem{}).
					Where("id IN (SELECT subtree_item_id FROM subtree_item_nodes WHERE filter_node_node_id IN (?))", sub)
			}
			var total int64
			if err := under().Count(&total).Error; err != nil || total != 3 {
				t.Fatalf("count: %d, %v", total, err)
			}
			var page1, page2 []string
			if err := under().Order("name").Limit(2).Pluck("name", &page1).Error; err != nil {
				t.Fatal(err)
			}
			if err := under().Order("name").Limit(2).Offset(2).Pluck("name", &page2).Error; err != nil {
				t.Fatal(err)
			}
			if !slices.Equal([]string{"i1", "i2"}, page1) || !slices.Equal([]string{"i4"}, page2) {
				t.Errorf("pages: %v %v", page1, page2)
			}
		})
	}
}

// The subquery is rendered into the caller's statement, so inside a transaction it sees that
// transaction's uncommitted rows, also when it was built on a Tree that is not bound to it.
func TestSubtreeIDsInsideATransaction(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			ctx := context.Background()
			sub, err := ct.SubtreeIDs(tenantF, []uint{id["a"]})
			if err != nil {
				t.Fatal(err)
			}
			err = gdb.Transaction(func(tx *gorm.DB) error {
				if err := ct.WithTx(tx).Add(ctx, &filterNode{Name: "new", Kind: "doc"}, up(id["a1"]), nil, tenantF); err != nil {
					return err
				}
				if names := namesIn(t, tx, ct, sub); !slices.Contains(names, "new") {
					t.Errorf("the uncommitted node is missing: %v", names)
				}
				return errRollback
			})
			if !errors.Is(err, errRollback) {
				t.Fatal(err)
			}
			if names := namesIn(t, gdb, ct, sub); slices.Contains(names, "new") {
				t.Errorf("the rolled-back node is still there: %v", names)
			}
		})
	}
}

// MySQL rejects a subquery that reads the node table inside an UPDATE or DELETE on the node table
// (error 1093). Wrapped in a derived table it works on every database.
func TestSubtreeIDsInAWriteOnTheNodeTable(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			silent := gdb.Session(&gorm.Session{Logger: logger.Discard})
			nodes := ct.NodeTableName()
			filtered, err := ct.SubtreeIDs(tenantF, []uint{id["a"]}, closuretree.Filter(closuretree.IsNull("archived_at")))
			if err != nil {
				t.Fatal(err)
			}
			direct := silent.Table(nodes).Where("node_id IN (?)", filtered).Update("kind", "direct").Error
			if isMySQL := db.DbType() == testdbs.DBTypeMysql; isMySQL != (direct != nil) {
				t.Errorf("a filtered subquery used directly: mysql=%v, error %v", isMySQL, direct)
			} else if isMySQL && !strings.Contains(direct.Error(), "1093") {
				t.Errorf("want MySQL error 1093, got %v", direct)
			}
			res := gdb.Table(nodes).Where("node_id IN (SELECT descendant_id FROM (?) AS sub)", filtered).Update("kind", "wrapped")
			if res.Error != nil || res.RowsAffected != 2 { // a and a1
				t.Errorf("wrapped in a derived table: %d rows, %v", res.RowsAffected, res.Error)
			}
			// without a filter the subquery reads only the closure table and needs no wrapping
			plain, err := ct.SubtreeIDs(tenantF, []uint{id["b"]})
			if err != nil {
				t.Fatal(err)
			}
			res = gdb.Table(nodes).Where("node_id IN (?)", plain).Update("kind", "plain")
			if res.Error != nil || res.RowsAffected != 2 { // b and b1
				t.Errorf("an unfiltered subquery used directly: %d rows, %v", res.RowsAffected, res.Error)
			}
		})
	}
}

// The subtree of node 5 has 1365 nodes; 85 of them are the non-matching node 85 and its subtree.
func TestSubtreeIDsOnLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct := largeTree(t, db)
			docs := closuretree.Filter(closuretree.Eq("kind", "doc"))
			count := func(what string, want int64, opts ...closuretree.ReadOption) {
				t.Helper()
				sub, err := ct.SubtreeIDs(tenantF, []uint{largeRealParent}, opts...)
				if err != nil {
					t.Fatal(err)
				}
				within(t, what, func() error {
					var n int64
					err := gdb.Table(ct.NodeTableName()).Where("node_id IN (?)", sub).Count(&n).Error
					if err == nil && n != want {
						t.Errorf("%s: want %d, got %d", what, want, n)
					}
					return err
				})
			}
			count("unfiltered", largeRealSubtree)
			count("prune", largeRealSubtree-largeHiddenSubtree, docs)
			count("per node", largeRealSubtree-1, docs, closuretree.PerNode())
			count("direct children", 4, closuretree.DepthRange(1, 1))
		})
	}
}

// pairCounts counts the pairs per ancestor through a derived table, FROM (?) AS cp GROUP BY
// cp.ancestor_id, and returns the counts by ancestor name; the virtual root is named "root". The
// map is never nil, so it compares equal to an empty map literal.
func pairCounts(t *testing.T, gdb *gorm.DB, id map[string]uint, pairs *gorm.DB) map[string]int64 {
	t.Helper()
	names := map[uint]string{0: "root"}
	for name, nodeID := range id {
		names[nodeID] = name
	}
	var rows []struct {
		AncestorID uint
		N          int64
	}
	err := gdb.Table("(?) AS cp", pairs).Select("cp.ancestor_id AS ancestor_id, COUNT(*) AS n").
		Group("cp.ancestor_id").Scan(&rows).Error
	if err != nil {
		t.Fatalf("count the pairs: %v", err)
	}
	counts := map[string]int64{}
	for _, r := range rows {
		name, ok := names[r.AncestorID]
		if !ok {
			name = fmt.Sprintf("unknown id %d", r.AncestorID)
		}
		counts[name] = r.N
	}
	return counts
}

// pairChecker builds the pairs of the fixture tenant and compares the counts per ancestor.
type pairChecker struct {
	t   *testing.T
	gdb *gorm.DB
	ct  *closuretree.Tree
	id  map[string]uint
}

func (c pairChecker) check(what string, want map[string]int64, opts ...closuretree.ReadOption) {
	c.t.Helper()
	pairs, err := c.ct.ClosurePairs(tenantF, opts...)
	if err != nil {
		c.t.Fatalf("%s: %v", what, err)
	}
	if diff := cmp.Diff(want, pairCounts(c.t, c.gdb, c.id, pairs)); diff != "" {
		c.t.Errorf("%s (-want +got):\n%s", what, diff)
	}
}

func TestClosurePairs(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			c := pairChecker{t, gdb, ct, id}
			// every node is paired with itself (depth 0) and with each of its descendants
			all := map[string]int64{"a": 6, "a1": 1, "x": 4, "x1": 1, "x2": 2, "x21": 1, "b": 2, "b1": 1}
			c.check("no options", all)
			withRoot := maps.Clone(all)
			withRoot["root"] = 8
			c.check("with the virtual root", withRoot, closuretree.IncludeVirtualRoot())
			c.check("strictly below", map[string]int64{"a": 5, "x": 3, "x2": 1, "b": 1}, closuretree.DepthRange(1, -1))
			c.check("direct children", map[string]int64{"a": 2, "x": 2, "x2": 1, "b": 1}, closuretree.DepthRange(1, 1))
			c.check("two ancestors", map[string]int64{"a": 6, "b": 2}, closuretree.ForAncestors(id["a"], id["b"]))
			c.check("an empty ancestor set selects nothing", map[string]int64{}, closuretree.ForAncestors())
			ids := []uint{id["a"]}
			opt := closuretree.ForAncestors(ids...)
			ids[0] = id["b"] // the option keeps its own copy
			c.check("the ids are copied", map[string]int64{"a": 6}, opt)
			c.check("naming the virtual root includes it", map[string]int64{"a": 6, "root": 8},
				closuretree.ForAncestors(id["a"], 0))
			c.check("the virtual root named and asked for", map[string]int64{"a": 6, "root": 8},
				closuretree.ForAncestors(id["a"], 0), closuretree.IncludeVirtualRoot())
		})
	}
}

func TestClosurePairsFiltered(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			c := pairChecker{t, gdb, ct, id}
			// with a filter the descendant and the ancestor must match; nodes in between are not
			// checked, so a still counts x1, x2 and x21 below the non-matching x
			active := closuretree.Filter(closuretree.IsNull("archived_at"))
			filtered := map[string]int64{"a": 5, "a1": 1, "x1": 1, "x2": 2, "x21": 1, "b": 2, "b1": 1}
			c.check("filter", filtered, active)
			c.check("a NULL comparison counts as no match", filtered, closuretree.Filter(closuretree.Eq("note", "keep")))
			c.check("PerNode changes nothing", filtered, active, closuretree.PerNode())
			filteredRoot := maps.Clone(filtered)
			filteredRoot["root"] = 7 // the virtual root always matches
			c.check("filter with the virtual root", filteredRoot, active, closuretree.IncludeVirtualRoot())
		})
	}
}

func TestClosurePairsErrors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			_, ct, _ := filterTree(t, db)
			var cases []errCase
			add := func(what string, want error, sub *gorm.DB, err error) {
				cases = append(cases, errCase{what, sub, err, want})
			}
			p, err := ct.ClosurePairs("")
			add("empty tenant", closuretree.ErrEmptyTenant, p, err)
			p, err = ct.ClosurePairs(tenantF, closuretree.Filter(closuretree.Eq("nope", 1)))
			add("unknown column", closuretree.ErrUnknownColumn, p, err)
			p, err = ct.ClosurePairs(tenantF, closuretree.DepthRange(-1, 2))
			add("invalid range", closuretree.ErrInvalidDepthRange, p, err)
			for _, e := range cases {
				e.verify(t)
			}
		})
	}
}

// A node of another tenant is paired only in that tenant, and the pairs of the fixture tenant do
// not change.
func TestClosurePairsTenants(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			item := &filterNode{Name: "o1", Kind: "doc"}
			if err := ct.Add(context.Background(), item, nil, nil, "other"); err != nil {
				t.Fatal(err)
			}
			id["o1"] = item.NodeId
			c := pairChecker{t, gdb, ct, id}
			c.check("the fixture tenant keeps its pairs", map[string]int64{
				"a": 6, "a1": 1, "x": 4, "x1": 1, "x2": 2, "x21": 1, "b": 2, "b1": 1, "root": 8,
			}, closuretree.IncludeVirtualRoot())
			other, err := ct.ClosurePairs("other", closuretree.IncludeVirtualRoot())
			if err != nil {
				t.Fatal(err)
			}
			// the node with itself, and with the virtual root
			want := map[string]int64{"o1": 1, "root": 1}
			if diff := cmp.Diff(want, pairCounts(t, gdb, id, other)); diff != "" {
				t.Errorf("pairs of the other tenant (-want +got):\n%s", diff)
			}
			checkFilteredPairsOfTwoTenants(t, c)
		})
	}
}

// checkFilteredPairsOfTwoTenants checks that with a filter the pairs of the fixture tenant are
// those without the other tenant, and that the other tenant's matching node o1 has its own pair only.
func checkFilteredPairsOfTwoTenants(t *testing.T, c pairChecker) {
	t.Helper()
	active := closuretree.Filter(closuretree.IsNull("archived_at"))
	c.check("filtered, the fixture tenant", map[string]int64{"a": 5, "a1": 1, "x1": 1, "x2": 2, "x21": 1, "b": 2, "b1": 1}, active)
	other, err := c.ct.ClosurePairs("other", active)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(map[string]int64{"o1": 1}, pairCounts(t, c.gdb, c.id, other)); diff != "" {
		t.Errorf("filtered pairs of the other tenant (-want +got):\n%s", diff)
	}
}

// MySQL rejects a LIMIT inside an IN subquery (error 1235); paging belongs in the outer query.
func TestSubtreeIDsLimitInsideIn(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			if db.DbType() != testdbs.DBTypeMysql {
				t.Skip("MySQL only")
			}
			gdb, ct, id := filterTree(t, db)
			sub, err := ct.SubtreeIDs(tenantF, []uint{id["a"]})
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			err = gdb.Session(&gorm.Session{Logger: logger.Discard}).Table(ct.NodeTableName()).
				Where("node_id IN (SELECT descendant_id FROM (?) AS s LIMIT 2)", sub).Pluck("name", &names).Error
			if err == nil || !strings.Contains(err.Error(), "1235") {
				t.Errorf("want MySQL error 1235, got %v", err)
			}
		})
	}
}

// The three output columns, joined in a caller's query inside a transaction, see the uncommitted
// rows although the Tree is not bound to it.
func TestClosurePairsInsideATransaction(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			ctx := context.Background()
			pairs, err := ct.ClosurePairs(tenantF, closuretree.ForAncestors(id["x"]))
			if err != nil {
				t.Fatal(err)
			}
			err = gdb.Transaction(func(tx *gorm.DB) error {
				if err := ct.WithTx(tx).Add(ctx, &filterNode{Name: "new", Kind: "doc"}, up(id["x1"]), nil, tenantF); err != nil {
					return err
				}
				var rows []struct {
					AncestorID   uint
					DescendantID uint
					Depth        int
					Name         string
				}
				q := tx.Table(ct.NodeTableName()+" AS n").
					Select("cp.ancestor_id, cp.descendant_id, cp.depth, n.name").
					Joins("JOIN (?) AS cp ON cp.descendant_id = n.node_id", pairs).
					Order("cp.depth, n.name")
				if err := q.Scan(&rows).Error; err != nil {
					return err
				}
				// x at depth 0; x1 and x2 at depth 1; "new" and x21 at depth 2
				if len(rows) != 5 || rows[0].Name != "x" || rows[0].Depth != 0 || rows[3].Name != "new" || rows[3].Depth != 2 || rows[3].AncestorID != id["x"] {
					t.Errorf("pairs of x with the uncommitted node: %+v", rows)
				}
				return errRollback
			})
			if !errors.Is(err, errRollback) {
				t.Fatal(err)
			}
		})
	}
}

// Node 5 has 1365 nodes in its subtree, node 21 has 341 (itself included in both), and the
// tenant has 56721 closure pairs (largeTreePairs).
func TestClosurePairsOnLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct := largeTree(t, db)
			counts := func(what string, want5, want21 int64, opts ...closuretree.ReadOption) {
				t.Helper()
				pairs, err := ct.ClosurePairs(tenantF, append(opts, closuretree.ForAncestors(5, 21))...)
				if err != nil {
					t.Fatal(err)
				}
				within(t, what, func() error {
					var rows []struct {
						AncestorID uint
						N          int64
					}
					err := gdb.Table(ct.NodeTableName()+" AS n").Select("cp.ancestor_id AS ancestor_id, COUNT(*) AS n").
						Joins("JOIN (?) AS cp ON cp.descendant_id = n.node_id", pairs).
						Group("cp.ancestor_id").Order("cp.ancestor_id").Scan(&rows).Error
					if err == nil && (len(rows) != 2 || rows[0].N != want5 || rows[1].N != want21) {
						t.Errorf("%s: %+v", what, rows)
					}
					return err
				})
			}
			counts("unfiltered", largeRealSubtree, 341)
			// node 85 does not match; its 84 descendants do, and nodes in between are not checked
			counts("filtered", 1364, 340, closuretree.Filter(closuretree.Eq("kind", "doc")))
			within(t, "every pair of the tenant", func() error {
				pairs, err := ct.ClosurePairs(tenantF)
				if err != nil {
					return err
				}
				// Each node is paired with itself and its ancestors (largeTreePairs).
				var n int64
				err = gdb.Table("(?) AS cp", pairs).Count(&n).Error
				if err == nil && n != largeTreePairs {
					t.Errorf("every pair of the tenant: %d", n)
				}
				return err
			})
		})
	}
}

// seedLargeJoinTable creates the join table large_item_nodes, with one row per node of the large
// tree (item i on node i), and an index on its node column.
func seedLargeJoinTable(t *testing.T, gdb *gorm.DB, ct *closuretree.Tree) {
	t.Helper()
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS large_item_nodes",
		"CREATE TABLE large_item_nodes (item_id BIGINT NOT NULL, node_id BIGINT NOT NULL, PRIMARY KEY (item_id, node_id))",
		// Without this index the join table is scanned once per closure row; on SQLite that took
		// seconds. The primary key leads with item_id and does not serve the join on node_id.
		"CREATE INDEX idx_large_item_nodes_node ON large_item_nodes (node_id)",
	} {
		if err := gdb.Exec(stmt).Error; err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := gdb.Exec("INSERT INTO large_item_nodes (item_id, node_id) SELECT node_id, node_id FROM "+
		ct.NodeTableName()+" WHERE tenant = ?", tenantF).Error; err != nil {
		t.Fatalf("fill the join table: %v", err)
	}
}

// Rows of the caller's join table counted per subtree through ClosurePairs: node 5 has 1365 items
// in its subtree, node 21 has 341.
func TestClosurePairsCountJoinedRows(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct := largeTree(t, db)
			seedLargeJoinTable(t, gdb, ct)
			counts := func(what string, want5, want21 int64, opts ...closuretree.ReadOption) {
				t.Helper()
				pairs, err := ct.ClosurePairs(tenantF, append(opts, closuretree.ForAncestors(5, 21))...)
				if err != nil {
					t.Fatal(err)
				}
				within(t, what, func() error {
					var rows []struct {
						AncestorID uint
						N          int64
					}
					err := gdb.Table("(?) AS cp", pairs).
						Select("cp.ancestor_id AS ancestor_id, COUNT(DISTINCT j.item_id) AS n").
						Joins("JOIN large_item_nodes AS j ON j.node_id = cp.descendant_id").
						Group("cp.ancestor_id").Order("cp.ancestor_id").Scan(&rows).Error
					if err == nil && (len(rows) != 2 || rows[0].N != want5 || rows[1].N != want21) {
						t.Errorf("%s: %+v", what, rows)
					}
					return err
				})
			}
			counts("items per subtree", 1365, 341)
			// node 85 does not match; its 84 descendants do
			counts("items per subtree, filtered", 1364, 340, closuretree.Filter(closuretree.Eq("kind", "doc")))
		})
	}
}
