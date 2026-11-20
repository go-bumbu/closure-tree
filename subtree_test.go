package closuretree_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

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
