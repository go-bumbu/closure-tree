package closuretree_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
)

// leafItem has no embedded Leaf. Of its two many2many relations only Nodes points to the node table.
type leafItem struct {
	ID      uint `gorm:"primaryKey"`
	Name    string
	Nodes   []*filterNode `gorm:"many2many:leaf_item_nodes;"`
	Related []*leafItem   `gorm:"many2many:leaf_item_related;"`
}

// twoRelItem has two many2many relations to the node table.
type twoRelItem struct {
	ID        uint `gorm:"primaryKey"`
	Name      string
	Primary   []*filterNode `gorm:"many2many:two_rel_primary;"`
	Secondary []*filterNode `gorm:"many2many:two_rel_secondary;"`
}

// fixtureNode returns a filterNode that names the node with the given id, for appending to a relation.
func fixtureNode(id uint) *filterNode {
	return &filterNode{Node: closuretree.Node{NodeId: id, Tenant: tenantF}}
}

// attach appends the nodes with the given ids to the relation field of item.
func attach(t *testing.T, gdb *gorm.DB, item any, field string, ids ...uint) {
	t.Helper()
	for _, id := range ids {
		if err := gdb.Model(item).Association(field).Append(fixtureNode(id)); err != nil {
			t.Fatal(err)
		}
	}
}

// seedLeafItems migrates leafItem and twoRelItem (dropping their tables first) and creates, in
// this order: "on-a" attached to a; "on-a1-x1" attached to a1 and x1; "on-x21" attached to x21;
// "on-b1" attached to b1; "loose" attached to nothing. And one twoRelItem "two" with Primary a1
// and Secondary b1.
func seedLeafItems(t *testing.T, gdb *gorm.DB, id map[string]uint) {
	t.Helper()
	if err := gdb.Migrator().DropTable("leaf_item_nodes", "leaf_item_related", &leafItem{},
		"two_rel_primary", "two_rel_secondary", &twoRelItem{}); err != nil {
		t.Fatal(err)
	}
	if err := gdb.AutoMigrate(&leafItem{}, &twoRelItem{}); err != nil {
		t.Fatal(err)
	}
	items := []struct {
		name  string
		nodes []string
	}{
		{"on-a", []string{"a"}},
		{"on-a1-x1", []string{"a1", "x1"}},
		{"on-x21", []string{"x21"}},
		{"on-b1", []string{"b1"}},
		{"loose", nil},
	}
	for _, it := range items {
		item := &leafItem{Name: it.name}
		if err := gdb.Create(item).Error; err != nil {
			t.Fatal(err)
		}
		for _, node := range it.nodes {
			attach(t, gdb, item, "Nodes", id[node])
		}
	}
	two := &twoRelItem{Name: "two"}
	if err := gdb.Create(two).Error; err != nil {
		t.Fatal(err)
	}
	attach(t, gdb, two, "Primary", id["a1"])
	attach(t, gdb, two, "Secondary", id["b1"])
}

// leafNames returns the names of the leafItems GetLeaves returns, in the order it returns them.
func leafNames(t *testing.T, ct *closuretree.Tree, parent uint, maxDepth int, opts ...closuretree.ReadOption) []string {
	t.Helper()
	var items []leafItem
	if err := ct.GetLeaves(context.Background(), &items, parent, maxDepth, tenantF, opts...); err != nil {
		t.Fatalf("GetLeaves: %v", err)
	}
	names := make([]string, 0, len(items))
	for _, it := range items {
		names = append(names, it.Name)
	}
	return names
}

func TestGetLeavesWithoutLeaf(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			ctx := context.Background()
			seedLeafItems(t, gdb, id)
			check := func(what string, got []string, want ...string) {
				t.Helper()
				if !slices.Equal(want, got) {
					t.Errorf("%s: want %v, got %v", what, want, got)
				}
			}
			// ordered by the primary key, which is the creation order; the item on two nodes comes once
			check("the parent itself is included", leafNames(t, ct, id["a"], 0), "on-a", "on-a1-x1", "on-x21")
			check("the whole forest", leafNames(t, ct, 0, 0), "on-a", "on-a1-x1", "on-x21", "on-b1")
			check("maxDepth 1: the parent and its children", leafNames(t, ct, id["a"], 1), "on-a", "on-a1-x1")
			check("DepthRange replaces maxDepth", leafNames(t, ct, id["a"], 1, closuretree.DepthRange(1, 1)), "on-a1-x1")
			check("this node only", leafNames(t, ct, id["a"], 0, closuretree.DepthRange(0, 0)), "on-a")
			// leafItem has no tenant column, so only the node side is tenant-scoped
			var none []leafItem
			if err := ct.GetLeaves(ctx, &none, id["a"], 0, "other"); err != nil || len(none) != 0 {
				t.Errorf("another tenant: %d, %v", len(none), err)
			}

			active := closuretree.Filter(closuretree.IsNull("archived_at"))
			check("the filter prunes the subtree of the non-matching node", leafNames(t, ct, id["a"], 0, active), "on-a", "on-a1-x1")
			check("per node", leafNames(t, ct, id["a"], 0, active, closuretree.PerNode()), "on-a", "on-a1-x1", "on-x21")
			ftree, err := ct.WithFilter(closuretree.IsNull("archived_at"))
			if err != nil {
				t.Fatal(err)
			}
			check("the default filter applies", leafNames(t, ftree, id["x"], 0))
			check("Unfiltered switches it off", leafNames(t, ftree, id["x"], 0, closuretree.Unfiltered()), "on-a1-x1", "on-x21")
		})
	}
}

func TestGetLeavesTargetsAndPreload(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			ctx := context.Background()
			seedLeafItems(t, gdb, id)
			var pointers []*leafItem
			if err := ct.GetLeaves(ctx, &pointers, id["a"], 0, tenantF); err != nil || len(pointers) != 3 {
				t.Fatalf("a []*T target: %d, %v", len(pointers), err)
			}
			if len(pointers[1].Nodes) != 2 {
				t.Errorf("the relation is preloaded by default: %+v", pointers[1])
			}
			var bare []leafItem
			if err := ct.GetLeaves(ctx, &bare, id["a"], 0, tenantF, closuretree.NoPreload()); err != nil || len(bare) != 3 {
				t.Fatalf("NoPreload: %d, %v", len(bare), err)
			}
			if len(bare[1].Nodes) != 0 {
				t.Errorf("NoPreload must leave the relation empty: %+v", bare[1])
			}

			var two []twoRelItem
			if err := ct.GetLeaves(ctx, &two, 0, 0, tenantF); !errors.Is(err, closuretree.ErrAmbiguousRelation) {
				t.Errorf("two relations to the node table: want ErrAmbiguousRelation, got %v", err)
			}
			if err := ct.GetLeaves(ctx, &two, id["b"], 0, tenantF, closuretree.Relation("Secondary")); err != nil || len(two) != 1 {
				t.Errorf("Relation(Secondary) under b: %d, %v", len(two), err)
			}
			two = nil
			if err := ct.GetLeaves(ctx, &two, id["b"], 0, tenantF, closuretree.Relation("Primary")); err != nil || len(two) != 0 {
				t.Errorf("Relation(Primary) under b: %d, %v", len(two), err)
			}
		})
	}
}

func TestGetLeavesErrors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			ctx := context.Background()
			seedLeafItems(t, gdb, id)
			cancelled, cancel := context.WithCancel(ctx)
			cancel() // a query would fail with context.Canceled
			var items []leafItem
			var words []string
			type noRelation struct {
				ID   uint
				Name string
			}
			var plain []noRelation
			errs := map[string]struct{ err, want error }{
				"empty tenant":        {ct.GetLeaves(cancelled, &items, 0, 0, ""), closuretree.ErrEmptyTenant},
				"nil target":          {ct.GetLeaves(cancelled, nil, 0, 0, tenantF), closuretree.ErrItemsNil},
				"not a pointer":       {ct.GetLeaves(cancelled, items, 0, 0, tenantF), closuretree.ErrItemsNotPointerToSlice},
				"no relation":         {ct.GetLeaves(cancelled, &plain, 0, 0, tenantF), closuretree.ErrLeafMissingM2M},
				"an unknown Relation": {ct.GetLeaves(cancelled, &items, 0, 0, tenantF, closuretree.Relation("Nope")), closuretree.ErrLeafMissingM2M},
				"a Relation that does not point to the nodes": {ct.GetLeaves(cancelled, &items, 0, 0, tenantF, closuretree.Relation("Related")), closuretree.ErrLeafMissingM2M},
				"unknown column": {ct.GetLeaves(cancelled, &items, 0, 0, tenantF, closuretree.Filter(closuretree.Eq("nope", 1))), closuretree.ErrUnknownColumn},
				"invalid range":  {ct.GetLeaves(cancelled, &items, 0, 0, tenantF, closuretree.DepthRange(2, 1)), closuretree.ErrInvalidDepthRange},
			}
			for what, e := range errs {
				if !errors.Is(e.err, e.want) {
					t.Errorf("%s: want %v, got %v", what, e.want, e.err)
				}
			}
			if err := ct.GetLeaves(cancelled, &words, 0, 0, tenantF); err == nil || errors.Is(err, context.Canceled) {
				t.Errorf("a slice of non-structs must be rejected before querying: %v", err)
			}
		})
	}
}

// tenantLeaf embeds Leaf, so its own tenant column is checked as well.
type tenantLeaf struct {
	closuretree.Leaf
	Name  string
	Nodes []*filterNode `gorm:"many2many:tenant_leaf_nodes;"`
}

// Two leaves are attached to the same node of tenantF; one of them carries another tenant.
func TestGetLeavesChecksTheTenantOfALeaf(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct, id := filterTree(t, db)
			ctx := context.Background()
			if err := gdb.Migrator().DropTable("tenant_leaf_nodes", &tenantLeaf{}); err != nil {
				t.Fatal(err)
			}
			if err := gdb.AutoMigrate(&tenantLeaf{}); err != nil {
				t.Fatal(err)
			}
			a1 := &filterNode{}
			a1.NodeId = id["a1"]
			for name, tenant := range map[string]string{"mine": tenantF, "foreign": "other"} {
				leaf := tenantLeaf{Leaf: closuretree.Leaf{Tenant: tenant}, Name: name}
				if err := gdb.Create(&leaf).Error; err != nil {
					t.Fatal(err)
				}
				if err := gdb.Model(&leaf).Association("Nodes").Append(a1); err != nil {
					t.Fatal(err)
				}
			}
			var leaves []tenantLeaf
			if err := ct.GetLeaves(ctx, &leaves, id["a"], 0, tenantF); err != nil || len(leaves) != 1 || leaves[0].Name != "mine" {
				t.Errorf("want only the leaf of the tenant, got %+v, %v", leaves, err)
			}
		})
	}
}

// largeItems is the number of items seedLargeLeafItems creates.
const largeItems = 200

// largeParentOf returns the parent of a node of the tree of buildLargeTree, where the children of
// node p are 4p-2 to 4p+1.
func largeParentOf(id uint) uint { return (id-2)/4 + 1 }

// seedLargeLeafItems drops and migrates the tables of leafItem and creates largeItems items, item i
// attached to node 5 + i*7 of the large tree. It returns how many items are attached to a node in
// the subtree of node 5 (largeRealParent) and of node 85 (largeHiddenRoot), computed from the
// tree's shape: an item under node 85 counts for node 5 as well.
func seedLargeLeafItems(t *testing.T, gdb *gorm.DB) (under5, under85 int) {
	t.Helper()
	if err := gdb.Migrator().DropTable("leaf_item_nodes", "leaf_item_related", &leafItem{}); err != nil {
		t.Fatal(err)
	}
	if err := gdb.AutoMigrate(&leafItem{}); err != nil {
		t.Fatal(err)
	}
	items := make([]*leafItem, largeItems)
	for i := range items {
		node := uint(largeRealParent + i*7)
		items[i] = &leafItem{Name: "item", Nodes: []*filterNode{fixtureNode(node)}}
		anc := node
		for anc != largeRealParent && anc != largeHiddenRoot && anc != 1 {
			anc = largeParentOf(anc)
		}
		switch anc {
		case largeHiddenRoot:
			under85++
			under5++
		case largeRealParent:
			under5++
		}
	}
	// Omit("Nodes.*") writes the join rows without upserting the nodes themselves
	if err := gdb.Omit("Nodes.*").CreateInBatches(items, 100).Error; err != nil {
		t.Fatal(err)
	}
	return under5, under85
}

// 200 items, each attached to one node of the large tree: node 5 + i*7 for item i. The subtree of
// node 5 holds the ids 5, 18 to 21, 70 to 85, 278 to 341 and 1110 to 1365 among them.
func TestGetLeavesOnLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("large tree")
	}
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb, ct := largeTree(t, db)
			ctx := context.Background()
			under5, under85 := seedLargeLeafItems(t, gdb)
			within(t, "unfiltered", func() error {
				var items []leafItem
				err := ct.GetLeaves(ctx, &items, largeRealParent, 0, tenantF, closuretree.NoPreload())
				if err == nil && len(items) != under5 {
					t.Errorf("want %d items, got %d", under5, len(items))
				}
				return err
			})
			within(t, "filtered", func() error {
				var items []leafItem
				err := ct.GetLeaves(ctx, &items, largeRealParent, 0, tenantF, closuretree.NoPreload(),
					closuretree.Filter(closuretree.Eq("kind", "doc")))
				if err == nil && len(items) != under5-under85 {
					t.Errorf("want %d items, got %d", under5-under85, len(items))
				}
				return err
			})
		})
	}
}
