package closuretree_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
)

// TestMain modifies how test are run,
// it makes sure that the needed DBs are ready and does cleanup in the end.
func TestMain(m *testing.M) {
	testdbs.InitDBS()
	// main block that runs tests
	code := m.Run()
	_ = testdbs.Clean()
	os.Exit(code)
}

// key us id, value is parent
type treeData struct {
	id     uint
	parent uint
	name   string
}

// This represents a tree like:
// 1 -  | - Electronics
// 2 -  |     | -  Mobile Phones
// 6 -  |     |      |  - Touch Screen
// 4 -  |     | -  Laptops
// 3 -  | - Clothing
// 5 -  |     | -  T-Shirt
var testTree1 = []treeData{
	{1, 0, "Electronics"},
	{2, 1, "Mobile Phones"},
	{3, 0, "Clothing"},
	{4, 1, "Laptops"},
	{5, 3, "T-Shirt"},
	{6, 2, "Touch Screen"},
}

// This represents a tree like:
// 7  -  | - Colors
// 8  -  |     | -  Warm
// 12 -  |     |      |  - Red
// 13 -  |     |      |  - Orange
// 10 -  |     | -  Cold
// 14 -  |     |      |  - Blue
// 9  -  | - Sizes
// 11 -  |     | -  Small
var testTree2 = []treeData{
	{7, 0, "Colors"},
	{8, 7, "Warm"},
	{9, 0, "Sizes"},
	{10, 7, "Cold"},
	{11, 9, "Small"},
	{12, 8, "Red"},
	{13, 8, "Orange"},
	{14, 10, "Blue"},
}

type TestPayload struct {
	closuretree.Node
	Name     string
	Children []*TestPayload `gorm:"-"`
}

// TestLeaf is used for GetLeaves tests. It embeds Leaf and has a many2many relation to TestPayload.
type TestLeaf struct {
	closuretree.Leaf
	Name  string
	Nodes []*TestPayload `gorm:"many2many:test_leaf_nodes;"`
}

type NodeDetails struct {
	Id     int
	Tenant string
	Err    string
}

const tenant1 = "t1"
const tenant2 = "t2"

// connAndClose creates a database connection for the test and disables idle connection
// pooling so connections are released immediately after each operation, preventing
// PostgreSQL connection exhaustion when many tests accumulate open pools.
func connAndClose(t *testing.T, db testdbs.TargetDb) *gorm.DB {
	t.Helper()
	gdb := db.ConnDbName(t.Name())
	if sqlDB, err := gdb.DB(); err == nil {
		sqlDB.SetMaxIdleConns(0)
	}
	return gdb
}

// dropTreeTables drops the node and closure tables for the given model,
// ensuring a clean state when tests run with -count > 1.
func dropTreeTables(gdb *gorm.DB, model any) {
	stmt := &gorm.Statement{DB: gdb}
	if err := stmt.Parse(model); err != nil {
		return
	}
	tbl := stmt.Schema.Table
	if tbl == "" {
		return
	}
	gdb.Exec("DROP TABLE IF EXISTS closure_tree_rel_" + tbl)
	gdb.Exec("DROP TABLE IF EXISTS closure_tree_meta_" + tbl)
	gdb.Exec("DROP TABLE IF EXISTS " + tbl)
}

func TestMetaTableCreated(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})

			_, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			// Verify meta table exists by inserting a row
			err = gdb.Exec("INSERT INTO closure_tree_meta_test_payloads (tenant, parent_id, min_halvings) VALUES ('t', 0, 99)").Error
			if err != nil {
				t.Errorf("meta table not created: %v", err)
			}
		})
	}
}

//nolint:gosec // int conversion is not critical here
func getNodeDetails(item any) (bool, int, string) {
	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = itemValue.Elem()
	}
	if itemValue.Kind() != reflect.Struct {
		return false, 0, ""
	}

	idField := itemValue.FieldByName("NodeId")
	var tenantField reflect.Value

	if !idField.IsValid() {
		// Look for the "NodeId" field in embedded structs
		for i := 0; i < itemValue.NumField(); i++ {
			field := itemValue.Field(i)
			if field.Kind() == reflect.Struct {
				idField = field.FieldByName("NodeId")
				tenantField = field.FieldByName("Tenant")
				if idField.IsValid() {
					break
				}
			}
		}
	} else {
		tenantField = itemValue.FieldByName("Tenant")
	}

	if idField.IsValid() && idField.Kind() == reflect.Uint && tenantField.IsValid() && tenantField.Kind() == reflect.String {
		return true, int(idField.Uint()), tenantField.String()
	}
	return false, 0, ""
}

func TestNodeSortOrderField(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			type SampleStruct struct {
				closuretree.Node
				Name string
			}
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, SampleStruct{})
			ct, err := closuretree.New(gdb, SampleStruct{})
			if err != nil {
				t.Fatal(err)
			}
			item := &SampleStruct{Name: "root"}
			if err := ct.Add(context.Background(), item, 0, 0, closuretree.DefaultTenant); err != nil {
				t.Fatal(err)
			}
			// SortOrder must be present on Node (zero value is fine for now)
			if item.SortOrder != 0.0 {
				t.Errorf("want SortOrder=0.0, got %v", item.SortOrder)
			}
		})
	}
}

func TestAddSortOrder(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// First node: afterNodeID=0, no siblings → sort_order = 0.0
			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			if a.SortOrder != 0.0 {
				t.Errorf("a: want SortOrder=0.0, got %v", a.SortOrder)
			}

			// Second node: afterNodeID=a → sort_order = 0.0 + 10.0 = 10.0
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, 0, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			if b.SortOrder != 10.0 {
				t.Errorf("b: want SortOrder=10.0, got %v", b.SortOrder)
			}

			// Third node appended: afterNodeID=b → sort_order = 10.0 + 10.0 = 20.0
			c := &TestPayload{Name: "c"}
			if err := ct.Add(ctx, c, 0, b.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			if c.SortOrder != 20.0 {
				t.Errorf("c: want SortOrder=20.0, got %v", c.SortOrder)
			}

			// Insert d before all (afterNodeID=0): sort_order = 0.0 - 10.0 = -10.0
			d := &TestPayload{Name: "d"}
			if err := ct.Add(ctx, d, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			if d.SortOrder != -10.0 {
				t.Errorf("d: want SortOrder=-10.0, got %v", d.SortOrder)
			}

			// Insert e between a(0.0) and b(10.0): afterNodeID=a → midpoint = 5.0
			e := &TestPayload{Name: "e"}
			if err := ct.Add(ctx, e, 0, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			if e.SortOrder != 5.0 {
				t.Errorf("e: want SortOrder=5.0, got %v", e.SortOrder)
			}

			// Verify SortOrder values on returned nodes via Descendants
			var got []TestPayload
			if err := ct.Descendants(ctx, 0, 1, tenant1, &got); err != nil {
				t.Fatal(err)
			}
			gotOrders := make(map[string]float64, len(got))
			for _, g := range got {
				gotOrders[g.Name] = g.SortOrder
			}
			wantOrders := map[string]float64{"a": 0.0, "b": 10.0, "c": 20.0, "d": -10.0, "e": 5.0}
			for name, want := range wantOrders {
				if gotOrders[name] != want {
					t.Errorf("node %s: want SortOrder=%v, got %v", name, want, gotOrders[name])
				}
			}

			// afterNodeID=0 on empty sibling set: verify SortOrder=0.0
			child := &TestPayload{Name: "child"}
			if err := ct.Add(ctx, child, a.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			if child.SortOrder != 0.0 {
				t.Errorf("child: want SortOrder=0.0 (empty sibling set), got %v", child.SortOrder)
			}
		})
	}
}

func TestAddSortOrderErrors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			parent := &TestPayload{Name: "parent"}
			if err := ct.Add(ctx, parent, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			child := &TestPayload{Name: "child"}
			if err := ct.Add(ctx, child, parent.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			// afterNodeID is not a sibling of root (child is under parent, not root)
			other := &TestPayload{Name: "other"}
			err = ct.Add(ctx, other, 0, child.NodeId, tenant1)
			if !errors.Is(err, closuretree.ErrInvalidAfterNode) {
				t.Errorf("wrong parent: want ErrInvalidAfterNode, got %v", err)
			}

			// afterNodeID in wrong tenant (parent belongs to tenant1, we ask with tenant2)
			err = ct.Add(ctx, other, 0, parent.NodeId, tenant2)
			if !errors.Is(err, closuretree.ErrInvalidAfterNode) {
				t.Errorf("wrong tenant: want ErrInvalidAfterNode, got %v", err)
			}

			// afterNodeID non-existent
			err = ct.Add(ctx, other, 0, 9999, tenant1)
			if !errors.Is(err, closuretree.ErrInvalidAfterNode) {
				t.Errorf("non-existent: want ErrInvalidAfterNode, got %v", err)
			}
		})
	}
}

func TestUpdateSortOrder(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// Build: a(0), b(10), c(20) at root
			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, 0, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			c := &TestPayload{Name: "c"}
			if err := ct.Add(ctx, c, 0, b.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}

			assertOrder := func(t *testing.T, want []string) {
				t.Helper()
				var got []TestPayload
				if err := ct.Descendants(ctx, 0, 1, tenant1, &got); err != nil {
					t.Fatal(err)
				}
				// Verify SortOrder values are strictly increasing per the expected order
				sortOrders := make(map[string]float64)
				for _, g := range got {
					sortOrders[g.Name] = g.SortOrder
				}
				for i := 1; i < len(want); i++ {
					prevOrder := sortOrders[want[i-1]]
					currOrder := sortOrders[want[i]]
					if currOrder < prevOrder {
						t.Errorf("sort order violation: %s(%v) should come before %s(%v)", want[i-1], prevOrder, want[i], currOrder)
					}
				}
			}

			// afterNodeID=nil leaves sort order unchanged (use a different name to avoid MySQL no-op)
			if err := ct.Update(ctx, a.NodeId, &TestPayload{Name: "a2"}, nil, nil, tenant1); err != nil {
				t.Fatal(err)
			}
			assertOrder(t, []string{"a2", "b", "c"})

			// afterNodeID=&0 moves b to first: b gets sort_order < a2's sort_order
			zero := uint(0)
			if err := ct.Update(ctx, b.NodeId, nil, nil, &zero, tenant1); err != nil {
				t.Fatal(err)
			}
			assertOrder(t, []string{"b", "a2", "c"})

			// afterNodeID=&c moves a2 after c
			cID := c.NodeId
			if err := ct.Update(ctx, a.NodeId, nil, nil, &cID, tenant1); err != nil {
				t.Fatal(err)
			}
			assertOrder(t, []string{"b", "c", "a2"})

			// reorder-only (item=nil, newParentID=nil, afterNodeID=&someID) is valid
			bID := b.NodeId
			if err := ct.Update(ctx, a.NodeId, nil, nil, &bID, tenant1); err != nil {
				t.Fatalf("reorder-only should not fail: %v", err)
			}

			// ErrNoOp when all three nil
			err = ct.Update(ctx, a.NodeId, nil, nil, nil, tenant1)
			if !errors.Is(err, closuretree.ErrNoOp) {
				t.Errorf("want ErrNoOp, got %v", err)
			}
		})
	}
}

func TestUpdateSortOrderErrors(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, 0, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			child := &TestPayload{Name: "child"}
			if err := ct.Add(ctx, child, a.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			// afterNodeID = self → ErrAfterNodeIsSelf
			aID := a.NodeId
			err = ct.Update(ctx, a.NodeId, nil, nil, &aID, tenant1)
			if !errors.Is(err, closuretree.ErrAfterNodeIsSelf) {
				t.Errorf("want ErrAfterNodeIsSelf, got %v", err)
			}

			// afterNodeID is not a sibling (child is under a, not root)
			childID := child.NodeId
			err = ct.Update(ctx, b.NodeId, nil, nil, &childID, tenant1)
			if !errors.Is(err, closuretree.ErrInvalidAfterNode) {
				t.Errorf("want ErrInvalidAfterNode (not a sibling), got %v", err)
			}

			// combined move + reorder: move b under a, place after child
			aNodeID := a.NodeId
			if err := ct.Update(ctx, b.NodeId, nil, &aNodeID, &childID, tenant1); err != nil {
				t.Fatalf("combined move+reorder should succeed: %v", err)
			}

			// afterNodeID = self when combined with move → ErrAfterNodeIsSelf
			rootID := uint(0)
			aID2 := a.NodeId
			err = ct.Update(ctx, a.NodeId, nil, &rootID, &aID2, tenant1)
			if !errors.Is(err, closuretree.ErrAfterNodeIsSelf) {
				t.Errorf("want ErrAfterNodeIsSelf for move+reorder-to-self, got %v", err)
			}
		})
	}
}

func TestAddNodes(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			type SampleStruct struct {
				closuretree.Node
				Name string
			}

			tcs := []struct {
				name             string
				topItem          any
				topItemDetails   NodeDetails
				topItemExpect    NodeDetails
				childItem        any
				childItemDetails NodeDetails
				childItemExpect  NodeDetails
			}{
				{
					name:             "Pointer to struct with ID field",
					topItem:          &SampleStruct{Name: "Sample"},
					childItem:        &SampleStruct{Name: "Sample2"},
					topItemDetails:   NodeDetails{Tenant: closuretree.DefaultTenant},
					childItemDetails: NodeDetails{Tenant: closuretree.DefaultTenant},
					topItemExpect:    NodeDetails{Id: 1, Tenant: closuretree.DefaultTenant},
					childItemExpect:  NodeDetails{Id: 2, Tenant: closuretree.DefaultTenant},
				},
				{
					name:             "struct with ID field",
					topItem:          SampleStruct{Name: "Sample"},
					childItem:        SampleStruct{Name: "Sample2"},
					topItemDetails:   NodeDetails{Tenant: closuretree.DefaultTenant},
					childItemDetails: NodeDetails{Tenant: closuretree.DefaultTenant},
					// values should not be populated because it's not a pointer
					topItemExpect:   NodeDetails{Id: 0, Tenant: ""},
					childItemExpect: NodeDetails{Id: 0, Tenant: ""},
				},
				{
					name:             "Ensure embedded NodeId is ignored",
					topItem:          &SampleStruct{Name: "Sample"},
					childItem:        &SampleStruct{Name: "Sample2", Node: closuretree.Node{NodeId: 4}},
					topItemDetails:   NodeDetails{Tenant: closuretree.DefaultTenant},
					childItemDetails: NodeDetails{Tenant: closuretree.DefaultTenant},
					topItemExpect:    NodeDetails{Id: 1, Tenant: closuretree.DefaultTenant},
					childItemExpect:  NodeDetails{Id: 2, Tenant: closuretree.DefaultTenant},
				},
				{
					name:             "asert tenant is set",
					topItem:          &SampleStruct{Name: "Sample"},
					childItem:        &SampleStruct{Name: "Sample2"},
					topItemDetails:   NodeDetails{Tenant: "ble"},
					childItemDetails: NodeDetails{Tenant: "ble"},
					topItemExpect:    NodeDetails{Id: 1, Tenant: "ble"},
					childItemExpect:  NodeDetails{Id: 2, Tenant: "ble"},
				},
				{
					name:             "Ensure embedded Tenant is ignored",
					topItem:          &SampleStruct{Name: "Sample"},
					childItem:        &SampleStruct{Name: "Sample2", Node: closuretree.Node{Tenant: "bla"}},
					topItemDetails:   NodeDetails{Tenant: closuretree.DefaultTenant},
					childItemDetails: NodeDetails{Tenant: closuretree.DefaultTenant},
					topItemExpect:    NodeDetails{Id: 1, Tenant: closuretree.DefaultTenant},
					childItemExpect:  NodeDetails{Id: 2, Tenant: closuretree.DefaultTenant},
				},
				{
					name:             "Avoid cross tenant Add",
					topItem:          &SampleStruct{Name: "Sample"},
					childItem:        &SampleStruct{Name: "Sample2"},
					topItemDetails:   NodeDetails{Tenant: "T1"},
					childItemDetails: NodeDetails{Tenant: "T2"},
					topItemExpect:    NodeDetails{Id: 1, Tenant: "T1"},
					childItemExpect:  NodeDetails{Err: closuretree.ErrParentNotFound.Error()},
				},
				{
					name:             "Struct without ID field",
					topItem:          &struct{ Name string }{Name: "NoID"},
					topItemDetails:   NodeDetails{Tenant: closuretree.DefaultTenant},
					childItemDetails: NodeDetails{Tenant: closuretree.DefaultTenant},
					topItemExpect:    NodeDetails{Err: closuretree.ErrItemIsNotTreeNode.Error()},
					childItemExpect:  NodeDetails{Err: closuretree.ErrItemIsNotTreeNode.Error()},
				},
			}

			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					gdb := db.ConnDbName(fmt.Sprintf("addnodes%d", i))
					dropTreeTables(gdb, tc.topItem)
					ct, err := closuretree.New(gdb, tc.topItem)
					if err != nil {
						if tc.topItemExpect.Err != "" {
							if diff := cmp.Diff(err.Error(), tc.topItemExpect.Err); diff != "" {
								t.Errorf("unexpected error (-want +got):\n%s", diff)
							}
						} else {
							t.Fatalf("unexpected error from New: %v", err)
						}
						return
					}

					// add topItem as parent
					err = ct.Add(context.Background(), tc.topItem, 0, 0, tc.topItemDetails.Tenant)
					if tc.topItemExpect.Err != "" {
						if err == nil {
							t.Fatal("expecting an error but got none")
						}
						if diff := cmp.Diff(err.Error(), tc.topItemExpect.Err); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
					} else {
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						hasId, idValue, tenant := getNodeDetails(tc.topItem)
						if !hasId {
							t.Fatal("unable to get node details")
						}
						got := NodeDetails{Id: idValue, Tenant: tenant}
						if diff := cmp.Diff(got, tc.topItemExpect); diff != "" {
							t.Errorf("unexpected value (-got +want)\n%s", diff)
						}
					}

					// add childItem to parent
					err = ct.Add(context.Background(), tc.childItem, 1, 0, tc.childItemDetails.Tenant)
					if tc.childItemExpect.Err != "" {
						if err == nil {
							t.Error("expecting an error but got none")
						}
						if diff := cmp.Diff(err.Error(), tc.childItemExpect.Err); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
					} else {
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						hasId, idValue, tenant := getNodeDetails(tc.childItem)
						if !hasId {
							t.Fatal("unable to get node details")
						}
						got := NodeDetails{Id: idValue, Tenant: tenant}
						if diff := cmp.Diff(got, tc.childItemExpect); diff != "" {
							t.Errorf("unexpected value (-got +want)\n%s", diff)
						}
					}
				})
			}
		})
	}
}

func populateTree(t *testing.T, ct *closuretree.Tree) {
	for _, item := range testTree1 {
		tagItem := TestPayload{
			Name: item.name,
			Node: closuretree.Node{
				NodeId: item.id,
				Tenant: tenant1,
			},
		}

		err := ct.Add(context.Background(), tagItem, item.parent, 0, tenant1)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, item := range testTree2 {
		tagItem := TestPayload{
			Name: item.name,
			Node: closuretree.Node{
				NodeId: item.id,
				Tenant: tenant2,
			},
		}

		err := ct.Add(context.Background(), tagItem, item.parent, 0, tenant2)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPopulateTree(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			var ct *closuretree.Tree
			var err error

			gdb := db.ConnDbName("populatetree")
			dropTreeTables(gdb, TestPayload{})
			ct, err = closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)
		})
	}
}

func TestTreeGetNode(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name        string
				nodeID      uint
				in          any
				wantPayload TestPayload
				tenant      string
				wantErr     error
			}{
				{
					name:        "get root node for tenant 1",
					nodeID:      1,
					in:          &TestPayload{},
					wantPayload: TestPayload{Name: "Electronics", Node: closuretree.Node{NodeId: 1, Tenant: tenant1}},
					tenant:      tenant1,
				},
				{
					name:        "get child node for tenant 1",
					nodeID:      2,
					in:          &TestPayload{},
					wantPayload: TestPayload{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2, Tenant: tenant1, ParentId: 1}},
					tenant:      tenant1,
				},
				{
					name:        "get node on Tenant 2",
					nodeID:      7,
					in:          &TestPayload{},
					wantPayload: TestPayload{Name: "Colors", Node: closuretree.Node{NodeId: 7, Tenant: tenant2}},
					tenant:      tenant2,
				},
				{
					name:    "expect err because of wrong type",
					nodeID:  7,
					in:      &map[string]string{},
					tenant:  tenant1,
					wantErr: closuretree.ErrItemIsNotTreeNode,
				},
				{
					name:    "expect err because not passing pointer",
					nodeID:  7,
					in:      TestPayload{},
					tenant:  tenant1,
					wantErr: closuretree.ErrItemNotPointerToStruct,
				},
				{
					name:    "empty result on wrong Tenant",
					nodeID:  7,
					in:      &TestPayload{},
					tenant:  tenant1,
					wantErr: closuretree.ErrNodeNotFound,
				},
				{
					name:    "empty tenant returns error",
					nodeID:  1,
					in:      &TestPayload{},
					tenant:  "",
					wantErr: closuretree.ErrEmptyTenant,
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					err := ct.GetNode(context.Background(), tc.nodeID, tc.tenant, tc.in)

					if tc.wantErr != nil {
						if err == nil {
							t.Fatalf("expected error: %v, but got none", tc.wantErr)
						}
						if !errors.Is(err, tc.wantErr) {
							t.Errorf("expected error: %v, but got %v", tc.wantErr, err)
						}
					} else {
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						if diff := cmp.Diff(tc.in, &tc.wantPayload); diff != "" {
							t.Errorf("unexpected result (-want +got):\n%s", diff)
						}
					}
				})
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name        string
				nodeID      uint
				in          any
				newParentID *uint
				wantPayload TestPayload
				tenant      string
				wantErr     error
			}{
				{
					name:        "get root node for tenant 1",
					nodeID:      1,
					in:          TestPayload{Name: "Banana"},
					wantPayload: TestPayload{Name: "Banana", Node: closuretree.Node{NodeId: 1, Tenant: tenant1}},
					tenant:      tenant1,
				},
				{
					name:    "expect err because of wrong type",
					nodeID:  7,
					in:      &map[string]string{},
					tenant:  tenant1,
					wantErr: closuretree.ErrItemIsNotTreeNode,
				},
				{
					name:    "empty result on wrong Tenant",
					nodeID:  7,
					in:      TestPayload{Name: "Banana"},
					tenant:  tenant1,
					wantErr: closuretree.ErrNodeNotFound,
				},
				{
					name:    "empty tenant returns error",
					nodeID:  1,
					in:      TestPayload{Name: "Banana"},
					tenant:  "",
					wantErr: closuretree.ErrEmptyTenant,
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					err := ct.Update(context.Background(), tc.nodeID, tc.in, tc.newParentID, nil, tc.tenant)
					if tc.wantErr != nil {
						if err == nil {
							t.Fatalf("expected error: %v, but got none", tc.wantErr)
						}
						if !errors.Is(err, tc.wantErr) {
							t.Errorf("expected error: %v, but got %v", tc.wantErr, err)
						}
					} else {
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}
						got := TestPayload{}
						err = ct.GetNode(context.Background(), tc.nodeID, tc.tenant, &got)
						if err != nil {
							t.Fatalf("unexpected error %v", err)
						}
						if diff := cmp.Diff(got, tc.wantPayload); diff != "" {
							t.Errorf("unexpected result (-want +got):\n%s", diff)
						}
					}
				})
			}
		})
	}
}

// assertNodeNameIs verifies that the node with the given id has the expected name.
func assertNodeNameIs(t *testing.T, ct *closuretree.Tree, id uint, tenant, want string) {
	t.Helper()
	got := TestPayload{}
	if err := ct.GetNode(context.Background(), id, tenant, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != want {
		t.Errorf("expected name %q, got %q", want, got.Name)
	}
}

// assertIsDirectChild verifies that childID is a direct child of parentID.
func assertIsDirectChild(t *testing.T, ct *closuretree.Tree, parentID, childID uint, tenant string) {
	t.Helper()
	children, err := ct.DescendantIds(context.Background(), parentID, 1, tenant)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range children {
		if id == childID {
			return
		}
	}
	t.Errorf("expected node %d to be a direct child of %d, got children: %v", childID, parentID, children)
}

func TestUpdateAndMove(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			setup := func(t *testing.T) *closuretree.Tree {
				t.Helper()
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				ct, err := closuretree.New(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				populateTree(t, ct)
				return ct
			}

			t.Run("both nil returns ErrNoOp", func(t *testing.T) {
				ct := setup(t)
				err := ct.Update(context.Background(), 1, nil, nil, nil, tenant1)
				if !errors.Is(err, closuretree.ErrNoOp) {
					t.Errorf("expected ErrNoOp, got %v", err)
				}
			})

			// id zero guard must fire before the no-op guard
			t.Run("id zero with both nil returns ErrNodeNotFound not ErrNoOp", func(t *testing.T) {
				ct := setup(t)
				err := ct.Update(context.Background(), 0, nil, nil, nil, tenant1)
				if !errors.Is(err, closuretree.ErrNodeNotFound) {
					t.Errorf("expected ErrNodeNotFound, got %v", err)
				}
			})

			t.Run("id zero returns ErrNodeNotFound", func(t *testing.T) {
				ct := setup(t)
				dest := uint(4)
				err := ct.Update(context.Background(), 0, TestPayload{Name: "x"}, &dest, nil, tenant1)
				if !errors.Is(err, closuretree.ErrNodeNotFound) {
					t.Errorf("expected ErrNodeNotFound, got %v", err)
				}
			})

			t.Run("move only without field update", func(t *testing.T) {
				ct := setup(t)
				// Move node 2 (Mobile Phones) under node 3 (Clothing), no field changes
				dest := uint(3)
				if err := ct.Update(context.Background(), 2, nil, &dest, nil, tenant1); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				assertIsDirectChild(t, ct, 3, 2, tenant1)
			})

			t.Run("update fields and move atomically", func(t *testing.T) {
				ct := setup(t)
				// Move node 2 (Mobile Phones, child of 1) under node 3 (Clothing), also rename it
				dest := uint(3)
				if err := ct.Update(context.Background(), 2, TestPayload{Name: "Updated Phones"}, &dest, nil, tenant1); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				assertNodeNameIs(t, ct, 2, tenant1, "Updated Phones")
				assertIsDirectChild(t, ct, 3, 2, tenant1)
			})

			t.Run("node not found with both item and newParentID", func(t *testing.T) {
				ct := setup(t)
				ctx := context.Background()
				dest := uint(3)
				err := ct.Update(ctx, 999, TestPayload{Name: "Ghost"}, &dest, nil, tenant1)
				if !errors.Is(err, closuretree.ErrNodeNotFound) {
					t.Errorf("expected ErrNodeNotFound, got %v", err)
				}
			})

			t.Run("move fails with cycle field update rolled back", func(t *testing.T) {
				ct := setup(t)
				// 6 is a descendant of 2; moving 2 under 6 is a cycle and must fail
				dest := uint(6)
				if err := ct.Update(context.Background(), 2, TestPayload{Name: "Should Not Persist"}, &dest, nil, tenant1); !errors.Is(err, closuretree.ErrInvalidMove) {
					t.Fatalf("expected ErrInvalidMove, got %v", err)
				}
				assertNodeNameIs(t, ct, 2, tenant1, "Mobile Phones")
			})

			t.Run("same parent with field update rolled back", func(t *testing.T) {
				ct := setup(t)
				// 1 is already the parent of 2
				dest := uint(1)
				if err := ct.Update(context.Background(), 2, TestPayload{Name: "Should Not Persist"}, &dest, nil, tenant1); !errors.Is(err, closuretree.ErrInvalidMove) {
					t.Fatalf("expected ErrInvalidMove, got %v", err)
				}
				assertNodeNameIs(t, ct, 2, tenant1, "Mobile Phones")
			})

			t.Run("invalid parent field update rolled back", func(t *testing.T) {
				ct := setup(t)
				dest := uint(999) // non-existent parent
				if err := ct.Update(context.Background(), 2, TestPayload{Name: "Should Not Persist"}, &dest, nil, tenant1); !errors.Is(err, closuretree.ErrParentNotFound) {
					t.Fatalf("expected ErrParentNotFound, got %v", err)
				}
				assertNodeNameIs(t, ct, 2, tenant1, "Mobile Phones")
			})

			t.Run("same parent at root with field update rolled back", func(t *testing.T) {
				ct := setup(t)
				// Node 1 (Electronics) is already a root node; parent is 0
				zero := uint(0)
				if err := ct.Update(context.Background(), 1, TestPayload{Name: "Should Not Persist"}, &zero, nil, tenant1); !errors.Is(err, closuretree.ErrInvalidMove) {
					t.Fatalf("expected ErrInvalidMove, got %v", err)
				}
				assertNodeNameIs(t, ct, 1, tenant1, "Electronics")
			})
		})
	}
}

func TestGetDescendants(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name        string
				parent      uint
				depth       int
				wantPayload []TestPayload
				wantIds     []uint
				tenant      string
			}{
				{
					name:   "get descendants on Tenant 1",
					parent: 1,
					depth:  0,
					wantPayload: []TestPayload{
						{Name: "Laptops", Node: closuretree.Node{NodeId: 4, ParentId: 1, Tenant: tenant1, SortOrder: -10}},
						{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2, ParentId: 1, Tenant: tenant1}},
						{Name: "Touch Screen", Node: closuretree.Node{NodeId: 6, ParentId: 2, Tenant: tenant1}},
					},
					wantIds: []uint{4, 2, 6},
					tenant:  tenant1,
				},
				{
					name:   "get descendants on Tenant 2",
					parent: 7,
					depth:  0,
					wantPayload: []TestPayload{
						{Name: "Cold", Node: closuretree.Node{NodeId: 10, ParentId: 7, Tenant: tenant2, SortOrder: -10}},
						{Name: "Warm", Node: closuretree.Node{NodeId: 8, ParentId: 7, Tenant: tenant2}},
						{Name: "Orange", Node: closuretree.Node{NodeId: 13, ParentId: 8, Tenant: tenant2, SortOrder: -10}},
						{Name: "Red", Node: closuretree.Node{NodeId: 12, ParentId: 8, Tenant: tenant2}},
						{Name: "Blue", Node: closuretree.Node{NodeId: 14, ParentId: 10, Tenant: tenant2}},
					},
					wantIds: []uint{10, 8, 13, 12, 14},
					tenant:  tenant2,
				},
				{
					name:   "get root items for tenant 1",
					parent: 0,
					depth:  1,
					wantPayload: []TestPayload{
						{Name: "Clothing", Node: closuretree.Node{NodeId: 3, Tenant: tenant1, SortOrder: -10}},
						{Name: "Electronics", Node: closuretree.Node{NodeId: 1, Tenant: tenant1}},
					},
					wantIds: []uint{3, 1},
					tenant:  tenant1,
				},
				{
					name:   "get root items for tenant 2",
					parent: 0,
					depth:  1,
					wantPayload: []TestPayload{
						{Name: "Sizes", Node: closuretree.Node{NodeId: 9, Tenant: tenant2, SortOrder: -10}},
						{Name: "Colors", Node: closuretree.Node{NodeId: 7, Tenant: tenant2}},
					},
					wantIds: []uint{9, 7},
					tenant:  tenant2,
				},
				{
					name:        "empty result on wrong Tenant",
					parent:      7,
					depth:       0,
					wantPayload: []TestPayload{},
					wantIds:     []uint{},
					tenant:      tenant1,
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					gotTags := []TestPayload{}
					err := ct.Descendants(context.Background(), tc.parent, tc.depth, tc.tenant, &gotTags)
					if err != nil {
						t.Fatal(err)
					}

					if diff := cmp.Diff(gotTags, tc.wantPayload); diff != "" {
						t.Errorf("unexpected result (-want +got):\n%s", diff)
					}

					got, err := ct.DescendantIds(context.Background(), tc.parent, tc.depth, tc.tenant)
					if err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(got, tc.wantIds); diff != "" {
						t.Errorf("unexpected result (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestGetTreeDescendants(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name        string
				parent      uint
				depth       int
				wantPayload []*TestPayload
				wantIds     []*closuretree.TreeNode
				tenant      string
			}{
				{
					name:   "get descendants on Tenant 1",
					parent: 1,
					depth:  0,
					wantPayload: []*TestPayload{
						{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2, ParentId: 1, Tenant: tenant1},
							Children: []*TestPayload{
								{Name: "Touch Screen", Node: closuretree.Node{NodeId: 6, ParentId: 2, Tenant: tenant1}},
							},
						},
						{Name: "Laptops", Node: closuretree.Node{NodeId: 4, ParentId: 1, Tenant: tenant1, SortOrder: -10}},
					},
					wantIds: []*closuretree.TreeNode{
						{
							NodeId: 2, ParentID: 1,
							Children: []*closuretree.TreeNode{
								{NodeId: 6, ParentID: 2},
							},
						},
						{NodeId: 4, ParentID: 1, SortOrder: -10},
					},
					tenant: tenant1,
				},
				{
					name:   "get descendants on Tenant 2",
					parent: 7,
					depth:  0,
					wantPayload: []*TestPayload{
						{Name: "Warm", Node: closuretree.Node{NodeId: 8, ParentId: 7, Tenant: tenant2},
							Children: []*TestPayload{
								{Name: "Red", Node: closuretree.Node{NodeId: 12, ParentId: 8, Tenant: tenant2}},
								{Name: "Orange", Node: closuretree.Node{NodeId: 13, ParentId: 8, Tenant: tenant2, SortOrder: -10}},
							}},
						{Name: "Cold", Node: closuretree.Node{NodeId: 10, ParentId: 7, Tenant: tenant2, SortOrder: -10},
							Children: []*TestPayload{
								{Name: "Blue", Node: closuretree.Node{NodeId: 14, ParentId: 10, Tenant: tenant2}},
							}},
					},
					wantIds: []*closuretree.TreeNode{
						{
							NodeId: 8, ParentID: 7,
							Children: []*closuretree.TreeNode{
								{NodeId: 12, ParentID: 8},
								{NodeId: 13, ParentID: 8, SortOrder: -10},
							},
						},
						{
							NodeId: 10, ParentID: 7, SortOrder: -10,
							Children: []*closuretree.TreeNode{
								{NodeId: 14, ParentID: 10},
							},
						},
					},
					tenant: tenant2,
				},
				{
					name:   "get root items for tenant 1",
					parent: 0,
					depth:  1,
					wantPayload: []*TestPayload{
						{Name: "Electronics", Node: closuretree.Node{NodeId: 1, Tenant: tenant1}},
						{Name: "Clothing", Node: closuretree.Node{NodeId: 3, Tenant: tenant1, SortOrder: -10}},
					},
					wantIds: []*closuretree.TreeNode{
						{NodeId: 1, ParentID: 0},
						{NodeId: 3, ParentID: 0, SortOrder: -10},
					},
					tenant: tenant1,
				},
				{
					name:        "empty result on wrong Tenant",
					parent:      7,
					depth:       0,
					wantPayload: []*TestPayload{},
					wantIds:     nil,
					tenant:      tenant1,
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					gotPayload := []*TestPayload{}
					err := ct.TreeDescendants(context.Background(), tc.parent, tc.depth, tc.tenant, &gotPayload)
					if err != nil {
						t.Fatal(err)
					}
					SortTestPayload(gotPayload)

					if diff := cmp.Diff(gotPayload, tc.wantPayload); diff != "" {
						t.Errorf("unexpected result (-want +got):\n%s", diff)
					}

					got, err := ct.TreeDescendantsIds(context.Background(), tc.parent, tc.depth, tc.tenant)
					if err != nil {
						t.Fatal(err)
					}
					closuretree.SortTree(got)
					closuretree.SortTree(tc.wantIds)

					if diff := cmp.Diff(got, tc.wantIds); diff != "" {
						t.Errorf("unexpected result (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func SortTestPayload(nodes []*TestPayload) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeId < nodes[j].NodeId
	})
	for _, node := range nodes {
		SortTestPayload(node.Children)
	}
}

func TestMove(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			setup := func(t *testing.T, name string) *closuretree.Tree {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				ct, err := closuretree.New(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				populateTree(t, ct)
				return ct
			}
			type idCheck struct {
				parent uint
				tenant string
				want   []uint
			}

			tcs := []struct {
				name    string
				origin  uint
				dest    uint
				tenant  string
				wantIds []idCheck // for every key in the map check the resulting slice
				wantErr error
			}{
				{
					name:   "move a parent node on Tenant 1",
					origin: 3,
					dest:   4,
					wantIds: []idCheck{
						{parent: 4, tenant: tenant1, want: []uint{3}},
						{parent: 3, tenant: tenant1, want: []uint{5}},
						{parent: 0, tenant: tenant1, want: []uint{1}},    // expect id 3 to not be bellow id 0
						{parent: 1, tenant: tenant1, want: []uint{2, 4}}, // expect id 3 to not be bellow id 0
					},
					tenant: tenant1,
				},
				{
					name:   "move a child node on Tenant 1",
					origin: 2,
					dest:   5,
					wantIds: []idCheck{
						{parent: 3, tenant: tenant1, want: []uint{5}},
						{parent: 5, tenant: tenant1, want: []uint{2}},
						{parent: 2, tenant: tenant1, want: []uint{6}},
						{parent: 1, tenant: tenant1, want: []uint{4}},
					},
					tenant: tenant1,
				},
				{
					name:   "move a child to the same depth",
					origin: 10,
					dest:   9,
					wantIds: []idCheck{
						{parent: 9, tenant: tenant2, want: []uint{11, 10}},
						{parent: 0, tenant: tenant2, want: []uint{7, 9}},
						{parent: 10, tenant: tenant2, want: []uint{14}},
					},
					tenant: tenant2,
				},
				{
					name:   "move a child to root",
					origin: 10,
					dest:   0,
					wantIds: []idCheck{
						{parent: 0, tenant: tenant2, want: []uint{7, 9, 10}},
						{parent: 10, tenant: tenant2, want: []uint{14}},
						{parent: 14, tenant: tenant2, want: []uint{}},
					},
					tenant: tenant2,
				},
				{
					name:   "dont move between tenants",
					origin: 3,
					dest:   8,
					wantIds: []idCheck{
						{parent: 3, tenant: tenant1, want: []uint{5}},
						{parent: 3, tenant: tenant2, want: []uint{}},
						{parent: 8, tenant: tenant2, want: []uint{12, 13}},
						{parent: 8, tenant: tenant1, want: []uint{}},
					},
					wantErr: closuretree.ErrParentNotFound,
					tenant:  tenant1,
				},
				{
					name:    "move node under its own descendant (should fail)",
					origin:  2,
					dest:    6, // 6 is a descendant of 2
					tenant:  tenant1,
					wantErr: closuretree.ErrInvalidMove,
				},
				{
					name:   "move leaf node to a different branch",
					origin: 6,
					dest:   4,
					tenant: tenant1,
					wantIds: []idCheck{
						{parent: 4, tenant: tenant1, want: []uint{6}},
						{parent: 2, tenant: tenant1, want: []uint{}}, // 6 no longer under 2
					},
				},
				{
					name:   "move root level node below another root level node",
					origin: 1,
					dest:   3,
					tenant: tenant1,
					wantIds: []idCheck{
						{parent: 3, tenant: tenant1, want: []uint{5, 1}},
						{parent: 1, tenant: tenant1, want: []uint{2, 4}},
						{parent: 0, tenant: tenant1, want: []uint{3}},
					},
				},
				{
					name:    "expect err when move node to same parent",
					origin:  2,
					dest:    1, // already the parent
					tenant:  tenant1,
					wantErr: closuretree.ErrInvalidMove,
				},
				{
					name:    "expect err when move node to same parent, root node edition",
					origin:  3,
					dest:    0, // already the parent
					tenant:  tenant1,
					wantErr: closuretree.ErrInvalidMove,
				},
				{
					name:   "move single node subtree (no children)",
					origin: 14,
					dest:   9,
					tenant: tenant2,
					wantIds: []idCheck{
						{parent: 9, tenant: tenant2, want: []uint{11, 14}},
						{parent: 10, tenant: tenant2, want: []uint{}}, // 14 no longer under 10
					},
				},
				{
					name:   "move a deep node to unrelated branch",
					origin: 11,
					dest:   13,
					tenant: tenant2,
					wantIds: []idCheck{
						{parent: 13, tenant: tenant2, want: []uint{11}},
					},
				},
				{
					name:   "move node to its sibling",
					origin: 10,
					dest:   11,
					tenant: tenant2,
					wantIds: []idCheck{
						{parent: 11, tenant: tenant2, want: []uint{10}},
						{parent: 10, tenant: tenant2, want: []uint{14}},
					},
				},
			}

			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					ct := setup(t, fmt.Sprintf("IT_move_%d", i))
					dest := tc.dest
					err := ct.Update(context.Background(), tc.origin, nil, &dest, nil, tc.tenant)
					if tc.wantErr != nil {
						if err == nil {
							t.Fatalf("expected error %v but got no error", tc.wantErr)
						}
						if !errors.Is(err, tc.wantErr) {
							t.Errorf("expected error %v, got %v", tc.wantErr, err)
						}
					} else {
						if err != nil {
							t.Errorf("unexpected error: %v", err)
						}
					}
					for _, checkId := range tc.wantIds {
						// check only one level deep
						got, err := ct.DescendantIds(context.Background(), checkId.parent, 1, checkId.tenant)
						if err != nil {
							t.Fatal(err)
						}

						// databases may return items at the same depth in non-deterministic order
						sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
						want := append([]uint{}, checkId.want...)
						sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

						if diff := cmp.Diff(want, got); diff != "" {
							t.Errorf("unexpected result (-want +got):\n%s", diff)
						}
					}

				})
			}
		})
	}
}

// used for debugging, e.g.
// items := []*TestPayload{}
//
// err = ct.TreeDescendants(context.Background(), 0, 0, checkId.tenant, &items)
// if err != nil {
// t.Fatal(err)
// }
// fmt.Println("================")
// printTreeTest(items, "")
// fmt.Println("================")
var _ = printTreeTest

func printTreeTest(nodes []*TestPayload, indent string) {
	for _, n := range nodes {
		fmt.Printf("%s%d=> %s\n", indent, n.NodeId, n.Name)
		if len(n.Children) > 0 {
			printTreeTest(n.Children, indent+"|- ")
		}
	}
}

// TestMoveBetweenParents_NoDuplicates verifies that moving a child node from one
// parent to another sibling parent does not produce duplicate entries in Descendants.
// This reproduces a bug where the Move INSERT creates a closure row identical to an
// existing one (same ancestor_id, descendant_id, depth) when old and new parents share
// the same depth from root. The duplicate causes Descendants to return the node twice.
func TestMoveBetweenParents_NoDuplicates(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()

			// Build a simple tree:
			//   ParentA (root)
			//     └── Child
			//   ParentB (root)
			parentA := &TestPayload{Name: "ParentA"}
			err = ct.Add(ctx, parentA, 0, 0, tenant1)
			if err != nil {
				t.Fatal(err)
			}

			child := &TestPayload{Name: "Child"}
			err = ct.Add(ctx, child, parentA.Id(), 0, tenant1)
			if err != nil {
				t.Fatal(err)
			}

			parentB := &TestPayload{Name: "ParentB"}
			err = ct.Add(ctx, parentB, 0, 0, tenant1)
			if err != nil {
				t.Fatal(err)
			}

			// Verify initial state: 3 descendants from root
			var before []TestPayload
			err = ct.Descendants(ctx, 0, 0, tenant1, &before)
			if err != nil {
				t.Fatal(err)
			}
			if len(before) != 3 {
				t.Fatalf("expected 3 descendants before move, got %d", len(before))
			}

			// Move Child from ParentA to ParentB
			parentBId := parentB.Id()
			err = ct.Update(ctx, child.Id(), nil, &parentBId, nil, tenant1)
			if err != nil {
				t.Fatalf("Move failed: %v", err)
			}

			// Verify: still exactly 3 descendants (no duplicates)
			var after []TestPayload
			err = ct.Descendants(ctx, 0, 0, tenant1, &after)
			if err != nil {
				t.Fatal(err)
			}

			if len(after) != 3 {
				t.Errorf("expected 3 descendants after move, got %d", len(after))
				for _, item := range after {
					t.Logf("  id=%d name=%s parentId=%d", item.Id(), item.Name, item.Parent())
				}
			}

			// Verify Child is now under ParentB
			want := []TestPayload{
				{Name: "ParentA", Node: closuretree.Node{NodeId: parentA.Id(), Tenant: tenant1}},
				{Name: "Child", Node: closuretree.Node{NodeId: child.Id(), ParentId: parentB.Id(), Tenant: tenant1}},
				{Name: "ParentB", Node: closuretree.Node{NodeId: parentB.Id(), Tenant: tenant1, SortOrder: -10}},
			}

			sort.Slice(after, func(i, j int) bool { return after[i].Id() < after[j].Id() })
			sort.Slice(want, func(i, j int) bool { return want[i].Id() < want[j].Id() })

			if diff := cmp.Diff(after, want); diff != "" {
				t.Errorf("unexpected result (-got +want):\n%s", diff)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			setup := func(t *testing.T, name string) *closuretree.Tree {
				gdb := connAndClose(t, db)
				dropTreeTables(gdb, TestPayload{})
				ct, err := closuretree.New(gdb, TestPayload{})
				if err != nil {
					t.Fatal(err)
				}
				populateTree(t, ct)
				return ct
			}

			type idCheck struct {
				parent uint
				tenant string
				want   []uint
			}

			tcs := []struct {
				name    string
				nodeId  uint
				tenant  string
				wantIds []idCheck // for every key in the map check the resulting slice
				wantErr string
			}{
				{
					name:   "delete a parent node on Tenant 1",
					nodeId: 3,
					tenant: tenant1,
					wantIds: []idCheck{
						{parent: 1, tenant: tenant1, want: []uint{2, 4, 6}},
						{parent: 0, tenant: tenant1, want: []uint{1, 2, 4, 6}},
					},
				},
				{
					name:   "delete a child node on Tenant 1",
					nodeId: 2,
					tenant: tenant1,
					wantIds: []idCheck{
						{parent: 0, tenant: tenant1, want: []uint{1, 3, 4, 5}},
						{parent: 1, tenant: tenant1, want: []uint{4}},
						{parent: 0, tenant: tenant2, want: []uint{7, 8, 9, 10, 11, 12, 13, 14}},
					},
				},
				{
					name:   "dont delete cross Tenant",
					nodeId: 2,
					tenant: tenant2,
					wantIds: []idCheck{
						{parent: 1, tenant: tenant1, want: []uint{2, 4, 6}},
					},
					wantErr: closuretree.ErrNodeNotFound.Error(),
				},
			}

			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					ct := setup(t, fmt.Sprintf("IT_delete_%d", i))
					err := ct.DeleteRecurse(context.Background(), tc.nodeId, tc.tenant)
					if tc.wantErr != "" {
						if err == nil {
							t.Fatalf("expected error \"%s\" but got no error at all", tc.wantErr)
						}
						if err.Error() != tc.wantErr {
							t.Errorf("unexpected error \"%s\" , want: \"%s\" ", err.Error(), tc.wantErr)
						}
					} else {
						if err != nil {
							t.Errorf("unexpected error \"%s\" ", err.Error())
						}
					}

					for _, checkId := range tc.wantIds {
						got, err := ct.DescendantIds(context.Background(), checkId.parent, 0, checkId.tenant)
						if err != nil {
							t.Fatal(err)
						}
						// some databases return items of the same level in a different order,
						// to make the test predictable we simply sort the result
						sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
						if diff := cmp.Diff(got, checkId.want); diff != "" {
							t.Errorf("unexpected result (-want +got):\n%s", diff)
						}
					}
				})
			}
		})
	}
}

func TestIsDescendant(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name         string
				ancestorID   uint
				descendantID uint
				tenant       string
				want         bool
			}{
				{
					name:         "node is a descendant",
					ancestorID:   2,
					descendantID: 6,
					tenant:       tenant1,
					want:         true,
				},
				{
					name:         "node is not a descendant",
					ancestorID:   5,
					descendantID: 3,
					tenant:       tenant1,
					want:         false,
				},
				{
					name:         "node is descendant in another tenant",
					ancestorID:   10,
					descendantID: 14,
					tenant:       tenant2,
					want:         true,
				},
				{
					name:         "node is not descendant - cross tenant",
					ancestorID:   10,
					descendantID: 14,
					tenant:       tenant1,
					want:         false,
				},
				{
					name:         "node is not its own descendant",
					ancestorID:   2,
					descendantID: 2,
					tenant:       tenant1,
					want:         false,
				},
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					got, err := ct.IsDescendant(context.Background(), tc.ancestorID, tc.descendantID, tc.tenant)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if got != tc.want {
						t.Errorf("IsDescendant(%d, %d) = %v; want %v", tc.ancestorID, tc.descendantID, got, tc.want)
					}
				})
			}
		})
	}
}

func TestIsChildOf(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			tcs := []struct {
				name      string
				nodeID    uint
				parentID  uint
				tenant    string
				wantChild bool
			}{
				{
					name:      "direct child",
					nodeID:    2,
					parentID:  1,
					tenant:    tenant1,
					wantChild: true,
				},
				{
					name:      "not a direct child (descendant only)",
					nodeID:    6,
					parentID:  1,
					tenant:    tenant1,
					wantChild: false,
				},
				{
					name:      "child in tenant 2",
					nodeID:    14,
					parentID:  10,
					tenant:    tenant2,
					wantChild: true,
				},
				{
					name:      "not child - cross tenant",
					nodeID:    14,
					parentID:  10,
					tenant:    tenant1,
					wantChild: false,
				},
			}

			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					got, err := ct.IsChildOf(context.Background(), tc.nodeID, tc.parentID, tc.tenant)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if got != tc.wantChild {
						t.Errorf("IsChildOf(%d, %d) = %v; want %v", tc.nodeID, tc.parentID, got, tc.wantChild)
					}
				})
			}
		})
	}
}

func TestGetLeaves(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := db.ConnDbName("getleaves")

			// Drop leaf-related tables before tree tables to respect FK order
			gdb.Exec("DROP TABLE IF EXISTS test_leaf_nodes")
			gdb.Exec("DROP TABLE IF EXISTS test_leaves")
			dropTreeTables(gdb, TestPayload{})

			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			// Migrate the leaf table (also creates the M2M join table)
			if err := gdb.AutoMigrate(&TestLeaf{}); err != nil {
				t.Fatal(err)
			}

			t.Run("invalid leaf struct - no Leaf embedded", func(t *testing.T) {
				type NoLeaf struct {
					ID   uint
					Name string
				}
				var leaves []NoLeaf
				err := ct.GetLeaves(context.Background(), &leaves, 1, 0, tenant1)
				if !errors.Is(err, closuretree.ErrItemIsNotTreeLeaf) {
					t.Errorf("expected ErrItemIsNotTreeLeaf, got %v", err)
				}
			})

			t.Run("invalid leaf struct - missing many2many tag", func(t *testing.T) {
				type NoM2M struct {
					closuretree.Leaf
					Name string
				}
				var leaves []NoM2M
				err := ct.GetLeaves(context.Background(), &leaves, 1, 0, tenant1)
				if err == nil {
					t.Error("expected error for missing many2many tag, got nil")
				}
			})

			t.Run("happy path - returns leaf associated with descendant node", func(t *testing.T) {
				// Create a leaf with tenant1
				leaf := TestLeaf{
					Leaf: closuretree.Leaf{Tenant: tenant1},
					Name: "leaf-mobile-phones",
				}
				if err := gdb.Create(&leaf).Error; err != nil {
					t.Fatal(err)
				}

				// Associate the leaf with node 2 (Mobile Phones, child of Electronics=1)
				node2 := &TestPayload{}
				node2.NodeId = 2
				if err := gdb.Model(&leaf).Association("Nodes").Append(node2); err != nil {
					t.Fatal(err)
				}

				// GetLeaves under node 1 (Electronics) should return our leaf
				var leaves []TestLeaf
				err := ct.GetLeaves(context.Background(), &leaves, 1, 0, tenant1)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(leaves) != 1 {
					t.Errorf("expected 1 leaf, got %d", len(leaves))
					return
				}
				if leaves[0].Name != "leaf-mobile-phones" {
					t.Errorf("expected leaf name 'leaf-mobile-phones', got %q", leaves[0].Name)
				}
			})

			t.Run("wrong tenant returns empty", func(t *testing.T) {
				var leaves []TestLeaf
				err := ct.GetLeaves(context.Background(), &leaves, 1, 0, tenant2)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(leaves) != 0 {
					t.Errorf("expected 0 leaves for wrong tenant, got %d", len(leaves))
				}
			})

			t.Run("non-existent parent returns empty", func(t *testing.T) {
				var leaves []TestLeaf
				err := ct.GetLeaves(context.Background(), &leaves, 9999, 0, tenant1)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(leaves) != 0 {
					t.Errorf("expected 0 leaves for non-existent parent, got %d", len(leaves))
				}
			})
		})
	}
}

// TestMoveSubtreeIntegrity verifies that moving a subtree to root preserves
// all descendant closure rows at every depth level.
func TestMoveSubtreeIntegrity(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := db.ConnDbName("movesubtreeintegrity")
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			// Move node 8 (Warm) to root. Its subtree includes 12 (Red) and 13 (Orange).
			// Before: 7 -> 8 -> {12, 13}
			// After:  8 -> {12, 13} at root level
			zero := uint(0)
			err = ct.Update(context.Background(), 8, nil, &zero, nil, tenant2)
			if err != nil {
				t.Fatalf("unexpected error moving node: %v", err)
			}

			// Node 8 should now be a root node
			rootChildren, err := ct.DescendantIds(context.Background(), 0, 1, tenant2)
			if err != nil {
				t.Fatal(err)
			}
			sort.Slice(rootChildren, func(i, j int) bool { return rootChildren[i] < rootChildren[j] })
			expectedRoots := []uint{7, 8, 9}
			if diff := cmp.Diff(expectedRoots, rootChildren); diff != "" {
				t.Errorf("root children after move (-want +got):\n%s", diff)
			}

			// Node 8's children (12 and 13) must still be reachable
			warmChildren, err := ct.DescendantIds(context.Background(), 8, 1, tenant2)
			if err != nil {
				t.Fatal(err)
			}
			sort.Slice(warmChildren, func(i, j int) bool { return warmChildren[i] < warmChildren[j] })
			expectedWarm := []uint{12, 13}
			if diff := cmp.Diff(expectedWarm, warmChildren); diff != "" {
				t.Errorf("children of node 8 after move (-want +got):\n%s", diff)
			}

			// All descendants of node 8 (full subtree) must be correct
			allDesc, err := ct.DescendantIds(context.Background(), 8, 0, tenant2)
			if err != nil {
				t.Fatal(err)
			}
			sort.Slice(allDesc, func(i, j int) bool { return allDesc[i] < allDesc[j] })
			expectedAll := []uint{12, 13}
			if diff := cmp.Diff(expectedAll, allDesc); diff != "" {
				t.Errorf("all descendants of node 8 after move (-want +got):\n%s", diff)
			}

			// Node 7 (Colors) should no longer have node 8 as a descendant
			colorsDesc, err := ct.DescendantIds(context.Background(), 7, 0, tenant2)
			if err != nil {
				t.Fatal(err)
			}
			sort.Slice(colorsDesc, func(i, j int) bool { return colorsDesc[i] < colorsDesc[j] })
			expectedColors := []uint{10, 14}
			if diff := cmp.Diff(expectedColors, colorsDesc); diff != "" {
				t.Errorf("descendants of node 7 after move (-want +got):\n%s", diff)
			}
		})
	}
}

// MultiLevelPayload tests that multi-level embedding works for Add and Update.
type BasePayload struct {
	closuretree.Node
	Description string
}

type MultiLevelPayload struct {
	BasePayload
	Name     string
	Children []*MultiLevelPayload `gorm:"-"`
}

func TestAddMultiLevelEmbed(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, MultiLevelPayload{})
			ct, err := closuretree.New(gdb, MultiLevelPayload{})
			if err != nil {
				t.Fatal(err)
			}

			item := &MultiLevelPayload{
				BasePayload: BasePayload{Description: "base desc"},
				Name:        "top",
			}
			err = ct.Add(context.Background(), item, 0, 0, closuretree.DefaultTenant)
			if err != nil {
				t.Fatal(err)
			}

			// NodeId and Tenant should be written back through the multi-level embedding
			if item.NodeId == 0 {
				t.Error("expected NodeId to be set on multi-level embedded struct, got 0")
			}
			if item.Tenant != closuretree.DefaultTenant {
				t.Errorf("expected Tenant %q, got %q", closuretree.DefaultTenant, item.Tenant)
			}
		})
	}
}

func TestUpdateMultiLevelEmbed(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, MultiLevelPayload{})
			ct, err := closuretree.New(gdb, MultiLevelPayload{})
			if err != nil {
				t.Fatal(err)
			}

			item := &MultiLevelPayload{
				BasePayload: BasePayload{Description: "original"},
				Name:        "original name",
			}
			err = ct.Add(context.Background(), item, 0, 0, closuretree.DefaultTenant)
			if err != nil {
				t.Fatal(err)
			}

			// Update both the top-level and nested embedded field
			err = ct.Update(context.Background(), item.NodeId, MultiLevelPayload{
				BasePayload: BasePayload{Description: "updated"},
				Name:        "updated name",
			}, nil, nil, closuretree.DefaultTenant)
			if err != nil {
				t.Fatal(err)
			}

			got := &MultiLevelPayload{}
			err = ct.GetNode(context.Background(), item.NodeId, closuretree.DefaultTenant, got)
			if err != nil {
				t.Fatal(err)
			}
			if got.Description != "updated" {
				t.Errorf("expected Description %q, got %q", "updated", got.Description)
			}
			if got.Name != "updated name" {
				t.Errorf("expected Name %q, got %q", "updated name", got.Name)
			}
		})
	}
}

func TestRenormalize(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// Build: a(0), b(10), c(20) at root using sequential afterNodeID adds
			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, 0, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			c := &TestPayload{Name: "c"}
			if err := ct.Add(ctx, c, 0, b.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}

			// Insert d before a (sort_order = -10.0), making order: d, a, b, c
			d := &TestPayload{Name: "d"}
			if err := ct.Add(ctx, d, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			// Insert ~60 nodes between a and b to exhaust the float gap and create a low min_halvings meta row
			for i := 0; i < 60; i++ {
				node := &TestPayload{Name: fmt.Sprintf("node_%d", i)}
				if err := ct.Add(ctx, node, 0, a.NodeId, tenant1); err != nil {
					t.Fatal(err)
				}
			}

			// Renormalize root: should reset sort_order and delete the meta row
			if err := ct.Renormalize(ctx, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			// After the Renormalize call, the meta row should be deleted
			var count int64
			gdb.Raw("SELECT COUNT(*) FROM closure_tree_meta_test_payloads WHERE tenant = ? AND parent_id = ?",
				tenant1, uint(0)).Scan(&count)
			if count > 0 {
				t.Error("after Renormalize, meta row should be deleted")
			}

			// Read back and verify nodes are renormalized (all nodes should be present)
			var got []TestPayload
			if err := ct.Descendants(ctx, 0, 1, tenant1, &got); err != nil {
				t.Fatal(err)
			}
			// Verify all nodes are present (d, a, node_59...node_0, b, c)
			expectedCount := 1 + 1 + 60 + 1 + 1 // d, a, 60 nodes, b, c
			if len(got) != expectedCount {
				t.Errorf("expected %d nodes after Renormalize, got %d", expectedCount, len(got))
			}
			// Verify that sort_order values are now properly spaced (multiples of 10)
			// after Renormalize, the first node should have sort_order = 10, second = 20, etc.
			for i, g := range got {
				expectedOrder := float64((i + 1) * 10)
				if g.SortOrder != expectedOrder {
					t.Errorf("node %d (%s): want SortOrder=%v, got %v", i, g.Name, expectedOrder, g.SortOrder)
				}
			}

			// Renormalize on non-existent parent: no-op, no error
			if err := ct.Renormalize(ctx, 9999, tenant1); err != nil {
				t.Errorf("expected no error for non-existent parent, got %v", err)
			}

			// Renormalize parentID=0 with no children of another tenant: no-op
			if err := ct.Renormalize(ctx, 0, tenant2); err != nil {
				t.Errorf("expected no error for tenant with no nodes, got %v", err)
			}
		})
	}
}

func TestDescendantsSortOrder(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// Build tree: root → [a, b, c] with sort orders a=0, b=10, c=20
			// then insert d before all (sort_order = -10.0)
			root := &TestPayload{Name: "root"}
			if err := ct.Add(ctx, root, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, root.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, root.NodeId, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			c := &TestPayload{Name: "c"}
			if err := ct.Add(ctx, c, root.NodeId, b.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			// Insert d before a (sort_order = -10.0)
			d := &TestPayload{Name: "d"}
			if err := ct.Add(ctx, d, root.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			// Descendants of root: expect d, a, b, c (by sort_order)
			var got []TestPayload
			if err := ct.Descendants(ctx, root.NodeId, 1, tenant1, &got); err != nil {
				t.Fatal(err)
			}
			wantNames := []string{"d", "a", "b", "c"}
			gotNames := make([]string, len(got))
			for i, g := range got {
				gotNames[i] = g.Name
			}
			if diff := cmp.Diff(wantNames, gotNames); diff != "" {
				t.Errorf("Descendants order (-want +got):\n%s", diff)
			}

			// DescendantIds: same order
			ids, err := ct.DescendantIds(ctx, root.NodeId, 1, tenant1)
			if err != nil {
				t.Fatal(err)
			}
			wantIDs := []uint{d.NodeId, a.NodeId, b.NodeId, c.NodeId}
			if diff := cmp.Diff(wantIDs, ids); diff != "" {
				t.Errorf("DescendantIds order (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTreeDescendantsSortOrder(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			root := &TestPayload{Name: "root"}
			if err := ct.Add(ctx, root, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, root.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, root.NodeId, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			// Insert c before a (sort_order = -10.0)
			c := &TestPayload{Name: "c"}
			if err := ct.Add(ctx, c, root.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			// TreeDescendants should return children of root in sort order: c, a, b
			var got []*TestPayload
			if err := ct.TreeDescendants(ctx, root.NodeId, 1, tenant1, &got); err != nil {
				t.Fatal(err)
			}
			// Expected: c, a, b (c has lowest sort_order = -10.0)
			wantNames := []string{"c", "a", "b"}
			gotNames := make([]string, len(got))
			for i, g := range got {
				gotNames[i] = g.Name
			}
			if diff := cmp.Diff(wantNames, gotNames); diff != "" {
				t.Errorf("TreeDescendants order (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTreeDescendantsIdsDeterministic(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			populateTree(t, ct)

			// Call TreeDescendantsIds multiple times and verify order is stable
			var first []*closuretree.TreeNode
			for i := 0; i < 10; i++ {
				got, err := ct.TreeDescendantsIds(context.Background(), 7, 0, tenant2)
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					first = got
				} else {
					if diff := cmp.Diff(first, got); diff != "" {
						t.Errorf("iteration %d produced different order (-first +current):\n%s", i, diff)
					}
				}
			}
		})
	}
}

func TestTreeDescendantsIdsSortOrder(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			root := &TestPayload{Name: "root"}
			if err := ct.Add(ctx, root, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, root.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, root.NodeId, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			// c inserted before a (sort_order = -10.0)
			c := &TestPayload{Name: "c"}
			if err := ct.Add(ctx, c, root.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}

			trees, err := ct.TreeDescendantsIds(ctx, root.NodeId, 1, tenant1)
			if err != nil {
				t.Fatal(err)
			}
			// Expected children of root in sort order: c (sort_order=-10), a (sort_order=0), b (sort_order=10)
			if len(trees) != 3 {
				t.Fatalf("want 3 root children, got %d", len(trees))
			}
			wantIDs := []uint{c.NodeId, a.NodeId, b.NodeId}
			for i, tn := range trees {
				if tn.NodeId != wantIDs[i] {
					t.Errorf("index %d: want NodeId %d, got %d", i, wantIDs[i], tn.NodeId)
				}
			}
		})
	}
}

func TestSortTree(t *testing.T) {
	nodes := []*closuretree.TreeNode{
		{NodeId: 3, SortOrder: 20.0},
		{NodeId: 1, SortOrder: 0.0},
		{NodeId: 2, SortOrder: 10.0},
	}
	closuretree.SortTree(nodes)
	wantIDs := []uint{1, 2, 3}
	for i, n := range nodes {
		if n.NodeId != wantIDs[i] {
			t.Errorf("index %d: want NodeId %d, got %d", i, wantIDs[i], n.NodeId)
		}
	}

	// Tie-break by NodeId
	nodes2 := []*closuretree.TreeNode{
		{NodeId: 5, SortOrder: 0.0},
		{NodeId: 3, SortOrder: 0.0},
		{NodeId: 4, SortOrder: 0.0},
	}
	closuretree.SortTree(nodes2)
	wantIDs2 := []uint{3, 4, 5}
	for i, n := range nodes2 {
		if n.NodeId != wantIDs2[i] {
			t.Errorf("tie-break index %d: want NodeId %d, got %d", i, wantIDs2[i], n.NodeId)
		}
	}
}

func TestSortOrderRegression(t *testing.T) {
	// Existing data migrated to the new schema gets sort_order=0 (the column default).
	// Ordering must fall back to node_id ASC when all sort_order values are equal,
	// preserving the pre-feature insertion-order semantics.
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// Add 5 nodes at root in insertion order (append each after the previous).
			names := []string{"first", "second", "third", "fourth", "fifth"}
			var prevID uint
			var nodeIDs []uint
			for _, name := range names {
				item := &TestPayload{Name: name}
				if err := ct.Add(ctx, item, 0, prevID, tenant1); err != nil {
					t.Fatal(err)
				}
				nodeIDs = append(nodeIDs, item.NodeId)
				prevID = item.NodeId
			}

			// Simulate pre-migration data: reset all sort_order to 0.
			// This is what AutoMigrate produces for existing rows.
			if err := gdb.Table("test_payloads").Where("1 = 1").UpdateColumn("sort_order", 0).Error; err != nil {
				t.Fatal(err)
			}

			// With sort_order=0 for all nodes, ordering must fall back to node_id ASC.
			ids, err := ct.DescendantIds(ctx, 0, 1, tenant1)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(nodeIDs, ids); diff != "" {
				t.Errorf("regression: node_id order not preserved when sort_order=0 for all (-want +got):\n%s", diff)
			}
		})
	}
}

//nolint:gocyclo // test function covers many assertion branches
func TestNeedsRenormalize(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// Fresh tree: should not need renormalize at buffer=0 or buffer=15
			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, 0, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}

			for _, buf := range []int{0, closuretree.DefaultHalvingsBuffer} {
				needs, err := ct.NeedsRenormalize(ctx, 0, tenant1, buf)
				if err != nil {
					t.Fatal(err)
				}
				if needs {
					t.Errorf("fresh tree should not need renormalize at buffer=%d", buf)
				}
			}

			// Insert ~37 times between a and next sibling — crosses buffer=15 threshold
			prevID := a.NodeId
			for i := 0; i < 37; i++ {
				node := &TestPayload{Name: fmt.Sprintf("n%d", i)}
				if err := ct.Add(ctx, node, 0, prevID, tenant1); err != nil {
					t.Fatal(err)
				}
				prevID = node.NodeId
			}

			// buffer=15: should now warn
			needs, err := ct.NeedsRenormalize(ctx, 0, tenant1, closuretree.DefaultHalvingsBuffer)
			if err != nil {
				t.Fatal(err)
			}
			if !needs {
				t.Error("after ~37 halvings, NeedsRenormalize(buffer=15) should return true")
			}

			// buffer=0: should NOT warn yet (not truly exhausted)
			needs, err = ct.NeedsRenormalize(ctx, 0, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if needs {
				t.Error("after ~37 halvings, NeedsRenormalize(buffer=0) should still be false")
			}

			// Exhaust completely: insert ~20 more times
			for i := 37; i < 60; i++ {
				node := &TestPayload{Name: fmt.Sprintf("n%d", i)}
				if err := ct.Add(ctx, node, 0, prevID, tenant1); err != nil {
					t.Fatal(err)
				}
				prevID = node.NodeId
			}

			// buffer=0: now truly exhausted
			needs, err = ct.NeedsRenormalize(ctx, 0, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !needs {
				t.Error("after ~60 halvings, NeedsRenormalize(buffer=0) should return true")
			}

			// After Renormalize, false at any buffer
			if err := ct.Renormalize(ctx, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			needs, err = ct.NeedsRenormalize(ctx, 0, tenant1, closuretree.DefaultHalvingsBuffer)
			if err != nil {
				t.Fatal(err)
			}
			if needs {
				t.Error("after Renormalize, NeedsRenormalize should return false")
			}

			// Non-existent parent: false, no error
			needs, err = ct.NeedsRenormalize(ctx, 9999, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if needs {
				t.Error("non-existent parentID should return false")
			}
		})
	}
}

func TestNeedsRenormalizeAny(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// Fresh tree: false
			needs, err := ct.NeedsRenormalizeAny(ctx, tenant1, closuretree.DefaultHalvingsBuffer)
			if err != nil {
				t.Fatal(err)
			}
			if needs {
				t.Error("fresh tree should return false")
			}

			// Build tree: parentA at root, parentB at root, a1 and a2 as children of parentA
			parentA := &TestPayload{Name: "parentA"}
			if err := ct.Add(ctx, parentA, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			parentB := &TestPayload{Name: "parentB"}
			if err := ct.Add(ctx, parentB, 0, parentA.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			a1 := &TestPayload{Name: "a1"}
			if err := ct.Add(ctx, a1, parentA.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			a2 := &TestPayload{Name: "a2"}
			if err := ct.Add(ctx, a2, parentA.NodeId, a1.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}

			// Exhaust children of parentA by inserting 60 times between a1 and a2
			prev := a1.NodeId
			for i := 0; i < 60; i++ {
				node := &TestPayload{Name: fmt.Sprintf("x%d", i)}
				if err := ct.Add(ctx, node, parentA.NodeId, prev, tenant1); err != nil {
					t.Fatal(err)
				}
				prev = node.NodeId
			}

			// parentA's children are exhausted; root children are still healthy
			needsA, err := ct.NeedsRenormalize(ctx, parentA.NodeId, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !needsA {
				t.Error("parentA children should be exhausted")
			}
			needsRoot, err := ct.NeedsRenormalize(ctx, 0, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if needsRoot {
				t.Error("root children should still be healthy")
			}

			// NeedsRenormalizeAny should detect the problem in parentA
			any, err := ct.NeedsRenormalizeAny(ctx, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !any {
				t.Error("NeedsRenormalizeAny should return true when any group is exhausted")
			}

			// After renormalizing parentA, NeedsRenormalizeAny should return false
			if err := ct.Renormalize(ctx, parentA.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			any, err = ct.NeedsRenormalizeAny(ctx, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if any {
				t.Error("after Renormalize, NeedsRenormalizeAny should return false")
			}
		})
	}
}

//nolint:gocyclo // test function covers many assertion branches
func TestRenormalizeAll(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			// Fresh tree: RenormalizeAll returns 0, no error
			n, err := ct.RenormalizeAll(ctx, tenant1, closuretree.DefaultHalvingsBuffer)
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Errorf("expected 0 groups renormalized on empty tree, got %d", n)
			}

			// Build two independent sibling groups:
			//   Group 1: root children (parentID=0) — rootA, rootB
			//   Group 2: children of rootA (parentID=rootA.NodeId) — childA1, childA2
			rootA := &TestPayload{Name: "rootA"}
			if err := ct.Add(ctx, rootA, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			rootB := &TestPayload{Name: "rootB"}
			if err := ct.Add(ctx, rootB, 0, rootA.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}
			childA1 := &TestPayload{Name: "childA1"}
			if err := ct.Add(ctx, childA1, rootA.NodeId, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			childA2 := &TestPayload{Name: "childA2"}
			if err := ct.Add(ctx, childA2, rootA.NodeId, childA1.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}

			// Exhaust Group 1 (root children): insert 60 times between rootA and rootB
			prev := rootA.NodeId
			for i := 0; i < 60; i++ {
				node := &TestPayload{Name: fmt.Sprintf("rn%d", i)}
				if err := ct.Add(ctx, node, 0, prev, tenant1); err != nil {
					t.Fatal(err)
				}
				prev = node.NodeId
			}

			// Exhaust Group 2 (children of rootA): insert 60 times between childA1 and childA2
			prev = childA1.NodeId
			for i := 0; i < 60; i++ {
				node := &TestPayload{Name: fmt.Sprintf("cn%d", i)}
				if err := ct.Add(ctx, node, rootA.NodeId, prev, tenant1); err != nil {
					t.Fatal(err)
				}
				prev = node.NodeId
			}

			// Both groups should be exhausted
			anyNeeds, err := ct.NeedsRenormalizeAny(ctx, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !anyNeeds {
				t.Fatal("expected at least one group to need renormalization before RenormalizeAll")
			}

			// RenormalizeAll at buffer=0 should fix both exhausted groups
			n, err = ct.RenormalizeAll(ctx, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if n < 2 {
				t.Errorf("expected at least 2 groups renormalized, got %d", n)
			}

			// After RenormalizeAll, NeedsRenormalizeAny should return false
			anyNeeds, err = ct.NeedsRenormalizeAny(ctx, tenant1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if anyNeeds {
				t.Error("after RenormalizeAll, NeedsRenormalizeAny should return false")
			}

			// Nodes are still retrievable after renormalization
			var rootChildren []TestPayload
			if err := ct.Descendants(ctx, 0, 1, tenant1, &rootChildren); err != nil {
				t.Fatal(err)
			}
			if len(rootChildren) == 0 {
				t.Error("root children should still exist after RenormalizeAll")
			}
		})
	}
}

func TestMetaWrittenOnAdd(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			gdb := connAndClose(t, db)
			dropTreeTables(gdb, TestPayload{})
			ct, err := closuretree.New(gdb, TestPayload{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			a := &TestPayload{Name: "a"}
			if err := ct.Add(ctx, a, 0, 0, tenant1); err != nil {
				t.Fatal(err)
			}
			b := &TestPayload{Name: "b"}
			if err := ct.Add(ctx, b, 0, a.NodeId, tenant1); err != nil {
				t.Fatal(err)
			}

			// After two adds, meta row for (tenant1, parentID=0) must exist
			var minH int
			err = gdb.Raw("SELECT min_halvings FROM closure_tree_meta_test_payloads WHERE tenant = ? AND parent_id = ?",
				tenant1, 0).Scan(&minH).Error
			if err != nil {
				t.Fatalf("meta row not found: %v", err)
			}
			if minH < 40 {
				t.Errorf("expected min_halvings >= 40 for fresh tree, got %d", minH)
			}
		})
	}
}
