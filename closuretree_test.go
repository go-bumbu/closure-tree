package closuretree_test

import (
	"context"
	"fmt"
	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/go-bumbu/testdbs"
	"github.com/google/go-cmp/cmp"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
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

type NodeDetails struct {
	Id     int
	Tenant string
	Err    string
}

const tenant1 = "t1"
const tenant2 = "t2"

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
func TestAddNodes(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			type SampleStruct struct {
				closuretree.Node
				Name string
			}

			tcs := []struct {
				name           string
				topItem        any
				topItemDetails NodeDetails
				topItemExpect  NodeDetails

				childItem        any
				childItemDetails NodeDetails
				childItemExpect  NodeDetails
			}{
				{
					name:            "Pointer to struct with ID field",
					topItem:         &SampleStruct{Name: "Sample"},
					childItem:       &SampleStruct{Name: "Sample2"},
					topItemExpect:   NodeDetails{Id: 1, Tenant: closuretree.DefaultTenant},
					childItemExpect: NodeDetails{Id: 2, Tenant: closuretree.DefaultTenant},
				},
				{
					name:      "struct with ID field",
					topItem:   SampleStruct{Name: "Sample"},
					childItem: SampleStruct{Name: "Sample2"},
					// values should not be populated because it's not a pointer
					topItemExpect:   NodeDetails{Id: 0, Tenant: ""},
					childItemExpect: NodeDetails{Id: 0, Tenant: ""},
				},
				{
					name:            "Ensure embedded NodeId is ignored",
					topItem:         &SampleStruct{Name: "Sample"},
					childItem:       &SampleStruct{Name: "Sample2", Node: closuretree.Node{NodeId: 4}},
					topItemExpect:   NodeDetails{Id: 1, Tenant: closuretree.DefaultTenant},
					childItemExpect: NodeDetails{Id: 2, Tenant: closuretree.DefaultTenant},
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
					name:            "Ensure embedded Tenant is ignored",
					topItem:         &SampleStruct{Name: "Sample"},
					childItem:       &SampleStruct{Name: "Sample2", Node: closuretree.Node{Tenant: "bla"}},
					topItemExpect:   NodeDetails{Id: 1, Tenant: closuretree.DefaultTenant},
					childItemExpect: NodeDetails{Id: 2, Tenant: closuretree.DefaultTenant},
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
					name:            "Struct without ID field",
					topItem:         &struct{ Name string }{Name: "NoID"},
					topItemExpect:   NodeDetails{Err: closuretree.ErrItemIsNotTreeNode.Error()},
					childItemExpect: NodeDetails{Err: closuretree.ErrItemIsNotTreeNode.Error()},
				},
			}

			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					var ct *closuretree.Tree
					var err error

					ct, _ = closuretree.New(db.ConnDbName(fmt.Sprintf("addnodes%d", i)), tc.topItem)

					// add topItem as parent
					err = ct.Add(context.Background(), tc.topItem, 0, tc.topItemDetails.Tenant)
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
					err = ct.Add(context.Background(), tc.childItem, 1, tc.childItemDetails.Tenant)
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

		err := ct.Add(context.Background(), tagItem, item.parent, tenant1)
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

		err := ct.Add(context.Background(), tagItem, item.parent, tenant2)
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

			ct, err = closuretree.New(db.ConnDbName("populatetree"), TestPayload{})
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
			var setupOnce sync.Once
			var ct *closuretree.Tree
			setup := func(t *testing.T) {
				var err error
				setupOnce.Do(func() {
					ct, err = closuretree.New(db.ConnDbName(t.Name()), TestPayload{})
					if err != nil {
						t.Fatal(err)
					}
					populateTree(t, ct)
				})
			}
			tcs := []struct {
				name        string
				nodeID      uint
				in          any
				wantPayload TestPayload
				tenant      string
				wantErr     string
			}{
				{
					name:        "get root node for tenant 1",
					nodeID:      1,
					in:          &TestPayload{},
					wantPayload: TestPayload{Name: "Electronics", Node: closuretree.Node{NodeId: 1, Tenant: tenant1}},
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
					name:        "expect err because of wrong type",
					nodeID:      7,
					in:          &map[string]string{},
					wantPayload: TestPayload{},
					tenant:      tenant1,
					wantErr:     closuretree.ErrItemIsNotTreeNode.Error(),
				},
				{
					name:        "expect err because not passing pointer",
					nodeID:      7,
					in:          TestPayload{},
					wantPayload: TestPayload{},
					tenant:      tenant1,
					wantErr:     "item needs to be a pointer to a struct",
				},
				{
					name:        "empty result on wrong Tenant",
					nodeID:      7,
					in:          &TestPayload{},
					wantPayload: TestPayload{},
					tenant:      tenant1,
					wantErr:     closuretree.ErrNodeNotFound.Error(),
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					setup(t)
					err := ct.GetNode(context.Background(), tc.nodeID, tc.tenant, tc.in)

					if tc.wantErr != "" {
						if err == nil {
							t.Fatalf("expected error: %s, but got none ", tc.wantErr)
						}
						if err.Error() != tc.wantErr {
							t.Errorf("expected error: %s, but got %v ", tc.wantErr, err.Error())
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
			var setupOnce sync.Once
			var ct *closuretree.Tree
			setup := func(t *testing.T) {
				var err error
				setupOnce.Do(func() {
					ct, err = closuretree.New(db.ConnDbName(t.Name()), TestPayload{})
					if err != nil {
						t.Fatal(err)
					}
					populateTree(t, ct)
				})
			}
			tcs := []struct {
				name        string
				nodeID      uint
				in          any
				wantPayload TestPayload
				tenant      string
				wantErr     string
			}{
				{
					name:        "get root node for tenant 1",
					nodeID:      1,
					in:          TestPayload{Name: "Banana"},
					wantPayload: TestPayload{Name: "Banana", Node: closuretree.Node{NodeId: 1, Tenant: tenant1}},
					tenant:      tenant1,
				},
				{
					name:        "expect err because of wrong type",
					nodeID:      7,
					in:          &map[string]string{},
					wantPayload: TestPayload{},
					tenant:      tenant1,
					wantErr:     closuretree.ErrItemIsNotTreeNode.Error(),
				},
				{
					name:        "empty result on wrong Tenant",
					nodeID:      7,
					in:          TestPayload{Name: "Banana"},
					wantPayload: TestPayload{},
					tenant:      tenant1,
					wantErr:     closuretree.ErrNodeNotFound.Error(),
				},
			}
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					setup(t)
					err := ct.Update(context.Background(), tc.nodeID, tc.in, tc.tenant)
					if tc.wantErr != "" {
						if err == nil {
							t.Fatalf("expected error: %s, but got none ", tc.wantErr)
						}
						if err.Error() != tc.wantErr {
							t.Errorf("expected error: %s, but got %v ", tc.wantErr, err.Error())
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

func TestGetDescendants(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			var setupOnce sync.Once
			var ct *closuretree.Tree
			setup := func(t *testing.T) {
				var err error
				setupOnce.Do(func() {
					ct, err = closuretree.New(db.ConnDbName(t.Name()), TestPayload{})
					if err != nil {
						t.Fatal(err)
					}
					populateTree(t, ct)
				})
			}
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
						{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2, ParentId: 1, Tenant: tenant1}},
						{Name: "Laptops", Node: closuretree.Node{NodeId: 4, ParentId: 1, Tenant: tenant1}},
						{Name: "Touch Screen", Node: closuretree.Node{NodeId: 6, ParentId: 2, Tenant: tenant1}},
					},
					wantIds: []uint{2, 4, 6},
					tenant:  tenant1,
				},
				{
					name:   "get descendants on Tenant 2",
					parent: 7,
					depth:  0,
					wantPayload: []TestPayload{
						{Name: "Warm", Node: closuretree.Node{NodeId: 8, ParentId: 7, Tenant: tenant2}},
						{Name: "Cold", Node: closuretree.Node{NodeId: 10, ParentId: 7, Tenant: tenant2}},
						{Name: "Red", Node: closuretree.Node{NodeId: 12, ParentId: 8, Tenant: tenant2}},
						{Name: "Orange", Node: closuretree.Node{NodeId: 13, ParentId: 8, Tenant: tenant2}},
						{Name: "Blue", Node: closuretree.Node{NodeId: 14, ParentId: 10, Tenant: tenant2}},
					},
					wantIds: []uint{8, 10, 12, 13, 14},
					tenant:  tenant2,
				},
				{
					name:   "get root items for tenant 1",
					parent: 0,
					depth:  1,
					wantPayload: []TestPayload{
						{Name: "Electronics", Node: closuretree.Node{NodeId: 1, Tenant: tenant1}},
						{Name: "Clothing", Node: closuretree.Node{NodeId: 3, Tenant: tenant1}},
					},
					wantIds: []uint{1, 3},
					tenant:  tenant1,
				},
				{
					name:   "get root items for tenant 2",
					parent: 0,
					depth:  1,
					wantPayload: []TestPayload{
						{Name: "Colors", Node: closuretree.Node{NodeId: 7, Tenant: tenant2}},
						{Name: "Sizes", Node: closuretree.Node{NodeId: 9, Tenant: tenant2}},
					},
					wantIds: []uint{7, 9},
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
					setup(t)
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
			var setupOnce sync.Once
			var ct *closuretree.Tree
			setup := func(t *testing.T) {
				var err error
				setupOnce.Do(func() {
					ct, err = closuretree.New(db.ConnDbName(t.Name()), TestPayload{})
					if err != nil {
						t.Fatal(err)
					}
					populateTree(t, ct)
				})
			}
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
						{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2, Tenant: tenant1},
							Children: []*TestPayload{
								{Name: "Touch Screen", Node: closuretree.Node{NodeId: 6, Tenant: tenant1}},
							},
						},
						{Name: "Laptops", Node: closuretree.Node{NodeId: 4, Tenant: tenant1}},
					},
					wantIds: []*closuretree.TreeNode{
						{
							NodeId: 2, AncestorID: 1,
							Children: []*closuretree.TreeNode{
								{NodeId: 6, AncestorID: 2},
							},
						},
						{NodeId: 4, AncestorID: 1},
					},
					tenant: tenant1,
				},
				{
					name:   "get descendants on Tenant 2",
					parent: 7,
					depth:  0,
					wantPayload: []*TestPayload{
						{Name: "Warm", Node: closuretree.Node{NodeId: 8, Tenant: tenant2},
							Children: []*TestPayload{
								{Name: "Red", Node: closuretree.Node{NodeId: 12, Tenant: tenant2}},
								{Name: "Orange", Node: closuretree.Node{NodeId: 13, Tenant: tenant2}},
							}},
						{Name: "Cold", Node: closuretree.Node{NodeId: 10, Tenant: tenant2},
							Children: []*TestPayload{
								{Name: "Blue", Node: closuretree.Node{NodeId: 14, Tenant: tenant2}},
							}},
					},
					//wantIds: []uint{8, 10, 12, 13, 14},
					wantIds: []*closuretree.TreeNode{
						{
							NodeId: 8, AncestorID: 7,
							Children: []*closuretree.TreeNode{
								{NodeId: 12, AncestorID: 8},
								{NodeId: 13, AncestorID: 8},
							},
						},
						{
							NodeId: 10, AncestorID: 7,
							Children: []*closuretree.TreeNode{
								{NodeId: 14, AncestorID: 10},
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
						{Name: "Clothing", Node: closuretree.Node{NodeId: 3, Tenant: tenant1}},
					},
					wantIds: []*closuretree.TreeNode{
						{NodeId: 1, AncestorID: 0},
						{NodeId: 3, AncestorID: 0},
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
					setup(t)
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
				ct, err := closuretree.New(db.ConnDbName(t.Name()), TestPayload{})
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
				wantErr string
			}{
				{
					name:   "move a parent node on Tenant 1",
					origin: 3,
					dest:   4,
					wantIds: []idCheck{
						{parent: 4, tenant: tenant1, want: []uint{3, 5}},
					},
					tenant: tenant1,
				},
				{
					name:   "move a child node on Tenant 1",
					origin: 2,
					dest:   5,
					wantIds: []idCheck{
						{parent: 3, tenant: tenant1, want: []uint{5, 2, 6}},
						{parent: 1, tenant: tenant1, want: []uint{4}},
					},
					tenant: tenant1,
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
					wantErr: closuretree.ErrNodeNotFound.Error(),
					tenant:  tenant1,
				},
			}

			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					ct := setup(t, fmt.Sprintf("IT_move_%d", i))
					err := ct.Move(context.Background(), tc.origin, tc.dest, tc.tenant)
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

						if diff := cmp.Diff(got, checkId.want); diff != "" {
							t.Errorf("unexpected result (-want +got):\n%s", diff)
						}
					}

				})
			}
		})
	}
}
func TestDelete(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			setup := func(t *testing.T, name string) *closuretree.Tree {
				ct, err := closuretree.New(db.ConnDbName(t.Name()), TestPayload{})
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
					wantErr: "node not found",
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
