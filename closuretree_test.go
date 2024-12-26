package closuretree_test

import (
	"fmt"
	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/google/go-cmp/cmp"
	"reflect"
	"sort"
	"sync"
	"testing"
)

// This represents a tree like:
// 0 - root
// 1 -  | - Electronics
// 2 -  |     | -  Mobile Phones
// 6 -  |     |      |  - Touch Screen
// 4 -  |     | -  Laptops
// 3 -  | - Clothing
// 5 -  |     | -  T-Shirt

// key us id, value is parent
type treeData struct {
	id     uint
	parent uint
	name   string
}

var testTree = []treeData{
	{1, 0, "Electronics"},
	{2, 1, "Mobile Phones"},
	{3, 0, "Clothing"},
	{4, 1, "Laptops"},
	{5, 3, "T-Shirt"},
	{6, 2, "Touch Screen"},
}

type TestPayload struct {
	closuretree.Node
	Name string
}

func TestAddNodes(t *testing.T) {
	for _, db := range targetDBs {
		t.Run(db.name, func(t *testing.T) {
			type SampleStruct struct {
				closuretree.Node
				Name string
			}

			tcs := []struct {
				name           string
				topItem        any
				childItem      any
				expectErr      string
				expectParentId uint64
				expectChildId  uint64
			}{
				{
					name:           "Pointer to struct with ID field",
					topItem:        &SampleStruct{Name: "Sample"},
					childItem:      &SampleStruct{Name: "Sample2"},
					expectParentId: 1,
					expectChildId:  2,
				},
				{
					name:           "struct with ID field",
					topItem:        SampleStruct{Name: "Sample"},
					childItem:      SampleStruct{Name: "Sample2"},
					expectParentId: 0, // ids are not populated because it's not a pointer
					expectChildId:  0,
				},
				{
					name:      "Struct without ID field",
					topItem:   &struct{ Name string }{Name: "NoID"},
					expectErr: closuretree.ItemIsNotBranchErr.Error(),
				},
			}

			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					var ct *closuretree.Tree
					var err error

					ct, _ = closuretree.New(db.conn, tc.topItem, fmt.Sprintf("IT_add_%d", i))

					// add topItem as parent
					err = ct.Add(tc.topItem, 0)
					if tc.expectErr != "" {
						if err == nil {
							t.Error("expecting an error but got none")
						}
						if diff := cmp.Diff(err.Error(), tc.expectErr); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
					} else {
						hasId, idValue := getId(tc.topItem)
						if !hasId || idValue != tc.expectParentId {
							t.Errorf("ID was not set correctly, got %v", idValue)
						}
					}

					// add childItem to parent
					err = ct.Add(tc.childItem, 1)
					if tc.expectErr != "" {
						if err == nil {
							t.Error("expecting an error but got none")
						}
						if diff := cmp.Diff(err.Error(), tc.expectErr); diff != "" {
							t.Errorf("unexpected error (-want +got):\n%s", diff)
						}
					} else {
						hasId, idValue := getId(tc.childItem)
						if !hasId || idValue != tc.expectChildId {
							t.Errorf("ID was not set correctly, got %v", idValue)
						}
					}
				})
			}
		})
	}
}
func TestPopulateTree(t *testing.T) {
	for _, db := range targetDBs {
		t.Run(db.name, func(t *testing.T) {
			var ct *closuretree.Tree
			var err error

			ct, err = closuretree.New(db.conn, TestPayload{}, "IT_populate_tree")
			if err != nil {
				t.Fatal(err)
			}

			for _, item := range testTree {
				tagItem := TestPayload{
					Name: item.name,
					Node: closuretree.Node{
						NodeId: item.id,
					},
				}

				err = ct.Add(tagItem, item.parent)
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestDescendants(t *testing.T) {
	for _, db := range targetDBs {
		t.Run(db.name, func(t *testing.T) {

			var setupOnce sync.Once
			var ct *closuretree.Tree
			setup := func(t *testing.T) {
				var err error
				setupOnce.Do(func() {
					ct, err = closuretree.New(db.conn, TestPayload{}, "IT_descendant")
					if err != nil {
						t.Fatal(err)
					}

					for _, item := range testTree {
						tagItem := TestPayload{
							Name: item.name,
							Node: closuretree.Node{
								NodeId: item.id,
							},
						}

						err = ct.Add(tagItem, item.parent)
						if err != nil {
							t.Fatal(err)
						}
					}
				})
			}

			t.Run("get all descendants", func(t *testing.T) {
				setup(t)

				gotTags := []TestPayload{}
				err := ct.Descendants(1, 0, &gotTags)
				if err != nil {
					t.Fatal(err)
				}
				want := []TestPayload{
					{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2}},
					{Name: "Laptops", Node: closuretree.Node{NodeId: 4}},
					{Name: "Touch Screen", Node: closuretree.Node{NodeId: 6}},
				}

				if diff := cmp.Diff(gotTags, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})

			t.Run("get only direct children", func(t *testing.T) {
				setup(t)

				gotTags := []TestPayload{}
				err := ct.Descendants(1, 1, &gotTags)
				if err != nil {
					t.Fatal(err)
				}
				want := []TestPayload{
					{Name: "Mobile Phones", Node: closuretree.Node{NodeId: 2}},
					{Name: "Laptops", Node: closuretree.Node{NodeId: 4}},
				}

				if diff := cmp.Diff(gotTags, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})

			t.Run("get all descendant Ids", func(t *testing.T) {
				setup(t)

				got, err := ct.DescendantIds(1, 0)
				if err != nil {
					t.Fatal(err)
				}
				want := []uint{2, 4, 6}

				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})
			t.Run("get only direct children Ids", func(t *testing.T) {
				setup(t)

				got, err := ct.DescendantIds(1, 1)
				if err != nil {
					t.Fatal(err)
				}
				want := []uint{2, 4}

				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})
		})
	}
}
func TestGetRoot(t *testing.T) {
	for _, db := range targetDBs {
		t.Run(db.name, func(t *testing.T) {
			var setupOnce sync.Once
			var ct *closuretree.Tree
			setup := func(t *testing.T) {
				var err error
				setupOnce.Do(func() {
					ct, err = closuretree.New(db.conn, TestPayload{}, "IT_roots")
					if err != nil {
						t.Fatal(err)
					}

					for _, item := range testTree {
						tagItem := TestPayload{
							Name: item.name,
							Node: closuretree.Node{
								NodeId: item.id,
							},
						}

						err = ct.Add(tagItem, item.parent)
						if err != nil {
							t.Fatal(err)
						}
					}
				})
			}

			t.Run("get all roots", func(t *testing.T) {
				setup(t)

				gotTags := []TestPayload{}

				err := ct.Roots(&gotTags)
				if err != nil {
					t.Fatal(err)
				}
				want := []TestPayload{
					{Name: "Electronics", Node: closuretree.Node{NodeId: 1}},
					{Name: "Clothing", Node: closuretree.Node{NodeId: 3}},
				}

				if diff := cmp.Diff(gotTags, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})

			t.Run("get all root ids", func(t *testing.T) {
				setup(t)

				got, err := ct.RootIds()
				if err != nil {
					t.Fatal(err)
				}
				// some databases return results non sorted
				sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })

				want := []uint{1, 3}
				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})

		})
	}
}
func TestMove(t *testing.T) {
	for _, db := range targetDBs {
		t.Run(db.name, func(t *testing.T) {

			t.Run("parent Note", func(t *testing.T) {
				var ct *closuretree.Tree
				var err error

				ct, err = closuretree.New(db.conn, TestPayload{}, "IT_move1")
				if err != nil {
					t.Fatal(err)
				}
				populateTree(t, ct)

				err = ct.Move(3, 4)
				if err != nil {
					t.Fatal(err)
				}

				got, err := ct.DescendantIds(4, 0)
				if err != nil {
					t.Fatal(err)
				}
				want := []uint{3, 5}
				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})

			t.Run("child node", func(t *testing.T) {

				// expect a tree like this:
				// 0 - root
				// 1 -  | - Electronics
				// 4 -  |     | -  Laptops
				// 3 -  | - Clothing
				// 5 -  |     | -  T-Shirt
				// 2 -  |     |      | -  Mobile Phones
				// 6 -  |     |      |      |  - Touch Screen

				var ct *closuretree.Tree
				var err error

				ct, err = closuretree.New(db.conn, TestPayload{}, "IT_move2")
				if err != nil {
					t.Fatal(err)
				}
				populateTree(t, ct)

				err = ct.Move(2, 5)
				if err != nil {
					t.Fatal(err)
				}

				// tree where it was moved to
				got, err := ct.DescendantIds(3, 0)
				if err != nil {
					t.Fatal(err)
				}
				want := []uint{5, 2, 6}
				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}

				// tree it was moved from
				got, err = ct.DescendantIds(1, 0)
				if err != nil {
					t.Fatal(err)
				}
				want = []uint{4}
				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}

			})

		})
	}
}
func TestDelete(t *testing.T) {
	for _, db := range targetDBs {
		t.Run(db.name, func(t *testing.T) {
			t.Run("parent Note", func(t *testing.T) {
				var ct *closuretree.Tree
				var err error

				ct, err = closuretree.New(db.conn, TestPayload{}, "IT_delete1")
				if err != nil {
					t.Fatal(err)
				}
				populateTree(t, ct)

				err = ct.DeleteRecurse(1)
				if err != nil {
					t.Fatal(err)
				}

				got, err := ct.RootIds()
				if err != nil {
					t.Fatal(err)
				}
				want := []uint{3}
				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}
			})

			t.Run("child node", func(t *testing.T) {

				var ct *closuretree.Tree
				var err error

				ct, err = closuretree.New(db.conn, TestPayload{}, "IT_delete2")
				if err != nil {
					t.Fatal(err)
				}
				populateTree(t, ct)

				err = ct.DeleteRecurse(2)
				if err != nil {
					t.Fatal(err)
				}

				got, err := ct.DescendantIds(1, 0)
				if err != nil {
					t.Fatal(err)
				}
				want := []uint{4}
				if diff := cmp.Diff(got, want); diff != "" {
					t.Errorf("unexpected result (-want +got):\n%s", diff)
				}

			})
		})
	}
}

func populateTree(t *testing.T, ct *closuretree.Tree) {
	for _, item := range testTree {
		tagItem := TestPayload{
			Name: item.name,
			Node: closuretree.Node{
				NodeId: item.id,
			},
		}

		err := ct.Add(tagItem, item.parent)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func getId(item any) (bool, uint64) {

	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = itemValue.Elem()
	}
	if itemValue.Kind() != reflect.Struct {
		return false, 0
	}

	idField := itemValue.FieldByName("NodeId")
	if !idField.IsValid() {
		// Look for the "ID" field in embedded structs
		for i := 0; i < itemValue.NumField(); i++ {
			field := itemValue.Field(i)
			if field.Kind() == reflect.Struct {
				idField = field.FieldByName("NodeId")
				if idField.IsValid() {
					break
				}
			}
		}
	}
	if idField.IsValid() && idField.Kind() == reflect.Uint {
		return true, idField.Uint()
	}
	return false, 0

}
