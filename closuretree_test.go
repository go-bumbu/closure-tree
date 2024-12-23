package closuretree_test

import (
	"fmt"
	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
	"reflect"
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

type TagComposition struct {
	closuretree.Branch
	Name string
}

func TestTreeIntegration(t *testing.T) {
	dbs := getTargetDBs(t)
	for dbName, db := range dbs {
		t.Run(dbName, func(t *testing.T) {
			t.Run("addNodesNoErr", func(t *testing.T) {
				testAddNodesNoErrs(db, t)
			})
			t.Run("get descendants", func(t *testing.T) {
				testGetDescendants(t, db)
			})
			t.Run("move", func(t *testing.T) {
				testMove(t, db)
			})
		})
	}
}

func testAddNodesNoErrs(db *gorm.DB, t *testing.T) {
	var ct *closuretree.Tree
	var err error

	ct, err = closuretree.New(db, TagComposition{}, "IT_add1")
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range testTree {
		tagItem := TagComposition{
			Name: item.name,
			Branch: closuretree.Branch{
				BranchId: item.id,
			},
		}

		err = ct.Add(tagItem, item.parent)
		if err != nil {
			t.Fatal(err)
		}
	}

}

func testGetDescendants(t *testing.T, db *gorm.DB) {

	var ct *closuretree.Tree
	var err error

	ct, err = closuretree.New(db, TagComposition{}, "IT_descendant1")
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range testTree {
		tagItem := TagComposition{
			Name: item.name,
			Branch: closuretree.Branch{
				BranchId: item.id,
			},
		}

		err = ct.Add(tagItem, item.parent)
		if err != nil {
			t.Fatal(err)
		}
	}

	gotTags := []TagComposition{}

	err = ct.Descendants(1, &gotTags)
	if err != nil {
		t.Fatal(err)
	}
	want := []TagComposition{
		{Name: "Electronics", Branch: closuretree.Branch{BranchId: 1}},
		{Name: "Mobile Phones", Branch: closuretree.Branch{BranchId: 2}},
		{Name: "Laptops", Branch: closuretree.Branch{BranchId: 4}},
		{Name: "Touch Screen", Branch: closuretree.Branch{BranchId: 6}},
	}

	if diff := cmp.Diff(gotTags, want); diff != "" {
		t.Errorf("unexpected result (-want +got):\n%s", diff)
	}

	t.Run("descendantIds", func(t *testing.T) {
		var ct *closuretree.Tree
		var err error

		ct, err = closuretree.New(db, TagComposition{}, "IT_descendant3")
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range testTree {
			tagItem := TagComposition{
				Branch: closuretree.Branch{
					BranchId: item.id,
				},
				Name: item.name,
			}

			err = ct.Add(tagItem, item.parent)
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := ct.DescendantIds(1)
		if err != nil {
			t.Fatal(err)
		}
		want := []uint{1, 2, 4, 6}

		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("unexpected result (-want +got):\n%s", diff)
		}
	})

}

func testMove(t *testing.T, db *gorm.DB) {

	t.Run("parent Note", func(t *testing.T) {
		var ct *closuretree.Tree
		var err error

		ct, err = closuretree.New(db, TagComposition{}, "IT_move1")
		if err != nil {
			t.Fatal(err)
		}

		for _, item := range testTree {
			tagItem := TagComposition{
				Name: item.name,
				Branch: closuretree.Branch{
					BranchId: item.id,
				},
			}

			err = ct.Add(tagItem, item.parent)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = ct.Move(3, 4)
		if err != nil {
			t.Fatal(err)
		}

		got, err := ct.DescendantIds(4)
		if err != nil {
			t.Fatal(err)
		}
		want := []uint{4, 3, 5}
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

		ct, err = closuretree.New(db, TagComposition{}, "move2")
		if err != nil {
			t.Fatal(err)
		}

		for _, item := range testTree {
			tagItem := TagComposition{
				Name: item.name,
				Branch: closuretree.Branch{
					BranchId: item.id,
				},
			}

			err = ct.Add(tagItem, item.parent)
			if err != nil {
				t.Fatal(err)
			}
		}

		err = ct.Move(2, 5)
		if err != nil {
			t.Fatal(err)
		}

		// tree where it was moved to
		got, err := ct.DescendantIds(3)
		if err != nil {
			t.Fatal(err)
		}
		want := []uint{3, 5, 2, 6}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("unexpected result (-want +got):\n%s", diff)
		}

		// tree it was moved from
		got, err = ct.DescendantIds(1)
		if err != nil {
			t.Fatal(err)
		}
		want = []uint{1, 4}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("unexpected result (-want +got):\n%s", diff)
		}

	})
}

func TestAdd(t *testing.T) {

	type SampleStruct struct {
		closuretree.Branch
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
			topItem:        &SampleStruct{Name: "Simple"},
			childItem:      &SampleStruct{Name: "Simple2"},
			expectParentId: 1,
			expectChildId:  2,
		},
		{
			name:           "struct with ID field",
			topItem:        SampleStruct{Name: "Simple"},
			childItem:      SampleStruct{Name: "Simple2"},
			expectParentId: 0, // ids are not populated because it's not a pointer
			expectChildId:  0,
		},
		{
			name:      "Struct without ID field",
			topItem:   &struct{ Name string }{Name: "NoID"},
			expectErr: closuretree.ItemIsNotBranchErr.Error(),
		},
	}

	dbs := getTargetDBs(t)
	for dbName, db := range dbs {
		t.Run(dbName, func(t *testing.T) {
			for i, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					var ct *closuretree.Tree
					var err error

					ct, _ = closuretree.New(db, tc.topItem, fmt.Sprintf("UT_add_%d", i))

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

func getId(item any) (bool, uint64) {

	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = itemValue.Elem()
	}
	if itemValue.Kind() != reflect.Struct {
		return false, 0
	}

	idField := itemValue.FieldByName("BranchId")
	if !idField.IsValid() {
		// Look for the "ID" field in embedded structs
		for i := 0; i < itemValue.NumField(); i++ {
			field := itemValue.Field(i)
			if field.Kind() == reflect.Struct {
				idField = field.FieldByName("BranchId")
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
