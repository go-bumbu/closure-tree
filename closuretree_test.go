package closuretree_test

import (
	closuretree "github.com/go-bumbu/closure-tree"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
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

const payloadComposition = "composition"

type TagComposition struct {
	closuretree.Branch
	Name string
}

const payloadIncludedId = "includedId"

type TagWithId struct {
	ID   uint `gorm:"primaryKey,uniqueIndex,autoIncrement"`
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
	t.Run(payloadComposition, func(t *testing.T) {
		var ct *closuretree.Tree
		var err error

		ct, err = closuretree.New(db, TagComposition{}, "add1")
		if err != nil {
			t.Fatal(err)
		}

		for _, item := range testTree {
			tagItem := TagComposition{
				Name: item.name,
				Branch: closuretree.Branch{
					ID: item.id,
				},
			}

			err = ct.Add(tagItem, item.parent)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run(payloadIncludedId, func(t *testing.T) {
		var ct *closuretree.Tree
		var err error

		ct, err = closuretree.New(db, TagWithId{}, "add2")
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range testTree {
			tagItem := TagWithId{
				ID:   item.id,
				Name: item.name,
			}

			err = ct.Add(tagItem, item.parent)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

func testGetDescendants(t *testing.T, db *gorm.DB) {
	t.Run(payloadComposition, func(t *testing.T) {
		var ct *closuretree.Tree
		var err error

		ct, err = closuretree.New(db, TagComposition{}, "descendant1")
		if err != nil {
			t.Fatal(err)
		}

		for _, item := range testTree {
			tagItem := TagComposition{
				Name: item.name,
				Branch: closuretree.Branch{
					ID: item.id,
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
			{Name: "Electronics", Branch: closuretree.Branch{ID: 1}},
			{Name: "Mobile Phones", Branch: closuretree.Branch{ID: 2}},
			{Name: "Laptops", Branch: closuretree.Branch{ID: 4}},
			{Name: "Touch Screen", Branch: closuretree.Branch{ID: 6}},
		}

		if diff := cmp.Diff(gotTags, want); diff != "" {
			t.Errorf("unexpected result (-want +got):\n%s", diff)
		}
	})
	t.Run(payloadIncludedId, func(t *testing.T) {
		var ct *closuretree.Tree
		var err error

		ct, err = closuretree.New(db, TagWithId{}, "descendant2")
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range testTree {
			tagItem := TagWithId{
				ID:   item.id,
				Name: item.name,
			}

			err = ct.Add(tagItem, item.parent)
			if err != nil {
				t.Fatal(err)
			}
		}

		gotTags := []TagWithId{}

		err = ct.Descendants(1, &gotTags)
		if err != nil {
			t.Fatal(err)
		}
		want := []TagWithId{
			{Name: "Electronics", ID: 1},
			{Name: "Mobile Phones", ID: 2},
			{Name: "Laptops", ID: 4},
			{Name: "Touch Screen", ID: 6},
		}

		if diff := cmp.Diff(gotTags, want); diff != "" {
			t.Errorf("unexpected result (-want +got):\n%s", diff)
		}
	})

	t.Run("descendantIds", func(t *testing.T) {
		var ct *closuretree.Tree
		var err error

		ct, err = closuretree.New(db, TagWithId{}, "descendant3")
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range testTree {
			tagItem := TagWithId{
				ID:   item.id,
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

		ct, err = closuretree.New(db, TagComposition{}, "move1")
		if err != nil {
			t.Fatal(err)
		}

		for _, item := range testTree {
			tagItem := TagComposition{
				Name: item.name,
				Branch: closuretree.Branch{
					ID: item.id,
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
					ID: item.id,
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
