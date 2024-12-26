package closuretree_test

import (
	"fmt"
	"github.com/glebarez/sqlite"
	ct "github.com/go-bumbu/closure-tree"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"strings"
)

// for this example we are going to use Tag, but any struct would do
type Tag struct {
	ct.Node // embed the Node struct to add a branch primary key
	Name    string
}

func ExampleTree_Descendants() {

	db := getGormDb()
	// A table suffix should be added, this allows to use multiple trees
	// two tables will be created: one for tags and one to keep the closure tree structure
	tree, _ := ct.New(db, Tag{}, "tags")

	// add nodes with a tree structure

	// This represents a tree like:
	// 1 -  colors
	// 2 -   | -  warm
	// 3 -   |      |  - orange
	// 5 -   | -  cold
	// 6 -  sizes
	// 7 -   | - small
	// 8 -   | - medium

	colorTag := Tag{Name: "colors"}
	// since we pass colorTag as pointer, the NodeId is going to be updated
	_ = tree.Add(&colorTag, 0)

	_ = tree.Add(Tag{Name: "warm", Node: ct.Node{}}, colorTag.NodeId)
	_ = tree.Add(Tag{Name: "orange", Node: ct.Node{}}, colorTag.NodeId)
	// you can specify an unique ID for the branch
	_ = tree.Add(Tag{Name: "cold", Node: ct.Node{NodeId: 5}}, colorTag.NodeId)

	sizes := Tag{Name: "sizes"}
	_ = tree.Add(&sizes, 0)
	_ = tree.Add(Tag{Name: "small"}, sizes.NodeId)
	_ = tree.Add(Tag{Name: "medium"}, sizes.NodeId)

	// Get the descendants of color
	descendants := []Tag{}
	_ = tree.Descendants(colorTag.NodeId, 0, &descendants)
	for _, item := range descendants {
		fmt.Printf("%d=> %s\n", item.NodeId, item.Name)
	}

	// Get only th descendants NodeIds
	descendantsIds, _ := tree.DescendantIds(colorTag.NodeId, 0)
	descendantsIdsStr := []string{}
	for _, item := range descendantsIds {
		descendantsIdsStr = append(descendantsIdsStr, fmt.Sprintf("%d", item))
	}
	fmt.Printf("ids: %s\n", strings.Join(descendantsIdsStr, ","))

	//err := tree.Roots(colorTag.NodeId, &descendants)
	//if err != nil {
	//	panic(err)
	//}

	// Output:
	// 2=> warm
	// 3=> orange
	// 5=> cold
	// ids: 2,3,5
}

func ExampleTree_Roots() {

	db := getGormDb()
	// A table suffix should be added, this allows to use multiple trees
	// two tables will be created: one for tags and one to keep the closure tree structure
	tree, _ := ct.New(db, Tag{}, "tags")

	// add nodes with a tree structure

	// This represents a tree like:
	// 1 -  colors
	// 2 -   | -  warm
	// 3 -   |      |  - orange
	// 5 -   | -  cold
	// 6 -  sizes
	// 7 -   | - small
	// 8 -   | - medium
	// 9 -  shapes

	colorTag := Tag{Name: "colors"}
	// since we pass colorTag as pointer, the NodeId is going to be updated
	_ = tree.Add(&colorTag, 0)

	_ = tree.Add(Tag{Name: "warm", Node: ct.Node{}}, colorTag.NodeId)
	_ = tree.Add(Tag{Name: "orange", Node: ct.Node{}}, colorTag.NodeId)
	// you can specify an unique ID for the branch
	_ = tree.Add(Tag{Name: "cold", Node: ct.Node{NodeId: 5}}, colorTag.NodeId)

	sizes := Tag{Name: "sizes"}
	_ = tree.Add(&sizes, 0)
	_ = tree.Add(Tag{Name: "small"}, sizes.NodeId)
	_ = tree.Add(Tag{Name: "medium"}, sizes.NodeId)

	_ = tree.Add(Tag{Name: "shapes"}, 0)

	// Get the roots of color
	roots := []Tag{}
	_ = tree.Roots(&roots)
	for _, item := range roots {
		fmt.Printf("%d=> %s\n", item.NodeId, item.Name)
	}

	// Get only the roots NodeIds
	rootIds, _ := tree.RootIds()
	descendantsIdsStr := []string{}
	for _, item := range rootIds {
		descendantsIdsStr = append(descendantsIdsStr, fmt.Sprintf("%d", item))
	}
	fmt.Printf("ids: %s\n", strings.Join(descendantsIdsStr, ","))

	// Output:
	// 1=> colors
	// 6=> sizes
	// 9=> shapes
	// ids: 1,6,9
}

// initialize your Gorm DB
func getGormDb() *gorm.DB {
	dbFile := "./tag_example.sqlite"
	if _, err := os.Stat(dbFile); err == nil {
		if err = os.Remove(dbFile); err != nil {
			panic(err)
		}
	}

	db, err := gorm.Open(sqlite.Open(dbFile), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic(err)
	}
	return db
}
