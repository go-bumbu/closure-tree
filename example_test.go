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
	// A table suffix should be added, this allows to use multiple trees on the same database
	// two tables will be created: one for tags and one to keep the closure tree structure
	tree, _ := ct.New(db, Tag{}, "tags")

	// add nodes with a tree structure

	// This represents a tree like:
	// 1 -  colors
	// 2 -   | -  warm
	// 3 -   |      |  - orange
	// 4 -   | -  cold
	// 5 -  sizes
	// 6 -   | - small
	// 7 -   | - medium
	tenant := "sampleTenant"

	colorTag := Tag{Name: "colors"}
	// since we pass colorTag as pointer, the NodeId is going to be updated
	_ = tree.Add(&colorTag, 0, tenant)

	_ = tree.Add(Tag{Name: "warm", Node: ct.Node{}}, colorTag.Id(), tenant)
	_ = tree.Add(Tag{Name: "orange", Node: ct.Node{}}, colorTag.Id(), tenant)
	// you can specify an unique ID for the branch
	_ = tree.Add(Tag{Name: "cold", Node: ct.Node{}}, colorTag.Id(), tenant)

	sizes := Tag{Name: "sizes"}
	_ = tree.Add(&sizes, 0, tenant)
	_ = tree.Add(Tag{Name: "small"}, sizes.NodeId, tenant)
	_ = tree.Add(Tag{Name: "medium"}, sizes.NodeId, tenant)

	// Get the descendants of color
	descendants := []Tag{}
	_ = tree.Descendants(colorTag.NodeId, 0, tenant, &descendants)
	for _, item := range descendants {
		fmt.Printf("%d=> %s\n", item.NodeId, item.Name)
	}

	// Get all the nodeIds starting on the root
	descendantsIds, _ := tree.DescendantIds(0, 0, tenant)
	descendantsIdsStr := []string{}
	for _, item := range descendantsIds {
		descendantsIdsStr = append(descendantsIdsStr, fmt.Sprintf("%d", item))
	}
	fmt.Printf("all ids: %s\n", strings.Join(descendantsIdsStr, ","))

	// Output:
	// 2=> warm
	// 3=> orange
	// 4=> cold
	// all ids: 1,5,2,3,4,6,7
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
