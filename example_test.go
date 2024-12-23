package closuretree_test

import (
	"fmt"
	"github.com/glebarez/sqlite"
	ct "github.com/go-bumbu/closure-tree"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
)

// for this example we are going to use Tag, but any struct would do
type Tag struct {
	ct.Branch // embed the Branch struct to add a branch primary key
	Name      string
}

func ExampleTree_Descendants() {

	db := getGormDb()
	// A table suffix should be added, this allows to use multiple trees
	// two tables will be created: one for tags and one to keep the closure tree structure
	tree, _ := ct.New(db, Tag{}, "tags")

	// add nodes with a tree structure

	colorTag := Tag{Name: "colors"}
	_ = tree.Add(&colorTag, 0)

	warmColor := Tag{Name: "warm", Branch: ct.Branch{}}
	_ = tree.Add(&warmColor, colorTag.BranchId)

	orangeTag := Tag{Name: "orange", Branch: ct.Branch{}}
	_ = tree.Add(&orangeTag, colorTag.BranchId)

	// specify an ID for the branch
	coldColor := Tag{Name: "cold", Branch: ct.Branch{BranchId: 5}}
	_ = tree.Add(&coldColor, colorTag.BranchId)

	descendants := []Tag{}
	_ = tree.Descendants(colorTag.BranchId, &descendants)

	for _, item := range descendants {
		fmt.Printf("id: %d, name: %s\n", item.BranchId, item.Name)
	}

	// Output:
	// id: 1, name: colors
	// id: 2, name: warm
	// id: 3, name: orange
	// id: 5, name: cold
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
