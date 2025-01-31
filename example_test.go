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

	db := getGormDb("tagTree.example")
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

type Book struct {
	ID     uint `gorm:"primarykey"`
	Name   string
	Genres []Genre `gorm:"many2many:books_genres;"`
}

type Genre struct {
	ct.Node // embed the Node struct to add a branch primary key
	Name    string
}

func ExampleTreeWithM2MRelations() {

	db := getGormDb("booksM2M.example")

	tree, err := ct.New2(db, Genre{})
	handleErr(err)
	_ = tree
	// add this sample data
	// 1  -  Science Fiction
	// 2  -   | -  Space Opera
	// 3  -   |      |  - Galactic Empires
	// 4  -   |      |  - Interstellar Wars
	// 5  -   | -  Hard Sci-Fi
	// 6  -   |      |  - Futuristic Technology
	// 7  -   |      |  - Quantum Exploration
	// 8  -  Fantasy
	// 9  -   | -  High Fantasy
	// 10 -   |      |  - Epic Quests
	// 11 -   |      |  - Mythical Creatures
	// 12 -   | -  Urban Fantasy
	// 13 -   |      |  - Magic in the Modern World
	// 14 -   |      |  - Supernatural Detectives

	tenant := "sampleTenant"

	scifi := Genre{Name: "Science Fiction"}
	err = tree.Add(&scifi, 0, tenant)
	handleErr(err)

	spaceOpera := Genre{Name: "Space Opera"}
	_ = tree.Add(&spaceOpera, scifi.NodeId, tenant)
	_ = tree.Add(Genre{Name: "Galactic Empires"}, spaceOpera.Id(), tenant)
	_ = tree.Add(Genre{Name: "Interstellar Wars"}, spaceOpera.Id(), tenant)

	hardScifi := Genre{Name: "Hard Sci-Fi"}
	_ = tree.Add(&hardScifi, scifi.NodeId, tenant)
	_ = tree.Add(Genre{Name: "Futuristic Technology"}, hardScifi.Id(), tenant)
	_ = tree.Add(Genre{Name: "Quantum Exploration"}, hardScifi.Id(), tenant)

	fantasy := Genre{Name: "Science Fiction"}
	_ = tree.Add(&fantasy, 0, tenant)

	highFantasy := Genre{Name: "High Fantasy"}
	_ = tree.Add(&highFantasy, fantasy.NodeId, tenant)
	_ = tree.Add(Genre{Name: "Epic Quests"}, highFantasy.Id(), tenant)
	_ = tree.Add(Genre{Name: "Mythical Creatures"}, highFantasy.Id(), tenant)

	urbanFantasy := Genre{Name: "Urban Fantasy"}
	_ = tree.Add(&urbanFantasy, fantasy.NodeId, tenant)
	_ = tree.Add(Genre{Name: "Magic in the Modern World"}, urbanFantasy.Id(), tenant)
	_ = tree.Add(Genre{Name: "Supernatural Detectives"}, urbanFantasy.Id(), tenant)

	// Create the Books table
	_ = db.AutoMigrate(Book{})

	// insert some Books
	books := []Book{
		{Name: "The Echoes of Eternity", Genres: []Genre{{Node: ct.Node{NodeId: 3}}, {Node: ct.Node{NodeId: 10}}}},
		{Name: "Chronicles of the Shadowlands", Genres: []Genre{{Node: ct.Node{NodeId: 6}}}},
		{Name: "Nebula’s Whisper", Genres: []Genre{{Node: ct.Node{NodeId: 4}}}},
		{Name: "The Clockwork Alchemist", Genres: []Genre{{Node: ct.Node{NodeId: 4}}, {Node: ct.Node{NodeId: 8}}}},
		{Name: "Through the Veil of Time", Genres: []Genre{{Node: ct.Node{NodeId: 13}}, {Node: ct.Node{NodeId: 14}}}},
		{Name: "Tides of an Emerald Sky", Genres: []Genre{{Node: ct.Node{NodeId: 14}}}},
	}
	db.Create(books) // pass a slice to insert multiple row

	// query space operas
	spaceOperaIds, _ := tree.DescendantIds(2, 0, tenant)
	var gotBooks []Book
	db.Model(&Book{}).InnerJoins("INNER JOIN books_genres ON books.id = books_genres.book_id").
		Preload("Genres").
		Where("books_genres.genre_node_id IN ?", spaceOperaIds).
		Distinct().
		Find(&gotBooks)

	fmt.Println("Space Operas:")
	for _, book := range gotBooks {
		fmt.Printf("- %s\n", book.Name)
	}
	//spew.Dump(gotBooks)

	// query Fantasy
	fantasyIds, _ := tree.DescendantIds(8, 0, tenant)
	fantasyIds = append(fantasyIds, 8)
	db.Model(&Book{}).InnerJoins("INNER JOIN books_genres ON books.id = books_genres.book_id").
		Preload("Genres").
		Where("books_genres.genre_node_id IN ?", fantasyIds).
		Distinct().
		Find(&gotBooks)

	fmt.Println("Fantasy:")
	for _, book := range gotBooks {
		fmt.Printf("- %s\n", book.Name)
	}

	// Output:
	//Space Operas:
	//- The Echoes of Eternity
	//- Nebula’s Whisper
	//- The Clockwork Alchemist
	//Fantasy:
	//- The Echoes of Eternity
	//- The Clockwork Alchemist
	//- Through the Veil of Time
	//- Tides of an Emerald Sky
}

type Song struct {
	ct.Leave
	Name   string
	Genres []Genre `gorm:"many2many:songs_genres;"`
}

func ExampleTreeWithLeaves() {

	db := getGormDb("booksM2M.example")

	tree, err := ct.New2(db, Genre{})
	handleErr(err)
	_ = tree
	// add this sample data
	// 1  -  Science Fiction
	// 2  -   | -  Space Opera
	// 3  -   |      |  - Galactic Empires
	// 4  -   |      |  - Interstellar Wars
	// 5  -   | -  Hard Sci-Fi
	// 6  -   |      |  - Futuristic Technology
	// 7  -   |      |  - Quantum Exploration
	// 8  -  Fantasy
	// 9  -   | -  High Fantasy
	// 10 -   |      |  - Epic Quests
	// 11 -   |      |  - Mythical Creatures
	// 12 -   | -  Urban Fantasy
	// 13 -   |      |  - Magic in the Modern World
	// 14 -   |      |  - Supernatural Detectives

	tenant := "sampleTenant"

	scifi := Genre{Name: "Science Fiction"}
	err = tree.Add(&scifi, 0, tenant)
	handleErr(err)

	spaceOpera := Genre{Name: "Space Opera"}
	_ = tree.Add(&spaceOpera, scifi.Id(), tenant)
	_ = tree.Add(Genre{Name: "Galactic Empires"}, spaceOpera.Id(), tenant)
	_ = tree.Add(Genre{Name: "Interstellar Wars"}, spaceOpera.Id(), tenant)

	hardScifi := Genre{Name: "Hard Sci-Fi"}
	_ = tree.Add(&hardScifi, scifi.NodeId, tenant)
	_ = tree.Add(Genre{Name: "Futuristic Technology"}, hardScifi.Id(), tenant)
	_ = tree.Add(Genre{Name: "Quantum Exploration"}, hardScifi.Id(), tenant)

	fantasy := Genre{Name: "Science Fiction"}
	_ = tree.Add(&fantasy, 0, tenant)

	highFantasy := Genre{Name: "High Fantasy"}
	_ = tree.Add(&highFantasy, fantasy.Id(), tenant)
	_ = tree.Add(Genre{Name: "Epic Quests"}, highFantasy.Id(), tenant)
	_ = tree.Add(Genre{Name: "Mythical Creatures"}, highFantasy.Id(), tenant)

	urbanFantasy := Genre{Name: "Urban Fantasy"}
	_ = tree.Add(&urbanFantasy, fantasy.Id(), tenant)
	_ = tree.Add(Genre{Name: "Magic in the Modern World"}, urbanFantasy.Id(), tenant)
	_ = tree.Add(Genre{Name: "Supernatural Detectives"}, urbanFantasy.Id(), tenant)

	// Create the Books table
	_ = db.AutoMigrate(Song{})

	// insert some Books
	songs := []Song{
		{Leave: ct.Leave{Tenant: tenant}, Name: "The Echoes of Eternity", Genres: []Genre{{Node: ct.Node{NodeId: 3}}, {Node: ct.Node{NodeId: 10}}}},
		{Leave: ct.Leave{Tenant: "another Tenant"}, Name: "Another tenants book", Genres: []Genre{{Node: ct.Node{NodeId: 3}}, {Node: ct.Node{NodeId: 10}}}},
		{Leave: ct.Leave{Tenant: tenant}, Name: "Chronicles of the Shadowlands", Genres: []Genre{{Node: ct.Node{NodeId: 6}}}},
		{Leave: ct.Leave{Tenant: tenant}, Name: "Nebula’s Whisper", Genres: []Genre{{Node: ct.Node{NodeId: 4}}}},
		{Leave: ct.Leave{Tenant: tenant}, Name: "The Clockwork Alchemist", Genres: []Genre{{Node: ct.Node{NodeId: 4}}, {Node: ct.Node{NodeId: 8}}}},
		{Leave: ct.Leave{Tenant: tenant}, Name: "Through the Veil of Time", Genres: []Genre{{Node: ct.Node{NodeId: 13}}, {Node: ct.Node{NodeId: 14}}}},
		{Leave: ct.Leave{Tenant: tenant}, Name: "Tides of an Emerald Sky", Genres: []Genre{{Node: ct.Node{NodeId: 14}}}},
	}
	db.Create(songs) // pass a slice to insert multiple row

	// query space operas
	var gotSongs []Song
	err = tree.GetLeaves(&gotSongs, 2, tenant)
	if err != nil {
		fmt.Print(err)
	}
	fmt.Println("Space Operas:")
	for _, book := range gotSongs {
		fmt.Printf("- %s\n", book.Name)
	}

	// query Fantasy
	err = tree.GetLeaves(&gotSongs, 8, tenant)
	if err != nil {
		fmt.Print(err)
	}
	fmt.Println("Fantasy:")
	for _, book := range gotSongs {
		fmt.Printf("- %s\n", book.Name)
	}

	// Output:
	//Space Operas:
	//- The Echoes of Eternity
	//- Nebula’s Whisper
	//- The Clockwork Alchemist
	//Fantasy:
	//- The Echoes of Eternity
	//- The Clockwork Alchemist
	//- Through the Veil of Time
	//- Tides of an Emerald Sky

}

func handleErr(err error) {
	if err != nil {
		fmt.Printf("[ERROR] %s\n", err.Error())
	}
}

// initialize your Gorm DB
func getGormDb(name string) *gorm.DB {
	if name == "" {
		name = "example"
	}
	dbFile := "./" + name + ".sqlite"
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
