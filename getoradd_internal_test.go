package closuretree

import (
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newInternalTree builds a Tree backed by a single-connection in-memory SQLite DB for
// white-box tests of unexported helpers. buildMatchConditions does no DB round-trip (it only
// parses the schema and quotes via the dialector), so :memory: is sufficient.
func newInternalTree(t *testing.T, model any) *Tree {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(1) // keep one in-memory DB across migration + use
	ct, err := New(db, model)
	if err != nil {
		t.Fatalf("new tree: %v", err)
	}
	return ct
}

type matchColSample struct {
	Node
	Name string
}

// TestBuildMatchConditionsQuotesColumns asserts the generated WHERE quotes the column
// identifier per dialect, so a node field mapping to a reserved-word column produces valid SQL.
func TestBuildMatchConditionsQuotesColumns(t *testing.T) {
	ct := newInternalTree(t, matchColSample{})

	where, args, err := ct.buildMatchConditions(matchColSample{Name: "x"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var b strings.Builder
	ct.db.QuoteTo(&b, "nodes.name")
	want := b.String() + " = ?"
	if where != want {
		t.Errorf("column not quoted: want %q, got %q", want, where)
	}
	if len(args) != 1 || args[0] != "x" {
		t.Errorf("args = %v, want [x]", args)
	}
}

type matchEmbeddedMeta struct {
	Color string
}

type matchEmbeddedSample struct {
	Node
	Name string
	Meta matchEmbeddedMeta `gorm:"embedded"`
}

// TestBuildMatchConditionsEmbeddedNamedStruct asserts a match value contributed by a
// gorm-embedded NAMED sub-struct is used in the WHERE (not silently dropped), which would
// otherwise cause GetOrAdd to match on fewer fields and reuse the wrong sibling.
func TestBuildMatchConditionsEmbeddedNamedStruct(t *testing.T) {
	ct := newInternalTree(t, matchEmbeddedSample{})

	_, args, err := ct.buildMatchConditions(matchEmbeddedSample{Meta: matchEmbeddedMeta{Color: "red"}})
	if err != nil {
		t.Fatalf("embedded field should produce a condition, got err: %v", err)
	}
	if len(args) != 1 || args[0] != "red" {
		t.Errorf("args = %v, want [red] (embedded field must be matched)", args)
	}
}
