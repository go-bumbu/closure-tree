package closuretree_test

import (
	closuretree "github.com/go-bumbu/closure-tree"
	"gorm.io/gorm"
)

// newTestTree constructs a Tree and runs Migrate, restoring the one-call convenience the tests
// relied on before New and Migrate were split. It returns New's or Migrate's error unwrapped so the
// error-path tests keep asserting on the exact construction error.
func newTestTree(db *gorm.DB, model any) (*closuretree.Tree, error) {
	ct, err := closuretree.New(db, model)
	if err != nil {
		return ct, err
	}
	return ct, ct.Migrate()
}
