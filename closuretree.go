package closuretree

import (
	"errors"
	"fmt"
	"gorm.io/gorm"
	"reflect"
)

const leaveName = "closure_tree_leave"
const closureName = "closure_tree_closure"

func New(db *gorm.DB, Item any, name string) (*Tree, error) {

	ln := leaveName
	cn := closureName
	if name != "" {
		ln = fmt.Sprintf("%s_%s", leaveName, name)
		cn = fmt.Sprintf("%s_%s", closureName, name)
	}

	ct := Tree{
		db:          db,
		leaveName:   ln,
		closureName: cn,
	}

	if !hasId(Item) {
		return nil, errors.New("item does not contain Field ID")
	}

	err := db.Table(ct.leaveName).AutoMigrate(Item)
	if err != nil {
		return nil, fmt.Errorf("unable to migreate leave: %v", err)
	}
	err = db.Table(ct.closureName).AutoMigrate(closureTree{})
	if err != nil {
		return nil, fmt.Errorf("unable to migrate closure: %v", err)
	}
	return &ct, nil
}

type Tree struct {
	db *gorm.DB

	// table names, allows multiple trees
	leaveName   string
	closureName string
}

// represents the table that store the relationships
type closureTree struct {
	AncestorID   uint `gorm:"not null,primaryKey,uniqueIndex"`
	DescendantID uint `gorm:"not null,primaryKey,uniqueIndex"`
	Depth        int
}

func (ct *Tree) Add(leaveItem any, parentID uint) error {
	if !hasId(leaveItem) {
		return errors.New("item does not contain Field ID")
	}

	// use reflection go get a concrete type that gorm can handle
	t := reflect.TypeOf(leaveItem)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	item := reflect.New(t).Interface()
	reflect.ValueOf(item).Elem().Set(reflect.ValueOf(leaveItem))

	return ct.db.Transaction(func(tx *gorm.DB) error {
		// todo verify that the parent exists if not 0 before conitnuing

		// create the single item
		err := tx.Table(ct.leaveName).Create(item).Error
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("unable to add leave: %v", err)
		}

		id, err := getID(item)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("unable to get Item ID: %v", err)
		}

		// Add reflexive relationship
		err = tx.Table(ct.closureName).Create(&closureTree{AncestorID: id, DescendantID: id, Depth: 0}).Error
		if err != nil {
			tx.Rollback()
			return err
		}

		if parentID != 0 {
			// Copy all ancestors of the parent to include the new tag
			sql := `INSERT INTO %s (ancestor_id, descendant_id, depth)
				SELECT ancestor_id, ?, depth + 1
				FROM %s
				WHERE descendant_id = ?;`
			sqlstr := fmt.Sprintf(sql, ct.closureName, ct.closureName)

			exec := tx.Exec(sqlstr, id, parentID)
			if exec.Error != nil {
				tx.Rollback()
				return err
			}
		}
		return nil
	})
}

// todo find max depth
func (ct *Tree) Descendants(parent uint, items interface{}) error {
	if items == nil {
		return errors.New("items cannot be nil")
	}

	// Check if items is a pointer to a slice using reflection
	itemsValue := reflect.ValueOf(items)
	if itemsValue.Kind() != reflect.Ptr {
		return errors.New("items must be a pointer to a slice")
	}

	// Get the underlying slice
	slice := itemsValue.Elem()
	if slice.Kind() != reflect.Slice {
		return errors.New("items must be a pointer to a slice")
	}

	sqlstr := fmt.Sprintf(descendantsQuery, ct.leaveName, ct.closureName)
	err := ct.db.Raw(sqlstr, parent).Scan(slice.Addr().Interface()).Error
	if err != nil {
		return fmt.Errorf("failed to fetch descendants: %w", err)
	}
	return nil
}

const descendantsQuery = `SELECT le.*
FROM %s AS le
JOIN %s AS ct ON ct.descendant_id = le.id
WHERE ct.ancestor_id = ?
ORDER BY ct.depth;`

func (ct *Tree) DescendantIds(parent uint) ([]uint, error) {
	ids := []uint{}
	sqlstr := fmt.Sprintf(descendantsIDQuery, ct.leaveName, ct.closureName)
	err := ct.db.Raw(sqlstr, parent).Scan(&ids).Error
	if err != nil {
		return nil, fmt.Errorf("failed to fetch descendants: %w", err)
	}
	return ids, nil
}

const descendantsIDQuery = `SELECT le.id
FROM %s AS le
JOIN %s AS ct ON ct.descendant_id = le.id
WHERE ct.ancestor_id = ?
ORDER BY ct.depth;`

// // TODO add delete recursive

// // todo find orphan items

func (ct *Tree) Move(LeaveId, newParentID uint) error {

	return ct.db.Transaction(func(tx *gorm.DB) error {
		var err error
		insertSql := fmt.Sprintf(moveQueryInsetNew, ct.closureName, ct.closureName, ct.closureName)
		exec1 := tx.Exec(insertSql, LeaveId, newParentID)
		err = exec1.Error
		if err != nil {
			tx.Rollback()
			return err
		}

		// Delete old closure relationships
		delSql := fmt.Sprintf(moveQueryDeleteOld, ct.closureName, ct.closureName, ct.closureName)
		exec2 := tx.Exec(delSql, LeaveId, newParentID)
		err = exec2.Error
		if err != nil {
			tx.Rollback()
			return err
		}

		return nil
	})
}

const moveQueryInsetNew = `INSERT INTO %s (ancestor_id, descendant_id, depth)
			SELECT p.ancestor_id, c.descendant_id, p.depth + c.depth + 1
			FROM %s p
			JOIN %s c ON c.ancestor_id = ?
			WHERE p.descendant_id = ?;`

const moveQueryDeleteOld = `
			WITH descendants AS (
				SELECT descendant_id
				FROM %s
				WHERE ancestor_id = ?
			),
			excluded_ancestors AS (
				SELECT ancestor_id
				FROM %s
				WHERE descendant_id = ?
			)
			DELETE FROM %s
			WHERE descendant_id IN (SELECT descendant_id FROM descendants)
			  AND ancestor_id NOT IN (SELECT ancestor_id FROM excluded_ancestors)
			  AND depth != 0;
			`
