package closuretree

import (
	"errors"
	"fmt"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

const branchTblName = "closure_tree_branch"
const closureTblName = "closure_tree_closure"

var ItemIsNotBranchErr = errors.New("the item does not embed Branch")

func New(db *gorm.DB, Item any, name string) (*Tree, error) {

	ln := branchTblName
	cn := closureTblName
	if name != "" {
		ln = strings.ToLower(fmt.Sprintf("%s_%s", branchTblName, name))
		cn = strings.ToLower(fmt.Sprintf("%s_%s", closureTblName, name))
	}

	ct := Tree{
		db:             db,
		branchTblName:  ln,
		closureTblName: cn,
	}

	if !hasBranch(Item) {
		return nil, ItemIsNotBranchErr
	}

	err := db.Table(ct.branchTblName).AutoMigrate(Item)
	if err != nil {
		return nil, fmt.Errorf("unable to migreate leave: %v", err)
	}
	err = db.Table(ct.closureTblName).AutoMigrate(closureTree{})
	if err != nil {
		return nil, fmt.Errorf("unable to migrate closure: %v", err)
	}
	return &ct, nil
}

type Tree struct {
	db *gorm.DB

	// table names, allows multiple trees
	branchTblName  string
	closureTblName string
}

// represents the table that store the relationships
type closureTree struct {
	AncestorID   uint `gorm:"not null,primaryKey,uniqueIndex"`
	DescendantID uint `gorm:"not null,primaryKey,uniqueIndex"`
	Depth        int
}

func (ct *Tree) Add(item any, parentID uint) error {
	if !hasBranch(item) {
		return ItemIsNotBranchErr
	}

	t := reflect.TypeOf(item)
	itemIsPointer := false
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		itemIsPointer = true
	}
	reflectItem := reflect.New(t).Interface()
	if itemIsPointer {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item).Elem())
	} else {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item))

	}

	err := ct.db.Transaction(func(tx *gorm.DB) error {
		// todo verify that the parent exists if not 0 before conitnuing

		// create the single reflectItem
		err := tx.Table(ct.branchTblName).Create(reflectItem).Error
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("unable to add leave: %v", err)
		}

		id, err := getID(reflectItem)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("unable to get Item ID: %v", err)
		}

		// Add reflexive relationship
		err = tx.Table(ct.closureTblName).Create(&closureTree{AncestorID: id, DescendantID: id, Depth: 0}).Error
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
			sqlstr := fmt.Sprintf(sql, ct.closureTblName, ct.closureTblName)

			exec := tx.Exec(sqlstr, id, parentID)
			if exec.Error != nil {
				tx.Rollback()
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// if topItem is a pointer copy the ID back into it
	if itemIsPointer {
		itemValue := reflect.ValueOf(item).Elem()
		reflectItemValue := reflect.ValueOf(reflectItem).Elem()

		idField := reflectItemValue.FieldByName(branchIdField)
		if idField.IsValid() && idField.CanSet() {
			itemValue.FieldByName(branchIdField).Set(idField)
		} else {
			return fmt.Errorf("field: %s is not accessible or settable", branchIdField)
		}
	}

	return nil
}

func (ct *Tree) Descendants(parent uint, maxDepth int, items interface{}) error {
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

	if maxDepth > 0 {
		// return aup to max depth
		sqlstr := fmt.Sprintf(descendantsQuery, ct.branchTblName, ct.closureTblName)
		err := ct.db.Raw(sqlstr, parent, maxDepth).Scan(slice.Addr().Interface()).Error
		if err != nil {
			return fmt.Errorf("failed to fetch descendants: %w", err)
		}
	} else {
		// return all children
		sqlstr := fmt.Sprintf(descendantsQueryAll, ct.branchTblName, ct.closureTblName)
		err := ct.db.Raw(sqlstr, parent).Scan(slice.Addr().Interface()).Error
		if err != nil {
			return fmt.Errorf("failed to fetch descendants: %w", err)
		}
	}

	return nil
}

const descendantsQuery = `SELECT le.*
FROM %s AS le
JOIN %s AS ct ON ct.descendant_id = le.branch_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ?
ORDER BY ct.depth;`

const descendantsQueryAll = `SELECT le.*
FROM %s AS le
JOIN %s AS ct ON ct.descendant_id = le.branch_id
WHERE ct.ancestor_id = ? AND ct.depth > 0
ORDER BY ct.depth;`

func (ct *Tree) DescendantIds(parent uint, maxDepth int) ([]uint, error) {
	ids := []uint{}
	if maxDepth > 0 {
		sqlstr := fmt.Sprintf(descendantsIDQuery, ct.branchTblName, ct.closureTblName)
		err := ct.db.Raw(sqlstr, parent, maxDepth).Scan(&ids).Error
		if err != nil {
			return nil, fmt.Errorf("failed to fetch descendants: %w", err)
		}
		return ids, nil
	} else {
		sqlstr := fmt.Sprintf(descendantsIDQueryAll, ct.branchTblName, ct.closureTblName)
		err := ct.db.Raw(sqlstr, parent).Scan(&ids).Error
		if err != nil {
			return nil, fmt.Errorf("failed to fetch descendants: %w", err)
		}
		return ids, nil
	}

}

const descendantsIDQuery = `SELECT le.branch_id
FROM %s AS le
JOIN %s AS ct ON ct.descendant_id = le.branch_id
WHERE ct.ancestor_id = ? AND ct.depth > 0 AND ct.depth <= ?
ORDER BY ct.depth;`

const descendantsIDQueryAll = `SELECT le.branch_id
FROM %s AS le
JOIN %s AS ct ON ct.descendant_id = le.branch_id
WHERE ct.ancestor_id = ? AND ct.depth > 0
ORDER BY ct.depth;`

// // TODO add delete recursive

// // todo find orphan items

func (ct *Tree) Move(LeaveId, newParentID uint) error {

	return ct.db.Transaction(func(tx *gorm.DB) error {
		var err error
		insertSql := fmt.Sprintf(moveQueryInsetNew, ct.closureTblName, ct.closureTblName, ct.closureTblName)
		exec1 := tx.Exec(insertSql, LeaveId, newParentID)
		err = exec1.Error
		if err != nil {
			tx.Rollback()
			return err
		}

		// Delete old closure relationships
		delSql := fmt.Sprintf(moveQueryDeleteOld, ct.closureTblName, ct.closureTblName, ct.closureTblName)
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
