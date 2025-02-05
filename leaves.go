package closuretree

import (
	"errors"
	"fmt"
	"gorm.io/gorm"
	"reflect"
	"strings"
)

// Leave is an embeddable ID to be used in closure tree, this is mandatory if you want to use leaves functionality
type Leave struct {
	LeaveId uint   `gorm:"AUTO_INCREMENT;PRIMARY_KEY;not null"`
	Tenant  string `gorm:"index"`
}

func (n *Leave) Id() uint {
	return n.LeaveId
}

var ItemIsNotTreeLeave = errors.New("the item does not embed Leave")

// isLeaveSlice uses reflection to verify if the passed item is a pointer to a slise that embedded Leave struct
// returns an error for every condition checked, returns nil if the passed item is as expected
func isLeaveSlice(item any) error {
	if item == nil {
		return fmt.Errorf("item is nil")
	}

	itemType := reflect.TypeOf(item)

	// Ensure item is a pointer
	if itemType.Kind() != reflect.Ptr {
		return fmt.Errorf("item is not a pointer")
	}

	// Ensure the pointer points to a slice
	sliceType := itemType.Elem()
	if sliceType.Kind() != reflect.Slice {
		return fmt.Errorf("item is not slice")
	}

	// Get the element type of the slice
	elemType := sliceType.Elem()
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("item is not a slice of structs")
	}

	// Check if the struct embeds Leave
	hasLeave := false
	hasManyToMany := false

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Check if the struct embeds Leave
		if field.Anonymous && field.Type == reflect.TypeOf(Leave{}) {
			hasLeave = true
		}

		// Check if the struct has a slice field with a gorm "many2many" annotation
		if field.Type.Kind() == reflect.Slice {
			gormTag := field.Tag.Get("gorm")
			if strings.Contains(gormTag, "many2many:") {
				hasManyToMany = true
			}
		}
	}

	if !hasLeave {
		return ItemIsNotTreeLeave
	}

	if !hasManyToMany {
		return fmt.Errorf("item struct does not contain a many2many gorm tag")
	}
	return nil
}

func getGormM2MTblName(item any) (string, string, error) {
	if item == nil {
		return "", "", fmt.Errorf("item is nil")
	}

	itemType := reflect.TypeOf(item)

	// Dereference the pointer to get the slice type
	sliceType := itemType.Elem()
	elemType := sliceType.Elem()

	// Iterate over the struct fields to find the many2many annotation
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Check if the field is a slice and has a gorm tag
		if field.Type.Kind() == reflect.Slice {
			gormTag := field.Tag.Get("gorm")

			// Extract the many2many table name
			if strings.Contains(gormTag, "many2many:") {
				parts := strings.Split(gormTag, ";")
				for _, part := range parts {
					if strings.HasPrefix(part, "many2many:") {

						return field.Name, strings.TrimPrefix(part, "many2many:"), nil
					}
				}
			}
		}
	}
	return "", "", fmt.Errorf("many2many annotation not found")
}

const nodeIdDBField = "node_id"
const leaveIDDBField = "leave_id"

func (ct *Tree) GetLeaves(target any, parentID uint, maxDepth int, tenant string) error {

	ids, err := ct.DescendantIds(parentID, maxDepth, tenant)
	if err != nil {
		return err
	}
	if parentID != 0 {
		ids = append(ids, parentID)
	}
	err = isLeaveSlice(target)
	if err != nil {
		return err
	}

	stmt := &gorm.Statement{DB: ct.db}
	err = stmt.Parse(target)
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}
	leaveTblName := stmt.Schema.Table

	fieldName, m2mTbl, err := getGormM2MTblName(target)
	if err != nil {
		return err
	}

	joinSql := fmt.Sprintf(leavesJoinQuery, m2mTbl, leaveTblName, leaveIDDBField, m2mTbl, singular(leaveTblName), leaveIDDBField)
	err = ct.db.Model(target).InnerJoins(joinSql).
		Preload(fieldName).
		Where(fmt.Sprintf(leavesWhereQuery, m2mTbl, singular(ct.nodesTbl), nodeIdDBField, leaveTblName), ids, tenant).
		Distinct().
		Find(target).Error

	return err
}

const leavesJoinQuery = `INNER JOIN %s ON %s.%s = %s.%s_%s`
const leavesWhereQuery = `%s.%s_%s IN ? AND %s.Tenant = ?`

// if the input string ends on s, return it without the s ending
// e.g. songs => song
func singular(in string) string {
	if strings.HasSuffix(in, "s") {
		return in[:len(in)-1]
	}
	return in
}
