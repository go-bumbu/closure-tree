package closuretree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jinzhu/inflection"
	"gorm.io/gorm"
)

// Leaf is an embeddable ID to be used in closure tree, this is mandatory if you want to use leaves functionality
type Leaf struct {
	LeafId uint   `gorm:"autoIncrement;primaryKey;not null;column:leaf_id"`
	Tenant string `gorm:"index"`
}

func (n *Leaf) Id() uint {
	return n.LeafId
}

var ErrItemIsNotTreeLeaf = errors.New("the item does not embed Leaf")

// isLeaveSlice uses reflection to verify if the passed item is a pointer to a slice that embedded Leaf struct
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

	// Check if the struct embeds Leaf
	hasLeaf := false
	hasManyToMany := false

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Check if the struct embeds Leaf
		if field.Anonymous && field.Type == reflect.TypeOf(Leaf{}) {
			hasLeaf = true
		}

		// Check if the struct has a slice field with a gorm "many2many" annotation
		if field.Type.Kind() == reflect.Slice {
			gormTag := field.Tag.Get("gorm")
			if strings.Contains(gormTag, "many2many:") {
				hasManyToMany = true
			}
		}
	}

	if !hasLeaf {
		return ErrItemIsNotTreeLeaf
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
const leafIDDBField = "leaf_id"

func (ct *Tree) GetLeaves(ctx context.Context, target any, parentID uint, maxDepth int, tenant string) error {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return err
	}

	ids, err := ct.DescendantIds(ctx, parentID, maxDepth, tenant)
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
	if err := validateTableName(m2mTbl); err != nil {
		return err
	}
	if err := validateTableName(leaveTblName); err != nil {
		return err
	}

	joinSql := fmt.Sprintf(leavesJoinQuery, m2mTbl, leaveTblName, leafIDDBField, m2mTbl, singular(leaveTblName), leafIDDBField)
	err = ct.db.WithContext(ctx).Model(target).InnerJoins(joinSql).
		Preload(fieldName).
		Where(fmt.Sprintf(leavesWhereQuery, m2mTbl, singular(ct.nodesTbl), nodeIdDBField, leaveTblName), ids, tenant).
		Distinct().
		Find(target).Error

	return err
}

const leavesJoinQuery = `INNER JOIN %s ON %s.%s = %s.%s_%s`
const leavesWhereQuery = `%s.%s_%s IN ? AND %s.tenant = ?`

// singular returns the singular form of the input string using proper English inflection rules.
func singular(in string) string {
	return inflection.Singular(in)
}
