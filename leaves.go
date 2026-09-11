package closuretree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

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

// ErrLeafMissingM2M is returned when a leaf type has no many2many association to the tree's nodes.
var ErrLeafMissingM2M = errors.New("item struct does not contain a many2many gorm tag")

// isLeaveSlice uses reflection to verify if the passed item is a pointer to a slice that embedded Leaf struct
// returns an error for every condition checked, returns nil if the passed item is as expected
func isLeaveSlice(item any) error {
	if item == nil {
		return ErrItemsNil
	}

	itemType := reflect.TypeOf(item)

	// Ensure item is a pointer
	if itemType.Kind() != reflect.Pointer {
		return ErrItemsNotPointerToSlice
	}

	// Ensure the pointer points to a slice
	sliceType := itemType.Elem()
	if sliceType.Kind() != reflect.Slice {
		return ErrItemsNotPointerToSlice
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
		return ErrLeafMissingM2M
	}
	return nil
}

// m2mFieldName returns the struct field name of the first many2many association on the element type
// of item (a pointer to a slice of structs), in declaration order. isLeaveSlice must have validated
// item first.
func m2mFieldName(item any) (string, error) {
	if item == nil {
		return "", ErrItemsNil
	}
	elemType := reflect.TypeOf(item).Elem().Elem()
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		if field.Type.Kind() == reflect.Slice && strings.Contains(field.Tag.Get("gorm"), "many2many:") {
			return field.Name, nil
		}
	}
	return "", ErrLeafMissingM2M
}

// GetLeaves loads into target every leaf (a struct embedding Leaf with a many2many relation to the
// tree's node type) associated with parentID or any of its descendants down to maxDepth, scoped to
// tenant. parentID=0 operates on the whole forest; maxDepth<=0 means unlimited. Results are ordered
// by the leaf primary key.
//
// The many2many join columns are resolved from the parsed GORM schema, so a custom NamingStrategy /
// SingularTable works. The descendant set is filtered by joining the closure table directly instead
// of materialising ids into an IN (...) list, so arbitrarily large subtrees do not hit the driver's
// bind-parameter limit.
func (ct *Tree) GetLeaves(ctx context.Context, target any, parentID uint, maxDepth int, tenant string) error {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return err
	}
	// Validate the target slice before hitting the database.
	if err := isLeaveSlice(target); err != nil {
		return err
	}
	if maxDepth <= 0 {
		maxDepth = absMaxDepth
	}

	stmt := &gorm.Statement{DB: ct.db}
	if err := stmt.Parse(target); err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}

	fieldName, err := m2mFieldName(target)
	if err != nil {
		return err
	}
	rel, ok := stmt.Schema.Relationships.Relations[fieldName]
	if !ok || rel.JoinTable == nil {
		return fmt.Errorf("no many2many relationship found for field %q", fieldName)
	}

	// Resolve the join columns from the schema. For a many2many, the reference with
	// OwnPrimaryKey=true maps the leaf's (source) primary key to its join-table foreign key; the
	// other maps the node's primary key to its join-table foreign key.
	joinTbl := rel.JoinTable.Table
	leafTbl := rel.Schema.Table
	var leafPKCol, leafJoinFKCol, nodeJoinFKCol string
	for _, ref := range rel.References {
		if ref.OwnPrimaryKey {
			leafPKCol = ref.PrimaryKey.DBName
			leafJoinFKCol = ref.ForeignKey.DBName
		} else {
			nodeJoinFKCol = ref.ForeignKey.DBName
		}
	}
	if leafPKCol == "" || leafJoinFKCol == "" || nodeJoinFKCol == "" {
		return fmt.Errorf("unable to resolve many2many join columns for field %q", fieldName)
	}
	if err := validateTableName(joinTbl); err != nil {
		return err
	}
	if err := validateTableName(leafTbl); err != nil {
		return err
	}

	// leaf -> join table -> closure table. ancestor_id=parentID with depth<=maxDepth selects
	// parentID itself (its depth-0 self row) plus every descendant down to maxDepth; for parentID=0
	// there is no self row, so it yields the depth 1..maxDepth nodes under the forest.
	joinLeaf := fmt.Sprintf(`INNER JOIN %s ON %s.%s = %s.%s`,
		joinTbl, leafTbl, leafPKCol, joinTbl, leafJoinFKCol)
	joinClosure := fmt.Sprintf(`INNER JOIN %s ON %s.descendant_id = %s.%s`,
		ct.relationsTbl, ct.relationsTbl, joinTbl, nodeJoinFKCol)
	whereSQL := fmt.Sprintf(`%s.ancestor_id = ? AND %s.depth <= ? AND %s.tenant = ? AND %s.tenant = ?`,
		ct.relationsTbl, ct.relationsTbl, ct.relationsTbl, leafTbl)
	orderSQL := fmt.Sprintf(`%s.%s ASC`, leafTbl, leafPKCol)

	err = ct.db.WithContext(ctx).Model(target).
		InnerJoins(joinLeaf).
		InnerJoins(joinClosure).
		Preload(fieldName).
		Where(whereSQL, parentID, maxDepth, tenant, tenant).
		Order(orderSQL).
		Distinct().
		Find(target).Error
	if err != nil {
		return fmt.Errorf("unable to query leaves: %w", err)
	}
	return nil
}
