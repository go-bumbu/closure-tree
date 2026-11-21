package closuretree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Leaf is an embeddable id and tenant for a model that GetLeaves reads. It is optional: GetLeaves
// reads any model with a many2many relation to the tree's nodes. Embedding Leaf adds a condition on
// the model's own tenant column, so a row of another tenant is not returned even when it is
// attached to a node of the requested tenant.
//
// Its tenant column keeps the collation of the caller's table; Migrate does not change it. On
// MySQL, to compare it exactly, run, with the column's current type, the statement in Tree without
// NOT NULL, since the column is nullable:
//
//	ALTER TABLE <table> MODIFY tenant varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
//
// Deprecated: GetLeaves no longer requires it and reads a plain model with a many2many relation to
// the nodes. A model without Leaf gets no tenant condition on its own table, so a caller who needs
// one adds it in their own query with SubtreeIDs, or keeps Leaf.
type Leaf struct {
	LeafId uint   `gorm:"autoIncrement;primaryKey;not null;column:leaf_id"`
	Tenant string `gorm:"index"`
}

func (n *Leaf) Id() uint {
	return n.LeafId
}

// ErrItemIsNotTreeLeaf was returned by GetLeaves for a model that did not embed Leaf.
//
// Deprecated: GetLeaves no longer returns it; a model without Leaf is accepted.
var ErrItemIsNotTreeLeaf = errors.New("the item does not embed Leaf")

// ErrLeafMissingM2M is returned when the model given to GetLeaves has no many2many relation to the
// tree's node table, or when the field named with Relation is not one.
var ErrLeafMissingM2M = errors.New("item struct does not contain a many2many gorm tag")

// missingM2MError is an ErrLeafMissingM2M with a message that says what is wrong with the model.
type missingM2MError struct{ msg string }

func (e *missingM2MError) Error() string { return e.msg }
func (e *missingM2MError) Unwrap() error { return ErrLeafMissingM2M }

// ErrAmbiguousRelation is returned when the model given to GetLeaves has several many2many
// relations to the tree's node table and no Relation option names one.
var ErrAmbiguousRelation = errors.New("several many2many relations point to the node table; name one with Relation")

// GetLeaves loads into target the rows of a model attached to parentID or to a node below it,
// scoped to tenant. target is a pointer to a slice of the model, []T or []*T. The model needs a
// many2many relation to the tree's node type; when it has several, name the field with Relation.
// The rows are ordered by the model's primary key, which must be a single column, and each row
// comes once, also when it is attached to several nodes of the subtree. parentID 0 is the virtual
// root and stands for every node of the tenant.
//
// maxDepth limits how far below parentID a node may be: the parent itself is included, and
// maxDepth <= 0 means no limit. A DepthRange option replaces maxDepth.
//
// GetLeaves honours the read options Filter, Unfiltered, PerNode, DepthRange, Relation and
// NoPreload; the others are ignored. The filter, and the Tree's default filter, apply to the nodes
// as in SubtreeIDs: without PerNode a node counts only when it and every node between parentID and
// it match, with PerNode when it matches itself, and a row is returned when it is attached to a
// node that counts. The relation is preloaded unless NoPreload is given; the preloaded nodes are not
// checked against the tenant or the filter. Preloading binds one value per returned row, so for
// results of many thousands of rows use NoPreload. A model that embeds Leaf is also checked
// against its own tenant column.
//
// For ordering, paging, counting or conditions on the model's own table, use SubtreeIDs in your own
// query instead.
func (ct *Tree) GetLeaves(ctx context.Context, target any, parentID uint, maxDepth int, tenant string, opts ...ReadOption) error {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return err
	}
	if err := checkLeafTarget(target); err != nil {
		return err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return err
	}
	q, err := ct.planLeaves(target, p.relation)
	if err != nil {
		return err
	}

	depth := depthRange{min: 0, max: maxDepth}
	if maxDepth <= 0 {
		depth.max = -1
	}
	if p.depth != nil {
		depth = *p.depth
	}
	sub := ct.subquery(ct.subtreeSQL(p, tenant, []uint{parentID}, depth))

	db := ct.db.WithContext(ctx).Model(target).Where(q.in, sub)
	if q.tenantCol != "" {
		db = db.Where(q.tenantCol+" = ?", tenant)
	}
	db = db.Order(q.order)
	if !p.noPreload {
		db = db.Preload(q.field)
	}
	if err := db.Find(target).Error; err != nil {
		return fmt.Errorf("unable to query leaves: %w", err)
	}
	return nil
}

// checkLeafTarget accepts a non-nil pointer to a slice of structs or of pointers to structs.
func checkLeafTarget(target any) error {
	if target == nil {
		return ErrItemsNil
	}
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Pointer || v.Type().Elem().Kind() != reflect.Slice {
		return ErrItemsNotPointerToSlice
	}
	if v.IsNil() {
		return ErrItemsNil
	}
	elem := v.Type().Elem().Elem()
	if elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("the target must be a slice of structs or of pointers to structs, not %s", v.Type().Elem())
	}
	return nil
}

// leafQuery holds the quoted SQL fragments of a GetLeaves statement on one model.
type leafQuery struct {
	field     string // the Go field of the relation, for Preload
	in        string // the row is attached to a node of the subquery bound to its placeholder
	tenantCol string // the model's tenant column, quoted; "" when the model does not embed Leaf
	order     string
}

// planLeaves resolves the relation of target's model to the node table, its join columns and the
// model's primary key and tenant column from the parsed schema, and validates the table names.
func (ct *Tree) planLeaves(target any, relation string) (*leafQuery, error) {
	stmt := &gorm.Statement{DB: ct.db}
	if err := stmt.Parse(target); err != nil {
		return nil, fmt.Errorf("error parsing schema: %w", err)
	}
	s := stmt.Schema
	rel, err := ct.leafRelation(s, relation)
	if err != nil {
		return nil, err
	}

	// The reference with OwnPrimaryKey maps the model's column to its foreign key in the join
	// table; the other maps the node's id to its foreign key in the join table.
	var ownCol, ownFK, nodeFK string
	var own, other int
	for _, ref := range rel.References {
		if ref.OwnPrimaryKey {
			ownCol, ownFK = ref.PrimaryKey.DBName, ref.ForeignKey.DBName
			own++
		} else {
			if ref.PrimaryKey.DBName != colNodeID {
				return nil, fmt.Errorf("the relation %s of %s must reference the node column %s, not %s",
					rel.Name, s.Name, colNodeID, ref.PrimaryKey.DBName)
			}
			nodeFK = ref.ForeignKey.DBName
			other++
		}
	}
	if own != 1 || other != 1 {
		return nil, fmt.Errorf("unable to resolve the many2many join columns of %s.%s: want one column on each side",
			s.Name, rel.Name)
	}
	if len(s.PrimaryFields) != 1 {
		return nil, fmt.Errorf("the model %s must have exactly one primary key column for GetLeaves, it has %d",
			s.Name, len(s.PrimaryFields))
	}

	joinTbl, tbl := rel.JoinTable.Table, s.Table
	if err := validateTableName(joinTbl); err != nil {
		return nil, err
	}
	if err := validateTableName(tbl); err != nil {
		return nil, err
	}

	q := &leafQuery{
		field: rel.Name,
		in: ct.quoteCol(tbl, ownCol) + " IN (SELECT " + ct.quoteCol(joinTbl, ownFK) +
			" FROM " + ct.quoteTable(joinTbl) + " WHERE " + ct.quoteCol(joinTbl, nodeFK) + " IN (?))",
		order: ct.quoteCol(tbl, s.PrimaryFields[0].DBName) + " ASC",
	}
	if col, embeds := leafTenantColumn(s); embeds {
		if col == "" {
			return nil, fmt.Errorf("the model %s embeds Leaf, but its tenant column is not in the schema", s.Name)
		}
		q.tenantCol = ct.quoteCol(tbl, col)
	}
	return q, nil
}

// leafRelation returns the many2many relation of the model to the tree's node table: the one of
// the named field, or else the only one there is.
func (ct *Tree) leafRelation(s *schema.Schema, name string) (*schema.Relationship, error) {
	toNodes := func(rel *schema.Relationship) bool {
		return rel != nil && rel.Type == schema.Many2Many && rel.JoinTable != nil &&
			rel.FieldSchema != nil && rel.FieldSchema.Table == ct.nodesTbl
	}
	if name != "" {
		if rel := s.Relationships.Relations[name]; toNodes(rel) {
			return rel, nil
		}
		return nil, &missingM2MError{fmt.Sprintf("the field %s of %s is not a many2many relation to the node table %s",
			name, s.Name, ct.nodesTbl)}
	}
	var found []*schema.Relationship
	for _, rel := range s.Relationships.Many2Many {
		if toNodes(rel) {
			found = append(found, rel)
		}
	}
	switch len(found) {
	case 0:
		return nil, &missingM2MError{fmt.Sprintf("%s has no many2many relation to the node table %s", s.Name, ct.nodesTbl)}
	case 1:
		return found[0], nil
	}
	names := make([]string, len(found))
	for i, rel := range found {
		names[i] = rel.Name
	}
	return nil, fmt.Errorf("%w: %s has %s", ErrAmbiguousRelation, s.Name, strings.Join(names, ", "))
}

// leafTenantColumn tells whether the model embeds Leaf, at any embedding depth, and returns the
// column of its Tenant field, or "" when the schema has none.
func leafTenantColumn(s *schema.Schema) (string, bool) {
	path, ok := leafPath(s.ModelType)
	if !ok {
		return "", false
	}
	tenantField, _ := reflect.TypeOf(Leaf{}).FieldByName(tenantIdField)
	path = append(path, tenantField.Index...)
	for _, f := range s.Fields {
		if slices.Equal(f.StructField.Index, path) {
			return f.DBName, true
		}
	}
	return "", true
}

// leafPath returns the index path of an embedded Leaf in t, found by type as findNodeValue finds
// Node. Embedding through a pointer counts too; such a step is written -i-1, as gorm writes it in
// the StructField.Index of an embedded field.
func leafPath(t reflect.Type) ([]int, bool) {
	leafType := reflect.TypeOf(Leaf{})
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.Anonymous {
			continue
		}
		ft, step := f.Type, i
		if ft.Kind() == reflect.Pointer {
			ft, step = ft.Elem(), -i-1
		}
		if ft == leafType {
			return []int{step}, true
		}
		if ft.Kind() == reflect.Struct {
			if sub, ok := leafPath(ft); ok {
				return append([]int{step}, sub...), true
			}
		}
	}
	return nil, false
}
