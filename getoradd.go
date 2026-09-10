package closuretree

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// FindChild looks up a single DIRECT child of parentID (depth=1 in the closure table) whose
// non-zero exported fields all equal those of match (query-by-example), scoped to tenant.
// parentID=0 matches among the root nodes.
//
// match must embed Node; only its non-Node, non-zero fields are used for the WHERE, so the
// caller never has to know internal ids or the tenant. If match has no non-zero field to
// match on, ErrEmptyMatch is returned.
//
// out must be a non-nil pointer to a struct that embeds Node. On a match, out is populated the
// same way GetNode does (including NodeId and the read-only ParentId) and found is true. When
// nothing matches, found is false and err is nil.
func (ct *Tree) FindChild(ctx context.Context, parentID uint, tenant string, match any, out any) (found bool, err error) {
	if !hasNode(match) {
		return false, ErrItemIsNotTreeNode
	}
	if !hasNode(out) {
		return false, ErrItemIsNotTreeNode
	}
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}
	if reflect.TypeOf(out).Kind() != reflect.Ptr {
		return false, ErrItemNotPointerToStruct
	}

	whereSQL, whereArgs, err := ct.buildMatchConditions(match)
	if err != nil {
		return false, err
	}
	return ct.findChildInTx(ct.db.WithContext(ctx), parentID, tenant, whereSQL, whereArgs, out)
}

// GetOrAdd is an idempotent get-or-create for a direct child of parentID. It looks up a direct
// child matching match (see FindChild for the match semantics); if one exists it loads it into
// item and returns created=false, otherwise it adds item under parentID (placed first among its
// siblings) and returns created=true. parentID=0 operates on the root level.
//
// When item is a pointer, its embedded Node — including NodeId — is populated on BOTH paths, so
// the returned node id can be chained as the parentID of the next level down. match must embed
// Node and have at least one non-zero field (else ErrEmptyMatch).
//
// Atomicity: the find and the add run in a single transaction, so a node is never left half
// created. However this does NOT fully serialize two callers creating the same brand-new child
// concurrently: because the child does not yet exist there is no row to lock, so both may pass
// the find and insert, yielding two sibling nodes with the same content. If several callers may
// race to create the same new child, serialize those calls (e.g. a single import worker) or
// deduplicate afterwards. Re-adding an already-existing child is race-free.
func (ct *Tree) GetOrAdd(ctx context.Context, item any, parentID uint, tenant string, match any) (created bool, err error) {
	if !hasNode(item) {
		return false, ErrItemIsNotTreeNode
	}
	if !hasNode(match) {
		return false, ErrItemIsNotTreeNode
	}
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}

	whereSQL, whereArgs, err := ct.buildMatchConditions(match)
	if err != nil {
		return false, err
	}

	reflectItem, t, itemIsPointer := stripNodeCopy(item)

	err = ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		found, ferr := ct.findChildInTx(tx, parentID, tenant, whereSQL, whereArgs, reflectItem)
		if ferr != nil {
			return ferr
		}
		if found {
			created = false
			return nil
		}
		created = true
		return ct.addInTx(tx, reflectItem, t, parentID, 0, tenant)
	})
	if err != nil {
		return false, err
	}

	if itemIsPointer {
		if created {
			// created path: preserve the caller's payload, set the new Node (matches Add)
			copyNodeBack(item, reflectItem, t)
		} else {
			// found path: fully hydrate item from the loaded row (matches GetNode)
			reflect.ValueOf(item).Elem().Set(reflect.ValueOf(reflectItem).Elem())
		}
	}

	return created, nil
}

// buildMatchConditions builds the WHERE fragment and bind args for a query-by-example match.
// It uses only the non-zero exported fields of match that are NOT owned by the embedded Node
// (so NodeId, Tenant, SortOrder and the read-only ParentId are always ignored). Column names
// come from the parsed GORM schema, never from user input; the values are returned as bind
// args. Returns ErrEmptyMatch when no usable non-zero field is present.
func (ct *Tree) buildMatchConditions(match any) (string, []any, error) {
	stmt := &gorm.Statement{DB: ct.db}
	if err := stmt.Parse(match); err != nil {
		return "", nil, fmt.Errorf("unable to parse match schema: %w", err)
	}

	v := reflect.ValueOf(match)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	nodeType := reflect.TypeOf(Node{})
	var conds []string
	var args []any
	for _, f := range stmt.Schema.Fields {
		if f.DBName == "" { // gorm:"-" fields have no column
			continue
		}
		if f.OwnerSchema != nil && f.OwnerSchema.ModelType == nodeType {
			continue
		}
		fieldVal := v.FieldByName(f.Name)
		if !fieldVal.IsValid() || fieldVal.IsZero() {
			continue
		}
		conds = append(conds, fmt.Sprintf("nodes.%s = ?", f.DBName))
		args = append(args, fieldVal.Interface())
	}
	if len(conds) == 0 {
		return "", nil, ErrEmptyMatch
	}
	return strings.Join(conds, " AND "), args, nil
}

// findChildInTx runs the direct-child query-by-example lookup on the given tx and scans the
// first match into out. It reports found=false (nil error) when no row matches.
func (ct *Tree) findChildInTx(tx *gorm.DB, parentID uint, tenant, whereSQL string, whereArgs []any, out any) (bool, error) {
	sqlstr := fmt.Sprintf(findChildQuery, ct.nodesTbl, ct.relationsTbl, whereSQL)
	args := make([]any, 0, len(whereArgs)+2)
	args = append(args, parentID, tenant)
	args = append(args, whereArgs...)

	result := tx.Raw(sqlstr, args...).Scan(out)
	if result.Error != nil {
		return false, fmt.Errorf("failed to find child: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return false, nil
	}
	return true, nil
}

// findChildQuery selects direct children (depth=1) of a parent, restricted by tenant and the
// caller's match conditions. child_rel.ancestor_id is the direct parent id (0 for root nodes),
// aliased to parent_id so the read-only Node.ParentId is populated exactly like GetNode.
const findChildQuery = `SELECT nodes.*, child_rel.ancestor_id AS parent_id
FROM %s AS nodes
JOIN %s AS child_rel
  ON child_rel.descendant_id = nodes.node_id
  AND child_rel.depth = 1
  AND child_rel.tenant = nodes.tenant
WHERE child_rel.ancestor_id = ? AND nodes.tenant = ? AND %s
ORDER BY nodes.sort_order ASC, nodes.node_id ASC
LIMIT 1`
