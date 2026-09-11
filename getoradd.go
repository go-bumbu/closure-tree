package closuretree

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// FindChild looks up a single DIRECT child of parentID (depth=1 in the closure table) whose fields
// named in matchFields all equal those of match (query-by-example), scoped to tenant. parentID=0
// matches among the root nodes.
//
// match must embed Node. matchFields names the fields that form the match key (Go field names or
// column names); each is matched by its actual value, INCLUDING zero values (false/0/""), and a nil
// pointer field matches as IS NULL. An empty matchFields returns ErrEmptyMatch; an unknown or
// Node-owned field returns ErrUnknownMatchField.
//
// out must be a non-nil pointer to a struct that embeds Node. On a match, out is populated the same
// way GetNode does (including NodeId and the read-only ParentId) and found is true. When nothing
// matches, found is false and err is nil.
func (ct *Tree) FindChild(ctx context.Context, parentID uint, tenant string, match any, matchFields []string, out any) (found bool, err error) {
	if err = checkItem(match); err != nil {
		return false, err
	}
	if err = checkItem(out); err != nil {
		return false, err
	}
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}
	if reflect.TypeOf(out).Kind() != reflect.Pointer {
		return false, ErrItemNotPointerToStruct
	}

	whereSQL, whereArgs, err := ct.buildMatchConditions(ctx, match, matchFields)
	if err != nil {
		return false, err
	}
	return ct.findChildInTx(ct.db.WithContext(ctx), parentID, tenant, whereSQL, whereArgs, out)
}

// GetOrAdd is an idempotent get-or-create for a direct child of parentID. It looks up a direct child
// whose fields named in matchFields equal those of item; if one exists it loads it into item and
// returns created=false, otherwise it adds item under parentID (placed first among its siblings) and
// returns created=true. parentID=0 operates on the root level.
//
// The match key is read from item itself (there is no separate match struct): matchFields names the
// fields — Go field names or column names — that identify the child, matched by their actual value
// including zero values (see FindChild). An empty matchFields returns ErrEmptyMatch; an unknown or
// Node-owned field returns ErrUnknownMatchField.
//
// When item is a pointer, its embedded Node — including NodeId — is populated on BOTH paths, so the
// returned node id can be chained as the parentID of the next level down.
//
// The two paths treat your payload differently: on create, item's non-Node fields are kept as you
// set them; on the found path item is fully overwritten with the stored row (like GetNode), so
// any fields you set beyond the match are replaced by the existing node's values.
//
// Atomicity: the find and the add run in a single transaction, so a node is never left half
// created. However this does NOT fully serialize two callers creating the same brand-new child
// concurrently: because the child does not yet exist there is no row to lock, so both may pass
// the find and insert, yielding two sibling nodes with the same content. If several callers may
// race to create the same new child, serialize those calls (e.g. a single import worker) or
// deduplicate afterwards. Re-adding an already-existing child is race-free.
func (ct *Tree) GetOrAdd(ctx context.Context, item any, parentID uint, tenant string, matchFields []string) (created bool, err error) {
	if err = checkItem(item); err != nil {
		return false, err
	}
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}

	whereSQL, whereArgs, err := ct.buildMatchConditions(ctx, item, matchFields)
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
			// created path: preserve the caller's payload, set the new Node (matches Add).
			// addInTx leaves the read-only ParentId at 0, so set it explicitly to keep both
			// paths consistent with the found path (which hydrates ParentId from the row).
			copyNodeBack(item, reflectItem, t)
			setNodeParentID(item, t, parentID)
		} else {
			// found path: fully hydrate item from the loaded row (matches GetNode)
			reflect.ValueOf(item).Elem().Set(reflect.ValueOf(reflectItem).Elem())
		}
	}

	return created, nil
}

// setNodeParentID sets the embedded Node's read-only ParentId field on dst (a pointer to a
// struct of type t) to parentID, so GetOrAdd's created path reports the same ParentId the found
// path hydrates from the row.
func setNodeParentID(dst any, t reflect.Type, parentID uint) {
	nodeVal, ok := findNodeValue(t, reflect.ValueOf(dst).Elem())
	if !ok {
		return
	}
	if pf := nodeVal.FieldByName("ParentId"); pf.IsValid() && pf.CanSet() {
		pf.SetUint(uint64(parentID))
	}
}

// buildMatchConditions builds the WHERE fragment and bind args for a query-by-example match on the
// fields named in matchFields. Each name is resolved against the model schema (Go field name or
// column name) and matched by its ACTUAL value — including zero values (false/0/"") — so a key that
// is legitimately zero is honored rather than silently dropped; a nil pointer field matches as
// IS NULL. Node-owned fields (NodeId/Tenant/SortOrder/ParentId) and unknown fields are rejected with
// ErrUnknownMatchField. An empty matchFields returns ErrEmptyMatch. Column identifiers are
// dialect-quoted, so a field mapping to a reserved-word column still produces valid SQL.
func (ct *Tree) buildMatchConditions(ctx context.Context, match any, matchFields []string) (string, []any, error) {
	if len(matchFields) == 0 {
		return "", nil, ErrEmptyMatch
	}
	stmt := &gorm.Statement{DB: ct.db}
	if err := stmt.Parse(match); err != nil {
		return "", nil, fmt.Errorf("unable to parse match schema: %w", err)
	}

	rv := reflect.Indirect(reflect.ValueOf(match))
	nodeType := reflect.TypeOf(Node{})
	var conds []string
	var args []any
	for _, name := range matchFields {
		f := stmt.Schema.LookUpField(name)
		if f == nil || f.DBName == "" { // unknown field, or gorm:"-" (no column)
			return "", nil, fmt.Errorf("%w: %q on %T", ErrUnknownMatchField, name, match)
		}
		if f.OwnerSchema != nil && f.OwnerSchema.ModelType == nodeType {
			return "", nil, fmt.Errorf("%w: %q is a Node-owned field", ErrUnknownMatchField, name)
		}
		var b strings.Builder
		ct.db.QuoteTo(&b, "nodes."+f.DBName)
		col := b.String()

		val, _ := f.ValueOf(ctx, rv)
		rvVal := reflect.ValueOf(val)
		if val == nil || (rvVal.Kind() == reflect.Pointer && rvVal.IsNil()) {
			conds = append(conds, col+" IS NULL")
			continue
		}
		if rvVal.Kind() == reflect.Pointer {
			val = rvVal.Elem().Interface() // bind the dereferenced value, not the pointer
		}
		conds = append(conds, col+" = ?")
		args = append(args, val)
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
