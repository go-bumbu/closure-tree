package closuretree

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// FindChild looks up a single DIRECT child of parentID (depth=1 in the closure table) whose fields
// named in matchFields all equal those of match (query-by-example), scoped to tenant. parentID=0
// matches among the root nodes.
//
// match must embed Node. matchFields names the fields that form the match key (Go field names or
// column names), resolved on match's own type as the read options resolve a column: a column name
// wins, and a Go field name that fields of several columns share, such as the model's own Name and
// the Name of a struct embedded with a column prefix, returns ErrUnknownMatchField: use the column
// name. Two differences from the read options remain: the columns of Node do not count when a Go
// field name is resolved, so a model's own Tenant field on another column resolves to that column,
// where the read options find the name ambiguous; and a read-only field excluded from migration is
// accepted, so the query fails when the table has no such column. Each is matched by its actual
// value, INCLUDING zero values (false/0/""). A nil
// pointer field, or a driver.Valuer field whose Value is nil (such as an invalid sql.NullString),
// matches as IS NULL; an error from Value is returned. A non-nil pointer field is matched by the
// value it points to, unless the pointer is a driver.Valuer (its Value method may have a pointer
// receiver): then its Value is matched. A byte-slice field that Add stores as NULL matches as IS
// NULL too: a nil one, also behind a pointer, and an empty one of a named type such as
// json.RawMessage that is not behind a pointer, which gorm writes as NULL; an empty []byte matches
// empty bytes. Eq binds the value it is given, so use IsNull there. An empty matchFields returns
// ErrEmptyMatch; an unknown or Node-owned field returns ErrUnknownMatchField.
//
// Every other value is bound as one value. Every value is checked as Eq checks its value: a field
// whose value is a slice or an array (other than a byte slice or a driver.Valuer), or whose type is
// a gorm.Valuer (also as a nil pointer), returns an error before any query. Such a field cannot be
// a match field, but an item with a gorm.Valuer field can still be added. gorm expands a list into
// several values on insert as well, so an item whose list field holds more than one element cannot
// be added; give such a field a serializer. A field with a gorm serializer is matched by its
// serialized value.
//
// out must be a non-nil pointer to a struct that embeds Node. On a match, out is populated the same
// way GetNode does (including NodeId and the read-only ParentId) and found is true. When nothing
// matches, found is false and err is nil.
//
// FindChild honours the read options Filter, Unfiltered and Columns; PerNode is ignored. A child
// that does not match the filter is not found; the parent's own match is not checked. Column names
// of every option are validated, so an unknown one returns ErrUnknownColumn.
func (ct *Tree) FindChild(ctx context.Context, parentID uint, tenant string, match any, matchFields []string, out any, opts ...ReadOption) (found bool, err error) {
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
	p, err := ct.planRead(opts)
	if err != nil {
		return false, err
	}
	return ct.findChildInTx(ct.db.WithContext(ctx), p, parentID, tenant, whereSQL, whereArgs, out)
}

// GetOrAdd is an idempotent get-or-create for a direct child of parentID. It looks up a direct child
// whose fields named in matchFields equal those of item; if one exists it loads it into item and
// returns created=false, otherwise it adds item under parentID (placed first among its siblings, or
// last with the PlaceLast option) and returns created=true. parentID=0 operates on the root level.
//
// The match key is read from item itself (there is no separate match struct): matchFields names the
// fields — Go field names or column names — that identify the child, resolved and matched by their
// actual value including zero values as in FindChild. An empty matchFields returns ErrEmptyMatch;
// an unknown or Node-owned field, or a Go field name that fields of several columns share, returns
// ErrUnknownMatchField before anything is added. A match field whose value is a list or a
// gorm.Valuer returns an error before any query, as in FindChild.
//
// When item is a pointer, its embedded Node — including NodeId — is populated on BOTH paths, so the
// returned node id can be chained as the parentID of the next level down.
//
// The two paths treat your payload differently: on create, item's non-Node fields are kept as you
// set them; on the found path item is fully overwritten with the stored row (like GetNode), so
// any fields you set beyond the match are replaced by the existing node's values.
//
// Atomicity: the find and the add run in a single transaction that first takes the tree's
// per-tenant write lock, so two callers racing to create the same brand-new child are serialized —
// the first creates it and the second finds it (created=false), leaving no duplicate siblings. See
// the concurrency note on Tree.
func (ct *Tree) GetOrAdd(ctx context.Context, item any, parentID uint, tenant string, matchFields []string, opts ...AddOption) (created bool, err error) {
	cfg := newAddConfig(opts)
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

	err = ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {
		found, ferr := ct.findChildInTx(tx, ct.plainPlan(), parentID, tenant, whereSQL, whereArgs, reflectItem)
		if ferr != nil {
			return ferr
		}
		if found {
			created = false
			return nil
		}
		created = true
		return ct.addInTx(tx, reflectItem, t, parentID, 0, cfg.last, tenant)
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
// fields named in matchFields. Each name is resolved on the schema of match by the rule of the read
// options (lookUpColumn): a column name wins; a Go field name counts the columns of fields that are
// not Node-owned, and one that several such columns share is rejected with ErrUnknownMatchField.
// Each field is matched by its ACTUAL value — including zero values (false/0/"") — so a key that
// is legitimately zero is honored rather than silently dropped. A value that eqValue reports as
// NULL matches as IS NULL, and so does a byte slice that Add stores as NULL (addedAsNull): the
// match compares with what Add stores, where Eq binds the value it is given. Every other value is
// bound through oneValue, as Eq binds it. The value gorm reads from the field (for a field with a
// serializer, a driver.Valuer that serializes it) is checked as Eq checks its value
// (checkBindValue), so a list or a gorm.Valuer is rejected before any query. Node-owned fields
// (NodeId/Tenant/SortOrder/ParentId) and unknown fields are rejected with ErrUnknownMatchField. An
// empty matchFields returns ErrEmptyMatch. Column identifiers are dialect-quoted, so a field
// mapping to a reserved-word column still produces valid SQL.
func (ct *Tree) buildMatchConditions(ctx context.Context, match any, matchFields []string) (string, []any, error) {
	if len(matchFields) == 0 {
		return "", nil, ErrEmptyMatch
	}
	stmt := &gorm.Statement{DB: ct.db}
	if err := stmt.Parse(match); err != nil {
		return "", nil, fmt.Errorf("unable to parse match schema: %w", err)
	}
	s := stmt.Schema
	matchable := func(f *schema.Field) bool { return !nodeOwned(s.ModelType, f) }

	rv := reflect.Indirect(reflect.ValueOf(match))
	var conds []string
	var args []any
	for _, name := range matchFields {
		f, err := lookUpColumn(s, name, matchable)
		if err != nil {
			return "", nil, fmt.Errorf("%w: %w", ErrUnknownMatchField, err)
		}
		if f == nil {
			// no column: an unknown name, a gorm:"-" field, or the Go name of a Node-owned field
			f = s.LookUpField(name)
			if f == nil || !nodeOwned(s.ModelType, f) {
				return "", nil, fmt.Errorf("%w: %q on %T", ErrUnknownMatchField, name, match)
			}
		}
		if nodeOwned(s.ModelType, f) {
			return "", nil, fmt.Errorf("%w: %q is a Node-owned field", ErrUnknownMatchField, name)
		}
		var b strings.Builder
		ct.db.QuoteTo(&b, "nodes."+f.DBName)
		col := b.String()

		raw, _ := f.ValueOf(ctx, rv)
		if err := checkBindValue(raw); err != nil {
			return "", nil, fmt.Errorf("match field %q: %w", name, err)
		}
		val, null, err := eqValue(raw)
		if err != nil {
			return "", nil, fmt.Errorf("match field %q: %w", name, err)
		}
		if null || addedAsNull(raw, val) {
			conds = append(conds, col+" IS NULL")
			continue
		}
		conds = append(conds, col+" = ?")
		args = append(args, oneValue(val))
	}
	return strings.Join(conds, " AND "), args, nil
}

// addedAsNull reports whether Add stores the byte slice that a match field holds as NULL, so that
// the match is IS NULL: a nil byte slice of any type, also behind a non-nil pointer; and an empty
// one of a named type, such as json.RawMessage, in a field that is not a pointer, which gorm's
// AddVar writes as an empty list, (NULL), where it binds an empty []byte as empty bytes. raw is the
// value read from the field and val the value eqValue made of it; a driver.Valuer is stored as its
// Value, which eqValue has already checked for NULL.
func addedAsNull(raw, val any) bool {
	if _, ok := val.(driver.Valuer); ok {
		return false
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Slice || rv.Type().Elem().Kind() != reflect.Uint8 {
		return false
	}
	if rv.IsNil() {
		return true
	}
	return rv.Len() == 0 && rv.Type() != reflect.TypeFor[[]byte]() && reflect.ValueOf(raw).Kind() != reflect.Pointer
}

// findChildInTx runs the direct-child query-by-example lookup planned by p on the given tx and
// scans the first match into out. It reports found=false (nil error) when no row matches.
func (ct *Tree) findChildInTx(tx *gorm.DB, p *readPlan, parentID uint, tenant, whereSQL string, whereArgs []any, out any) (bool, error) {
	sqlstr, args := ct.findChildSQL(p, parentID, tenant, whereSQL, whereArgs)
	result := tx.Raw(sqlstr, args...).Scan(out)
	if result.Error != nil {
		return false, fmt.Errorf("failed to find child: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return false, nil
	}
	return true, nil
}
