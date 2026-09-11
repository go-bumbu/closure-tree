package closuretree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

var validTableName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func validateTableName(name string) error {
	if !validTableName.MatchString(name) {
		return fmt.Errorf("invalid table name %q: must match [a-z][a-z0-9_]*", name)
	}
	return nil
}

const closureTblName = "closure_tree_rel"
const ancestorIDMapKey = "ancestorId"

var (
	ErrItemIsNotTreeNode      = errors.New("the item does not embed Node")
	ErrNilItem                = errors.New("item must not be a nil pointer")
	ErrParentNotFound         = errors.New("wrong parent ID")
	ErrNodeNotFound           = errors.New("node not found")
	ErrInvalidMove            = errors.New("invalid move")
	ErrItemNotPointerToStruct = errors.New("item needs to be a pointer to a struct")
	ErrEmptyMatch             = errors.New("no match fields provided")
	ErrUnknownMatchField      = errors.New("match field not found on the model")
	ErrNoOp                   = errors.New("update called with no item, no new parent, and no new sort order")
	ErrInvalidAfterNode       = errors.New("afterNodeID is not a sibling of the target parent")
	ErrAfterNodeIsSelf        = errors.New("afterNodeID cannot be the node itself")

	ErrItemsNil                    = errors.New("items cannot be nil")
	ErrItemsNotPointerToSlice      = errors.New("items must be a pointer to a slice")
	ErrSliceElemNotPointerToStruct = errors.New("slice element type must be a pointer to a struct")
)

// Tree represents the access to the closure tree allowing to CRUD nodes on the tree of items.
//
// Concurrency: a Tree is safe to share across goroutines. Every structural write (Add, Update,
// DeleteRecurse, GetOrAdd, Renormalize) first takes a per-tenant lock, so writes within a tenant
// serialize and cannot corrupt the tree. This prevents the patterns that would otherwise race under
// the default isolation level: two conflicting moves ("move A under B" and "move B under A") forming
// a cycle, Add racing DeleteRecurse of the same parent leaving orphaned nodes or stale closure rows,
// and two GetOrAdd calls creating duplicate siblings. Writes for different tenants run concurrently,
// and reads never take the lock.
//
// The lock is a SELECT ... FOR UPDATE on the tenant's anchor row on PostgreSQL and MySQL (released
// when the transaction ends); on SQLite, which already serializes writers per database, it is the
// anchor upsert. SQLite callers should set a busy_timeout so a waiting writer blocks briefly instead
// of failing immediately with SQLITE_BUSY.
type Tree struct {
	db *gorm.DB
	// table names, allows multiple trees
	nodesTbl     string
	relationsTbl string
	metaTbl      string
	lockTbl      string
	col2FieldMap map[string]string
	model        any // node model retained so Migrate can AutoMigrate it
}

// New returns a Tree for the given item on the specified gorm database. It parses and validates the
// schema and the derived table names but does NOT create any tables: call Migrate to create or
// update the schema. Separating the two means New needs no DDL privilege and callers decide when
// (and how often) migration runs. On MySQL, New verifies the server is 8.0+.
func New(db *gorm.DB, item any) (*Tree, error) {
	ct, err := newTree(db, item)
	if err != nil {
		return nil, err
	}
	if isMySQLDialect(db) {
		if err := checkMySQLVersion(db); err != nil {
			return nil, err
		}
	}
	return ct, nil
}

// newTree parses the schema and validates the item but does not run migrations.
func newTree(db *gorm.DB, item any) (*Tree, error) {
	if err := checkItem(item); err != nil {
		return nil, err
	}

	stmt := &gorm.Statement{DB: db}
	err := stmt.Parse(item)
	if err != nil {
		return nil, fmt.Errorf("error parsing schema: %w", err)
	}
	name := stmt.Schema.Table
	relTbl := strings.ToLower(fmt.Sprintf("%s_%s", closureTblName, name))
	metaTbl := strings.ToLower(fmt.Sprintf("closure_tree_meta_%s", name))
	lockTbl := strings.ToLower(fmt.Sprintf("closure_tree_lock_%s", name))

	if err := validateTableName(name); err != nil {
		return nil, err
	}
	if err := validateTableName(relTbl); err != nil {
		return nil, err
	}
	if err := validateTableName(metaTbl); err != nil {
		return nil, err
	}
	if err := validateTableName(lockTbl); err != nil {
		return nil, err
	}

	// Generate a map of column names to field names
	columnFieldMap := make(map[string]string)
	for _, field := range stmt.Schema.Fields {
		columnFieldMap[field.DBName] = field.Name
	}
	columnFieldMap["ancestor_id"] = ancestorIDMapKey

	ct := &Tree{
		db:           db,
		nodesTbl:     name,
		col2FieldMap: columnFieldMap,
		relationsTbl: relTbl,
		metaTbl:      metaTbl,
		lockTbl:      lockTbl,
		model:        item,
	}

	return ct, nil
}

// Migrate creates or updates the four tables this Tree manages — the node table, the closure
// relationship table, the sort-order metadata table, and the per-tenant lock table — via GORM
// AutoMigrate. It is separate from
// New so that construction needs no DDL privilege and so migration can be run explicitly (e.g. once
// at startup, guarded against concurrent runners). AutoMigrate is additive: it will not drop or
// alter an existing index, so see the note on closureTree when upgrading a pre-0.10 schema.
func (ct *Tree) Migrate() error {
	if err := ct.db.AutoMigrate(ct.model); err != nil {
		return fmt.Errorf("unable to migrate node table: %w", err)
	}
	if err := ct.db.Table(ct.relationsTbl).AutoMigrate(closureTree{}); err != nil {
		return fmt.Errorf("unable to migrate closure table: %w", err)
	}
	if err := ct.db.Table(ct.metaTbl).AutoMigrate(closureTreeMeta{}); err != nil {
		return fmt.Errorf("unable to migrate meta table: %w", err)
	}
	if err := ct.db.Table(ct.lockTbl).AutoMigrate(closureTreeLock{}); err != nil {
		return fmt.Errorf("unable to migrate lock table: %w", err)
	}
	return nil
}

func isMySQLDialect(db *gorm.DB) bool {
	return db.Name() == "mysql"
}

func checkMySQLVersion(db *gorm.DB) error {
	var version string
	if err := db.Raw("SELECT VERSION()").Scan(&version).Error; err != nil {
		return fmt.Errorf("unable to check MySQL version: %w", err)
	}
	// SplitN with a non-empty separator always returns at least one element, so parts[0] is safe.
	parts := strings.SplitN(version, ".", 2)
	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 8 {
		return fmt.Errorf("MySQL 8.0+ required; got %s", version)
	}
	return nil
}

// NodeTableName returns the table name of the stored Nodes, used if you need to interact directly
// with the database
func (ct *Tree) NodeTableName() string {
	return ct.nodesTbl
}

// ClosureTableName returns the table name of the node closure tree relationship, used if you need to interact directly
// with the database
func (ct *Tree) ClosureTableName() string {
	return ct.relationsTbl
}

// represents the table that store the relationships
// Note: if upgrading from a previous version, manually run: DROP INDEX idx_desc_ten
type closureTree struct {
	AncestorID   uint   `gorm:"not null;index:idx_anc_ten_dep,composite:1;uniqueIndex:idx_closure_uniq,composite:a"`
	DescendantID uint   `gorm:"not null;index:idx_desc_ten_dep,composite:1;uniqueIndex:idx_closure_uniq,composite:b"`
	Tenant       string `gorm:"not null;index:idx_anc_ten_dep,composite:2;index:idx_desc_ten_dep,composite:2;uniqueIndex:idx_closure_uniq,composite:c"`
	Depth        int    `gorm:"not null;default:0;check:chk_depth,depth >= 0;index:idx_anc_ten_dep,composite:3;index:idx_desc_ten_dep,composite:3;uniqueIndex:idx_closure_uniq,composite:d"`
}

// closureTreeLock holds one anchor row per tenant. Every structural write locks its tenant's row
// (SELECT ... FOR UPDATE, or the plain upsert on SQLite) as the first statement of its transaction,
// so conflicting writes within a tenant serialize instead of racing on the lock-free guards. See
// lockTenant.
type closureTreeLock struct {
	Tenant string `gorm:"not null;primaryKey"`
}

// DefaultTenant is used in the database as a stub if not tenant was passed
const DefaultTenant = "DefaultTenant"

// ErrEmptyTenant is returned when an empty tenant string is passed to any tree operation.
var ErrEmptyTenant = errors.New("tenant must not be empty; pass closuretree.DefaultTenant to use the default")

func validateTenant(in string) (string, error) {
	if in == "" {
		return "", ErrEmptyTenant
	}
	return in, nil
}

// derefUint returns the pointed-to value, or 0 for a nil pointer. Used to fold Add's *uint
// parentID/afterNodeID (where nil is equivalent to 0) onto the internal uint helpers.
func derefUint(p *uint) uint {
	if p == nil {
		return 0
	}
	return *p
}

// Add adds a new entry into the node Database under a specific parent, owned by a specific tenant.
// The passed item has to embed a Node struct, but any value set on the Node is ignored.
//
// parentID and afterNodeID follow Update's *uint convention: a nil parentID (or &0) adds at the
// root, &id adds under that node; a nil afterNodeID (or &0) places first among the siblings, &id
// places after that sibling.
func (ct *Tree) Add(ctx context.Context, item any, parentID *uint, afterNodeID *uint, tenant string) error {
	if err := checkItem(item); err != nil {
		return err
	}
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}

	pid, aid := derefUint(parentID), derefUint(afterNodeID)
	reflectItem, t, itemIsPointer := stripNodeCopy(item)

	err = ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {
		return ct.addInTx(tx, reflectItem, t, pid, aid, tenant)
	})
	if err != nil {
		return err
	}

	// if the item is a pointer copy the Node (including the new NodeId) back into it
	if itemIsPointer {
		copyNodeBack(item, reflectItem, t)
	}

	return nil
}

// stripNodeCopy returns a fresh, addressable copy of item (dereferenced if a pointer) so
// that any value the caller set on the embedded Node is ignored on write. itemIsPointer
// reports whether item was passed as a pointer; t is the (dereferenced) struct type.
func stripNodeCopy(item any) (reflectItem any, t reflect.Type, itemIsPointer bool) {
	t = reflect.TypeOf(item)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
		itemIsPointer = true
	}
	reflectItem = reflect.New(t).Interface()
	if itemIsPointer {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item).Elem())
	} else {
		reflect.ValueOf(reflectItem).Elem().Set(reflect.ValueOf(item))
	}
	return reflectItem, t, itemIsPointer
}

// copyNodeBack copies the embedded Node field (including NodeId, Tenant and SortOrder) from
// src into dst. Both must point to structs of the same type t that embed Node. Caller payload
// fields on dst are left untouched.
func copyNodeBack(dst, src any, t reflect.Type) {
	srcV := reflect.ValueOf(src).Elem()
	dstV := reflect.ValueOf(dst).Elem()
	if srcNode, ok := findNodeValue(t, srcV); ok {
		if dstNode, ok := findNodeValue(t, dstV); ok && dstNode.CanSet() {
			dstNode.Set(srcNode)
		}
	}
}

// addInTx runs the node-creation body of Add inside an existing transaction: it validates
// the parent and afterNodeID, computes the sort order, creates the node row and its closure
// relationships. reflectItem must be a fresh (Node-stripped) copy of the caller's item and t
// its type; on success reflectItem holds the newly assigned NodeId. tenant must already be
// validated. Must be called inside a transaction.
func (ct *Tree) addInTx(tx *gorm.DB, reflectItem any, t reflect.Type, parentID, afterNodeID uint, tenant string) error {
	// Check if the parent node exists and the tenant is the same (inside tx to avoid TOCTOU)
	if parentID != 0 {
		var parent Node
		err := tx.Table(ct.nodesTbl).
			Where("node_id = ? AND tenant = ?", parentID, tenant).
			First(&parent).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParentNotFound
			}
			return fmt.Errorf("unable to check parent node: %w", err)
		}
	}

	// Validate afterNodeID is a sibling of parentID (if non-zero)
	if afterNodeID != 0 {
		if err := ct.validateAfterNode(tx, parentID, afterNodeID, tenant); err != nil {
			return err
		}
	}
	// Compute the sort order for the new node
	sortOrder, err := ct.placeAmongSiblings(tx, parentID, afterNodeID, tenant)
	if err != nil {
		return err
	}

	// Set Node fields (including SortOrder) on the item before Create
	v := reflect.ValueOf(reflectItem).Elem()
	if nodeField, ok := findNodeValue(t, v); ok && nodeField.CanSet() {
		nodeField.Set(reflect.ValueOf(Node{NodeId: 0, Tenant: tenant, SortOrder: sortOrder}))
	}

	// create the Node item
	err = tx.Table(ct.nodesTbl).Create(reflectItem).Error
	if err != nil {
		return fmt.Errorf("unable to add node: %w", err)
	}

	id, gotTennant, err := getNodeData(reflectItem)
	if err != nil {
		return fmt.Errorf("unable to get Item ID: %w", err)
	}

	// Add reflexive relationship
	err = tx.Table(ct.relationsTbl).Create(&closureTree{AncestorID: id, DescendantID: id, Tenant: gotTennant, Depth: 0}).Error
	if err != nil {
		return fmt.Errorf("unable to add reflexive relationship: %w", err)
	}

	if parentID == 0 {
		// Create a root note relationship
		sqlstr := fmt.Sprintf(addRootRelQuery, ct.relationsTbl)
		ex := tx.Exec(sqlstr, id, gotTennant)
		if ex.Error != nil {
			return fmt.Errorf("unable to add root relationship: %w", ex.Error)
		}
	} else {
		// Copy all ancestors of the parent to include the new tag
		sqlstr := fmt.Sprintf(addRelsQuery, ct.relationsTbl, ct.relationsTbl)
		ex := tx.Exec(sqlstr, id, gotTennant, parentID, gotTennant)
		if ex.Error != nil {
			return fmt.Errorf("unable to add ancestor relationships: %w", ex.Error)
		}
	}
	return nil
}

// Virtual root: ancestor_id 0 is a synthetic node that is never stored in the nodes table.
// Every node — not just roots — gets one closure row with ancestor_id=0 whose depth equals the
// node's absolute level in the tree (1 for a root, 2 for its children, and so on). Root nodes get
// it directly via addRootRelQuery; deeper nodes inherit it because addRelsQuery copies all of the
// parent's ancestor rows (including the parent's ancestor_id=0 row) with depth+1. This row is
// load-bearing: the depth filter and the level-based queries rely on it existing for every node.
const addRelsQuery = `INSERT INTO %s (ancestor_id, descendant_id, tenant, depth)
			SELECT ancestor_id, ?, ?, depth + 1
			FROM %s
			WHERE descendant_id = ? AND tenant = ?;`

const addRootRelQuery = `INSERT INTO %s (ancestor_id, descendant_id, tenant, depth) VALUES (0, ?, ?, 1);`

// DeleteRecurse deletes the node identified by nodeId together with its entire subtree
// (all descendants at any depth), scoped to tenant. It also removes every closure relationship
// referencing the deleted nodes and cleans up their sort-order metadata. The operation is
// transactional. It returns ErrNodeNotFound if no node with that id exists for the given tenant,
// and ErrEmptyTenant if tenant is empty.
func (ct *Tree) DeleteRecurse(ctx context.Context, nodeId uint, tenant string) error {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	return ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {

		// delete the nodes
		delNodesSql := fmt.Sprintf(deleteNodesRec, ct.nodesTbl, ct.relationsTbl, ct.nodesTbl)
		exec1 := tx.Exec(delNodesSql, nodeId, tenant, tenant)
		if exec1.Error != nil {
			return fmt.Errorf("deleteRecurse: failed to delete nodes: %w", exec1.Error)
		}

		// make sure we don't delete relations if no node was deleted
		if exec1.RowsAffected == 0 {
			// note: for now we assume that if no row were affected we could not find either the node to move
			// or the new parent, either because they don't exist or because they belong to another tenant
			return ErrNodeNotFound
		}

		// Clean up sort-order metadata BEFORE deleting the closure rows: this cleanup identifies
		// descendants via the relations table, so those rows must still exist when it runs.
		// Deleted nodes can no longer have children, so their meta rows are stale.
		// This includes both deleted descendants and the root node itself.
		if err := tx.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE tenant = ? AND parent_id IN (
				SELECT descendant_id FROM %s WHERE ancestor_id = ? AND tenant = ?
			)`, ct.metaTbl, ct.relationsTbl),
			tenant, nodeId, tenant,
		).Error; err != nil {
			return fmt.Errorf("deleteRecurse: failed to clean metadata for descendants: %w", err)
		}

		// Also delete the meta row for the root node itself, since it no longer exists
		if err := tx.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE tenant = ? AND parent_id = ?`, ct.metaTbl),
			tenant, nodeId,
		).Error; err != nil {
			return fmt.Errorf("deleteRecurse: failed to clean metadata for root: %w", err)
		}

		// Delete the closure relationships last, after the metadata cleanup above has used them.
		delRelSql := fmt.Sprintf(deleteRelationsQuery, ct.relationsTbl, ct.relationsTbl)
		exec2 := tx.Exec(delRelSql, nodeId, tenant, tenant)
		if exec2.Error != nil {
			return fmt.Errorf("deleteRecurse: failed to delete relations: %w", exec2.Error)
		}

		return nil
	})
}

const deleteNodesRec = `WITH nodes_to_delete AS (
    SELECT nodes.node_id
    FROM %s AS nodes
    JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
    WHERE ct.ancestor_id = ? AND nodes.tenant = ?
)
DELETE FROM %s
WHERE node_id IN (SELECT node_id FROM nodes_to_delete)
  AND tenant = ?;`

const deleteRelationsQuery = `WITH descendants AS (
	SELECT descendant_id FROM %s WHERE ancestor_id = ? AND tenant = ?
)
DELETE FROM %s
WHERE tenant = ?
  AND (
      descendant_id IN (SELECT descendant_id FROM descendants)
   OR ancestor_id  IN (SELECT descendant_id FROM descendants)
  );`
