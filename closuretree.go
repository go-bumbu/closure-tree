package closuretree

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var validTableName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func validateTableName(name string) error {
	if !validTableName.MatchString(name) {
		return fmt.Errorf("invalid table name %q: must match [a-z][a-z0-9_]*", name)
	}
	return nil
}

const closureTblName = "closure_tree_rel"

var (
	ErrItemIsNotTreeNode      = errors.New("the item does not embed Node")
	ErrNilItem                = errors.New("item must not be a nil pointer")
	ErrParentNotFound         = errors.New("wrong parent ID")
	ErrNodeNotFound           = errors.New("node not found")
	ErrInvalidMove            = errors.New("invalid move")
	ErrItemNotPointerToStruct = errors.New("item needs to be a pointer to a struct")
	ErrEmptyMatch             = errors.New("no match fields provided")
	// ErrUnknownMatchField is returned by FindChild and GetOrAdd for a match field that is not a
	// column of the match type, that is a field of the embedded Node, or that is a Go field name
	// that fields of several columns share, such as the model's own Name and the Name of a struct
	// embedded with a column prefix: use the column name. A column name wins over a Go field name.
	// The names resolve as in the read options, except that the columns of Node do not count when a
	// Go field name is resolved (a model's own Tenant field on another column resolves to that
	// column), and that a read-only field excluded from migration is accepted.
	ErrUnknownMatchField = errors.New("match field not found on the model")
	ErrNoOp              = errors.New("update called with no item, no new parent, and no new sort order")
	ErrInvalidAfterNode  = errors.New("afterNodeID is not a sibling of the target parent")
	ErrAfterNodeIsSelf   = errors.New("afterNodeID cannot be the node itself")
	// ErrNodeColumn is returned by New for a model with a field of its own, one that is not a field
	// of the embedded Node, mapped to a column of Node: node_id, parent_id, tenant or sort_order. Such
	// a field takes the column from Node's field, so Add would store its value instead of the
	// tenant, id or sort order the tree sets. When the column is parent_id, the error also matches
	// ErrParentIDColumn.
	ErrNodeColumn = errors.New("the model maps a field of its own to a column of the embedded Node")
	// ErrParentIDColumn is returned by New, together with ErrNodeColumn, for a model with a field
	// of its own mapped to column parent_id.
	ErrParentIDColumn = errors.New("the model maps a field other than Node.ParentId to column parent_id")

	ErrItemsNil                    = errors.New("items cannot be nil")
	ErrItemsNotPointerToSlice      = errors.New("items must be a pointer to a slice")
	ErrSliceElemNotPointerToStruct = errors.New("slice element type must be a pointer to a struct")
)

// Tree represents the access to the closure tree allowing to CRUD nodes on the tree of items.
//
// Concurrency: a Tree is safe to share across goroutines. Every structural write (Add, GetOrAdd,
// Update, DeleteRecurse, DeleteRecurseMany, DeleteTenant, Renormalize, BuildFromParents), and
// LockTenant, first takes a per-tenant lock, so writes within a tenant serialize and cannot corrupt
// the tree. This prevents the patterns that would otherwise race under the default isolation level:
// two conflicting moves ("move A under B" and "move B under A") forming a cycle, Add racing
// DeleteRecurse of the same parent leaving orphaned nodes or stale closure rows, and two GetOrAdd
// calls creating duplicate siblings. Writes for different tenants run concurrently, and reads never
// take the lock. The transactions the library opens itself run at READ COMMITTED on PostgreSQL and
// MariaDB.
//
// On MariaDB, tree writes of different tenants can still deadlock (error 1213). A write that
// deletes closure rows, such as DeleteRecurse, DeleteRecurseMany, DeleteTenant, BuildFromParents
// with Rebuild, or an Update that moves a node, locks the closure rows it scans and waits for a row
// that another transaction holds. The closure table's index on the tenant keeps most of these scans
// within the tenant's rows, but not all: the tenant-wide deletes of DeleteTenant and
// BuildFromParents force it (FORCE INDEX; Migrate must have added the index, or they fail with an
// error that says so), while the other deletes, planned on stale statistics as on just-loaded
// tables, can still scan other tenants' rows. A deadlock never leaves the tree half-written:
// MariaDB rolls back the whole transaction that loses it.
//
// A write on a Tree not bound to a transaction runs in a transaction the library opens itself, and
// on MariaDB the library repeats that transaction when it loses a deadlock: up to 5 attempts in all,
// with a short random pause before each repeat (at most 375 ms over all of them). When every
// attempt loses, the write returns an error that wraps the last deadlock error, so errors.As still
// finds the driver's error (for github.com/go-sql-driver/mysql a *mysql.MySQLError with Number
// 1213); when ctx ends during a pause, the error wraps both that deadlock error and ctx.Err(). A
// lock wait timeout (error 1205) is not repeated, and PostgreSQL and SQLite make one attempt.
// In the library's measurements on MariaDB 11.8 no such write failed, and none needed a second
// attempt (README, "Concurrent writes on MariaDB"). Because a write can run more than once, the
// gorm hooks of the model run once per attempt: see Add and GetOrAdd.
//
// A write inside a caller's transaction (WithTx) is never repeated: MariaDB has rolled back the
// caller's whole transaction, which the caller must roll back and redo (see WithTx). There
// deadlocks remain: while a caller's transaction is open at MariaDB's default isolation level
// (REPEATABLE READ), its tree writes keep the locks of the rows they scanned until it ends, so
// writes of other tenants wait on it and can deadlock with it (in the library's measurements only
// between two imports with BuildFromParents, on gap locks of the closure table's tenant index,
// which READ COMMITTED does not take), and any tree write of
// the transaction, an Add too, can be the one that loses. A write on a Tree not bound to a
// transaction that MariaDB rolls back instead is repeated.
//
// MySQL shares gorm's driver with MariaDB and is accepted from 8.0.17, but not tested: the library
// treats it as MariaDB, except that its tenant collation is utf8mb4_0900_bin and its tree reads do
// not lift the recursion limit (see TreeDescendants).
//
// The lock is a SELECT ... FOR UPDATE on the tenant's anchor row on PostgreSQL and MariaDB (released
// when the transaction ends); on SQLite, which already serializes writers per database, it is the
// anchor upsert. SQLite callers should set a busy_timeout so a waiting writer blocks briefly instead
// of failing immediately with SQLITE_BUSY.
//
// Tenants are compared exactly on every database: "Acme", "acme", "acmé" and "acme " are four
// tenants. On MariaDB, Migrate sets the collation utf8mb4_nopad_bin on the tenant column of the
// tree's four tables while the tree holds no rows. A tree with rows keeps its collation unless
// gorm's AutoMigrate modifies its tenant column (see Migrate); under MariaDB's default,
// utf8mb4_uca1400_ai_ci, tenants that differ only in case or accents are one tenant, and
// DeleteTenant deletes all of them. To change it, stop writes to the tree,
// store one spelling per tenant, then run the statement below on each of the four tables, with the
// column's current type in place of varchar(191) (on MySQL, with utf8mb4_0900_bin). The four tables
// are the node table (NodeTableName) and, named after it, closure_tree_rel_<node table>
// (ClosureTableName), closure_tree_meta_<node table> and closure_tree_lock_<node table>:
//
//	ALTER TABLE <table> MODIFY tenant varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin NOT NULL;
//
// Each rebuilds its table. Resume writes only after all four: until then the tables compare tenants
// differently. Migrate returns ErrTenantCollation until all four agree.
type Tree struct {
	db *gorm.DB
	// table names, allows multiple trees
	nodesTbl     string
	relationsTbl string
	metaTbl      string
	lockTbl      string
	tenantIdx    string // the closure table's index on tenant, as gorm names it (see closureTree)
	nodeIdx      string // the node table's index on (tenant, node_id), as gorm names it (see Node)
	dialect      dialect
	mysql        bool        // on dialectMariaDB: New found a MySQL server, not MariaDB (see checkVersion)
	retry        retryPolicy // how writeTx repeats a transaction it opened itself (dialect.retryPolicy)
	model        any         // node model retained so Migrate can AutoMigrate it

	schema        *schema.Schema // the parsed model schema
	defaultFilter []resolvedCond // set by WithFilter
}

// New returns a Tree for the given item on the specified gorm database. It parses and validates the
// schema and the derived table names but does NOT create any tables: call Migrate to create or
// update the schema. Separating the two means New needs no DDL privilege and callers decide when
// (and how often) migration runs. On gorm's "mysql" driver, New verifies the server is MariaDB 11.8
// or later (or MySQL 8.0.17 or later, accepted but not tested).
//
// The model must be Node or embed it, and no field of its own may map to a column of Node
// (node_id, parent_id, tenant or sort_order): New returns ErrNodeColumn for such a field.
//
// db may be a chained handle, such as db.Set("gorm:table_options", ...): the Tree keeps its
// settings and starts every call from a copy of its statement, so its calls do not change db.
func New(db *gorm.DB, item any) (*Tree, error) {
	ct, err := newTree(db, item)
	if err != nil {
		return nil, err
	}
	ct.mysql, err = ct.dialect.checkVersion(db)
	if err != nil {
		return nil, err
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
	if err := checkNodeColumns(stmt.Schema); err != nil {
		return nil, err
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

	d := dialectOf(db)
	ct := &Tree{
		// A new session, so that every call starts from a copy of db's statement: on a chained
		// handle, such as db.Set("gorm:table_options", ...), Migrate's Table calls would otherwise
		// change the statement this Tree and the caller share. The settings of db are kept.
		db:           db.Session(&gorm.Session{}),
		nodesTbl:     name,
		relationsTbl: relTbl,
		metaTbl:      metaTbl,
		lockTbl:      lockTbl,
		tenantIdx:    db.NamingStrategy.IndexName(relTbl, "Tenant"),    // gorm's call for closureTree.Tenant's index tag
		nodeIdx:      db.NamingStrategy.IndexName(name, "tenant_node"), // gorm's call for Node's composite index
		dialect:      d,
		retry:        d.retryPolicy(),
		model:        item,
		schema:       stmt.Schema,
	}

	return ct, nil
}

// checkNodeColumns returns ErrNodeColumn when a field of the model that is not one of Node's own
// maps to a column of a Node field, and ErrParentIDColumn as well when that column is parent_id.
func checkNodeColumns(s *schema.Schema) error {
	nodeCols := map[string]bool{}
	for _, f := range s.Fields {
		if f.DBName != "" && nodeOwned(s.ModelType, f) {
			nodeCols[f.DBName] = true
		}
	}
	for _, f := range s.Fields {
		if !nodeCols[f.DBName] || nodeOwned(s.ModelType, f) {
			continue
		}
		if f.DBName == colParentID {
			return fmt.Errorf("%w: field %s, column %s: %w", ErrNodeColumn, f.Name, f.DBName, ErrParentIDColumn)
		}
		return fmt.Errorf("%w: field %s, column %s", ErrNodeColumn, f.Name, f.DBName)
	}
	return nil
}

// Migrate creates or updates the four tables this Tree manages — the node table, the closure
// relationship table, the sort-order metadata table, and the per-tenant lock table — via GORM
// AutoMigrate. It is separate from
// New so that construction needs no DDL privilege and so migration can be run explicitly (e.g. once
// at startup, guarded against concurrent runners). AutoMigrate is additive: it will not drop or
// alter an existing index. So a closure table created by v0.9.x keeps that version's indexes
// idx_anc_ten_dep, idx_desc_ten_dep and idx_closure_uniq and its check chk_depth next to the
// per-table ones Migrate creates (idx_<closure table>_anc_ten_dep and so on), which duplicate
// them. An index idx_<closure table>_tenant left from v0.8 or earlier is the tenant index
// described below, not a leftover: keep it. AutoMigrate creates the indexes a table lacks, so on
// a tree created before the closure table had its index on the tenant column, the next Migrate
// adds it. The index is named idx_<closure table>_tenant, shortened by gorm's naming strategy when
// that is too long; Migrate builds it unless an index of exactly that name exists. Building it on a
// large closure table takes time. MariaDB builds it in place and lets writes continue, except at its
// start and end, where it needs a short exclusive metadata lock: it waits for that lock until the
// open transactions that used the closure table end, and while it waits, every new statement on the
// closure table, reads too, queues behind it. So run Migrate when no long transaction uses the
// tree. PostgreSQL blocks writes to the closure table until the index is built. To avoid that,
// build it beforehand, outside a transaction, with
//
//	CREATE INDEX CONCURRENTLY idx_<closure table>_tenant ON <closure table> (tenant);
//
// and exactly that name. A concurrent build that fails leaves an invalid index of that name, which
// Migrate takes as present: drop it and build it again.
//
// On MariaDB, as its last step, Migrate gives the tenant column of the four tables the collation
// utf8mb4_nopad_bin (utf8mb4_0900_bin on MySQL) while none of them has a row, so that tenants are
// compared exactly (see Tree);
// that needs the ALTER privilege. That step never changes the tenant collation of a tree with rows,
// and returns ErrTenantCollation when the tenant columns of such a tree do not share one collation.
// gorm's AutoMigrate, which runs before it, can: it modifies the tenant column of the node table or
// the closure table when its type, size, default or comment differs from the model (an adopted
// table, a changed DefaultStringSize), and does so without a collation, which gives it the default
// collation of its table, rows or not. It leaves the tenant columns of the meta and lock tables
// alone, as they are part of the primary key, so on a tree with rows Migrate then returns
// ErrTenantCollation unless that default is the collation of the other tenant columns.
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
	return ct.ensureTenantCollation()
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
type closureTree struct {
	// Index names use GORM's empty-name composite form so each is scoped per closure table
	// (idx_<reltable>_<id>). A hardcoded name would collide when two trees are migrated into one
	// database, because index names are schema-global on SQLite/Postgres. priority preserves the
	// column order: (ancestor_id, tenant, depth), (descendant_id, tenant, depth), and the unique
	// (ancestor_id, descendant_id, tenant, depth). The depth check is likewise left unnamed so GORM
	// scopes it per table (chk_<reltable>_depth) — on MySQL check-constraint names are schema-global
	// (MariaDB scopes them per table).
	//
	// The fourth index, on (tenant) alone, is named idx_<reltable>_tenant (Tree.tenantIdx; gorm
	// shortens a long name). It keeps the tenant-wide deletes (DeleteTenant, BuildFromParents)
	// within the tenant's rows, so on MariaDB they lock fewer rows of other tenants; there they force
	// it (Tree.tenantDeleteSQL). On MariaDB it is as good as (tenant, ancestor_id,
	// descendant_id, depth) only because the table has no primary key: InnoDB then clusters it on the
	// unique index, whose columns every secondary index carries. A primary key would change that.
	// Migrate adds it to a tree created before it existed.
	AncestorID   uint   `gorm:"not null;index:,composite:anc_ten_dep,priority:1;uniqueIndex:,composite:uniq,priority:1"`
	DescendantID uint   `gorm:"not null;index:,composite:desc_ten_dep,priority:1;uniqueIndex:,composite:uniq,priority:2"`
	Tenant       string `gorm:"not null;index;index:,composite:anc_ten_dep,priority:2;index:,composite:desc_ten_dep,priority:2;uniqueIndex:,composite:uniq,priority:3"`
	Depth        int    `gorm:"not null;default:0;check:depth >= 0;index:,composite:anc_ten_dep,priority:3;index:,composite:desc_ten_dep,priority:3;uniqueIndex:,composite:uniq,priority:4"`
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
// places after that sibling. The PlaceLast option places the node last instead of first; combined
// with a non-zero afterNodeID it returns ErrConflictingPlacement.
//
// A parent that is not a node of tenant returns ErrParentNotFound. So does a node row without a
// closure row at depth 1 (not linked into the tree), as before BuildFromParents, with a message
// that names BuildFromParents; GetOrAdd and a move with Update reject such a parent the same way.
//
// Add creates the row from a shallow copy of item, and of an item passed as a pointer it sets only
// the embedded Node (to the new node). On MariaDB an Add on a Tree not bound to a transaction that
// loses a deadlock is repeated (see the concurrency note on Tree), each attempt with a fresh copy
// of item, so the model's gorm hooks (BeforeCreate, AfterCreate, ...) run once per attempt, also
// in attempts whose work MariaDB then rolled back. Keep hooks free of effects outside the database,
// or let them tolerate repeats. The copy shares item's pointer, slice and map fields: a hook that
// writes through one of them changes what item points to, in every attempt, the failed ones too,
// so do not rely on such writes.
func (ct *Tree) Add(ctx context.Context, item any, parentID *uint, afterNodeID *uint, tenant string, opts ...AddOption) error {
	if err := checkItem(item); err != nil {
		return err
	}
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}

	pid, aid := derefUint(parentID), derefUint(afterNodeID)
	cfg := newAddConfig(opts)
	if cfg.last && aid != 0 {
		return ErrConflictingPlacement
	}
	var reflectItem any
	var t reflect.Type
	var itemIsPointer bool
	err = ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {
		// A fresh copy per attempt, so that a repeated attempt does not see what gorm or the
		// model's hooks wrote into the copy of a failed one.
		reflectItem, t, itemIsPointer = stripNodeCopy(item)
		return ct.addInTx(tx, reflectItem, t, pid, aid, cfg.last, tenant)
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
// validated. With last set the node goes after the current last sibling and afterNodeID must be 0.
// Must be called inside a transaction.
func (ct *Tree) addInTx(tx *gorm.DB, reflectItem any, t reflect.Type, parentID, afterNodeID uint, last bool, tenant string) error {
	// Check if the parent node exists and the tenant is the same (inside tx to avoid TOCTOU)
	if parentID != 0 {
		if err := ct.requireParent(tx, parentID, tenant); err != nil {
			return err
		}
	}

	// Validate afterNodeID is a sibling of parentID (if non-zero)
	if afterNodeID != 0 {
		if err := ct.validateAfterNode(tx, parentID, afterNodeID, tenant); err != nil {
			return err
		}
	}
	// Compute the sort order for the new node
	var sortOrder float64
	var err error
	if last {
		var halvings int
		sortOrder, halvings, err = ct.lastSortOrder(tx, parentID, 0, tenant)
		if err != nil {
			return fmt.Errorf("unable to compute sort order: %w", err)
		}
		if err := ct.upsertMetaHalvings(tx, parentID, tenant, halvings); err != nil {
			return fmt.Errorf("unable to update sort order metadata: %w", err)
		}
	} else {
		sortOrder, err = ct.placeAmongSiblings(tx, parentID, afterNodeID, tenant)
		if err != nil {
			return err
		}
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

// requireParent returns ErrParentNotFound unless parentID, not 0, is a node of tenant that is part of
// the tree: a node row with its closure row at depth 1 (the parent's parent, or the virtual root 0
// for a root), which every node added, moved or built has. A node row without one, as before
// BuildFromParents, returns a wrapped ErrParentNotFound that names BuildFromParents: a node under
// it would get no virtual root row. One statement, on the primary key of the
// node table and the (descendant_id, tenant, depth) index of the closure table. Must be called
// inside a transaction.
func (ct *Tree) requireParent(tx *gorm.DB, parentID uint, tenant string) error {
	var row struct{ Linked *uint }
	res := tx.Raw(fmt.Sprintf(requireParentQuery, ct.nodesTbl, ct.relationsTbl), tenant, parentID, tenant).Scan(&row)
	if res.Error != nil {
		return fmt.Errorf("unable to check parent node: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrParentNotFound
	}
	if row.Linked == nil {
		return fmt.Errorf("%w: node %d is not linked into the tree; build the tree with BuildFromParents", ErrParentNotFound, parentID)
	}
	return nil
}

const requireParentQuery = `SELECT r.ancestor_id AS linked FROM %s n
	LEFT JOIN %s r ON r.descendant_id = n.node_id AND r.tenant = ? AND r.depth = 1
	WHERE n.node_id = ? AND n.tenant = ? LIMIT 1`

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
//
// A nodeId of 0, the virtual root, is not rejected: it deletes the tenant's whole tree, meaning
// every node that has closure rows, and returns ErrNodeNotFound when there is none. Node rows
// without closure rows, as in a table filled before BuildFromParents, are not deleted. Prefer
// DeleteTenant to delete a whole tenant; it deletes those rows too.
func (ct *Tree) DeleteRecurse(ctx context.Context, nodeId uint, tenant string) error {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	return ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {

		// delete the nodes
		delNodesSql := fmt.Sprintf(deleteNodesRec, ct.nodesTbl, ct.relationsTbl)
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
		delRelSql := fmt.Sprintf(deleteRelationsQuery, ct.relationsTbl)
		exec2 := tx.Exec(delRelSql, nodeId, tenant, nodeId, tenant, tenant)
		if exec2.Error != nil {
			return fmt.Errorf("deleteRecurse: failed to delete relations: %w", exec2.Error)
		}

		return nil
	})
}

// deleteNodesRec deletes the nodes of the subtree of a node. Table arguments: node table, closure
// table. Placeholders: (node id, tenant, tenant).
// The subtree is read in a derived table (FROM (SELECT ...) AS s), not a CTE before the DELETE: MariaDB
// accepts no WITH before DELETE, and the derived table also keeps MySQL from rejecting the read of
// the table the statement deletes from (error 1093).
const deleteNodesRec = `DELETE FROM %[1]s
WHERE node_id IN (
    SELECT node_id FROM (
        SELECT nodes.node_id
        FROM %[1]s AS nodes
        JOIN %[2]s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
        WHERE ct.ancestor_id = ? AND nodes.tenant = ?
    ) AS nodes_to_delete
)
  AND tenant = ?;`

// deleteRelationsQuery deletes the closure rows of the subtree of a node. Table argument: closure
// table. Placeholders: (node id, tenant, node id, tenant, tenant). The subtree is read in a derived
// table, as in deleteNodesRec.
const deleteRelationsQuery = `DELETE FROM %[1]s
WHERE (
      descendant_id IN (SELECT descendant_id FROM (
          SELECT descendant_id FROM %[1]s WHERE ancestor_id = ? AND tenant = ?) AS descendants)
   OR ancestor_id  IN (SELECT descendant_id FROM (
          SELECT descendant_id FROM %[1]s WHERE ancestor_id = ? AND tenant = ?) AS descendants2)
  )
  AND tenant = ?;`
