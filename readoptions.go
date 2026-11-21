package closuretree

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// ErrUnknownColumn is returned when a read option names a column that cannot be named. The name
// may be the database column name or the Go field name; a column name wins. A column can be named
// when the field gorm maps it to has a column name and is not both read-only and excluded from
// migration; the computed parent cannot be named. A Go field name that fields of several columns
// that can be named share, such as the model's own Name and the Name of a struct embedded with a
// column prefix, cannot be named either: use the column name.
var ErrUnknownColumn = errors.New("unknown column")

// ErrInvalidDepthRange is returned by every read when DepthRange has a negative minimum, or a
// maximum that is not negative and is smaller than the minimum.
var ErrInvalidDepthRange = errors.New("invalid depth range")

type condOp uint8

const (
	opEq condOp = iota
	opIsNull
	opNotNull
	opIn
)

// Cond is one condition of a Filter, on a column of the node table. Build it with Eq, IsNull,
// NotNull or In.
type Cond struct {
	column string
	op     condOp
	values []any
}

// Eq matches the nodes whose column equals value. A nil value, a nil pointer, or a driver.Valuer
// whose Value is nil matches NULL instead; an error from Value is returned by the read, or by
// WithFilter when the condition is a default filter. A non-nil pointer is dereferenced, unless it
// is a driver.Valuer. The value is bound as one value: a slice or an array (other than a byte slice
// or a driver.Valuer), a gorm expression, a *gorm.DB, a gorm.Valuer or a sql.NamedArg is rejected
// with an error before the read queries. The check is by type, so a nil pointer of a gorm.Valuer
// type is rejected too.
func Eq(column string, value any) Cond {
	return Cond{column: column, op: opEq, values: []any{value}}
}

// IsNull matches the nodes whose column is NULL.
func IsNull(column string) Cond { return Cond{column: column, op: opIsNull} }

// NotNull matches the nodes whose column is not NULL.
func NotNull(column string) Cond { return Cond{column: column, op: opNotNull} }

// In matches the nodes whose column equals one of the values. Without values it matches nothing.
// It never matches NULL, so In(column, nil) matches nothing; use IsNull. The values are copied, so
// changing the slice afterwards does not change the condition. Pass the values one by one: a value
// that is itself a slice or an array (other than a byte slice or a driver.Valuer), a gorm
// expression, a *gorm.DB, a gorm.Valuer or a sql.NamedArg is rejected with an error before the
// read queries. A tree read binds the values up to three times, so a very long list reaches the
// database's limit of bound values sooner there (SQLite 32766, PostgreSQL and MySQL 65535 per
// statement).
func In(column string, values ...any) Cond {
	return Cond{column: column, op: opIn, values: slices.Clone(values)}
}

// readConfig collects the ReadOptions of one call.
type readConfig struct {
	filter     []Cond
	perNode    bool
	unfiltered bool
	columns    []string
	hasColumns bool

	depth        *depthRange
	ancestors    []uint
	hasAncestors bool
	virtualRoot  bool
	relation     string
	noPreload    bool
}

// depthRange is the range of DepthRange; max < 0 means no upper limit.
type depthRange struct{ min, max int }

// ReadOption changes how a read selects its nodes or which columns it returns.
type ReadOption func(*readConfig)

// Filter keeps only the nodes that match every condition. Every read that takes options honours
// it; what a non-matching node does differs per read and is described on each. For this call it
// replaces the Tree's default filter (see WithFilter); several Filter options are joined. An empty
// Filter has no effect, so the default filter still applies.
func Filter(conds ...Cond) ReadOption {
	return func(c *readConfig) {
		c.filter = append(c.filter, conds...)
	}
}

// PerNode changes how Descendants, DescendantIds, SubtreeIDs, SubtreeIDsOf and GetLeaves apply
// the filter. By default they prune: a node counts only if it and every node between the requested
// parent and it match. With PerNode a node counts when it matches itself, whatever its ancestors
// do. The other reads ignore it.
func PerNode() ReadOption { return func(c *readConfig) { c.perNode = true } }

// Unfiltered switches the Tree's default filter off for this call, in every read that takes options.
func Unfiltered() ReadOption { return func(c *readConfig) { c.unfiltered = true } }

// Columns limits the columns a read returns to node_id, tenant, sort_order and the named ones,
// given as database column name or Go field name, so Columns() with no names returns only those
// three. Without Columns a read returns every column of the node table. The parent is always
// computed from the closure rows. GetNode, FindChild, Descendants, TreeDescendants and Ancestors
// honour it; the other reads ignore it. Fields that are not selected stay zero, so do not pass the
// item to Update: Update writes every field, and the columns that were not selected would be
// overwritten with zero values.
func Columns(names ...string) ReadOption {
	return func(c *readConfig) {
		c.hasColumns = true
		c.columns = append(c.columns, names...)
	}
}

// DepthRange limits how far from the starting node a read looks: 0 is the node itself, 1 its
// direct children, and so on. max < 0 means no upper limit. DepthRange(0, 0) is this node only,
// DepthRange(1, -1) is everything strictly below it, DepthRange(1, 1) is the direct children. A
// negative min, or a max that is not negative and is smaller than min, makes every read return
// ErrInvalidDepthRange before it queries. SubtreeIDs, SubtreeIDsOf, ClosurePairs and GetLeaves
// honour it, each with its own default; the other reads ignore it. The last DepthRange wins.
func DepthRange(min, max int) ReadOption {
	return func(c *readConfig) { c.depth = &depthRange{min: min, max: max} }
}

// ForAncestors limits ClosurePairs to the pairs whose ancestor is one of ids. An id 0 selects the
// pairs of the virtual root. Without ids it selects nothing. The ids are copied, and several
// ForAncestors options are joined. The other reads ignore it.
func ForAncestors(ids ...uint) ReadOption {
	ids = slices.Clone(ids)
	return func(c *readConfig) {
		c.hasAncestors = true
		c.ancestors = append(c.ancestors, ids...)
	}
}

// IncludeVirtualRoot makes ClosurePairs without ForAncestors include the pairs of the virtual root
// (ancestor 0), which it leaves out otherwise. With ForAncestors it has no effect: the virtual root
// is included when ForAncestors names id 0. The other reads ignore it.
func IncludeVirtualRoot() ReadOption { return func(c *readConfig) { c.virtualRoot = true } }

// Relation names the field of the leaf model that holds the relation to the nodes, for GetLeaves
// when the model has more than one. The other reads ignore it.
func Relation(fieldName string) ReadOption { return func(c *readConfig) { c.relation = fieldName } }

// NoPreload makes GetLeaves skip loading the relation of the leaves. The other reads ignore it.
func NoPreload() ReadOption { return func(c *readConfig) { c.noPreload = true } }

type resolvedCond struct {
	column string // database column name
	op     condOp
	values []any // one value for opEq, any number for opIn, none otherwise
}

type readPlan struct {
	conds   []resolvedCond // nil means no filter
	perNode bool
	columns []string // database column names to select; nil means every column of the node table

	depth        *depthRange // nil: the read's own default
	ancestors    []uint      // the ids of ForAncestors
	hasAncestors bool        // ForAncestors was given, also with no ids
	virtualRoot  bool
	relation     string
	noPreload    bool
}

// column resolves a column or Go field name to the database column name of a column that exists
// in the node table. A column name wins. A column is judged by the field gorm maps it to, also when
// it is found by a Go name that another field of the column has. A Go field name that fields of
// several columns that can be named share, such as the model's own Name and the Name of a struct
// embedded with a column prefix, is rejected: gorm's own lookup by Go name returns the last field
// declared.
func (ct *Tree) column(name string) (string, error) {
	f, ok := ct.schema.FieldsByDBName[name]
	if !ok {
		var cols []string
		for _, sf := range ct.schema.Fields {
			if sf.Name != name || sf.DBName == "" || slices.Contains(cols, sf.DBName) {
				continue
			}
			if mf := ct.schema.FieldsByDBName[sf.DBName]; mf != nil && hasColumn(mf) {
				f = mf
				cols = append(cols, sf.DBName)
			}
		}
		if len(cols) > 1 {
			return "", fmt.Errorf("%w: %q is the Go name of the columns %s; use the column name",
				ErrUnknownColumn, name, strings.Join(cols, ", "))
		}
	}
	if f == nil || !hasColumn(f) {
		return "", fmt.Errorf("%w: %q", ErrUnknownColumn, name)
	}
	return f.DBName, nil
}

// hasColumn tells whether the field can be named in a read option: it has a column name and is not
// both read-only and excluded from migration. A writable field excluded from migration is written
// by Add and Update, so its column exists; a read-only one excluded from migration, as
// Node.ParentId, is a computed value.
func hasColumn(f *schema.Field) bool {
	return f.DBName != "" && (!f.IgnoreMigration || f.Creatable || f.Updatable)
}

func (ct *Tree) resolveConds(conds []Cond) ([]resolvedCond, error) {
	if len(conds) == 0 {
		return nil, nil
	}
	out := make([]resolvedCond, 0, len(conds))
	for _, c := range conds {
		col, err := ct.column(c.column)
		if err != nil {
			return nil, err
		}
		rc := resolvedCond{column: col, op: c.op}
		switch c.op {
		case opEq:
			if err := checkFilterValue(c.column, c.values[0]); err != nil {
				return nil, err
			}
			v, null, err := eqValue(c.values[0])
			if err != nil {
				return nil, fmt.Errorf("filter on %q: %w", c.column, err)
			}
			if null {
				rc.op = opIsNull
			} else {
				rc.values = []any{oneValue(v)}
			}
		case opIn:
			for _, v := range c.values {
				if err := checkFilterValue(c.column, v); err != nil {
					return nil, err
				}
			}
			for _, v := range c.values {
				rc.values = append(rc.values, oneValue(v))
			}
		}
		out = append(out, rc)
	}
	return out, nil
}

// checkFilterValue rejects a value that gorm would not bind as one value: a list, which gorm
// expands into several placeholders; a gorm expression, a *gorm.DB or a gorm.Valuer, which gorm
// renders as SQL; and a sql.NamedArg, which gorm binds without a placeholder. A byte slice and a
// driver.Valuer that is not a gorm.Valuer are bound as one value. A non-nil pointer is checked
// through. The check is by type and comes before the nil pointer check, so a nil pointer of a
// gorm.Valuer type is rejected too.
func checkFilterValue(column string, v any) error {
	switch v.(type) {
	case nil:
		return nil
	case *gorm.DB, clause.Expression, clause.Column, clause.Table, gorm.Valuer:
		return fmt.Errorf("filter on %q: a value of type %T is rendered as SQL, not bound", column, v)
	case sql.NamedArg:
		return fmt.Errorf("filter on %q: a value of type %T is not bound as one value", column, v)
	case driver.Valuer:
		return nil
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		return checkFilterValue(column, rv.Elem().Interface())
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Array || (t.Kind() == reflect.Slice && t.Elem() != reflect.TypeFor[byte]()) {
		return fmt.Errorf("filter on %q: a value of type %T is a list; pass the values one by one", column, v)
	}
	return nil
}

// eqValue prepares the value of an equality match, as gorm's eqNil does: a nil value or a nil
// pointer is NULL; a driver.Valuer is NULL when its Value is nil, and is kept as it is otherwise;
// any other pointer is dereferenced, and is NULL when it points to a nil interface. An error from
// Value is returned.
func eqValue(v any) (val any, null bool, err error) {
	rv := reflect.ValueOf(v)
	if v == nil || (rv.Kind() == reflect.Pointer && rv.IsNil()) {
		return nil, true, nil
	}
	if valuer, ok := v.(driver.Valuer); ok {
		dv, err := valuer.Value()
		if err != nil {
			return nil, false, err
		}
		return v, dv == nil, nil
	}
	if rv.Kind() == reflect.Pointer {
		e := rv.Elem().Interface()
		return e, e == nil, nil
	}
	return v, false, nil
}

// bytesValue binds a byte slice as one value. gorm renders a subquery again when it embeds it in
// another statement, and then expands a plain byte slice that directly follows "(" into its bytes,
// as the first value of an IN list does.
type bytesValue []byte

// Value returns the bytes.
func (b bytesValue) Value() (driver.Value, error) { return []byte(b), nil }

// oneValue returns a byte slice of any type as a bytesValue, unless it is a driver.Valuer, and any
// other value unchanged.
func oneValue(v any) any {
	if _, ok := v.(driver.Valuer); ok || v == nil {
		return v
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return bytesValue(rv.Bytes())
	}
	return v
}

// planRead applies the options and validates them against the model. Every read runs it, so an
// unknown column fails the same way in all of them.
func (ct *Tree) planRead(opts []ReadOption) (*readPlan, error) {
	var cfg readConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	p := &readPlan{
		perNode:      cfg.perNode,
		depth:        cfg.depth,
		ancestors:    cfg.ancestors,
		hasAncestors: cfg.hasAncestors,
		virtualRoot:  cfg.virtualRoot,
		relation:     cfg.relation,
		noPreload:    cfg.noPreload,
	}
	if d := cfg.depth; d != nil && (d.min < 0 || (d.max >= 0 && d.max < d.min)) {
		return nil, fmt.Errorf("%w: %d to %d", ErrInvalidDepthRange, d.min, d.max)
	}

	switch {
	case len(cfg.filter) > 0:
		conds, err := ct.resolveConds(cfg.filter)
		if err != nil {
			return nil, err
		}
		p.conds = conds
	case !cfg.unfiltered:
		p.conds = ct.defaultFilter
	}

	if !cfg.hasColumns {
		return p, nil
	}
	p.columns = []string{colNodeID, colTenant, colSortOrder}
	seen := map[string]bool{colNodeID: true, colTenant: true, colSortOrder: true}
	for _, name := range cfg.columns {
		col, err := ct.column(name)
		if err != nil {
			return nil, err
		}
		if !seen[col] {
			seen[col] = true
			p.columns = append(p.columns, col)
		}
	}
	return p, nil
}

// plainPlan is the plan of a read without options and without the default filter: every column of
// the node table. It is for writes that read.
func (ct *Tree) plainPlan() *readPlan {
	return &readPlan{}
}

// WithFilter returns a copy of the Tree with a default filter. GetNode, FindChild, Descendants,
// DescendantIds, TreeDescendants, TreeDescendantsIds, Ancestors, AncestorIds, ChildCounts,
// SubtreeIDs, SubtreeIDsOf, ClosurePairs and GetLeaves apply it; IsDescendant and IsChildOf take no
// read options and are not filtered, and writes are never filtered. conds replace any default
// filter the Tree already has; with no conds the copy keeps the same default filter. A read can
// replace the default with its own Filter that has conditions, or switch it off with Unfiltered.
// The original Tree is not changed. It returns ErrUnknownColumn when a condition names an unknown
// column, and an error for a value that Eq or In rejects. The conditions are resolved when
// WithFilter is called, including whether an Eq value is NULL; a pointer driver.Valuer is bound as
// it is and read again by every query, so it must not change afterwards.
func (ct *Tree) WithFilter(conds ...Cond) (*Tree, error) {
	cp := *ct
	if len(conds) == 0 {
		return &cp, nil
	}
	resolved, err := ct.resolveConds(conds)
	if err != nil {
		return nil, err
	}
	cp.defaultFilter = resolved
	return &cp, nil
}
