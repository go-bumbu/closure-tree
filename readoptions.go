package closuretree

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// ErrUnknownColumn is returned when a read option names a column the model does not have in its
// table. The name may be the database column name or the Go field name.
var ErrUnknownColumn = errors.New("unknown column")

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

// Eq matches the nodes whose column equals value. A nil value, or a nil pointer, matches NULL
// instead; a non-nil pointer is dereferenced.
func Eq(column string, value any) Cond {
	return Cond{column: column, op: opEq, values: []any{value}}
}

// IsNull matches the nodes whose column is NULL.
func IsNull(column string) Cond { return Cond{column: column, op: opIsNull} }

// NotNull matches the nodes whose column is not NULL.
func NotNull(column string) Cond { return Cond{column: column, op: opNotNull} }

// In matches the nodes whose column equals one of the values. Without values it matches nothing.
// The values are copied, so changing the slice afterwards does not change the condition.
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
}

// ReadOption changes how a read selects its nodes or which columns it returns.
type ReadOption func(*readConfig)

// Filter keeps only the nodes that match every condition. For this call it replaces the Tree's
// default filter (see WithFilter); several Filter options are joined. An empty Filter has no
// effect, so the default filter still applies.
func Filter(conds ...Cond) ReadOption {
	return func(c *readConfig) {
		c.filter = append(c.filter, conds...)
	}
}

// PerNode makes the filter apply to every node on the path between the root of the read and each
// result, instead of to the result only.
func PerNode() ReadOption { return func(c *readConfig) { c.perNode = true } }

// Unfiltered switches the Tree's default filter off for this call.
func Unfiltered() ReadOption { return func(c *readConfig) { c.unfiltered = true } }

// Columns limits the columns a read returns to the named ones, given as database column name or Go
// field name. node_id, tenant and sort_order are always returned, so Columns() with no names
// returns only those three. Without Columns a read returns every column of the node table.
func Columns(names ...string) ReadOption {
	return func(c *readConfig) {
		c.hasColumns = true
		c.columns = append(c.columns, names...)
	}
}

type resolvedCond struct {
	column string // database column name
	op     condOp
	values []any // one value for opEq, any number for opIn, none otherwise
}

type readPlan struct {
	conds   []resolvedCond // nil means no filter
	perNode bool
	columns []string // database column names to select; nil means every column of the node table
}

// column resolves a column or Go field name to the database column name of a column that exists
// in the node table.
func (ct *Tree) column(name string) (string, error) {
	f := ct.schema.LookUpField(name)
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
			v := c.values[0]
			if err := checkFilterValue(c.column, v); err != nil { // a *gorm.DB is caught before the dereference
				return nil, err
			}
			if rv := reflect.ValueOf(v); v != nil && rv.Kind() == reflect.Pointer {
				if rv.IsNil() {
					v = nil
				} else {
					v = rv.Elem().Interface()
				}
			}
			if err := checkFilterValue(c.column, v); err != nil {
				return nil, err
			}
			if v == nil {
				rc.op = opIsNull
			} else {
				rc.values = []any{v}
			}
		case opIn:
			for _, v := range c.values {
				if err := checkFilterValue(c.column, v); err != nil {
					return nil, err
				}
			}
			if len(c.values) > 0 {
				rc.values = c.values
			}
		}
		out = append(out, rc)
	}
	return out, nil
}

// checkFilterValue rejects a value that gorm would not bind as one value: a list, which gorm
// expands into several placeholders, and a gorm expression or *gorm.DB, which gorm renders as SQL.
// A byte slice and a driver.Valuer are bound as one value.
func checkFilterValue(column string, v any) error {
	switch v.(type) {
	case nil:
		return nil
	case *gorm.DB, clause.Expression, clause.Column, clause.Table:
		return fmt.Errorf("filter on %q: a value of type %T is rendered as SQL, not bound", column, v)
	case driver.Valuer:
		return nil
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Array || (t.Kind() == reflect.Slice && t.Elem() != reflect.TypeFor[byte]()) {
		return fmt.Errorf("filter on %q: a value of type %T is a list; pass the values one by one", column, v)
	}
	return nil
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
	p := &readPlan{perNode: cfg.perNode}

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

// WithFilter returns a copy of the Tree whose reads apply conds by default. It replaces any default
// filter the Tree already has; with no conds the copy has no default filter. A read can replace the
// default with its own Filter that has conditions (an empty Filter has no effect), or switch it off
// with Unfiltered. Writes are never filtered. The original Tree is not changed. It returns
// ErrUnknownColumn when a condition names an unknown column.
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
