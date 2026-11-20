package closuretree

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ErrNoAncestors is returned when no ancestors are given to SubtreeIDs or SubtreeIDsOf. An empty
// list selects nothing, so it is an error rather than an empty subquery.
var ErrNoAncestors = errors.New("no ancestors given")

// SubtreeIDs returns a subquery that selects the ids of the nodes in the subtrees of the given
// ancestors, for the caller to embed in its own query. It executes nothing. The output column is
// named descendant_id; that name is public API and stays stable. Embed it as
//
//	db.Where("fk IN (?)", sub)
//
// By default the subtree of each ancestor includes the ancestor itself (depth 0 and below). Ancestor
// 0 is the virtual root and stands for every node of the tenant. Duplicate and overlapping ancestors
// are fine; the subquery is meant for IN and does not remove duplicate ids itself.
//
// SubtreeIDs honours the read options Filter, Unfiltered, PerNode and DepthRange; the others are
// ignored. With a filter and without PerNode a node is left out when it or any node between it and
// the requested ancestor does not match; the requested ancestor must match too, also when the depth
// range leaves the ancestor itself out. With PerNode only the node itself is tested. A requested
// ancestor that does not match selects nothing in the pruning form.
//
// The slice form binds one value per ancestor, so a set of many thousands of ancestors should go
// through SubtreeIDsOf. With a filter the subquery reads the node table, and MySQL rejects that
// inside an UPDATE or DELETE on the node table (error 1093): wrap it as
// "IN (SELECT descendant_id FROM (?) AS sub)". Do not put a LIMIT inside it (MySQL error 1235);
// page in the outer query.
func (ct *Tree) SubtreeIDs(tenant string, ancestors []uint, opts ...ReadOption) (*gorm.DB, error) {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	if len(ancestors) == 0 {
		return nil, ErrNoAncestors
	}
	return ct.subtree(tenant, ancestors, opts)
}

// SubtreeIDsOf is SubtreeIDs with the ancestors given as a subquery that selects a single column of
// node ids. A nil subquery is ErrNoAncestors, and a subquery whose Error is set returns that error,
// because gorm would render it as nothing and the statement would read IN (). A subquery that only
// fails when rendered is rejected too. Everything else is as
// in SubtreeIDs.
func (ct *Tree) SubtreeIDsOf(tenant string, ancestors *gorm.DB, opts ...ReadOption) (*gorm.DB, error) {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	if ancestors == nil {
		return nil, ErrNoAncestors
	}
	if ancestors.Error != nil {
		return nil, ancestors.Error
	}
	if err := renderable(ancestors); err != nil {
		return nil, err
	}
	return ct.subtree(tenant, ancestors, opts)
}

// renderable renders a subquery once in a dry-run session with a silent logger and returns an error
// when that fails or produces no SQL. gorm renders a subquery that cannot be built (for example a
// model it cannot parse) as nothing, and the statement would read IN ().
func renderable(sub *gorm.DB) error {
	var rows []map[string]any
	dry := sub.Session(&gorm.Session{DryRun: true, Logger: logger.Discard}).Find(&rows)
	if dry.Error != nil {
		return fmt.Errorf("the ancestors subquery cannot be rendered: %w", dry.Error)
	}
	if dry.Statement.SQL.Len() == 0 {
		return errors.New("the ancestors subquery cannot be rendered: it produces no SQL")
	}
	return nil
}

func (ct *Tree) subtree(tenant string, ancestors any, opts []ReadOption) (*gorm.DB, error) {
	p, err := ct.planRead(opts)
	if err != nil {
		return nil, err
	}
	depth := depthRange{min: 0, max: -1}
	if p.depth != nil {
		depth = *p.depth
	}
	sqlstr, args := ct.subtreeSQL(p, tenant, ancestors, depth)
	return ct.subquery(sqlstr, args), nil
}

// subquery wraps a statement as a *gorm.DB that executes nothing by itself: a fresh session of the
// Tree's database with the statement set by Raw. gorm renders it into the statement it is passed to.
func (ct *Tree) subquery(sql string, args []any) *gorm.DB {
	return ct.db.Session(&gorm.Session{NewDB: true}).Raw(sql, args...)
}
