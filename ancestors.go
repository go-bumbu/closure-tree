package closuretree

import (
	"context"
	"fmt"
)

// Ancestors appends the ancestors of nodeID to items, a pointer to a []T or []*T, root first and
// the direct parent last. The node itself and the virtual root (id 0) are left out, so a root
// node has no ancestors. The parent of every ancestor is filled. Rows are copied into the element
// type as in Descendants. It is one query plus an existence check; no recursion is involved. On
// any error items keeps the length it had, so no row of a failed read is left in it.
//
// ErrNodeNotFound means one of two things: nodeID does not exist in the tenant (the virtual root
// included), or it exists but does not match the filter. A filter also applies to the ancestors:
// one that does not match is skipped and the path continues above it.
//
// Ancestors honours the read options Filter, Unfiltered and Columns; PerNode is ignored. Column
// names of every option are validated, so an unknown one returns ErrUnknownColumn.
func (ct *Tree) Ancestors(ctx context.Context, nodeID uint, tenant string, items any, opts ...ReadOption) error {
	structType, add, reset, err := appendTarget(items)
	if err != nil {
		return err
	}
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return err
	}
	toElem, err := ct.modelToElem(ctx, structType)
	if err != nil {
		return err
	}
	if err := ct.requireNode(ctx, p, nodeID, tenant); err != nil {
		return err
	}
	sqlstr, args := ct.ancestorsSQL(p, false, nodeID, tenant)
	if err := ct.scanEach(ctx, sqlstr, args, toElem, "failed to fetch ancestors", add); err != nil {
		reset()
		return err
	}
	return nil
}

// AncestorIDs returns the ids of the ancestors of nodeID, root first, as Ancestors does. A root
// node returns an empty, non-nil slice.
//
// AncestorIDs honours the read options Filter and Unfiltered, with the meaning they have in
// Ancestors; PerNode and Columns are ignored. Column names of every option are validated, so an
// unknown one returns ErrUnknownColumn.
func (ct *Tree) AncestorIDs(ctx context.Context, nodeID uint, tenant string, opts ...ReadOption) ([]uint, error) {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return nil, err
	}
	if err := ct.requireNode(ctx, p, nodeID, tenant); err != nil {
		return nil, err
	}
	ids := []uint{}
	sqlstr, args := ct.ancestorsSQL(p, true, nodeID, tenant)
	if err := ct.db.WithContext(ctx).Raw(sqlstr, args...).Scan(&ids).Error; err != nil {
		return nil, fmt.Errorf("failed to fetch ancestors: %w", err)
	}
	return ids, nil
}

// requireNode returns ErrNodeNotFound unless the node exists in the tenant and matches the plan.
func (ct *Tree) requireNode(ctx context.Context, p *readPlan, nodeID uint, tenant string) error {
	sqlstr, args := ct.nodeExistsSQL(p, nodeID, tenant)
	var found []int
	if err := ct.db.WithContext(ctx).Raw(sqlstr, args...).Scan(&found).Error; err != nil {
		return fmt.Errorf("failed to check node: %w", err)
	}
	if len(found) == 0 {
		return ErrNodeNotFound
	}
	return nil
}
