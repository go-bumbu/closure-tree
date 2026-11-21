package closuretree

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// ErrVirtualRoot is returned by DeleteRecurseMany when ids contains 0, the virtual root. DeleteRecurse
// does not return it: it accepts id 0 (see there).
var ErrVirtualRoot = errors.New("node id 0 is the virtual root; use DeleteTenant to delete a whole tenant")

// idChunkSize is the number of ids bound to one statement of DeleteRecurseMany.
const idChunkSize = 500

// DeleteRecurseMany deletes every node in ids together with its entire subtree, scoped to tenant,
// in one transaction. It returns the number of node rows deleted; a node that is listed twice, or
// that lies in the subtree of another listed node, is counted once. Ids that do not exist for the
// tenant are ignored, so no ErrNodeNotFound is returned. It returns ErrVirtualRoot, deleting
// nothing, if ids contains 0, and ErrEmptyTenant if tenant is empty. With no ids it returns 0
// without opening a transaction.
func (ct *Tree) DeleteRecurseMany(ctx context.Context, ids []uint, tenant string) (deleted int64, err error) {
	tenant, err = validateTenant(tenant)
	if err != nil {
		return 0, err
	}
	seen := make(map[uint]struct{}, len(ids))
	unique := make([]uint, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			return 0, ErrVirtualRoot
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return 0, nil
	}

	err = ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {
		for start := 0; start < len(unique); start += idChunkSize {
			chunk := unique[start:min(start+idChunkSize, len(unique))]

			res := tx.Exec(fmt.Sprintf(deleteNodesRecMany, ct.nodesTbl, ct.relationsTbl, ct.nodesTbl),
				chunk, tenant, tenant)
			if res.Error != nil {
				return fmt.Errorf("deleteRecurseMany: failed to delete nodes: %w", res.Error)
			}
			// A chunk that deletes nothing (unknown ids, or nodes an earlier chunk already removed
			// with their ancestor) has nothing to clean up either.
			if res.RowsAffected == 0 {
				continue
			}
			deleted += res.RowsAffected

			// The metadata and closure cleanup run in the order DeleteRecurse uses: the metadata
			// identifies the subtree through the closure rows, which therefore go last.
			if err := tx.Exec(
				fmt.Sprintf(`DELETE FROM %s WHERE tenant = ? AND parent_id IN (
					SELECT descendant_id FROM %s WHERE ancestor_id IN ? AND tenant = ?
				)`, ct.metaTbl, ct.relationsTbl),
				tenant, chunk, tenant,
			).Error; err != nil {
				return fmt.Errorf("deleteRecurseMany: failed to clean metadata: %w", err)
			}
			if err := tx.Exec(fmt.Sprintf(deleteRelationsQueryMany, ct.relationsTbl, ct.relationsTbl),
				chunk, tenant, tenant).Error; err != nil {
				return fmt.Errorf("deleteRecurseMany: failed to delete relations: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

const deleteNodesRecMany = `WITH nodes_to_delete AS (
    SELECT nodes.node_id
    FROM %s AS nodes
    JOIN %s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
    WHERE ct.ancestor_id IN ? AND nodes.tenant = ?
)
DELETE FROM %s
WHERE node_id IN (SELECT node_id FROM nodes_to_delete)
  AND tenant = ?;`

const deleteRelationsQueryMany = `WITH descendants AS (
	SELECT descendant_id FROM %s WHERE ancestor_id IN ? AND tenant = ?
)
DELETE FROM %s
WHERE tenant = ?
  AND (
      descendant_id IN (SELECT descendant_id FROM descendants)
   OR ancestor_id  IN (SELECT descendant_id FROM descendants)
  );`

// DeleteTenant deletes every node, closure row and metadata row of tenant in one transaction. It is
// the supported way to drop a tenant's whole tree; DeleteRecurseMany rejects the virtual root. The
// tenant's lock row is kept: on PostgreSQL a writer waiting on that row would otherwise proceed
// without the lock. A tenant that has no rows in any of the tree's tables is not an error and
// nothing is written. That existence check runs before the lock is taken; on a Tree bound to a
// transaction (WithTx) it reads that transaction's snapshot. It returns ErrEmptyTenant if tenant is
// empty.
//
// The tenant is compared with the collation of the tenant columns (see Tree). On a MySQL tree that
// kept a non-exact collation, tenants that differ only in case or accents are one tenant, and all
// of them are deleted.
func (ct *Tree) DeleteTenant(ctx context.Context, tenant string) error {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return err
	}
	known := false
	for _, tbl := range []string{ct.nodesTbl, ct.relationsTbl, ct.metaTbl, ct.lockTbl} {
		var found []int
		err := ct.db.WithContext(ctx).
			Raw(fmt.Sprintf("SELECT 1 FROM %s WHERE tenant = ? LIMIT 1", tbl), tenant).
			Scan(&found).Error
		if err != nil {
			return fmt.Errorf("deleteTenant: failed to look up tenant: %w", err)
		}
		if len(found) > 0 {
			known = true
			break
		}
	}
	if !known {
		return nil
	}
	return ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {
		for _, tbl := range []string{ct.nodesTbl, ct.relationsTbl, ct.metaTbl} {
			if err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE tenant = ?", tbl), tenant).Error; err != nil {
				return fmt.Errorf("deleteTenant: failed to delete from %s: %w", tbl, err)
			}
		}
		return nil
	})
}
