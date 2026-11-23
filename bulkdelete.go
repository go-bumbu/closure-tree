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
//
// On MariaDB it is the write most likely to deadlock with other tenants' writes (see the concurrency
// note on Tree). When the library repeats its transaction after a deadlock, the count is that of
// the attempt that succeeded.
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
		// Counted per attempt: a repeated attempt deletes the rows again.
		deleted = 0
		for start := 0; start < len(unique); start += idChunkSize {
			chunk := unique[start:min(start+idChunkSize, len(unique))]

			res := tx.Exec(fmt.Sprintf(deleteNodesRecMany, ct.nodesTbl, ct.relationsTbl),
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
			if err := tx.Exec(fmt.Sprintf(deleteRelationsQueryMany, ct.relationsTbl),
				chunk, tenant, chunk, tenant, tenant).Error; err != nil {
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

// deleteNodesRecMany is deleteNodesRec for the subtrees of several nodes. Placeholders: (node ids,
// tenant, tenant).
const deleteNodesRecMany = `DELETE FROM %[1]s
WHERE node_id IN (
    SELECT node_id FROM (
        SELECT nodes.node_id
        FROM %[1]s AS nodes
        JOIN %[2]s AS ct ON ct.descendant_id = nodes.node_id AND ct.tenant = nodes.tenant
        WHERE ct.ancestor_id IN ? AND nodes.tenant = ?
    ) AS nodes_to_delete
)
  AND tenant = ?;`

// deleteRelationsQueryMany is deleteRelationsQuery for the subtrees of several nodes. Placeholders:
// (node ids, tenant, node ids, tenant, tenant).
const deleteRelationsQueryMany = `DELETE FROM %[1]s
WHERE (
      descendant_id IN (SELECT descendant_id FROM (
          SELECT descendant_id FROM %[1]s WHERE ancestor_id IN ? AND tenant = ?) AS descendants)
   OR ancestor_id  IN (SELECT descendant_id FROM (
          SELECT descendant_id FROM %[1]s WHERE ancestor_id IN ? AND tenant = ?) AS descendants2)
  )
  AND tenant = ?;`

// tenantDeleteSQL returns the statement that deletes all rows of a tenant from tbl, one of the
// Tree's tables. Placeholder: (tenant). On MariaDB the closure table's statement forces its tenant
// index, so that it reads only the tenant's rows: without it MariaDB scans the whole table when the
// tenant holds a large share of it, statistics or not, and locks the rows of other tenants, whose
// writes then deadlock with it. The hint is FORCE INDEX in the multi-table form of DELETE, which
// MySQL (accepted, untested) accepts too; its single-table DELETE takes no index hint. FORCE INDEX
// fails (error 1176) when the table lacks the index, as on a tree that Migrate has
// not migrated since the index was added: see tenantIndexMissing. The node and meta tables, and the
// other databases, get the plain statement.
func (ct *Tree) tenantDeleteSQL(tbl string) string {
	if ct.dialect == dialectMariaDB && tbl == ct.relationsTbl {
		return fmt.Sprintf(`DELETE %[1]s FROM %[1]s FORCE INDEX (%[2]s) WHERE tenant = ?`, tbl, ct.tenantIdx)
	}
	return fmt.Sprintf(`DELETE FROM %s WHERE tenant = ?`, tbl)
}

// tenantIndexMissing wraps err, the error of a tenant delete, with the advice to run Migrate when
// it is MariaDB's error for a missing index (1176): the closure table lacks the tenant index that
// tenantDeleteSQL forces. Any other error is returned as it is.
func (ct *Tree) tenantIndexMissing(err error) error {
	return indexMissing(err, ct.relationsTbl, ct.tenantIdx)
}

// indexMissing wraps err with the advice to run Migrate when it is MariaDB's error for a missing
// index (1176), here the index idx of table that a statement forces. Any other error is returned as
// it is.
func indexMissing(err error, table, idx string) error {
	if errorTextInChain(err, mariadbNoSuchKeyText) {
		return fmt.Errorf("the table %s lacks its index %s: run Migrate to add it: %w", table, idx, err)
	}
	return err
}

// DeleteTenant deletes every node, closure row and metadata row of tenant in one transaction. It is
// the supported way to drop a tenant's whole tree; DeleteRecurseMany rejects the virtual root. The
// tenant's lock row is kept: on PostgreSQL a writer waiting on that row would otherwise proceed
// without the lock. A tenant that has no rows in any of the tree's tables is not an error and
// nothing is written. That existence check runs before the lock is taken; on a Tree bound to a
// transaction (WithTx) it reads that transaction's snapshot. It returns ErrEmptyTenant if tenant is
// empty.
//
// The tenant is compared with the collation of the tenant columns (see Tree). On a MariaDB tree
// that kept a non-exact collation, tenants that differ only in case or accents are one tenant, and
// all of them are deleted.
//
// On MariaDB the delete of the tenant's closure rows forces the closure table's tenant index, so
// that it reads only the tenant's rows, also for a tenant that holds most of the table, instead of
// scanning and locking other tenants' rows. On a tree whose closure table lacks the index (Migrate
// has not run since it was added) it returns an error that says to run Migrate, and deletes nothing.
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
			if err := tx.Exec(ct.tenantDeleteSQL(tbl), tenant).Error; err != nil {
				return fmt.Errorf("deleteTenant: failed to delete from %s: %w", tbl, ct.tenantIndexMissing(err))
			}
		}
		return nil
	})
}
