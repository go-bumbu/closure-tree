package closuretree

import (
	"context"
	"fmt"
	"reflect"

	"gorm.io/gorm"
)

// currentParent returns the direct parent id of node id (0 for a root node) and whether the node
// exists, by reading its depth-1 closure row. Must be called inside a transaction.
func (ct *Tree) currentParent(tx *gorm.DB, id uint, tenant string) (parentID uint, found bool, err error) {
	var row struct{ AncestorID uint }
	res := tx.Raw(
		fmt.Sprintf(`SELECT ancestor_id FROM %s WHERE descendant_id = ? AND depth = 1 AND tenant = ? LIMIT 1`, ct.relationsTbl),
		id, tenant,
	).Scan(&row)
	if res.Error != nil {
		return 0, false, fmt.Errorf("unable to look up current parent: %w", res.Error)
	}
	return row.AncestorID, res.RowsAffected > 0, nil
}

// nodeExists reports whether a node with the given id exists for tenant. Must run inside a tx.
func (ct *Tree) nodeExists(tx *gorm.DB, id uint, tenant string) (bool, error) {
	var count int64
	if err := tx.Table(ct.nodesTbl).Where("node_id = ? AND tenant = ?", id, tenant).Count(&count).Error; err != nil {
		return false, fmt.Errorf("unable to verify node exists: %w", err)
	}
	return count > 0, nil
}

// updateFieldsInTx applies the column updates to node id within tx. It returns ErrNodeNotFound if
// no such node exists for tenant; a no-op update (unchanged values) is not an error, even on MySQL
// where it reports RowsAffected=0.
func (ct *Tree) updateFieldsInTx(tx *gorm.DB, id uint, updateMap map[string]any, tenant string) error {
	res := tx.Table(ct.nodesTbl).Where("node_id = ? AND tenant = ?", id, tenant).Updates(updateMap)
	if res.Error != nil {
		return fmt.Errorf("unable to update node: %w", res.Error)
	}
	if res.RowsAffected > 0 {
		return nil
	}
	// MySQL reports RowsAffected=0 for a no-op update (all values unchanged), so an explicit
	// existence check is needed to tell a missing node from an idempotent one.
	exists, err := ct.nodeExists(tx, id, tenant)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	return nil
}

// Update modifies a node's fields and/or moves it to a new parent, and/or reorders it, atomically.
//
// Pass a non-nil item to update fields (must embed Node; Node fields are ignored).
// Pass a non-nil newParentID to move the node: &0 moves to root, &someID moves under someID.
// Pass a non-nil afterNodeID to set sort order: &0 places first, &someID places after that sibling.
// Passing all three nil returns ErrNoOp.
func (ct *Tree) Update(ctx context.Context, id uint, item any, newParentID *uint, afterNodeID *uint, tenant string) error {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	if id == 0 {
		return ErrNodeNotFound
	}
	if item == nil && newParentID == nil && afterNodeID == nil {
		return ErrNoOp
	}
	if item != nil {
		if err = checkItem(item); err != nil {
			return err
		}
	}

	var updateMap map[string]any
	if item != nil {
		updateMap, err = ct.buildUpdateMap(item, id, tenant)
		if err != nil {
			return err
		}
	}

	return ct.writeTx(ctx, tenant, func(tx *gorm.DB) error {
		if item != nil {
			if err := ct.updateFieldsInTx(tx, id, updateMap, tenant); err != nil {
				return err
			}
		}
		if newParentID != nil {
			if err := ct.maybeMoveInTx(tx, id, *newParentID, afterNodeID != nil, tenant); err != nil {
				return err
			}
			// A5: a move with no explicit afterNodeID appends the node among its new siblings,
			// giving it a fresh sort_order (moveInTx alone keeps the old one) and refreshing meta.
			if afterNodeID == nil {
				if err := ct.placeLastInTx(tx, id, *newParentID, tenant); err != nil {
					return err
				}
			}
		}
		if afterNodeID != nil {
			if err := ct.reorderInTx(tx, id, *afterNodeID, newParentID, tenant); err != nil {
				return err
			}
		}
		return nil
	})
}

// maybeMoveInTx moves node id to newPID unless hasReorder is true and the node is
// already under newPID (same-parent reorder). Without this guard, moveInTx would return
// ErrInvalidMove for a same-parent reorder before the reorder gets to run.
func (ct *Tree) maybeMoveInTx(tx *gorm.DB, id, newPID uint, hasReorder bool, tenant string) error {
	if hasReorder {
		curParent, _, err := ct.currentParent(tx, id, tenant)
		if err != nil {
			return err
		}
		if curParent == newPID {
			return nil // already under target parent; let reorder handle the rest
		}
	}
	return ct.moveInTx(tx, id, newPID, tenant)
}

// reorderInTx updates the sort_order of node id to place it after afterID among
// siblings of the effective parent. afterID=0 means place first.
// newParentID is non-nil when the node was just moved; it determines the effective parent.
func (ct *Tree) reorderInTx(tx *gorm.DB, id, afterID uint, newParentID *uint, tenant string) error {
	// ErrAfterNodeIsSelf check first
	if afterID == id {
		return ErrAfterNodeIsSelf
	}

	// Resolve effective parent
	var effectiveParentID uint
	if newParentID != nil {
		effectiveParentID = *newParentID
	} else {
		// Look up current parent (also verifies the node exists)
		parentID, found, err := ct.currentParent(tx, id, tenant)
		if err != nil {
			return err
		}
		if !found {
			return ErrNodeNotFound
		}
		effectiveParentID = parentID
	}

	// Validate afterID (if non-zero)
	if afterID != 0 {
		if err := ct.validateAfterNode(tx, effectiveParentID, afterID, tenant); err != nil {
			return err
		}
	}

	// Compute new sort_order
	sortOrder, err := ct.placeAmongSiblings(tx, effectiveParentID, afterID, tenant)
	if err != nil {
		return err
	}

	res := tx.Exec(
		fmt.Sprintf(`UPDATE %s SET sort_order = ? WHERE node_id = ? AND tenant = ?`, ct.nodesTbl),
		sortOrder, id, tenant,
	)
	if res.Error != nil {
		return fmt.Errorf("unable to update sort order: %w", res.Error)
	}
	return nil
}

// buildUpdateMap builds the column→value map for an Update call using reflection.
func (ct *Tree) buildUpdateMap(item any, id uint, tenant string) (map[string]any, error) {
	reflectItem, t, _ := stripNodeCopy(item)
	v := reflect.ValueOf(reflectItem).Elem()
	if nodeField, ok := findNodeValue(t, v); ok && nodeField.CanSet() {
		nodeField.Set(reflect.ValueOf(Node{NodeId: id, Tenant: tenant}))
	}

	updateStmt := &gorm.Statement{DB: ct.db}
	if err := updateStmt.Parse(reflectItem); err != nil {
		return nil, fmt.Errorf("unable to parse item schema: %w", err)
	}
	updateMap := make(map[string]any)
	for _, f := range updateStmt.Schema.Fields {
		if f.DBName == "" || !f.Updatable {
			continue
		}
		if f.OwnerSchema != nil && f.OwnerSchema.ModelType == reflect.TypeOf(Node{}) {
			continue
		}
		fieldVal := reflect.ValueOf(reflectItem).Elem().FieldByName(f.Name)
		if fieldVal.IsValid() {
			updateMap[f.DBName] = fieldVal.Interface()
		}
	}
	return updateMap, nil
}

// moveInTx performs the closure-table move of node id to parent newPID within tx.
func (ct *Tree) moveInTx(tx *gorm.DB, id, newPID uint, tenant string) error {
	// Same-parent guard (uses tx to avoid TOCTOU)
	var sameParentCount int64
	if err := tx.Table(ct.relationsTbl).
		Where("ancestor_id = ? AND descendant_id = ? AND depth = 1 AND tenant = ?", newPID, id, tenant).
		Count(&sameParentCount).Error; err != nil {
		return fmt.Errorf("unable to check current parent: %w", err)
	}
	if sameParentCount > 0 {
		return ErrInvalidMove
	}

	if newPID != 0 {
		// Cycle guard: ensure new parent is not a descendant of id (uses tx)
		var descCount int64
		if err := tx.Table(ct.relationsTbl).
			Where("ancestor_id = ? AND descendant_id = ? AND tenant = ?", id, newPID, tenant).
			Limit(1).Count(&descCount).Error; err != nil {
			return fmt.Errorf("unable to check for cycle: %w", err)
		}
		if descCount > 0 {
			return ErrInvalidMove
		}
	}

	// STEP 1: Delete all connections from outside the subtree into the subtree.
	// Must happen before insert to avoid unique-constraint conflicts on depth changes.
	delSql := fmt.Sprintf(moveDeleteExternalPaths, ct.relationsTbl, ct.relationsTbl)
	delExec := tx.Exec(delSql, id, tenant, tenant)
	if delExec.Error != nil {
		return fmt.Errorf("unable to delete external closure paths: %w", delExec.Error)
	}
	// Note: in the combined path (item != nil), this check is redundant because the
	// field update above already verified the node exists. It is kept intentionally to
	// preserve the move invariant and avoid coupling the two code paths.
	if delExec.RowsAffected == 0 {
		return ErrNodeNotFound
	}

	// STEP 2: Insert new connections from destination's ancestors to the subtree.
	return ct.insertNewPathsInTx(tx, id, newPID, tenant)
}

// insertNewPathsInTx inserts closure rows connecting the moved subtree to its new ancestors.
func (ct *Tree) insertNewPathsInTx(tx *gorm.DB, id, newPID uint, tenant string) error {
	if newPID == 0 {
		insertSql := fmt.Sprintf(moveQueryInsertNewToRoot, ct.relationsTbl, ct.relationsTbl)
		insExec := tx.Exec(insertSql, id, tenant)
		if insExec.Error == nil && insExec.RowsAffected == 0 {
			return ErrNodeNotFound
		}
		if insExec.Error != nil {
			return fmt.Errorf("unable to insert closure paths: %w", insExec.Error)
		}
		return nil
	}
	insertSql := fmt.Sprintf(moveQueryInsertNew, ct.relationsTbl, ct.relationsTbl, ct.relationsTbl)
	insExec := tx.Exec(insertSql, id, newPID, tenant, tenant)
	if insExec.Error == nil && insExec.RowsAffected == 0 {
		return ErrParentNotFound
	}
	if insExec.Error != nil {
		return fmt.Errorf("unable to insert closure paths: %w", insExec.Error)
	}
	return nil
}

const moveQueryInsertNewToRoot = `
INSERT INTO  %s (ancestor_id, descendant_id, depth, tenant)
SELECT 0, c.descendant_id, c.depth + 1, c.tenant
FROM  %s c
WHERE c.ancestor_id = ? AND c.tenant = ?;
`

const moveQueryInsertNew = `
INSERT INTO %s (ancestor_id, descendant_id, depth, tenant)
SELECT p.ancestor_id, c.descendant_id, p.depth + c.depth + 1, p.tenant
FROM %s p
JOIN %s c ON c.ancestor_id = ?
WHERE p.descendant_id = ? AND p.tenant = ? AND c.tenant = ?;
`

// moveDeleteExternalPaths removes all closure rows that come from ancestors OUTSIDE the
// moved subtree to nodes INSIDE it. This clears the old parent-chain connections while
// preserving all internal subtree links (self-links and intra-subtree edges).
// Uses a CTE so MySQL 8.0+ can materialise the subtree before the DELETE, avoiding
// Error 1093 ("can't specify target table in FROM clause").
const moveDeleteExternalPaths = `
WITH subtree AS (
    SELECT descendant_id FROM %s WHERE ancestor_id = ? AND tenant = ?
)
DELETE FROM %s
WHERE descendant_id IN (SELECT descendant_id FROM subtree)
AND ancestor_id NOT IN (SELECT descendant_id FROM subtree)
AND tenant = ?`
