package closuretree

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"gorm.io/gorm"
)

// closureTreeMeta stores per-(tenant,parent) sort-order health metadata.
type closureTreeMeta struct {
	Tenant      string `gorm:"not null;primaryKey"`
	ParentID    uint   `gorm:"not null;primaryKey;column:parent_id"`
	MinHalvings int    `gorm:"not null;default:9999"`
}

// validateAfterNode checks that afterNodeID is a direct child of parentID in the closure table.
// Returns ErrInvalidAfterNode if not found.
func (ct *Tree) validateAfterNode(tx *gorm.DB, parentID, afterNodeID uint, tenant string) error {
	var count int64
	err := tx.Table(ct.relationsTbl).
		Where("ancestor_id = ? AND descendant_id = ? AND depth = 1 AND tenant = ?", parentID, afterNodeID, tenant).
		Count(&count).Error
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrInvalidAfterNode
	}
	return nil
}

// computeSortOrder computes the sort_order for a new node to be placed after afterNodeID
// among siblings of parentID. afterNodeID=0 means place first.
// Must be called inside a transaction.
func (ct *Tree) computeSortOrder(tx *gorm.DB, parentID, afterNodeID uint, tenant string) (float64, int, error) {
	if afterNodeID == 0 {
		// Place first: find minimum sort_order among siblings
		var minOrder *float64
		row := tx.Raw(
			fmt.Sprintf(`SELECT MIN(n.sort_order) FROM %s n
JOIN %s r ON r.descendant_id = n.node_id AND r.depth = 1 AND r.tenant = n.tenant
WHERE r.ancestor_id = ? AND n.tenant = ?`, ct.nodesTbl, ct.relationsTbl),
			parentID, tenant,
		).Row()
		if err := row.Scan(&minOrder); err != nil {
			return 0, 0, err
		}
		if minOrder == nil {
			return 0.0, 9999, nil // no siblings
		}
		newOrder := *minOrder - 10.0
		return newOrder, halvingsRemaining(newOrder, *minOrder), nil
	}

	// Get sort_order of afterNodeID
	var afterOrder float64
	err := tx.Raw(
		fmt.Sprintf(`SELECT sort_order FROM %s WHERE node_id = ? AND tenant = ?`, ct.nodesTbl),
		afterNodeID, tenant,
	).Scan(&afterOrder).Error
	if err != nil {
		return 0, 0, err
	}

	// Get sort_order of next sibling (first sibling after afterNodeID in sort order)
	var nextOrder *float64
	row := tx.Raw(
		fmt.Sprintf(`SELECT n.sort_order FROM %s n
JOIN %s r ON r.descendant_id = n.node_id AND r.depth = 1 AND r.tenant = n.tenant
WHERE r.ancestor_id = ? AND n.tenant = ? AND n.node_id != ?
  AND (n.sort_order > ? OR (n.sort_order = ? AND n.node_id > ?))
ORDER BY n.sort_order ASC, n.node_id ASC
LIMIT 1`, ct.nodesTbl, ct.relationsTbl),
		parentID, tenant, afterNodeID, afterOrder, afterOrder, afterNodeID,
	).Row()
	if err := row.Scan(&nextOrder); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			newOrder := afterOrder + 10.0
			return newOrder, halvingsRemaining(afterOrder, newOrder), nil // no next sibling, append at end
		}
		return 0, 0, err
	}
	if nextOrder == nil {
		newOrder := afterOrder + 10.0
		return newOrder, halvingsRemaining(afterOrder, newOrder), nil // append at end
	}

	mid := (afterOrder + *nextOrder) / 2
	if mid == afterOrder || mid == *nextOrder {
		return math.Nextafter(afterOrder, math.Inf(1)), 0, nil
	}
	h := halvingsRemaining(afterOrder, mid)
	if h2 := halvingsRemaining(mid, *nextOrder); h2 < h {
		h = h2
	}
	return mid, h, nil
}

// placeAmongSiblings computes the sort_order for a node placed after afterID among the children
// of parentID and records the resulting halvings in the meta table. afterID=0 places first.
// Must be called inside a transaction.
func (ct *Tree) placeAmongSiblings(tx *gorm.DB, parentID, afterID uint, tenant string) (float64, error) {
	sortOrder, halvings, err := ct.computeSortOrder(tx, parentID, afterID, tenant)
	if err != nil {
		return 0, fmt.Errorf("unable to compute sort order: %w", err)
	}
	if err := ct.upsertMetaHalvings(tx, parentID, tenant, halvings); err != nil {
		return 0, fmt.Errorf("unable to update sort order metadata: %w", err)
	}
	return sortOrder, nil
}

// placeLastInTx gives node id a fresh sort_order after the current last child of parentID
// (excluding id itself) and refreshes the meta. Used when a move supplies no afterNodeID, so the
// moved node appends among its new siblings instead of keeping its old sort_order.
func (ct *Tree) placeLastInTx(tx *gorm.DB, id, parentID uint, tenant string) error {
	var maxOrder *float64
	err := tx.Raw(
		fmt.Sprintf(`SELECT MAX(n.sort_order) FROM %s n
JOIN %s r ON r.descendant_id = n.node_id AND r.depth = 1 AND r.tenant = n.tenant
WHERE r.ancestor_id = ? AND n.tenant = ? AND n.node_id != ?`, ct.nodesTbl, ct.relationsTbl),
		parentID, tenant, id,
	).Scan(&maxOrder).Error
	if err != nil {
		return fmt.Errorf("unable to find last sibling: %w", err)
	}
	var newOrder float64
	var halvings int
	if maxOrder == nil {
		newOrder, halvings = 0.0, 9999 // no other siblings
	} else {
		newOrder = *maxOrder + 10.0
		halvings = halvingsRemaining(*maxOrder, newOrder)
	}
	if err := ct.upsertMetaHalvings(tx, parentID, tenant, halvings); err != nil {
		return fmt.Errorf("unable to update sort order metadata: %w", err)
	}
	return tx.Exec(
		fmt.Sprintf(`UPDATE %s SET sort_order = ? WHERE node_id = ? AND tenant = ?`, ct.nodesTbl),
		newOrder, id, tenant,
	).Error
}

// halvingsRemaining returns how many times the gap [a, b] can be bisected
// before (a+b)/2 == a in float64 arithmetic. Returns 0 if gap <= 0.
// Caps at 9999 to avoid int overflow when the gap is astronomically large.
func halvingsRemaining(a, b float64) int {
	gap := b - a
	if gap <= 0 {
		return 0
	}
	ulp := math.Nextafter(a, math.Inf(1)) - a
	if !(ulp > 0) { // rejects ulp<=0 and ulp==NaN (when a==+Inf)
		return 0
	}
	h := math.Floor(math.Log2(gap / (2 * ulp)))
	if math.IsInf(h, 1) || h > 9999 {
		return 9999
	}
	if math.IsNaN(h) || h < 0 {
		return 0
	}
	return int(h)
}

// upsertMetaHalvings records halvings as the new minimum for (tenant, parentID)
// if it is lower than the current stored value, or inserts a new row.
// Must be called inside a transaction.
func (ct *Tree) upsertMetaHalvings(tx *gorm.DB, parentID uint, tenant string, halvings int) error {
	// Try to lower the existing value
	res := tx.Exec(
		fmt.Sprintf(`UPDATE %s SET min_halvings = ? WHERE tenant = ? AND parent_id = ? AND min_halvings > ?`,
			ct.metaTbl), halvings, tenant, parentID, halvings)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		return nil
	}
	// Row doesn't exist or already has equal/lower value — insert, ignore conflict
	var insertSQL string
	if isMySQLDialect(tx) {
		insertSQL = fmt.Sprintf(
			`INSERT INTO %s (tenant, parent_id, min_halvings) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE min_halvings = LEAST(min_halvings, VALUES(min_halvings))`,
			ct.metaTbl)
	} else {
		insertSQL = fmt.Sprintf(
			`INSERT INTO %s (tenant, parent_id, min_halvings) VALUES (?, ?, ?) ON CONFLICT (tenant, parent_id) DO UPDATE SET min_halvings = EXCLUDED.min_halvings WHERE %s.min_halvings > EXCLUDED.min_halvings`,
			ct.metaTbl, ct.metaTbl)
	}
	return tx.Exec(insertSQL, tenant, parentID, halvings).Error
}

// Renormalize rewrites sort_order for all direct children of parentID as 10.0, 20.0, 30.0, …
// preserving their current relative order (sort_order ASC, node_id ASC).
// parentID=0 renormalizes root nodes. No-op if there are no children.
// Runs in a single transaction; rolls back if any update fails.
func (ct *Tree) Renormalize(ctx context.Context, parentID uint, tenant string) error {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return err
	}
	return ct.db.WithContext(ctx).Transaction(func(tx *gorm.DB) (txErr error) {
		sqlstr := fmt.Sprintf(`SELECT n.node_id FROM %s n
JOIN %s r ON r.descendant_id = n.node_id AND r.depth = 1 AND r.tenant = n.tenant
WHERE r.ancestor_id = ? AND n.tenant = ?
ORDER BY n.sort_order ASC, n.node_id ASC`, ct.nodesTbl, ct.relationsTbl)

		rows, err := tx.Raw(sqlstr, parentID, tenant).Rows()
		if err != nil {
			return fmt.Errorf("renormalize: failed to fetch children: %w", err)
		}
		defer func() {
			if e := rows.Close(); e != nil && txErr == nil {
				txErr = e
			}
		}()

		var ids []uint
		for rows.Next() {
			var id uint
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("renormalize: failed to scan id: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for i, id := range ids {
			sortOrder := float64((i + 1) * 10)
			if err := tx.Exec(
				fmt.Sprintf(`UPDATE %s SET sort_order = ? WHERE node_id = ? AND tenant = ?`, ct.nodesTbl),
				sortOrder, id, tenant,
			).Error; err != nil {
				return fmt.Errorf("renormalize: failed to update node %d: %w", id, err)
			}
		}
		// Reset metadata: delete the row so it is recreated fresh on next insertion.
		if err := tx.Exec(
			fmt.Sprintf(`DELETE FROM %s WHERE tenant = ? AND parent_id = ?`, ct.metaTbl),
			tenant, parentID,
		).Error; err != nil {
			return fmt.Errorf("renormalize: failed to reset metadata: %w", err)
		}
		return nil
	})
}

// RenormalizeAll renormalizes every sibling group under tenant where the halvings
// remaining is at or below halvingsBuffer. Returns the number of groups renormalized.
// Use DefaultHalvingsBuffer for the recommended early-warning threshold.
// Each group is renormalized in its own transaction. On error, returns the number
// of groups successfully renormalized so far along with the error.
func (ct *Tree) RenormalizeAll(ctx context.Context, tenant string, halvingsBuffer int) (int, error) {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return 0, err
	}

	var parentIDs []uint
	err = ct.db.WithContext(ctx).Table(ct.metaTbl).
		Where("tenant = ? AND min_halvings <= ?", tenant, halvingsBuffer).
		Pluck("parent_id", &parentIDs).Error
	if err != nil {
		return 0, fmt.Errorf("RenormalizeAll: failed to query metadata: %w", err)
	}

	for i, parentID := range parentIDs {
		if err := ct.Renormalize(ctx, parentID, tenant); err != nil {
			return i, fmt.Errorf("RenormalizeAll: failed to renormalize parent %d: %w", parentID, err)
		}
	}
	return len(parentIDs), nil
}

// DefaultHalvingsBuffer is the recommended halvingsBuffer for NeedsRenormalize.
// It fires when ≤15 bisection halvings remain between any consecutive sibling pair,
// giving ample time to call Renormalize before float64 precision is exhausted.
const DefaultHalvingsBuffer = 15

// NeedsRenormalize reports whether the sort_order spacing under parentID is getting
// close to float64 exhaustion. halvingsBuffer=0 fires only when the next insertion
// would collide; use DefaultHalvingsBuffer (15) for an early warning.
// Returns false with no error if parentID has no children or no metadata yet.
func (ct *Tree) NeedsRenormalize(ctx context.Context, parentID uint, tenant string, halvingsBuffer int) (bool, error) {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}

	var meta closureTreeMeta
	err = ct.db.WithContext(ctx).Table(ct.metaTbl).
		Where("tenant = ? AND parent_id = ?", tenant, parentID).
		First(&meta).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("NeedsRenormalize: %w", err)
	}
	return meta.MinHalvings <= halvingsBuffer, nil
}

// NeedsRenormalizeAny reports whether any sibling group under the given tenant has
// sort_order spacing close to float64 exhaustion. Equivalent to calling NeedsRenormalize
// for every parent in the tree, but in a single query.
func (ct *Tree) NeedsRenormalizeAny(ctx context.Context, tenant string, halvingsBuffer int) (bool, error) {
	var err error
	tenant, err = validateTenant(tenant)
	if err != nil {
		return false, err
	}

	var count int64
	err = ct.db.WithContext(ctx).Table(ct.metaTbl).
		Where("tenant = ? AND min_halvings <= ?", tenant, halvingsBuffer).
		Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("NeedsRenormalizeAny: %w", err)
	}
	return count > 0, nil
}
