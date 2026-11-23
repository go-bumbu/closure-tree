package closuretree

import (
	"context"
	"fmt"
)

// ChildCounts returns, for each id in ids, the number of its direct children. Id 0 is the virtual
// root, so it counts the root nodes. The map holds only the ids that have at least one counted
// child: a leaf, an unknown id and an id of another tenant are absent, and so is a parent whose
// children are all filtered out. The map is never nil; without ids it is empty and no query runs.
// Duplicate ids are counted once.
//
// A filter counts the children that match it. It does not check the parent itself, so a parent
// that does not match still has its matching children counted. The ids are sent in chunks of
// idChunkSize ids, one statement per chunk. It reads the closure table, and joins the node table
// only when there is a filter.
//
// ChildCounts honours the read options Filter and Unfiltered; the others are ignored. Column
// names of every option are validated, so an unknown one returns ErrUnknownColumn.
func (ct *Tree) ChildCounts(ctx context.Context, ids []uint, tenant string, opts ...ReadOption) (map[uint]int64, error) {
	tenant, err := validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	p, err := ct.planRead(opts)
	if err != nil {
		return nil, err
	}
	counts := map[uint]int64{}
	seen := make(map[uint]struct{}, len(ids))
	unique := make([]uint, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; !dup {
			seen[id] = struct{}{}
			unique = append(unique, id)
		}
	}
	for start := 0; start < len(unique); start += idChunkSize {
		chunk := unique[start:min(start+idChunkSize, len(unique))]
		sqlstr, args := ct.childCountsSQL(p, chunk, tenant)
		if err := ct.scanCounts(ctx, sqlstr, args, counts); err != nil {
			return nil, err
		}
	}
	return counts, nil
}

// scanCounts runs a statement that returns (id, count) rows and stores them in counts.
func (ct *Tree) scanCounts(ctx context.Context, sqlstr string, args []any, counts map[uint]int64) (err error) {
	rows, err := ct.db.WithContext(ctx).Raw(sqlstr, args...).Rows()
	if err != nil {
		return fmt.Errorf("failed to count children: %w", err)
	}
	defer func() {
		e := rows.Close()
		if err == nil { // don't overwrite the original error
			err = e
		}
	}()
	for rows.Next() {
		var id uint
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		counts[id] = n
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	return nil
}
