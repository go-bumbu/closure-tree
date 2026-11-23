package closuretree

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-bumbu/testdbs"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// errInjected is the failure the tests below inject into one attempt of a write.
var errInjected = errors.New("injected failure")

// retryHookItem counts in a column how often its BeforeCreate hook ran on the value gorm creates,
// and marks a field without a column.
type retryHookItem struct {
	Node
	Name    string
	Hooks   int
	Scratch string `gorm:"-"`
}

func (r *retryHookItem) BeforeCreate(*gorm.DB) error {
	r.Hooks++
	r.Scratch = "hook"
	return nil
}

// retryInjected makes ct repeat a write once when an attempt fails with errInjected, calling pause
// (when not nil) between the two attempts.
func retryInjected(ct *Tree, pause func()) {
	ct.retry = retryPolicy{
		attempts:  2,
		retryable: func(err error) bool { return errors.Is(err, errInjected) },
		pause: func(context.Context, int) error {
			if pause != nil {
				pause()
			}
			return nil
		},
	}
}

// failOnce registers fn under name with register, which is one of the Register methods of ct's
// callbacks, so that the first statement for which match is true fails with errInjected after it
// ran; the callback is removed with remove at the end of the test. It returns a counter of the
// statements match accepted.
func failOnce(t *testing.T, register func(string, func(*gorm.DB)) error, remove func(string) error, match func(*gorm.DB) bool) *int {
	t.Helper()
	const name = "test:fail_once"
	seen := new(int)
	if err := register(name, func(db *gorm.DB) {
		if db.Error != nil || !match(db) {
			return
		}
		*seen++
		if *seen == 1 {
			_ = db.AddError(errInjected)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := remove(name); err != nil {
			t.Error(err)
		}
	})
	return seen
}

// failNodeCreateOnce fails the first create of a row of ct's node table after it ran, and returns a
// counter of those creates.
func failNodeCreateOnce(t *testing.T, ct *Tree) *int {
	t.Helper()
	cb := ct.db.Callback().Create()
	return failOnce(t, cb.After("gorm:create").Register, cb.Remove, func(db *gorm.DB) bool {
		return db.Statement.Table == ct.nodesTbl
	})
}

// A retried Add creates the row from a fresh copy of the caller's item: the hook of the failed
// attempt leaves no trace in the stored row, and the caller's item gets only the new Node.
func TestAddRetriedAttemptStartsFromTheCallersItem(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, retryHookItem{})
			retryInjected(ct, nil)
			creates := failNodeCreateOnce(t, ct)

			item := retryHookItem{Name: "a", Scratch: "mine"}
			if err := ct.Add(context.Background(), &item, nil, nil, "t1"); err != nil {
				t.Fatalf("add: %v", err)
			}
			if *creates != 2 {
				t.Fatalf("want the node created on 2 attempts, got %d", *creates)
			}
			if item.NodeId == 0 || item.Hooks != 0 || item.Scratch != "mine" {
				t.Errorf("want the caller's item with a new node id and its own Hooks 0 and Scratch %q, got %+v", "mine", item)
			}
			var stored []retryHookItem
			if err := ct.db.Table(ct.nodesTbl).Where("tenant = ?", "t1").Find(&stored).Error; err != nil {
				t.Fatal(err)
			}
			if len(stored) != 1 || stored[0].NodeId != item.NodeId || stored[0].Hooks != 1 {
				t.Errorf("want one stored row %d with Hooks 1, got %+v", item.NodeId, stored)
			}
		})
	}
}

// A GetOrAdd whose create failed starts its next attempt from a fresh copy of the caller's item.
// When it creates the node, the hook of the failed attempt leaves no trace in the stored row and
// the caller's item gets only the new Node. When it finds the node another call added between the
// attempts, the caller's item is overwritten with the stored row, as on every found path: gorm's
// scan zeroes the copy first, so the field without a column is empty, not the hook's value.
func TestGetOrAddRetriedAttemptStartsFromTheCallersItem(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ctx := context.Background()
			t.Run("created", func(t *testing.T) {
				ct := newDBInternalTree(t, db, retryHookItem{})
				retryInjected(ct, nil)
				creates := failNodeCreateOnce(t, ct)

				item := retryHookItem{Name: "a", Scratch: "mine"}
				created, err := ct.GetOrAdd(ctx, &item, 0, "t1", []string{"Name"})
				if err != nil {
					t.Fatalf("get or add: %v", err)
				}
				if !created || *creates != 2 {
					t.Fatalf("want the node created on the second of 2 attempts, got created %v after %d creates", created, *creates)
				}
				if item.NodeId == 0 || item.Hooks != 0 || item.Scratch != "mine" {
					t.Errorf("want the caller's item with a new node id and its own Hooks 0 and Scratch %q, got %+v", "mine", item)
				}
				var stored []retryHookItem
				if err := ct.db.Table(ct.nodesTbl).Where("tenant = ?", "t1").Find(&stored).Error; err != nil {
					t.Fatal(err)
				}
				if len(stored) != 1 || stored[0].NodeId != item.NodeId || stored[0].Hooks != 1 {
					t.Errorf("want one stored row %d with Hooks 1, got %+v", item.NodeId, stored)
				}
			})
			t.Run("found", func(t *testing.T) {
				ct := newDBInternalTree(t, db, retryHookItem{})
				other := retryHookItem{Name: "a"}
				retryInjected(ct, func() {
					if err := ct.Add(ctx, &other, nil, nil, "t1"); err != nil {
						t.Errorf("add between the attempts: %v", err)
					}
				})
				creates := failNodeCreateOnce(t, ct)

				item := retryHookItem{Name: "a", Scratch: "mine"}
				created, err := ct.GetOrAdd(ctx, &item, 0, "t1", []string{"Name"})
				if err != nil {
					t.Fatalf("get or add: %v", err)
				}
				if created || *creates != 2 {
					t.Fatalf("want the node added between the attempts found, got created %v after %d creates", created, *creates)
				}
				want := retryHookItem{Node: Node{NodeId: other.NodeId, Tenant: "t1", SortOrder: other.SortOrder}, Name: "a", Hooks: 1}
				if item != want {
					t.Errorf("want the stored row %+v, got %+v", want, item)
				}
			})
		})
	}
}

// A retried DeleteRecurseMany counts the nodes deleted by the attempt that succeeded only.
func TestDeleteRecurseManyRetriedAttemptCountsOnce(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			ctx := context.Background()
			var roots []uint
			for _, name := range []string{"r1", "r2", "r3"} {
				root := txSample{Name: name}
				if err := ct.Add(ctx, &root, nil, nil, "t1"); err != nil {
					t.Fatal(err)
				}
				roots = append(roots, root.NodeId)
			}
			if err := ct.Add(ctx, &txSample{Name: "child"}, &roots[0], nil, "t1"); err != nil {
				t.Fatal(err)
			}
			retryInjected(ct, nil)
			// Fail the closure delete, which runs after the node delete.
			cb := ct.db.Callback().Raw()
			closureDeletes := failOnce(t, cb.After("gorm:raw").Register, cb.Remove, func(db *gorm.DB) bool {
				return strings.Contains(db.Statement.SQL.String(), "WITH descendants AS")
			})

			deleted, err := ct.DeleteRecurseMany(ctx, roots, "t1")
			if err != nil {
				t.Fatalf("delete: %v", err)
			}
			if *closureDeletes != 2 {
				t.Fatalf("want the closure delete on 2 attempts, got %d", *closureDeletes)
			}
			if deleted != 4 {
				t.Errorf("want 4 deleted, got %d", deleted)
			}
			var left int64
			if err := ct.db.Table(ct.nodesTbl).Where("tenant = ?", "t1").Count(&left).Error; err != nil {
				t.Fatal(err)
			}
			if left != 0 {
				t.Errorf("want no node left, got %d", left)
			}
		})
	}
}

// failNodeCreates fails every create of a row of ct's node table, after it ran, with err, and
// returns a counter of those creates.
func failNodeCreates(t *testing.T, ct *Tree, err error) *int {
	t.Helper()
	const name = "test:fail_node_creates"
	seen := new(int)
	cb := ct.db.Callback().Create()
	if regErr := cb.After("gorm:create").Register(name, func(db *gorm.DB) {
		if db.Error == nil && db.Statement.Table == ct.nodesTbl {
			*seen++
			_ = db.AddError(err)
		}
	}); regErr != nil {
		t.Fatal(regErr)
	}
	t.Cleanup(func() {
		if err := cb.Remove(name); err != nil {
			t.Error(err)
		}
	})
	return seen
}

// Under the MySQL policy a write on the library's own transaction that deadlocks on every attempt
// makes 5 attempts and returns the last deadlock, still reachable as the driver's error 1213; a lock
// wait timeout (error 1205) is returned after one attempt. The errors are injected, so this runs on
// every database; the pause is skipped.
func TestMySQLPolicyReturnsTheDeadlockAfterTheLastAttempt(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'},
		Message: "Deadlock found when trying to get lock; try restarting transaction"}
	timeout := &mysql.MySQLError{Number: 1205, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
		Message: "Lock wait timeout exceeded; try restarting transaction"}
	tests := []struct {
		name     string
		err      *mysql.MySQLError
		attempts int
	}{
		{"deadlock", deadlock, 5},
		{"lockWaitTimeout", timeout, 1},
	}
	for _, db := range testdbs.DBs() {
		for _, tt := range tests {
			t.Run(db.DbType()+"/"+tt.name, func(t *testing.T) {
				ct := newDBInternalTree(t, db, txSample{})
				ct.retry = dialectMySQL.retryPolicy()
				ct.retry.pause = noPause
				creates := failNodeCreates(t, ct, tt.err)

				err := ct.Add(context.Background(), &txSample{Name: "a"}, nil, nil, "t1")
				if *creates != tt.attempts {
					t.Errorf("want %d attempts, got %d", tt.attempts, *creates)
				}
				var got *mysql.MySQLError
				if !errors.As(err, &got) || got.Number != tt.err.Number {
					t.Fatalf("want the driver's error %d, got %v", tt.err.Number, err)
				}
				if tt.attempts > 1 && !strings.Contains(err.Error(), fmt.Sprintf("each of %d attempts", tt.attempts)) {
					t.Errorf("want the error to give the number of attempts, got %v", err)
				}
				var left int64
				if err := ct.db.Table(ct.nodesTbl).Where("tenant = ?", "t1").Count(&left).Error; err != nil {
					t.Fatal(err)
				}
				if left != 0 {
					t.Errorf("want no node stored, got %d", left)
				}
			})
		}
	}
}
