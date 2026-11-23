package closuretree

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-bumbu/testdbs"
	"gorm.io/gorm"
)

var (
	errRetryable = errors.New("retryable failure")
	errOther     = errors.New("other failure")
)

// noPause is a pause that returns at once.
func noPause(context.Context, int) error { return nil }

// scripted returns a function that returns errs[i] on its i-th call and nil once errs runs out,
// and a counter of its calls.
func scripted(errs ...error) (fn func() error, calls *int) {
	calls = new(int)
	return func() error {
		*calls++
		if *calls <= len(errs) {
			return errs[*calls-1]
		}
		return nil
	}, calls
}

func TestRetryPolicyRun(t *testing.T) {
	isRetryable := func(err error) bool { return errors.Is(err, errRetryable) }
	five := retryPolicy{attempts: 5, retryable: isRetryable, pause: noPause}
	tests := []struct {
		name      string
		policy    retryPolicy
		errs      []error
		wantCalls int
		wantErr   error  // matched with errors.Is; nil means no error
		wantText  string // a substring of the error, when not empty
	}{
		{name: "success at once", policy: five, wantCalls: 1},
		{name: "success after two failures", policy: five, errs: []error{errRetryable, errRetryable}, wantCalls: 3},
		{name: "other error not retried", policy: five, errs: []error{errOther}, wantCalls: 1, wantErr: errOther},
		{name: "other error after a retry", policy: five, errs: []error{errRetryable, errOther}, wantCalls: 2, wantErr: errOther},
		{
			name: "every attempt fails", policy: five,
			errs:      []error{errRetryable, errRetryable, errRetryable, errRetryable, errRetryable, errRetryable},
			wantCalls: 5, wantErr: errRetryable, wantText: "each of 5 attempts",
		},
		{name: "zero policy makes one attempt", errs: []error{errRetryable}, wantCalls: 1, wantErr: errRetryable},
		{
			name: "one attempt", policy: retryPolicy{attempts: 1, retryable: isRetryable, pause: noPause},
			errs: []error{errRetryable}, wantCalls: 1, wantErr: errRetryable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, calls := scripted(tt.errs...)
			err := tt.policy.run(context.Background(), fn)
			if *calls != tt.wantCalls {
				t.Errorf("want %d calls, got %d", tt.wantCalls, *calls)
			}
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want %v, got %v", tt.wantErr, err)
			}
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Errorf("want %q in the error, got %q", tt.wantText, err)
			}
		})
	}
}

// The error of the last attempt comes back unchanged, apart from the count, so errors.As still
// reaches a driver error.
func TestRetryPolicyRunKeepsTheLastError(t *testing.T) {
	type codeErr struct{ error }
	last := codeErr{errRetryable}
	p := retryPolicy{attempts: 2, retryable: func(error) bool { return true }, pause: noPause}
	fn, _ := scripted(codeErr{errors.New("first")}, last)
	err := p.run(context.Background(), fn)
	var got codeErr
	if !errors.As(err, &got) || got != last {
		t.Fatalf("want the last attempt's error, got %v", err)
	}
}

// The pause is asked for each retry, with the number of the attempt that failed.
func TestRetryPolicyRunPausesBetweenAttempts(t *testing.T) {
	var paused []int
	p := retryPolicy{
		attempts:  4,
		retryable: func(error) bool { return true },
		pause: func(_ context.Context, attempt int) error {
			paused = append(paused, attempt)
			return nil
		},
	}
	fn, _ := scripted(errRetryable, errRetryable, errRetryable, errRetryable)
	_ = p.run(context.Background(), fn)
	if want := []int{1, 2, 3}; !slices.Equal(paused, want) {
		t.Errorf("want pauses after attempts %v, got %v", want, paused)
	}
}

// When the context ends during a pause, the loop stops and returns both the failed attempt's error
// and the context's.
func TestRetryPolicyRunStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := retryPolicy{
		attempts:  5,
		retryable: func(error) bool { return true },
		pause: func(ctx context.Context, _ int) error {
			cancel()
			return ctx.Err()
		},
	}
	fn, calls := scripted(errRetryable, errRetryable)
	err := p.run(ctx, fn)
	if *calls != 1 {
		t.Errorf("want 1 call, got %d", *calls)
	}
	if !errors.Is(err, errRetryable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("want the attempt's error and context.Canceled, got %v", err)
	}
}

func TestRetryPauseCap(t *testing.T) {
	want := map[int]time.Duration{
		1: 25 * time.Millisecond, 2: 50 * time.Millisecond, 3: 100 * time.Millisecond,
		4: 200 * time.Millisecond, 5: 200 * time.Millisecond, 64: 200 * time.Millisecond,
	}
	for attempt, w := range want {
		if got := retryPauseCap(attempt); got != w {
			t.Errorf("attempt %d: want cap %v, got %v", attempt, w, got)
		}
	}
}

// The pause is drawn from 0 up to the cap, both included.
func TestRetryPauseDelayBounds(t *testing.T) {
	for attempt := 1; attempt <= 5; attempt++ {
		limit := retryPauseCap(attempt)
		lowest := retryPauseDelay(attempt, func(int64) int64 { return 0 })
		highest := retryPauseDelay(attempt, func(n int64) int64 { return n - 1 })
		if lowest != 0 || highest != limit {
			t.Errorf("attempt %d: want pauses from 0 to %v, got %v to %v", attempt, limit, lowest, highest)
		}
		for range 100 {
			if d := retryPauseDelay(attempt, rand.Int64N); d < 0 || d > limit {
				t.Fatalf("attempt %d: pause %v outside [0, %v]", attempt, d, limit)
			}
		}
	}
}

func TestRetryPause(t *testing.T) {
	if err := retryPause(context.Background(), 1); err != nil {
		t.Fatalf("want no error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := retryPause(ctx, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed >= retryPauseCap(4) {
		t.Errorf("a done context should end the pause at once, it took %v", elapsed)
	}
}

// writeTx repeats only a transaction it opens itself: on a Tree whose handle is not in a
// transaction, also one made with WithTx, and never inside a caller's transaction.
func TestWriteTxRetriesOnlyItsOwnTransaction(t *testing.T) {
	for _, db := range testdbs.DBs() {
		t.Run(db.DbType(), func(t *testing.T) {
			ct := newDBInternalTree(t, db, txSample{})
			ct.retry = retryPolicy{
				attempts:  2,
				retryable: func(err error) bool { return errors.Is(err, errRetryable) },
				pause:     noPause,
			}
			ctx := context.Background()
			cases := []struct {
				name      string
				tree      func(t *testing.T) *Tree
				wantCalls int
				wantErr   error
			}{
				{name: "own transaction", tree: func(*testing.T) *Tree { return ct }, wantCalls: 2},
				{name: "WithTx outside a transaction", tree: func(*testing.T) *Tree { return ct.WithTx(ct.db) }, wantCalls: 2},
				{name: "caller transaction", tree: func(t *testing.T) *Tree {
					tx := ct.db.Begin()
					if tx.Error != nil {
						t.Fatal(tx.Error)
					}
					t.Cleanup(func() { tx.Rollback() })
					return ct.WithTx(tx)
				}, wantCalls: 1, wantErr: errRetryable},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					calls := 0
					err := c.tree(t).writeTx(ctx, "t1", func(*gorm.DB) error {
						calls++
						if calls == 1 {
							return errRetryable
						}
						return nil
					})
					if calls != c.wantCalls {
						t.Errorf("want %d calls, got %d", c.wantCalls, calls)
					}
					if !errors.Is(err, c.wantErr) || (c.wantErr == nil && err != nil) {
						t.Errorf("want error %v, got %v", c.wantErr, err)
					}
				})
			}
		})
	}
}
