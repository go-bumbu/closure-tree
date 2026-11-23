package closuretree

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// retryPolicy says how writeTx repeats a transaction it opened itself when an attempt ends with an
// error after which the database has rolled the whole transaction back, so the next attempt can run
// the write again from the start. The zero value makes one attempt. Each dialect sets its policy
// (dialect.retryPolicy); internal tests replace it.
type retryPolicy struct {
	attempts  int              // attempts in total; below 2 means one attempt
	retryable func(error) bool // reports whether an attempt that ended with the error may be repeated
	// pause waits before the attempt after attempt (counted from 1); it returns ctx.Err() when ctx
	// ends first.
	pause func(ctx context.Context, attempt int) error
}

// run calls fn until it returns nil or an error that p.retryable rejects, at most p.attempts times,
// with p.pause between two calls. When every attempt fails with a retryable error, it returns the
// last one wrapped with the count; when ctx ends during a pause, it returns the failed attempt's
// error joined with ctx.Err(). The errors are wrapped with %w, so errors.Is and errors.As reach both.
func (p retryPolicy) run(ctx context.Context, fn func() error) error {
	if p.attempts < 2 {
		return fn()
	}
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || !p.retryable(err) {
			return err
		}
		if attempt == p.attempts {
			return fmt.Errorf("each of %d attempts failed: %w", p.attempts, err)
		}
		if perr := p.pause(ctx, attempt); perr != nil {
			return fmt.Errorf("attempt %d of %d failed: %w; retry stopped: %w", attempt, p.attempts, err, perr)
		}
	}
}

// The pause before a retry grows from retryPauseBase, doubling per attempt, up to retryPauseMax.
const (
	retryPauseBase = 25 * time.Millisecond
	retryPauseMax  = 200 * time.Millisecond
)

// retryPause waits before the attempt after attempt (counted from 1) for a random time from 0 up to
// retryPauseCap(attempt) ("full jitter"): two writers that failed together then retry apart. It
// returns ctx.Err() at once when ctx ends first.
func retryPause(ctx context.Context, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(retryPauseDelay(attempt, rand.Int64N))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// retryPauseDelay draws the pause after attempt from [0, retryPauseCap(attempt)] with randN, which
// returns a number from [0, n) like rand.Int64N.
func retryPauseDelay(attempt int, randN func(n int64) int64) time.Duration {
	return time.Duration(randN(int64(retryPauseCap(attempt)) + 1))
}

// retryPauseCap is the longest pause after attempt (counted from 1): retryPauseBase doubled per
// earlier attempt, at most retryPauseMax.
func retryPauseCap(attempt int) time.Duration {
	limit := retryPauseBase
	for i := 1; i < attempt && limit < retryPauseMax; i++ {
		limit *= 2
	}
	return min(limit, retryPauseMax)
}
