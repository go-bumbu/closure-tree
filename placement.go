package closuretree

import "errors"

// ErrConflictingPlacement is returned when PlaceLast is combined with a non-zero afterNodeID.
var ErrConflictingPlacement = errors.New("PlaceLast cannot be combined with afterNodeID")

// addConfig holds the settings collected from the AddOptions of one Add or GetOrAdd call.
type addConfig struct {
	last bool
}

// AddOption changes where Add and GetOrAdd place the new node among its siblings.
type AddOption func(*addConfig)

// PlaceLast places the new node after the current last sibling instead of first. It cannot be
// combined with a non-zero afterNodeID (Add returns ErrConflictingPlacement).
func PlaceLast() AddOption {
	return func(c *addConfig) { c.last = true }
}

// newAddConfig applies opts in order, skipping nil entries.
func newAddConfig(opts []AddOption) addConfig {
	var c addConfig
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return c
}
