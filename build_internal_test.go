package closuretree

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPlanLinks(t *testing.T) {
	ids := []uint{1, 2, 3, 4, 5}
	valid := []ParentLink{{1, 0}, {3, 1}, {2, 1}, {4, 0}, {5, 3}}
	with := func(extra ...ParentLink) []ParentLink { return append(append([]ParentLink{}, valid...), extra...) }

	t.Run("valid input keeps input order", func(t *testing.T) {
		plan, err := planLinks(ids, valid, false)
		if err != nil {
			t.Fatal(err)
		}
		want := map[uint][]uint{0: {1, 4}, 1: {3, 2}, 3: {5}}
		if diff := cmp.Diff(want, plan.children); diff != "" {
			t.Errorf("children (-want +got):\n%s", diff)
		}
		if plan.parent[5] != 3 || plan.parent[1] != 0 {
			t.Errorf("parents: %v", plan.parent)
		}
	})

	bad := []struct {
		name    string
		links   []ParentLink
		want    error
		mention []string // the message names at least one of these ids
	}{
		{"missing node", []ParentLink{{1, 0}, {2, 1}, {3, 1}, {4, 0}}, ErrLinksMismatch, []string{"5"}},
		{"unknown node", with(ParentLink{9, 0}), ErrLinksMismatch, []string{"9"}},
		{"duplicate node", with(ParentLink{2, 0}), ErrLinksMismatch, []string{"2"}},
		{"node id zero", with(ParentLink{0, 0}), ErrLinksMismatch, []string{"0"}},
		{"dangling parent", []ParentLink{{1, 0}, {2, 1}, {3, 99}, {4, 0}, {5, 3}}, ErrOrphanLink, []string{"3"}},
		{"self parent", []ParentLink{{1, 0}, {2, 2}, {3, 1}, {4, 0}, {5, 3}}, ErrLinkCycle, []string{"2"}},
		{"cycle", []ParentLink{{1, 3}, {2, 1}, {3, 2}, {4, 0}, {5, 0}}, ErrLinkCycle, []string{"1", "2", "3"}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planLinks(ids, tc.links, false)
			if plan != nil || !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got plan=%v err=%v", tc.want, plan, err)
			}
			named := false
			for _, id := range tc.mention {
				named = named || strings.Contains(err.Error(), id)
			}
			if !named {
				t.Errorf("the message names none of %v: %q", tc.mention, err.Error())
			}
		})
	}

	t.Run("orphans to root", func(t *testing.T) {
		plan, err := planLinks(ids, []ParentLink{{1, 0}, {2, 1}, {3, 99}, {4, 0}, {5, 3}}, true)
		if err != nil {
			t.Fatal(err)
		}
		if plan.parent[3] != 0 || !cmp.Equal(plan.children[0], []uint{1, 3, 4}) {
			t.Errorf("node 3 is not a root in input order: %v", plan.children[0])
		}
	})

	t.Run("the list of offending ids is capped", func(t *testing.T) {
		many := make([]uint, 30)
		for i := range many {
			many[i] = uint(i + 1)
		}
		_, err := planLinks(many, valid, false) // 25 nodes have no link
		if !errors.Is(err, ErrLinksMismatch) || !strings.Contains(err.Error(), "and 15 more") {
			t.Errorf("want a capped list ending in \"and 15 more\", got %v", err)
		}
	})
}

// reportedIDs returns the ids an idsError lists after its last ": ".
func reportedIDs(err error) []string {
	msg := err.Error()
	return strings.Split(msg[strings.LastIndex(msg, ": ")+2:], ", ")
}

func TestPlanLinksCycles(t *testing.T) {
	t.Run("a chain leading into a cycle names a node of the cycle, not the chain's start", func(t *testing.T) {
		// 1 -> 2 -> 3 -> 2: node 1 leads into the cycle 2, 3 but is not on it
		_, err := planLinks([]uint{1, 2, 3}, []ParentLink{{1, 2}, {2, 3}, {3, 2}}, false)
		if !errors.Is(err, ErrLinkCycle) {
			t.Fatalf("want ErrLinkCycle, got %v", err)
		}
		if got := reportedIDs(err); len(got) != 1 || (got[0] != "2" && got[0] != "3") {
			t.Errorf("want one id, 2 or 3, got %v in %q", got, err.Error())
		}
	})

	t.Run("two disjoint cycles are each named once", func(t *testing.T) {
		// 1 <-> 2 and 3 -> 4 -> 5 -> 3, plus a valid root 6
		links := []ParentLink{{1, 2}, {2, 1}, {3, 5}, {4, 3}, {5, 4}, {6, 0}}
		_, err := planLinks([]uint{1, 2, 3, 4, 5, 6}, links, false)
		if !errors.Is(err, ErrLinkCycle) {
			t.Fatalf("want ErrLinkCycle, got %v", err)
		}
		got := reportedIDs(err)
		inFirst := func(s string) bool { return s == "1" || s == "2" }
		inSecond := func(s string) bool { return s == "3" || s == "4" || s == "5" }
		oneOfEach := len(got) == 2 && (inFirst(got[0]) && inSecond(got[1]) || inSecond(got[0]) && inFirst(got[1]))
		if !oneOfEach {
			t.Errorf("want one id of each cycle, got %v in %q", got, err.Error())
		}
	})
}

func TestPlanLinksOrphanRootKeepsItsChildren(t *testing.T) {
	// node 1's parent 99 does not exist; 2 is under 1 and 3 under 2
	plan, err := planLinks([]uint{1, 2, 3}, []ParentLink{{1, 99}, {2, 1}, {3, 2}}, true)
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint][]uint{0: {1}, 1: {2}, 2: {3}}
	if diff := cmp.Diff(want, plan.children); diff != "" {
		t.Errorf("children (-want +got):\n%s", diff)
	}
	if plan.parent[1] != 0 || plan.parent[2] != 1 || plan.parent[3] != 2 {
		t.Errorf("parents: %v", plan.parent)
	}
}

func TestPlanLinksLongChain(t *testing.T) {
	// a chain of 100,000 nodes, deepest first so the first walk climbs the whole chain; the walk
	// must not recurse
	const n = 100000
	ids := make([]uint, 0, n)
	links := make([]ParentLink, 0, n)
	for id := uint(n); id >= 1; id-- {
		ids = append(ids, id)
		links = append(links, ParentLink{NodeID: id, ParentID: id - 1})
	}
	plan, err := planLinks(ids, links, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.children[0]) != 1 || plan.parent[n] != n-1 {
		t.Errorf("want one root and node %d under %d, got roots %v, parent %d", n, n-1, plan.children[0], plan.parent[n])
	}
}
