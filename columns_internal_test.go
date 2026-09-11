package closuretree

import (
	"sync"
	"testing"

	"gorm.io/gorm/schema"
)

// columnName parses model's GORM schema (default naming strategy, matching what the library relies
// on) and returns the database column name for the named struct field.
func columnName(t *testing.T, model any, field string) string {
	t.Helper()
	s, err := schema.Parse(model, &sync.Map{}, schema.NamingStrategy{})
	if err != nil {
		t.Fatalf("parse schema for %T: %v", model, err)
	}
	f := s.LookUpField(field)
	if f == nil {
		t.Fatalf("field %s not found on %T", field, model)
	}
	return f.DBName
}

// TestColumnNamesMatchSchema pins the column-name constants (used by the raw SQL and the row
// scanner) to the names GORM actually derives from the structs. If a struct field's column is
// renamed without updating both the constant and the hand-written SQL, this fails loudly instead of
// letting the scanner silently skip an unrecognized column.
func TestColumnNamesMatchSchema(t *testing.T) {
	checks := []struct {
		model any
		field string
		want  string
	}{
		{Node{}, "NodeId", colNodeID},
		{Node{}, "ParentId", colParentID},
		{Node{}, "Tenant", colTenant},
		{Node{}, "SortOrder", colSortOrder},
		{closureTree{}, "AncestorID", colAncestorID},
		{closureTree{}, "DescendantID", colDescendantID},
		{closureTree{}, "Tenant", colTenant},
		{closureTree{}, "Depth", colDepth},
		{closureTreeMeta{}, "Tenant", colTenant},
		{closureTreeMeta{}, "ParentID", colParentID},
		{closureTreeMeta{}, "MinHalvings", colMinHalvings},
		{closureTreeLock{}, "Tenant", colTenant},
	}
	for _, c := range checks {
		if got := columnName(t, c.model, c.field); got != c.want {
			t.Errorf("%T.%s column = %q, want %q (SQL and scanner would diverge)", c.model, c.field, got, c.want)
		}
	}
}
