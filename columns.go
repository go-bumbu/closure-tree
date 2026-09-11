package closuretree

// Canonical database column names shared by the raw SQL (writers) and the row scanner via
// col2FieldMap (readers). They are the single declared source for these names.
//
// The SQL strings still spell the names inline for readability, but they are not free to drift:
// TestColumnNamesMatchSchema asserts every constant here equals the column name GORM derives from
// the Node struct and this package's own closure/meta/lock structs. So renaming a struct field's
// column can't silently mismatch the hand-written SQL (which the scanner would then quietly skip) —
// it fails that test loudly instead, pointing at the SQL to update.
const (
	colNodeID       = "node_id"
	colParentID     = "parent_id"
	colTenant       = "tenant"
	colSortOrder    = "sort_order"
	colAncestorID   = "ancestor_id"
	colDescendantID = "descendant_id"
	colDepth        = "depth"
	colMinHalvings  = "min_halvings"
)
