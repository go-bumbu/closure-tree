package closuretree

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"gorm.io/gorm/schema"
)

// pointerToSlice validates that items is a non-nil pointer to a slice and returns the slice's
// reflect.Value, so callers can share the check. Returns ErrItemsNil or ErrItemsNotPointerToSlice.
func pointerToSlice(items any) (reflect.Value, error) {
	if items == nil {
		return reflect.Value{}, ErrItemsNil
	}
	itemsVal := reflect.ValueOf(items)
	if itemsVal.Kind() != reflect.Pointer {
		return reflect.Value{}, ErrItemsNotPointerToSlice
	}
	sliceVal := itemsVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return reflect.Value{}, ErrItemsNotPointerToSlice
	}
	return sliceVal, nil
}

func validateItems(items any) error {
	sliceVal, err := pointerToSlice(items)
	if err != nil {
		return err
	}
	elemType := sliceVal.Type().Elem()
	if elemType.Kind() != reflect.Pointer || elemType.Elem().Kind() != reflect.Struct {
		return ErrSliceElemNotPointerToStruct
	}
	return nil
}

// scanEach runs the query and passes every row to add as a new *T, where T is elemType. Rows are
// scanned with gorm's scanner into the Tree's model, which gorm parsed in New; when T is another
// type, the model's fields are then copied into T (see modelToElem). gorm skips the result columns
// the model has no field for, such as cte_depth.
func (ct *Tree) scanEach(ctx context.Context, sqlstr string, args []any, elemType reflect.Type, add func(elem reflect.Value)) (err error) {
	toElem := ct.modelToElem(ctx, elemType)
	db := ct.db.WithContext(ctx)
	rows, err := db.Raw(sqlstr, args...).Rows()
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer func() {
		e := rows.Close()
		if err == nil { // don't overwrite the original error
			err = e
		}
	}()

	for rows.Next() {
		row := reflect.New(ct.schema.ModelType)
		if err := db.ScanRows(rows, row.Interface()); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		add(toElem(row))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	return nil
}

// modelToElem returns the conversion of a scanned *Model into a *T, where T is elemType: the
// identity when T is the model, otherwise a copy of every model field into the field of T that
// has the same Go name and type. T is often not a type gorm can parse: the element of a
// TreeDescendants read typically wraps the model and adds an untagged Children []*T field, which
// gorm takes for a relation without a foreign key. A field T lacks is not copied, so T may be
// smaller than the model.
func (ct *Tree) modelToElem(ctx context.Context, elemType reflect.Type) func(row reflect.Value) reflect.Value {
	if elemType == ct.schema.ModelType {
		return func(row reflect.Value) reflect.Value { return row }
	}
	type fieldCopy struct {
		src *schema.Field
		dst []int
	}
	var copies []fieldCopy
	if elemType.Kind() == reflect.Struct {
		for _, f := range ct.schema.Fields {
			if f.DBName == "" || !f.Readable {
				continue
			}
			dst, ok := elemType.FieldByName(f.Name)
			if ok && dst.IsExported() && dst.Type == f.StructField.Type {
				copies = append(copies, fieldCopy{src: f, dst: dst.Index})
			}
		}
	}
	return func(row reflect.Value) reflect.Value {
		elem := reflect.New(elemType)
		for _, c := range copies {
			if dst, err := elem.Elem().FieldByIndexErr(c.dst); err == nil {
				dst.Set(c.src.ReflectValueOf(ctx, row.Elem()))
			}
		}
		return elem
	}
}

// nodeOf returns the Node of v, a pointer to a struct that is or embeds Node.
func nodeOf(v reflect.Value) (Node, bool) {
	t, sv := v.Type().Elem(), v.Elem()
	if t == reflect.TypeOf(Node{}) {
		return sv.Interface().(Node), true
	}
	nv, ok := findNodeValue(t, sv)
	if !ok {
		return Node{}, false
	}
	return nv.Interface().(Node), true
}

// linkTree appends every node to the Children field of its parent, found through Node.ParentId,
// and returns the nodes whose parent is not among them. nodes are pointers to structs that embed
// Node; a node without Node, or whose parent has no Children field of type []*T, is left unlinked.
// Siblings, and the returned roots, are ordered by sort_order, then node id.
func linkTree(nodes []reflect.Value) []reflect.Value {
	type entry struct {
		v       reflect.Value
		node    Node
		hasNode bool
	}
	entries := make([]entry, 0, len(nodes))
	byID := make(map[uint]reflect.Value, len(nodes))
	for _, v := range nodes {
		n, ok := nodeOf(v)
		entries = append(entries, entry{v: v, node: n, hasNode: ok})
		if ok {
			byID[n.NodeId] = v
		}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].node.SortOrder != entries[j].node.SortOrder {
			return entries[i].node.SortOrder < entries[j].node.SortOrder
		}
		return entries[i].node.NodeId < entries[j].node.NodeId
	})

	var roots []reflect.Value
	for _, e := range entries {
		parent, found := byID[e.node.ParentId]
		if !e.hasNode || !found {
			roots = append(roots, e.v)
			continue
		}
		children := parent.Elem().FieldByName("Children")
		if children.IsValid() && children.CanSet() && children.Type() == reflect.SliceOf(e.v.Type()) {
			children.Set(reflect.Append(children, e.v))
		}
	}
	return roots
}
