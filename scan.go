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

// scanEach runs the query and passes every row to add twice: as the scanned *Model and as a new
// *T, where T is elemType. Rows are scanned with gorm's scanner into the Tree's model, which gorm
// parsed in New; when T is another type, the model's fields are then copied into T (see
// modelToElem). gorm skips the result columns the model has no field for, such as cte_depth. An
// element type that cannot take the model's fields fails before the query runs.
func (ct *Tree) scanEach(ctx context.Context, sqlstr string, args []any, elemType reflect.Type, add func(elem, row reflect.Value)) (err error) {
	toElem, err := ct.modelToElem(ctx, elemType)
	if err != nil {
		return err
	}
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
		add(toElem(row), row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}
	return nil
}

// fieldCopy copies one model field into the element field at dst.
type fieldCopy struct {
	src   *schema.Field
	dst   []int
	deref bool // src is a pointer: nil leaves dst zero, otherwise the pointed-to value is copied
	conv  bool // the value is converted to the type of dst
}

// modelToElem returns the conversion of a scanned *Model into a *T, where T is elemType: the
// identity when T is the model, otherwise a copy of every model field into the exported field of
// T that has the same Go name. T is often not a type gorm can parse: the element of a
// TreeDescendants read typically wraps the model and adds an untagged Children []*T field, which
// gorm takes for a relation without a foreign key. A field T lacks, or has unexported, is not
// copied, so T may be smaller than the model. A value whose type differs is assigned when it is
// assignable and converted when it is convertible; a pointer field is dereferenced first, and a
// nil pointer leaves the element field zero. Any other type difference returns an error.
func (ct *Tree) modelToElem(ctx context.Context, elemType reflect.Type) (func(row reflect.Value) reflect.Value, error) {
	if elemType == ct.schema.ModelType {
		return func(row reflect.Value) reflect.Value { return row }, nil
	}
	var copies []fieldCopy
	if elemType.Kind() == reflect.Struct {
		for _, f := range ct.schema.Fields {
			if f.DBName == "" || !f.Readable {
				continue
			}
			dst, ok := elemType.FieldByName(f.Name)
			if !ok || !dst.IsExported() {
				continue
			}
			c, err := planFieldCopy(f, dst)
			if err != nil {
				return nil, err
			}
			copies = append(copies, c)
		}
	}
	return func(row reflect.Value) reflect.Value {
		elem := reflect.New(elemType)
		for _, c := range copies {
			dst, err := elem.Elem().FieldByIndexErr(c.dst)
			if err != nil {
				continue
			}
			v := c.src.ReflectValueOf(ctx, row.Elem())
			if c.deref {
				if v.IsNil() {
					continue
				}
				v = v.Elem()
			}
			if c.conv {
				v = v.Convert(dst.Type())
			}
			dst.Set(v)
		}
		return elem
	}, nil
}

// planFieldCopy decides how the model field src is copied into the element field dst.
func planFieldCopy(src *schema.Field, dst reflect.StructField) (fieldCopy, error) {
	c := fieldCopy{src: src, dst: dst.Index}
	from := src.StructField.Type
	switch {
	case from.AssignableTo(dst.Type):
	case from.ConvertibleTo(dst.Type):
		c.conv = true
	case from.Kind() == reflect.Pointer && from.Elem().AssignableTo(dst.Type):
		c.deref = true
	case from.Kind() == reflect.Pointer && from.Elem().ConvertibleTo(dst.Type):
		c.deref, c.conv = true, true
	default:
		return fieldCopy{}, fmt.Errorf("cannot assign type %s to field %s", from, src.Name)
	}
	return c, nil
}

// nodeOf returns the Node of v, a pointer to a struct that is or embeds Node.
func nodeOf(v reflect.Value) Node {
	t, sv := v.Type().Elem(), v.Elem()
	if t == reflect.TypeOf(Node{}) {
		return sv.Interface().(Node)
	}
	nv, _ := findNodeValue(t, sv)
	return nv.Interface().(Node)
}

// treeEntry is one node of a tree read: the element handed to the caller and the Node of the
// scanned model row it came from.
type treeEntry struct {
	elem reflect.Value
	node Node
}

// linkTree appends every element to the Children field of its parent, found through the Node of
// the scanned rows, and returns the elements whose parent is not among them. An element whose
// parent has no Children field of type []*T is left unlinked. Siblings, and the returned roots,
// are ordered by sort_order, then node id.
func linkTree(entries []treeEntry) []reflect.Value {
	byID := make(map[uint]reflect.Value, len(entries))
	for _, e := range entries {
		byID[e.node.NodeId] = e.elem
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
		if !found {
			roots = append(roots, e.elem)
			continue
		}
		children := parent.Elem().FieldByName("Children")
		if children.IsValid() && children.CanSet() && children.Type() == reflect.SliceOf(e.elem.Type()) {
			children.Set(reflect.Append(children, e.elem))
		}
	}
	return roots
}
