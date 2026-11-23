package closuretree

import (
	"context"
	"fmt"
	"math"
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

// scanEach runs the query and passes every row to add twice: as the scanned *Model and as the new
// *T that toElem, built by modelToElem, makes of it. Rows are scanned with gorm's scanner into the
// Tree's model, which gorm parsed in New. gorm skips the result columns the model has no field
// for, such as cte_depth. The caller builds toElem before any query, so an element type that
// cannot take the model's fields fails first; an error of toElem, a value out of range, stops the
// read at that row and is returned as it is. errLabel starts the error of a failed query
// ("<errLabel>: <cause>").
func (ct *Tree) scanEach(ctx context.Context, sqlstr string, args []any, toElem func(row reflect.Value) (reflect.Value, error), errLabel string, add func(elem, row reflect.Value)) (err error) {
	db := ct.db.WithContext(ctx)
	rows, err := db.Raw(sqlstr, args...).Rows()
	if err != nil {
		return fmt.Errorf("%s: %w", errLabel, err)
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
		elem, err := toElem(row)
		if err != nil {
			return err
		}
		add(elem, row)
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

// modelToElem returns the conversion of a scanned *Model into a *T, where T is elemType. T is often
// not a type gorm can parse: the element of a TreeDescendants read typically wraps the model and
// adds an untagged Children []*T field, which gorm takes for a relation without a foreign key.
//
//   - When T is the model, the row is returned as it is.
//   - When T embeds the model directly (an anonymous field of exactly the model type), the row is
//     copied into that field and nothing else is set.
//   - Otherwise each model field is copied to the exported field of T at the same path of Go names
//     (Name, or Addr.Name for a field of an embedded struct). When T has no field at that path, the
//     field is copied by its own Go name alone, but only if no other model field has that name. A
//     field T lacks is not copied, so T may be smaller than the model.
//
// A value whose type differs is assigned when it is assignable and converted when it is
// convertible; a pointer field is dereferenced first, and a nil pointer leaves the element field
// zero. A nil pointer to an embedded struct on the way to an element field is allocated. Two
// conversions Go allows are never made (see convertible): an integer to a string, and a slice to an
// array or to a pointer to an array. Any other type difference returns an error, and so does a T
// that is not a struct. A narrowing conversion of a number is made, but a value outside the range
// of the element field's type (see overflows) makes the returned function return an error.
func (ct *Tree) modelToElem(ctx context.Context, elemType reflect.Type) (func(row reflect.Value) (reflect.Value, error), error) {
	model := ct.schema.ModelType
	if elemType == model {
		return func(row reflect.Value) (reflect.Value, error) { return row, nil }, nil
	}
	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("cannot read nodes into %s: the element type must be a struct or a pointer to a struct", elemType)
	}
	if i, ok := embeddedField(elemType, model); ok && elemType.Field(i).IsExported() {
		return func(row reflect.Value) (reflect.Value, error) {
			elem := reflect.New(elemType)
			elem.Elem().Field(i).Set(row.Elem())
			return elem, nil
		}, nil
	}
	copies, err := ct.planCopies(elemType)
	if err != nil {
		return nil, err
	}
	return func(row reflect.Value) (reflect.Value, error) {
		elem := reflect.New(elemType)
		for _, c := range copies {
			dst, ok := settableField(elem.Elem(), c.dst)
			if !ok {
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
				if overflows(v, dst.Type()) {
					return reflect.Value{}, fmt.Errorf("cannot copy field %s of node %d: value %v overflows %s",
						c.src.Name, nodeOf(row).NodeId, v, dst.Type())
				}
				v = v.Convert(dst.Type())
			}
			dst.Set(v)
		}
		return elem, nil
	}, nil
}

// planCopies lists the copies of the model's fields into the struct type elemType (see
// modelToElem). A field is copied when gorm reads it: it has a column name and is readable.
func (ct *Tree) planCopies(elemType reflect.Type) ([]fieldCopy, error) {
	var fields []*schema.Field
	leafCount := map[string]int{}
	for _, f := range ct.schema.Fields {
		if f.DBName != "" && f.Readable {
			fields = append(fields, f)
			leafCount[f.Name]++
		}
	}
	if embedded, ok := embeddedField(elemType, ct.schema.ModelType); ok {
		// The embedded model's type is not exported, so the field cannot be set as a whole; its
		// exported fields can. Each one goes to the same path of Go names below the embedded
		// model, which has the same type. gorm's own index of a field is not used: it marks a
		// pointer-embedded struct with negative entries.
		copies := make([]fieldCopy, 0, len(fields))
		for _, f := range fields {
			dst, ok := fieldByPath(ct.schema.ModelType, f.BindNames)
			if !ok {
				continue
			}
			copies = append(copies, fieldCopy{src: f, dst: append([]int{embedded}, dst.Index...)})
		}
		return copies, nil
	}
	var copies []fieldCopy
	for _, f := range fields {
		dst, ok := fieldByPath(elemType, f.BindNames)
		if !ok && leafCount[f.Name] == 1 {
			dst, ok = elemType.FieldByName(f.Name)
			ok = ok && dst.IsExported()
		}
		if !ok {
			continue
		}
		c, err := planFieldCopy(f, dst)
		if err != nil {
			return nil, err
		}
		copies = append(copies, c)
	}
	return copies, nil
}

// embeddedField returns the index of the anonymous field of t whose type is exactly model.
func embeddedField(t, model reflect.Type) (int, bool) {
	for i := 0; i < t.NumField(); i++ {
		if f := t.Field(i); f.Anonymous && f.Type == model {
			return i, true
		}
	}
	return 0, false
}

// fieldByPath walks names, a path of Go field names, from the struct type t and returns the field
// at its end with the full index path. Each step may be a promoted field; a step that is not the
// last may be a pointer to a struct. The field must be settable: the last one exported, and every
// one before it exported or embedded.
func fieldByPath(t reflect.Type, names []string) (reflect.StructField, bool) {
	var found reflect.StructField
	var index []int
	for i, name := range names {
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return reflect.StructField{}, false
		}
		f, ok := t.FieldByName(name)
		if !ok || (!f.IsExported() && (i == len(names)-1 || !f.Anonymous)) {
			return reflect.StructField{}, false
		}
		index = append(index, f.Index...)
		found, t = f, f.Type
	}
	found.Index = index
	return found, len(names) > 0
}

// settableField returns the field of the struct v at index, a path of field indexes, allocating
// every nil pointer to a struct on the way, as gorm does when it scans into such a field. It
// reports false when a nil pointer cannot be set, as one embedded through an unexported type.
func settableField(v reflect.Value, index []int) (reflect.Value, bool) {
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				if !v.CanSet() {
					return reflect.Value{}, false
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v, true
}

// planFieldCopy decides how the model field src is copied into the element field dst.
func planFieldCopy(src *schema.Field, dst reflect.StructField) (fieldCopy, error) {
	c := fieldCopy{src: src, dst: dst.Index}
	from := src.StructField.Type
	switch {
	case from.AssignableTo(dst.Type):
	case convertible(from, dst.Type):
		c.conv = true
	case from.Kind() == reflect.Pointer && from.Elem().AssignableTo(dst.Type):
		c.deref = true
	case from.Kind() == reflect.Pointer && convertible(from.Elem(), dst.Type):
		c.deref, c.conv = true, true
	default:
		return fieldCopy{}, fmt.Errorf("cannot assign type %s to field %s", from, src.Name)
	}
	return c, nil
}

// convertible is ConvertibleTo without two conversions Go allows: an integer to a string, which
// gives the UTF-8 encoding of the code point (65 becomes "A"), and a slice to an array or to a
// pointer to an array, which panics at run time when the slice is too short.
func convertible(from, to reflect.Type) bool {
	if isInt(from.Kind()) || isUint(from.Kind()) {
		if to.Kind() == reflect.String {
			return false
		}
	}
	if from.Kind() == reflect.Slice &&
		(to.Kind() == reflect.Array || (to.Kind() == reflect.Pointer && to.Elem().Kind() == reflect.Array)) {
		return false
	}
	return from.ConvertibleTo(to)
}

// overflows reports whether converting the number v to the numeric type to would leave the range
// of to: an integer would wrap, and a float would give an integer Go does not define. A float is
// truncated toward zero first, as the conversion does, and NaN and the infinities overflow every
// integer type; a float converted to a float overflows when it is finite and too large. A
// conversion to a float from an integer may lose precision but never overflows, and values that
// are not numbers never do.
func overflows(v reflect.Value, to reflect.Type) bool {
	k := to.Kind()
	switch {
	case isInt(v.Kind()):
		x := v.Int()
		if isInt(k) {
			return to.OverflowInt(x)
		}
		if isUint(k) {
			return x < 0 || to.OverflowUint(uint64(x))
		}
	case isUint(v.Kind()):
		x := v.Uint()
		if isInt(k) {
			return x > math.MaxInt64 || to.OverflowInt(int64(x))
		}
		if isUint(k) {
			return to.OverflowUint(x)
		}
	case v.Kind() == reflect.Float32 || v.Kind() == reflect.Float64:
		x := math.Trunc(v.Float())
		switch {
		case isInt(k):
			limit := math.Ldexp(1, to.Bits()-1)
			return !(x >= -limit && x < limit) // NaN fails every comparison
		case isUint(k):
			return !(x >= 0 && x < math.Ldexp(1, to.Bits()))
		case k == reflect.Float32 || k == reflect.Float64:
			return to.OverflowFloat(v.Float())
		}
	}
	return false
}

func isInt(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Int64
}

func isUint(k reflect.Kind) bool {
	return k >= reflect.Uint && k <= reflect.Uintptr
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

// childrenIndex returns the index path of the Children field of the tree element type elemType, a
// *T. The field must be a []*T that can be reached on every element. It returns an error naming T
// when T has no Children field, when embedded structs at the same depth both have one (Go promotes
// neither), when the field has another type, and when it is promoted through an embedded pointer
// to an unexported type, which reflection cannot allocate. An embedded unexported struct value is
// no obstacle: the exported fields promoted through it can be set.
func childrenIndex(elemType reflect.Type) ([]int, error) {
	t := elemType.Elem()
	f, ok := t.FieldByName("Children")
	if !ok {
		if embedsField(t, "Children", map[reflect.Type]bool{}) {
			return nil, fmt.Errorf("cannot read a tree into %s: its field Children is ambiguous, embedded structs at the same depth both have one", t)
		}
		return nil, fmt.Errorf("cannot read a tree into %s: it has no field Children; add one of type %s, or read with Descendants", t, reflect.SliceOf(elemType))
	}
	if f.Type != reflect.SliceOf(elemType) {
		return nil, fmt.Errorf("cannot read a tree into %s: its field Children is %s, not %s", t, f.Type, reflect.SliceOf(elemType))
	}
	outer := t
	for _, x := range f.Index[:len(f.Index)-1] {
		embedded := outer.Field(x)
		outer = embedded.Type
		if outer.Kind() == reflect.Pointer {
			if !embedded.IsExported() {
				return nil, fmt.Errorf("cannot read a tree into %s: its field Children cannot be reached, it is promoted through the embedded pointer %s to an unexported type", t, outer)
			}
			outer = outer.Elem()
		}
	}
	return f.Index, nil
}

// embedsField reports whether the struct type t, or a struct embedded in it at any depth, by value
// or by pointer, declares a field called name.
func embedsField(t reflect.Type, name string, seen map[reflect.Type]bool) bool {
	if seen[t] {
		return false
	}
	seen[t] = true
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Name == name {
			return true
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if f.Anonymous && ft.Kind() == reflect.Struct && embedsField(ft, name, seen) {
			return true
		}
	}
	return false
}

// linkTree appends every element to the Children field of its parent, found through the Node of
// the scanned rows, and returns the elements whose parent is not among them. children is the index
// path of the Children field in the element type, which childrenIndex has checked before the query
// ran, so the field is reached on every element, leaves included, allocating nil embedded pointers
// on the way. Siblings, and the returned roots, are ordered by sort_order, then node id.
func linkTree(entries []treeEntry, children []int) []reflect.Value {
	childrenOf := make(map[uint]reflect.Value, len(entries))
	for _, e := range entries {
		childrenOf[e.node.NodeId], _ = settableField(e.elem.Elem(), children)
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].node.SortOrder != entries[j].node.SortOrder {
			return entries[i].node.SortOrder < entries[j].node.SortOrder
		}
		return entries[i].node.NodeId < entries[j].node.NodeId
	})

	var roots []reflect.Value
	for _, e := range entries {
		siblings, found := childrenOf[e.node.ParentId]
		if !found {
			roots = append(roots, e.elem)
			continue
		}
		siblings.Set(reflect.Append(siblings, e.elem))
	}
	return roots
}
