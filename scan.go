package closuretree

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
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

func scanRowsToNodes(rows *sql.Rows, columns []string, col2FieldMap map[string]string, elemType reflect.Type) (
	map[int64]reflect.Value, map[int64]int64, error,
) {
	nodes := make(map[int64]reflect.Value)
	ancestorMap := make(map[int64]int64)

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		node, nodeID, ancestorID, err := mapRowToStruct(values, columns, col2FieldMap, elemType)
		if err != nil {
			return nil, nil, err
		}
		nodes[nodeID] = node
		ancestorMap[nodeID] = ancestorID
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row iteration error: %w", err)
	}

	return nodes, ancestorMap, nil
}

// toInt64 safely converts database integer values to int64, handling different driver types.
func toInt64(v any) (int64, bool) {
	if v == nil {
		return 0, true
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case float64:
		return int64(n), true
	case string:
		p, err := strconv.ParseInt(n, 10, 64)
		return p, err == nil
	case []byte:
		p, err := strconv.ParseInt(string(n), 10, 64)
		return p, err == nil
	}
	return 0, false
}

// trySetFromString attempts to parse s and assign it to fieldVal for numeric kinds.
// Returns true if the assignment was made.
func trySetFromString(s string, fieldVal reflect.Value) bool {
	switch fieldVal.Kind() {
	case reflect.Float32, reflect.Float64:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			fieldVal.SetFloat(f)
			return true
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			fieldVal.SetInt(n)
			return true
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n, err := strconv.ParseUint(s, 10, 64); err == nil {
			fieldVal.SetUint(n)
			return true
		}
	}
	return false
}

func mapRowToStruct(values []any, columns []string, col2FieldMap map[string]string, elemType reflect.Type) (
	reflect.Value, int64, int64, error,
) {
	newElem := reflect.New(elemType.Elem())
	var nodeID, ancestorID int64

	for i, col := range columns {
		fieldName, ok := col2FieldMap[col]
		if !ok {
			continue
		}

		var value any
		if b, ok := values[i].([]byte); ok {
			value = string(b)
		} else {
			value = values[i]
		}

		if fieldName == nodeIDField {
			n, ok := toInt64(value)
			if !ok {
				return reflect.Value{}, 0, 0, fmt.Errorf("cannot convert nodeID column value to int64: %T", value)
			}
			nodeID = n
		}
		if fieldName == ancestorIDMapKey {
			n, ok := toInt64(value)
			if !ok {
				return reflect.Value{}, 0, 0, fmt.Errorf("cannot convert ancestorID column value to int64: %T", value)
			}
			ancestorID = n
			// Also populate ParentId on the struct so TreeDescendants is
			// consistent with GetNode and Descendants.
			if pf := newElem.Elem().FieldByName("ParentId"); pf.IsValid() && pf.CanSet() && n >= 0 {
				pf.SetUint(uint64(n))
			}
		}

		fieldVal := newElem.Elem().FieldByName(fieldName)
		if !fieldVal.IsValid() || !fieldVal.CanSet() {
			continue
		}

		if value == nil {
			continue
		}

		// Some drivers (e.g. PostgreSQL) return numeric columns as []byte/string.
		// Handle the string → numeric conversion explicitly before reflection assignment.
		if s, ok := value.(string); ok && trySetFromString(s, fieldVal) {
			continue
		}

		val := reflect.ValueOf(value)
		if val.Type().AssignableTo(fieldVal.Type()) {
			fieldVal.Set(val)
		} else if val.Type().ConvertibleTo(fieldVal.Type()) {
			fieldVal.Set(val.Convert(fieldVal.Type()))
		} else {
			return reflect.Value{}, 0, 0, fmt.Errorf("cannot assign type %s to field %s", val.Type(), fieldName)
		}
	}

	return newElem, nodeID, ancestorID, nil
}

func buildTreeHierarchy(nodes map[int64]reflect.Value, ancestorMap map[int64]int64) []reflect.Value {
	var roots []reflect.Value

	// Process in sorted key order to ensure deterministic children ordering
	keys := make([]int64, 0, len(nodes))
	for k := range nodes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ni := nodes[keys[i]].Elem()
		nj := nodes[keys[j]].Elem()
		sfi := ni.FieldByName("SortOrder")
		sfj := nj.FieldByName("SortOrder")
		if sfi.IsValid() && sfj.IsValid() {
			si := sfi.Float()
			sj := sfj.Float()
			if si != sj {
				return si < sj
			}
		}
		return keys[i] < keys[j]
	})

	for _, nodeID := range keys {
		node := nodes[nodeID]
		ancestorID, hasAncestor := ancestorMap[nodeID]
		if !hasAncestor {
			roots = append(roots, node)
			continue
		}

		parent, found := nodes[ancestorID]
		if !found {
			roots = append(roots, node)
			continue
		}

		childrenField := parent.Elem().FieldByName("Children")
		if childrenField.IsValid() {
			childrenField.Set(reflect.Append(childrenField, node))
		}
	}
	return roots
}
