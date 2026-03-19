package closuretree

import (
	"errors"
	"reflect"
)

// Node is an embeddable ID to be used in closure tree, this is mandatory.
// ParentId is ignored during write operations, it is only populated during read.
type Node struct {
	NodeId   uint   `gorm:"autoIncrement;primaryKey;not null;index:idx_node_tenant,composite:2" json:"id"`
	ParentId uint   `json:"parentId" gorm:"column:parent_id;->;-:migration"` // field is Read-only, no migration
	Tenant   string `gorm:"not null;index:idx_node_tenant,composite:1" json:"tenant"`
}

func (n *Node) Id() uint {
	return n.NodeId
}

func (n *Node) Parent() uint {
	return n.ParentId
}

const nodeIDField = "NodeId"
const tenantIdField = "Tenant"

// hasNode uses reflection to verify if the passed struct has the embedded Node struct
func hasNode(item any) bool {
	if item == nil {
		return false
	}

	itemType := reflect.TypeOf(item)
	if itemType.Kind() == reflect.Ptr {
		itemType = itemType.Elem()
	}

	if itemType.Kind() != reflect.Struct {
		return false
	}

	return hasNodeType(itemType)
}

func hasNodeType(t reflect.Type) bool {
	if t == reflect.TypeOf(Node{}) {
		return true
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct && hasNodeType(f.Type) {
			return true
		}
	}
	return false
}

func getNodeData(item interface{}) (uint, string, error) {
	if item == nil {
		return 0, "", errors.New("getNodeData: item cannot be nil")
	}

	itemType, itemValue := dereference(item)
	if itemType.Kind() != reflect.Struct {
		return 0, "", errors.New("getNodeData: item is not a struct")
	}

	// Try to extract data if it's a Node struct
	if itemType == reflect.TypeOf(Node{}) {
		return extractNodeFields(itemValue)
	}

	// Try to extract from anonymous embedded Node (supports multi-level embedding)
	if v, ok := findNodeValue(itemType, itemValue); ok {
		return extractNodeFields(v)
	}

	return 0, "", errors.New("struct Node not found")
}

// findNodeValue recursively searches for an embedded Node field and returns its reflect.Value.
func findNodeValue(t reflect.Type, v reflect.Value) (reflect.Value, bool) {
	nodeType := reflect.TypeOf(Node{})
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.Anonymous {
			continue
		}
		fv := v.Field(i)
		if f.Type == nodeType {
			return fv, true
		}
		if f.Type.Kind() == reflect.Struct {
			if found, ok := findNodeValue(f.Type, fv); ok {
				return found, true
			}
		}
	}
	return reflect.Value{}, false
}

func dereference(item interface{}) (reflect.Type, reflect.Value) {
	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	return t, v
}

func extractNodeFields(val reflect.Value) (uint, string, error) {
	var tenant string
	var id uint

	tenantField := val.FieldByName(tenantIdField)
	if tenantField.IsValid() {
		tenant = tenantField.String()
	}

	idField := val.FieldByName(nodeIDField)
	if idField.IsValid() && idField.CanUint() {
		id = uint(idField.Uint())
	}

	return id, tenant, nil
}
