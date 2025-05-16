package closuretree

import (
	"errors"
	"reflect"
)

// Node is an embeddable ID to be used in closure tree, this is mandatory.
type Node struct {
	NodeId uint   `gorm:"AUTO_INCREMENT;PRIMARY_KEY;not null" json:"id"`
	Tenant string `gorm:"index" json:"tenant"`
}

func (n *Node) Id() uint {
	return n.NodeId
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

	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		if field.Anonymous {
			if field.Type == reflect.TypeOf(Node{}) {
				return true
			}
		}

		if field.Name == nodeIDField && field.Type == reflect.TypeOf(uint(0)) {
			return true
		}
	}
	return false
}

func getNodeData(item interface{}) (uint, string, error) {
	if item == nil {
		return 0, "", errors.New("getTenant: item cannot be nil")
	}

	itemType, itemValue := dereference(item)
	if itemType.Kind() != reflect.Struct {
		return 0, "", errors.New("getTenant: item is not a struct")
	}

	// Try to extract data if it's a Node struct
	if itemType == reflect.TypeOf(Node{}) {
		return extractNodeFields(itemValue)
	}

	// Try to extract from anonymous embedded Node
	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		fieldValue := itemValue.Field(i)

		if field.Anonymous && field.Type == reflect.TypeOf(Node{}) {
			return extractNodeFields(fieldValue)
		}
	}

	return 0, "", errors.New("struct Node not found")
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
