package closuretree

import (
	"errors"
	"reflect"
)

// Node is an embeddable ID to be used in closure tree, this is not mandatory.
type Node struct {
	NodeId uint   `gorm:"AUTO_INCREMENT;PRIMARY_KEY;not null"`
	Tenant string `gorm:"index"`
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

	itemType := reflect.TypeOf(item)
	itemValue := reflect.ValueOf(item)
	if itemType.Kind() == reflect.Ptr {
		itemType = itemType.Elem()
		itemValue = itemValue.Elem()
	}

	if itemType.Kind() != reflect.Struct {
		return 0, "", errors.New("getTenant: item is not a struct")
	}
	tenant := ""
	id := uint(0)
	// Check if the struct is the Node struct itself
	if itemType == reflect.TypeOf(Node{}) {
		tenantField := itemValue.FieldByName(tenantIdField)
		if tenantField.IsValid() {
			tenant = tenantField.String()
		}
		idField := itemValue.FieldByName(nodeIDField)
		if idField.IsValid() && idField.CanUint() {
			id = uint(idField.Uint())
		}
		return id, tenant, nil
	}

	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		fieldValue := itemValue.Field(i)
		if field.Anonymous {
			if field.Type == reflect.TypeOf(Node{}) {
				embeddedTenant := fieldValue.FieldByName(tenantIdField)
				if embeddedTenant.IsValid() {
					tenant = embeddedTenant.String()
				}
				embeddedId := fieldValue.FieldByName(nodeIDField)
				if embeddedId.IsValid() && embeddedId.CanUint() {
					id = uint(embeddedId.Uint())
				}
				return id, tenant, nil
			}
		}
	}

	return 0, "", errors.New("struct Node not found")
}
