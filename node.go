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

const branchIdField = "NodeId"

// hasNode uses reflection to verify if the passed struct has the embedded branch struct
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

		if field.Name == branchIdField && field.Type == reflect.TypeOf(uint(0)) {
			return true
		}
	}

	return false
}

func getID(item interface{}) (uint, error) {
	if item == nil {
		return 0, errors.New("topItem is nil")
	}

	itemType := reflect.TypeOf(item)
	itemValue := reflect.ValueOf(item)
	if itemType.Kind() == reflect.Ptr {
		itemType = itemType.Elem()
		itemValue = itemValue.Elem()
	}

	if itemType.Kind() != reflect.Struct {
		return 0, errors.New("topItem is not a struct")
	}

	// Check if the struct is the Node struct itself
	if itemType == reflect.TypeOf(Node{}) {
		idField := itemValue.FieldByName(branchIdField)
		if idField.IsValid() && idField.CanUint() {

			return uint(idField.Uint()), nil
		}
	}

	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		fieldValue := itemValue.Field(i)

		if field.Anonymous {
			if field.Type == reflect.TypeOf(Node{}) {
				embeddedID := fieldValue.FieldByName(branchIdField)
				if embeddedID.IsValid() && embeddedID.CanUint() {
					return uint(embeddedID.Uint()), nil
				}
			}
		}
	}

	return 0, errors.New("struct Node not found")
}
