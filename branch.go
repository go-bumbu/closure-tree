package closuretree

import (
	"errors"
	"reflect"
)

// Branch is an embeddable ID to be used in closure tree, this is not mandatory.
type Branch struct {
	ID uint `gorm:"primaryKey,uniqueIndex,autoIncrement"`
}

// hasId uses reflection to verify if the passed struct has a field ID, needed for closure tree to work
func hasId(item interface{}) bool {
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

		// Check for anonymous fields (composition)
		if field.Anonymous {
			// Check if the embedded type matches Branch
			if field.Type == reflect.TypeOf(Branch{}) {
				return true
			}

			// Check if the embedded type has an ID field of type uint
			embeddedType := field.Type
			if embeddedType.Kind() == reflect.Ptr {
				embeddedType = embeddedType.Elem()
			}
			if embeddedType.Kind() == reflect.Struct {
				for j := 0; j < embeddedType.NumField(); j++ {
					embeddedField := embeddedType.Field(j)
					if embeddedField.Name == "ID" && embeddedField.Type == reflect.TypeOf(uint(0)) {
						return true
					}
				}
			}
		}

		if field.Name == "ID" && field.Type == reflect.TypeOf(uint(0)) {
			return true
		}
	}

	return false
}

func getID(item interface{}) (uint, error) {
	if item == nil {
		return 0, errors.New("item is nil")
	}

	itemType := reflect.TypeOf(item)
	itemValue := reflect.ValueOf(item)
	if itemType.Kind() == reflect.Ptr {
		itemType = itemType.Elem()
		itemValue = itemValue.Elem()
	}

	if itemType.Kind() != reflect.Struct {
		return 0, errors.New("item is not a struct")
	}

	// Check for direct match with Branch type
	if itemType == reflect.TypeOf(Branch{}) {
		idField := itemValue.FieldByName("ID")
		if idField.IsValid() && idField.CanUint() {
			return uint(idField.Uint()), nil
		}
	}

	// Check fields in the struct
	for i := 0; i < itemType.NumField(); i++ {
		field := itemType.Field(i)
		fieldValue := itemValue.Field(i)

		// Check for anonymous fields (composition)
		if field.Anonymous {
			// Check if the embedded type matches Branch
			if field.Type == reflect.TypeOf(Branch{}) {
				embeddedID := fieldValue.FieldByName("ID")
				if embeddedID.IsValid() && embeddedID.CanUint() {
					return uint(embeddedID.Uint()), nil
				}
			}

			// Check if the embedded type has an ID field of type uint
			embeddedType := field.Type
			embeddedValue := fieldValue
			if embeddedType.Kind() == reflect.Ptr {
				embeddedType = embeddedType.Elem()
				embeddedValue = embeddedValue.Elem()
			}
			if embeddedType.Kind() == reflect.Struct {
				for j := 0; j < embeddedType.NumField(); j++ {
					embeddedField := embeddedType.Field(j)
					embeddedFieldValue := embeddedValue.Field(j)
					if embeddedField.Name == "ID" && embeddedField.Type == reflect.TypeOf(uint(0)) {
						if embeddedFieldValue.IsValid() && embeddedFieldValue.CanUint() {
							return uint(embeddedFieldValue.Uint()), nil
						}
					}
				}
			}
		}

		// Check for a field named "ID" with type uint
		if field.Name == "ID" && field.Type == reflect.TypeOf(uint(0)) {
			if fieldValue.IsValid() && fieldValue.CanUint() {
				return uint(fieldValue.Uint()), nil
			}
		}
	}

	return 0, errors.New("ID field not found")
}
