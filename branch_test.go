package closuretree

import (
	"testing"
)

type tag struct {
	Name string
	Branch
}

type nonEmbeddingStruct struct {
	Name string
}

func TestHasBranch(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected bool
	}{
		{
			name:     "Struct is Branch",
			input:    Branch{},
			expected: true,
		},
		{
			name:     "Struct is Branch pointer",
			input:    &Branch{},
			expected: true,
		},
		{
			name:     "Struct that embeds Branch",
			input:    tag{},
			expected: true,
		},
		{
			name:     "Pointer to struct that embeds Branch",
			input:    &tag{},
			expected: true,
		},
		{
			name:     "Struct that does not embed Branch",
			input:    nonEmbeddingStruct{Name: "test"},
			expected: false,
		},
		{
			name:     "Pointer to struct that does not embed Branch",
			input:    &nonEmbeddingStruct{Name: "test"},
			expected: false,
		},
		{
			name:     "Non-struct input (string)",
			input:    "not a struct",
			expected: false,
		},
		{
			name:     "Non-struct input (integer)",
			input:    123,
			expected: false,
		},
		{
			name:     "Nil input",
			input:    nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasBranch(tt.input)
			if result != tt.expected {
				t.Errorf("HasLeave(%v) = %v; want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetId(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint
		hasError bool
	}{
		{
			name:     "Struct is Branch",
			input:    Branch{BranchId: 1},
			expected: 1,
			hasError: false,
		},
		{
			name:     "Struct is Branch pointer",
			input:    &Branch{BranchId: 2},
			expected: 2,
			hasError: false,
		},
		{
			name:     "Struct that embeds Branch",
			input:    tag{Branch: Branch{BranchId: 5}},
			expected: 5,
			hasError: false,
		},
		{
			name:     "Pointer to struct that embeds Branch",
			input:    &tag{Branch: Branch{BranchId: 6}},
			expected: 6,
			hasError: false,
		},

		{
			name:     "Struct that does not embed Branch",
			input:    nonEmbeddingStruct{Name: "test"},
			expected: 0,
			hasError: true,
		},
		{
			name:     "Pointer to struct that does not embed Branch",
			input:    &nonEmbeddingStruct{Name: "test"},
			expected: 0,
			hasError: true,
		},

		{
			name:     "Non-struct input (string)",
			input:    "not a struct",
			expected: 0,
			hasError: true,
		},
		{
			name:     "Non-struct input (integer)",
			input:    123,
			expected: 0,
			hasError: true,
		},
		{
			name:     "Nil input",
			input:    nil,
			expected: 0,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getID(tt.input)
			if (err != nil) != tt.hasError {
				t.Errorf("HasLeave(%v) unexpected error state: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("HasLeave(%v) = %v; want %v", tt.input, result, tt.expected)
			}
		})
	}
}
