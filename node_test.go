package closuretree

import (
	"testing"
)

type tag struct {
	Name string
	Node
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
			name:     "Struct is Node",
			input:    Node{},
			expected: true,
		},
		{
			name:     "Struct is Node pointer",
			input:    &Node{},
			expected: true,
		},
		{
			name:     "Struct that embeds Node",
			input:    tag{},
			expected: true,
		},
		{
			name:     "Pointer to struct that embeds Node",
			input:    &tag{},
			expected: true,
		},
		{
			name:     "Struct that does not embed Node",
			input:    nonEmbeddingStruct{Name: "test"},
			expected: false,
		},
		{
			name:     "Pointer to struct that does not embed Node",
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
			result := hasNode(tt.input)
			if result != tt.expected {
				t.Errorf("HasLeave(%v) = %v; want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetNodeData(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
		expectId uint
		hasError bool
	}{
		{
			name:     "Struct is Node",
			input:    Node{NodeId: 1, Tenant: "t1"},
			expectId: 1,
			expected: "t1",
		},
		{
			name:     "Struct is Node pointer",
			input:    &Node{NodeId: 2, Tenant: "t2"},
			expectId: 2,
			expected: "t2",
		},
		{
			name:     "Struct that embeds Node",
			input:    tag{Node: Node{NodeId: 5, Tenant: "t3"}},
			expectId: 5,
			expected: "t3",
		},
		{
			name:     "Pointer to struct that embeds Node",
			input:    &tag{Node: Node{NodeId: 6, Tenant: "t4"}},
			expectId: 6,
			expected: "t4",
		},

		{
			name:     "Struct that does not embed Node",
			input:    nonEmbeddingStruct{Name: "test"},
			hasError: true,
		},
		{
			name:     "Pointer to struct that does not embed Node",
			input:    &nonEmbeddingStruct{Name: "test"},
			hasError: true,
		},

		{
			name:     "Non-struct input (string)",
			input:    "not a struct",
			hasError: true,
		},
		{
			name:     "Non-struct input (integer)",
			input:    123,
			hasError: true,
		},
		{
			name:     "Nil input",
			input:    nil,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, tenant, err := getNodeData(tt.input)
			if (err != nil) != tt.hasError {
				t.Errorf("HasLeave(%v) unexpected error state: %v", tt.input, err)
			}
			if tenant != tt.expected {
				t.Errorf("HasLeave(%v) = %v; want %v", tt.input, tenant, tt.expected)
			}
			if id != tt.expectId {
				t.Errorf("HasLeave(%v) = %v; want %v", tt.input, tenant, tt.expected)
			}
		})
	}
}
