package closuretree

import (
	"math"
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
				t.Errorf("hasNode(%v) = %v; want %v", tt.input, result, tt.expected)
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
				t.Errorf("getNodeData(%v) unexpected error state: %v", tt.input, err)
			}
			if tenant != tt.expected {
				t.Errorf("getNodeData(%v) tenant = %v; want %v", tt.input, tenant, tt.expected)
			}
			if id != tt.expectId {
				t.Errorf("getNodeData(%v) id = %v; want %v", tt.input, id, tt.expectId)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		want   int64
		wantOK bool
	}{
		{"nil", nil, 0, true},
		{"int64", int64(42), 42, true},
		{"int", int(99), 99, true},
		{"int32", int32(7), 7, true},
		{"uint32", uint32(123), 123, true},
		{"uint", uint(55), 55, true},
		{"uint64", uint64(100), 100, true},
		{"uint64 overflow", uint64(math.MaxInt64 + 1), 0, false},
		{"uint overflow", uint(math.MaxUint), 0, false},
		{"float64", float64(3), 3, true},
		{"[]byte", []byte("456"), 456, true},
		{"[]byte invalid", []byte("abc"), 0, false},
		{"unsupported type", "string", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toInt64(tt.input)
			if ok != tt.wantOK {
				t.Errorf("toInt64(%v) ok = %v; want %v", tt.input, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("toInt64(%v) = %v; want %v", tt.input, got, tt.want)
			}
		})
	}
}
