package cel2sql

import (
	"testing"

	"github.com/google/cel-go/cel"
)

func TestConvert(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
		cel.Variable("location", cel.StringType),
		cel.Variable("age", cel.IntType),
		cel.Variable("active", cel.BoolType),
	)
	if err != nil {
		t.Fatalf("failed to create CEL environment: %v", err)
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "equality",
			input:    "name == 'Alice'",
			expected: "name = 'Alice'",
		},
		{
			name:     "not equal",
			input:    "location != 'NYC'",
			expected: "location <> 'NYC'",
		},
		{
			name:     "integer comparison",
			input:    "age > 21",
			expected: "age > 21",
		},
		{
			name:     "and",
			input:    "name == 'Alice' && location == 'NYC'",
			expected: "name = 'Alice' AND location = 'NYC'",
		},
		{
			name:     "or",
			input:    "location == 'NYC' || location == 'LA'",
			expected: "location = 'NYC' OR location = 'LA'",
		},
		{
			name:     "startsWith",
			input:    "name.startsWith('A')",
			expected: "name LIKE CONCAT('A', '%')",
		},
		{
			name:     "contains",
			input:    "name.contains('li')",
			expected: "name LIKE CONCAT('%', 'li', '%')",
		},
		{
			name:     "negation",
			input:    "!active",
			expected: "NOT (active)",
		},
		{
			name:     "matches",
			input:    "name.matches('^A.*')",
			expected: "name REGEXP '^A.*'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Convert(env, tt.input)
			if err != nil {
				t.Fatalf("Convert(%q) error: %v", tt.input, err)
			}
			if got != tt.expected {
				t.Errorf("Convert(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestConvertInvalidFilter(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Variable("name", cel.StringType),
	)
	if err != nil {
		t.Fatalf("failed to create CEL environment: %v", err)
	}

	// Reference an undeclared variable.
	_, err = Convert(env, "unknown_field == 'x'")
	if err == nil {
		t.Error("expected error for undeclared variable, got nil")
	}
}
