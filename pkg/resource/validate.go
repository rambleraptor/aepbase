package resource

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
)

// standardFields are managed by the system and should not be validated as user input.
var validationStandardFields = map[string]bool{
	"id": true, "path": true, "create_time": true, "update_time": true,
}

// validateRequired checks that all required fields in the schema are present in fields.
func validateRequired(schema *openapi.Schema, fields map[string]any) error {
	var missing []string
	for _, name := range schema.Required {
		if validationStandardFields[name] {
			continue
		}
		if _, ok := fields[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

// validateTypes checks that each field value matches the type declared in the schema.
func validateTypes(schema *openapi.Schema, fields map[string]any) error {
	for name, val := range fields {
		if validationStandardFields[name] {
			continue
		}
		prop, ok := schema.Properties[name]
		if !ok {
			continue // unknown fields are ignored (not in schema)
		}
		if val == nil {
			continue // null is allowed for any type
		}
		if err := checkType(name, val, prop.Type); err != nil {
			return err
		}
	}
	return nil
}

// validateRequiredWithFiles is like validateRequired but treats file fields
// as "present" if they were uploaded in the multipart request.
func validateRequiredWithFiles(schema *openapi.Schema, fields map[string]any, fileFields map[string]bool, uploaded map[string]bool) error {
	var missing []string
	for _, name := range schema.Required {
		if validationStandardFields[name] {
			continue
		}
		if fileFields[name] {
			if !uploaded[name] {
				missing = append(missing, name)
			}
			continue
		}
		if _, ok := fields[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

// validateTypesSkipping runs validateTypes but skips any properties listed in
// skip — used to avoid type-checking file-field sentinel values.
func validateTypesSkipping(schema *openapi.Schema, fields map[string]any, skip map[string]bool) error {
	for name, val := range fields {
		if validationStandardFields[name] || skip[name] {
			continue
		}
		prop, ok := schema.Properties[name]
		if !ok {
			continue
		}
		if val == nil {
			continue
		}
		if err := checkType(name, val, prop.Type); err != nil {
			return err
		}
	}
	return nil
}

// validateEnums checks that each field value is one of the allowed enum values
// declared for that field. Fields without an entry in enums are unconstrained.
// Values of nil are allowed (to permit clearing a field).
func validateEnums(enums map[string][]string, fields map[string]any) error {
	if len(enums) == 0 {
		return nil
	}
	for name, val := range fields {
		if val == nil {
			continue
		}
		allowed, ok := enums[name]
		if !ok {
			continue
		}
		if err := checkEnum(name, val, allowed); err != nil {
			return err
		}
	}
	return nil
}

// checkEnum validates that val is one of the allowed enum values, if any are defined.
// Enum values are strings, so val must be a string when enum is set.
func checkEnum(name string, val any, enum []string) error {
	if len(enum) == 0 {
		return nil
	}
	s, ok := val.(string)
	if !ok {
		return fmt.Errorf("field %q must be one of %s", name, strings.Join(enum, ", "))
	}
	for _, allowed := range enum {
		if s == allowed {
			return nil
		}
	}
	return fmt.Errorf("field %q must be one of %s", name, strings.Join(enum, ", "))
}

// checkType validates a single value against an expected OpenAPI type.
func checkType(name string, val any, expectedType string) error {
	switch expectedType {
	case "string":
		if _, ok := val.(string); !ok {
			return fmt.Errorf("field %q must be a string", name)
		}
	case "integer":
		// JSON numbers are float64; accept any numeric value that is an integer.
		switch v := val.(type) {
		case float64:
			if v != float64(int64(v)) {
				return fmt.Errorf("field %q must be an integer", name)
			}
		case json.Number:
			if _, err := v.Int64(); err != nil {
				return fmt.Errorf("field %q must be an integer", name)
			}
		default:
			return fmt.Errorf("field %q must be an integer", name)
		}
	case "number":
		switch val.(type) {
		case float64, json.Number:
			// ok
		default:
			return fmt.Errorf("field %q must be a number", name)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("field %q must be a boolean", name)
		}
	case "array":
		if _, ok := val.([]any); !ok {
			return fmt.Errorf("field %q must be an array", name)
		}
	case "object":
		if _, ok := val.(map[string]any); !ok {
			return fmt.Errorf("field %q must be an object", name)
		}
	}
	return nil
}

// stripReadOnlyFields removes any fields that are marked readOnly in the schema.
// Returns the list of stripped field names (for logging/debugging).
func stripReadOnlyFields(schema *openapi.Schema, fields map[string]any) []string {
	var stripped []string
	for name := range fields {
		if validationStandardFields[name] {
			continue
		}
		if prop, ok := schema.Properties[name]; ok {
			if prop.ReadOnly {
				delete(fields, name)
				stripped = append(stripped, name)
			}
		}
	}
	return stripped
}
