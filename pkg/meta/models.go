package meta

import (
	"encoding/json"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
)

type ResourceDefinition struct {
	ID             string              `json:"id,omitempty"`
	Path           string              `json:"path,omitempty"`
	Singular       string              `json:"singular"`
	Plural         string              `json:"plural"`
	Description    string              `json:"description,omitempty"`
	Examples       map[string]any      `json:"examples,omitempty"`
	Schema         openapi.Schema      `json:"schema"`
	// Enums declares the allowed string values for enum-constrained fields.
	// The map is keyed by field (property) name and holds the list of allowed
	// values. Only string fields may be constrained. Absent fields are
	// unconstrained.
	Enums          map[string][]string `json:"enums,omitempty"`
	// FileFields lists the names of properties that hold binary file contents
	// (marked with x-aepbase-file-field: true and type: binary in the schema).
	// File fields are an aepbase-specific, non-AEP extension and are only
	// honored when file-field support is enabled on the server.
	FileFields     []string            `json:"file_fields,omitempty"`
	Parents        []string            `json:"parents,omitempty"`
	Singleton      bool                `json:"singleton,omitempty"`
	UserSettableId bool                `json:"user_settable_create,omitempty"`
	CreateTime     string              `json:"create_time,omitempty"`
	UpdateTime     string              `json:"update_time,omitempty"`
}

// MarshalJSON re-injects x-aepbase-file-field: true on each property listed
// in FileFields. openapi.Schema drops unknown extensions on round-trip, so
// without this the marker is missing from every meta-handler response.
func (d ResourceDefinition) MarshalJSON() ([]byte, error) {
	// Marshal via an alias type to avoid recursing into this method.
	type alias ResourceDefinition
	base, err := json.Marshal(alias(d))
	if err != nil {
		return nil, err
	}
	if len(d.FileFields) == 0 {
		return base, nil
	}
	var doc map[string]any
	if err := json.Unmarshal(base, &doc); err != nil {
		return nil, err
	}
	schema, ok := doc["schema"].(map[string]any)
	if !ok {
		return base, nil
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return base, nil
	}
	for _, name := range d.FileFields {
		prop, ok := props[name].(map[string]any)
		if !ok {
			continue
		}
		prop["x-aepbase-file-field"] = true
	}
	return json.Marshal(doc)
}
