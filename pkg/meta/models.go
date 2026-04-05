package meta

import (
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
	Parents        []string            `json:"parents,omitempty"`
	Singleton      bool                `json:"singleton,omitempty"`
	UserSettableId bool                `json:"user_settable_create,omitempty"`
	CreateTime     string              `json:"create_time,omitempty"`
	UpdateTime     string              `json:"update_time,omitempty"`
}
