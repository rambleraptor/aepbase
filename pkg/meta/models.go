package meta

import (
	"github.com/aep-dev/aep-lib-go/pkg/openapi"
)

type ResourceDefinition struct {
	ID          string            `json:"id,omitempty"`
	Path        string            `json:"path,omitempty"`
	Singular    string            `json:"singular"`
	Plural      string            `json:"plural"`
	Description string            `json:"description,omitempty"`
	Examples    map[string]any    `json:"examples,omitempty"`
	Schema      openapi.Schema    `json:"schema"`
	Parents     []string          `json:"parents,omitempty"`
	Singleton   bool              `json:"singleton,omitempty"`
	CreateTime  string            `json:"create_time,omitempty"`
	UpdateTime  string            `json:"update_time,omitempty"`
}
