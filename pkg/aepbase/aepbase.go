package aepbase

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/aep-dev/aep-lib-go/pkg/api"
	"github.com/aep-dev/aep-lib-go/pkg/openapi"

	"github.com/rambleraptor/aepbase/pkg/db"
	"github.com/rambleraptor/aepbase/pkg/meta"
	"github.com/rambleraptor/aepbase/pkg/operation"
	"github.com/rambleraptor/aepbase/pkg/resource"
)

// CustomMethodConfig defines a custom method to register on a resource.
type CustomMethodConfig struct {
	// HTTP method: "POST" or "GET".
	Method string
	// Request body schema (required for POST methods).
	RequestSchema *openapi.Schema
	// Response body schema (required).
	ResponseSchema *openapi.Schema
	// Handler is the HTTP handler for the custom method.
	// The request will have path values extracted for all parent IDs and the resource ID.
	Handler http.HandlerFunc
	// Async, when true, makes this a long-running operation.
	// The handler runs in the background and callers receive an Operation
	// resource (HTTP 202) that can be polled for completion.
	Async bool
}

// customMethodRegistration stores a registered custom method with its handler.
type customMethodRegistration struct {
	resourceSingular string
	methodName       string
	config           CustomMethodConfig
}

type State struct {
	mu                    sync.RWMutex
	API                   *api.API
	mux                   *http.ServeMux
	DB                    *sql.DB
	ServerURL             string
	CORSAllowedOrigins    []string
	customMethods         []customMethodRegistration
	pendingCustomMethods  []customMethodRegistration
	resourceDescriptions  map[string]string              // singular -> description
	resourceExamples      map[string]map[string]any      // singular -> field -> example value
	singletonResources    map[string]bool                // singular -> true for singleton resources
	resourceEnums         map[string]map[string][]string // singular -> field name -> allowed enum values
}

// metaResourceSingular is the singular name for the built-in meta resource.
// It is registered in the API for OpenAPI generation but routes are handled
// separately by the meta package.
const metaResourceSingular = "aep-resource-definition"

// operationResourceSingular is the singular name for the built-in operation resource.
const operationResourceSingular = "operation"

func NewState(d *sql.DB, serverURL string) *State {
	s := &State{
		DB:        d,
		ServerURL: serverURL,
		API: &api.API{
			ServerURL: serverURL,
			Name:      "aepbase",
			Resources: make(map[string]*api.Resource),
			Contact: &api.Contact{
				Name: "aepbase",
				URL:  "https://github.com/rambleraptor/aepbase",
			},
		},
		singletonResources: make(map[string]bool),
		resourceEnums:      make(map[string]map[string][]string),
		resourceDescriptions: map[string]string{
			"aep-resource-definition": "A resource definition. Create these to dynamically add new API endpoints.",
		},
		resourceExamples: map[string]map[string]any{
			"aep-resource-definition": {
				"singular": "book",
				"plural":   "books",
				"description": "A book published by a publisher.",
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title":  map[string]any{"type": "string"},
						"author": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
	// Register the meta resource so it appears in the OpenAPI spec.
	metaResource := &api.Resource{
		Singular: "aep-resource-definition",
		Plural:   "aep-resource-definitions",
		Schema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"id":          {Type: "string", ReadOnly: true, Description: "The unique identifier for this resource definition."},
				"path":        {Type: "string", ReadOnly: true, Description: "The full resource path (e.g. aep-resource-definitions/book)."},
				"singular":    {Type: "string", Description: "The singular name of the resource (e.g. book)."},
				"plural":      {Type: "string", Description: "The plural name of the resource, used as the URL collection path (e.g. books)."},
				"description": {Type: "string", Description: "A human-readable description of the resource."},
				"examples":    {Type: "object", Description: "Example values for the resource's fields, keyed by field name."},
				"schema":      {Type: "object", Description: "The JSON Schema defining the resource's properties."},
				"parents":         {Type: "array", Items: &openapi.Schema{Type: "string"}, Description: "The singular names of parent resources for nested resources."},
				"user_settable_create": {Type: "boolean", Description: "Whether clients can set the resource ID on creation."},
				"create_time": {Type: "string", Format: "date-time", ReadOnly: true, Description: "The time this resource definition was created."},
				"update_time": {Type: "string", Format: "date-time", ReadOnly: true, Description: "The time this resource definition was last updated."},
			},
		},
		Children: []*api.Resource{},
		Methods: api.Methods{
			Get:    &api.GetMethod{},
			List:   &api.ListMethod{},
			Create: &api.CreateMethod{},
			Update: &api.UpdateMethod{},
			Delete: &api.DeleteMethod{},
		},
	}
	metaResource.API = s.API
	s.API.Resources[metaResourceSingular] = metaResource

	// Register the operation resource for OpenAPI generation.
	operationResource := &api.Resource{
		Singular: "operation",
		Plural:   "operations",
		Schema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"id":          {Type: "string", ReadOnly: true, Description: "The unique identifier for this operation."},
				"path":        {Type: "string", ReadOnly: true, Description: "The full resource path (e.g. operations/abc123)."},
				"done":        {Type: "boolean", ReadOnly: true, Description: "Whether the operation has completed."},
				"error":       {Type: "object", ReadOnly: true, Description: "Error details if the operation failed."},
				"response":    {Type: "object", ReadOnly: true, Description: "The result of the operation, available when done is true."},
				"create_time": {Type: "string", Format: "date-time", ReadOnly: true, Description: "The time the operation was created."},
			},
		},
		Children: []*api.Resource{},
		Methods: api.Methods{
			Get:  &api.GetMethod{},
			List: &api.ListMethod{},
		},
	}
	operationResource.API = s.API
	s.API.Resources[operationResourceSingular] = operationResource

	s.rebuildMux()
	return s
}

func (s *State) Handler() http.Handler {
	return &muxWrapper{state: s}
}

func (s *State) GetAPI() *api.API {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.API
}

func (s *State) AddResource(def meta.ResourceDefinition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Build the schema with standard AEP fields added (copy properties to avoid mutating the original).
	schema := def.Schema
	schema.Properties = make(openapi.Properties)
	for k, v := range def.Schema.Properties {
		schema.Properties[k] = v
	}
	// Singletons don't have user-provided or system-generated IDs.
	if !def.Singleton {
		schema.Properties["id"] = openapi.Schema{Type: "string", ReadOnly: true}
	}
	schema.Properties["path"] = openapi.Schema{Type: "string", ReadOnly: true}
	schema.Properties["create_time"] = openapi.Schema{Type: "string", Format: "date-time", ReadOnly: true}
	schema.Properties["update_time"] = openapi.Schema{Type: "string", Format: "date-time", ReadOnly: true}

	// Validate parents exist.
	for _, parentSingular := range def.Parents {
		if _, ok := s.API.Resources[parentSingular]; !ok {
			return fmt.Errorf("parent resource %q not found", parentSingular)
		}
	}

	var methods api.Methods
	if def.Singleton {
		// Singletons only support Get and Update.
		methods = api.Methods{
			Get:    &api.GetMethod{},
			Update: &api.UpdateMethod{},
		}
	} else {
		methods = api.Methods{
			Get:    &api.GetMethod{},
			List:   &api.ListMethod{SupportsFilter: true, SupportsSkip: true},
			Create: &api.CreateMethod{SupportsUserSettableCreate: def.UserSettableId},
			Update: &api.UpdateMethod{},
			Delete: &api.DeleteMethod{},
			Apply:  &api.ApplyMethod{},
		}
	}

	r := &api.Resource{
		Singular: def.Singular,
		Plural:   def.Plural,
		Parents:  def.Parents,
		Children: []*api.Resource{},
		Schema:   &schema,
		Methods:  methods,
	}
	r.API = s.API

	// Wire as child of parents.
	for _, parentSingular := range def.Parents {
		if p, ok := s.API.Resources[parentSingular]; ok {
			p.Children = append(p.Children, r)
		}
	}

	// Create the SQLite table using the user-defined schema (before standard fields were added).
	parentRefs := make([]db.ParentRef, len(def.Parents))
	for i, ps := range def.Parents {
		parentRefs[i] = db.ParentRef{Singular: ps}
	}
	columns := userColumnsFromSchema(&def.Schema)
	if err := db.CreateResourceTable(s.DB, def.Plural, parentRefs, columns); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	s.API.Resources[def.Singular] = r
	if def.Singleton {
		s.singletonResources[def.Singular] = true
	}
	if def.Description != "" {
		s.resourceDescriptions[def.Singular] = def.Description
	}
	if len(def.Examples) > 0 {
		s.resourceExamples[def.Singular] = def.Examples
	}
	if len(def.Enums) > 0 {
		s.resourceEnums[def.Singular] = def.Enums
	}

	// Apply any custom methods that were registered before this resource existed.
	var remaining []customMethodRegistration
	for _, reg := range s.pendingCustomMethods {
		if reg.resourceSingular == def.Singular {
			if err := s.addCustomMethodLocked(reg.resourceSingular, reg.methodName, reg.config); err != nil {
				return fmt.Errorf("applying pending custom method %q: %w", reg.methodName, err)
			}
		} else {
			remaining = append(remaining, reg)
		}
	}
	s.pendingCustomMethods = remaining

	s.rebuildMux()
	return nil
}

func (s *State) RemoveResource(singular string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.API.Resources[singular]
	if !ok {
		return fmt.Errorf("resource %q not found", singular)
	}

	// Check no children depend on this.
	if len(r.Children) > 0 {
		names := make([]string, len(r.Children))
		for i, c := range r.Children {
			names[i] = c.Singular
		}
		return fmt.Errorf("cannot delete resource %q: children depend on it: %v", singular, names)
	}

	// Remove from parent's children list.
	for _, parentSingular := range r.Parents {
		if p, ok := s.API.Resources[parentSingular]; ok {
			for i, c := range p.Children {
				if c.Singular == singular {
					p.Children = append(p.Children[:i], p.Children[i+1:]...)
					break
				}
			}
		}
	}

	if err := db.DropResourceTable(s.DB, r.Plural); err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}

	delete(s.API.Resources, singular)
	delete(s.singletonResources, singular)
	delete(s.resourceDescriptions, singular)
	delete(s.resourceExamples, singular)
	delete(s.resourceEnums, singular)
	s.rebuildMux()
	return nil
}

func (s *State) UpdateResourceSchema(def meta.ResourceDefinition, oldDef meta.ResourceDefinition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.API.Resources[def.Singular]
	if !ok {
		return fmt.Errorf("resource %q not found", def.Singular)
	}

	// Update description and examples.
	if def.Description != "" {
		s.resourceDescriptions[def.Singular] = def.Description
	}
	if len(def.Examples) > 0 {
		s.resourceExamples[def.Singular] = def.Examples
	}
	// Enums are replaced wholesale on update (mirrors meta patch semantics).
	if len(def.Enums) > 0 {
		s.resourceEnums[def.Singular] = def.Enums
	} else {
		delete(s.resourceEnums, def.Singular)
	}

	oldProps := oldDef.Schema.Properties
	newProps := def.Schema.Properties

	// Detect additions, removals, and renames/type changes.
	var added []db.ColumnDef
	var removed []string

	for name, prop := range newProps {
		if oldProp, exists := oldProps[name]; !exists {
			added = append(added, db.ColumnDef{
				Name:    name,
				SQLType: db.SchemaTypeToSQLite(prop.Type, prop.Format),
			})
		} else if prop.Type != oldProp.Type {
			return fmt.Errorf("cannot change type of property %q from %q to %q", name, oldProp.Type, prop.Type)
		}
	}
	for name := range oldProps {
		if _, exists := newProps[name]; !exists {
			removed = append(removed, name)
		}
	}

	// Apply additions.
	for _, col := range added {
		if err := db.AddColumn(s.DB, def.Plural, col); err != nil {
			return fmt.Errorf("adding column %q: %w", col.Name, err)
		}
	}

	// Apply removals via table recreate.
	if len(removed) > 0 {
		parentRefs := make([]db.ParentRef, len(def.Parents))
		for i, ps := range def.Parents {
			parentRefs[i] = db.ParentRef{Singular: ps}
		}
		keepCols := userColumnsFromSchema(&def.Schema)
		if err := db.RemoveColumns(s.DB, def.Plural, parentRefs, keepCols); err != nil {
			return fmt.Errorf("removing columns: %w", err)
		}
	}

	// Update the in-memory schema (copy to avoid mutating the original).
	schema := def.Schema
	schema.Properties = make(openapi.Properties)
	for k, v := range def.Schema.Properties {
		schema.Properties[k] = v
	}
	if !s.singletonResources[def.Singular] {
		schema.Properties["id"] = openapi.Schema{Type: "string", ReadOnly: true}
	}
	schema.Properties["path"] = openapi.Schema{Type: "string", ReadOnly: true}
	schema.Properties["create_time"] = openapi.Schema{Type: "string", Format: "date-time", ReadOnly: true}
	schema.Properties["update_time"] = openapi.Schema{Type: "string", Format: "date-time", ReadOnly: true}
	r.Schema = &schema

	s.rebuildMux()
	return nil
}

func (s *State) rebuildMux() {
	mux := http.NewServeMux()
	meta.RegisterRoutes(mux, s)
	operation.RegisterRoutes(mux, s.DB)
	mux.HandleFunc("GET /openapi.json", s.serveOpenAPI)
	for _, r := range s.API.Resources {
		// Skip built-in resources — their routes are registered separately above.
		if r.Singular == metaResourceSingular || r.Singular == operationResourceSingular {
			continue
		}
		if s.singletonResources[r.Singular] {
			// Singleton resources use different route patterns.
			singletonPath := s.buildSingletonRoutePath(r)
			resource.RegisterSingletonRoutes(mux, s.DB, r, singletonPath, s.resourceEnums[r.Singular])
			continue
		}
		// Collect custom method handlers for this resource.
		cmHandlers := make(map[string]resource.CustomMethodHandler)
		for _, reg := range s.customMethods {
			if reg.resourceSingular == r.Singular {
				cmHandlers[reg.methodName] = resource.CustomMethodHandler{
					Method:  reg.config.Method,
					Handler: reg.config.Handler,
				}
			}
		}
		resource.RegisterRoutes(mux, s.DB, r, cmHandlers, s.resourceEnums[r.Singular])
	}
	s.mux = mux
}

// buildSingletonRoutePath constructs the HTTP route pattern for a singleton resource.
// E.g., "/users/{user_id}/config" for a config singleton under users.
func (s *State) buildSingletonRoutePath(r *api.Resource) string {
	var parts []string
	if len(r.Parents) > 0 {
		parentRes := s.API.Resources[r.Parents[0]]
		if parentRes != nil {
			parentElems := parentRes.PatternElems()
			parts = append(parts, parentElems...)
		}
	}
	parts = append(parts, r.Singular)
	return "/" + strings.Join(parts, "/")
}

func (s *State) serveOpenAPI(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	a := s.API
	descriptions := make(map[string]string, len(s.resourceDescriptions))
	for k, v := range s.resourceDescriptions {
		descriptions[k] = v
	}
	examples := make(map[string]map[string]any, len(s.resourceExamples))
	for k, v := range s.resourceExamples {
		examples[k] = v
	}
	singletons := make(map[string]bool, len(s.singletonResources))
	for k, v := range s.singletonResources {
		singletons[k] = v
	}
	s.mu.RUnlock()

	jsonBytes, err := a.ConvertToOpenAPIBytes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonBytes, err = enrichOpenAPI(jsonBytes, a.Name, descriptions, examples)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonBytes, err = fixSingletonOpenAPI(jsonBytes, a, singletons)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)
}

// builtinResources is the set of resource singular names that are built-in
// and should be excluded from the exported OpenAPI spec.
var builtinResources = map[string]bool{
	metaResourceSingular:      true,
	operationResourceSingular: true,
}

// ExportUserResourcesOpenAPI generates the OpenAPI spec filtered to only
// user-defined resources, excluding built-in endpoints like
// /aep-resource-definitions and /operations.
func (s *State) ExportUserResourcesOpenAPI() ([]byte, error) {
	s.mu.RLock()
	// Build a filtered copy of the API with only user-defined resources.
	filtered := &api.API{
		ServerURL: s.API.ServerURL,
		Name:      s.API.Name,
		Contact:   s.API.Contact,
		Resources: make(map[string]*api.Resource),
	}
	descriptions := make(map[string]string)
	examples := make(map[string]map[string]any)
	for k, v := range s.API.Resources {
		if builtinResources[k] {
			continue
		}
		filtered.Resources[k] = v
	}
	for k, v := range s.resourceDescriptions {
		if builtinResources[k] {
			continue
		}
		descriptions[k] = v
	}
	for k, v := range s.resourceExamples {
		if builtinResources[k] {
			continue
		}
		examples[k] = v
	}
	s.mu.RUnlock()

	singletons := make(map[string]bool, len(s.singletonResources))
	for k, v := range s.singletonResources {
		singletons[k] = v
	}

	jsonBytes, err := filtered.ConvertToOpenAPIBytes()
	if err != nil {
		return nil, err
	}
	jsonBytes, err = enrichOpenAPI(jsonBytes, filtered.Name, descriptions, examples)
	if err != nil {
		return nil, err
	}
	return fixSingletonOpenAPI(jsonBytes, filtered, singletons)
}

// errorResponseSchema returns a reusable error response schema.
func errorResponseSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"error": map[string]any{
				"type":        "object",
				"description": "Error details.",
				"properties": map[string]any{
					"code":    map[string]any{"type": "integer", "description": "The HTTP status code.", "example": 400},
					"message": map[string]any{"type": "string", "description": "A human-readable error message.", "example": "something went wrong"},
				},
				"example": map[string]any{
					"code":    400,
					"message": "something went wrong",
				},
			},
		},
	}
}

// enrichOpenAPI post-processes the generated OpenAPI spec to add:
// 1. x-aep-resource type fields
// 2. Component schema descriptions
// 3. Operation tags
// 4. Richer operation descriptions
// 5. 4xx error responses
func enrichOpenAPI(data []byte, apiName string, descriptions map[string]string, examples map[string]map[string]any) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	// Enrich info section.
	if info, ok := doc["info"].(map[string]any); ok {
		info["description"] = "A dynamic API backend following the AEP standard. Define resources at runtime and get automatic CRUD endpoints, OpenAPI specs, and more."
		info["version"] = "0.1.0"
		info["license"] = map[string]any{
			"name": "MIT",
			"url":  "https://opensource.org/licenses/MIT",
		}
	}

	components, _ := doc["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)

	// Build singular->plural lookup from x-aep-resource annotations.
	singularToPlural := make(map[string]string)
	for name, s := range schemas {
		schema, ok := s.(map[string]any)
		if !ok {
			continue
		}
		xaep, ok := schema["x-aep-resource"].(map[string]any)
		if !ok {
			continue
		}
		// 1. Inject type if missing.
		if _, hasType := xaep["type"]; !hasType {
			xaep["type"] = apiName + "/" + name
		}
		if plural, ok := xaep["plural"].(string); ok {
			singularToPlural[name] = plural
		}

		// 2. Component descriptions.
		if desc, ok := descriptions[name]; ok && desc != "" {
			schema["description"] = desc
		}
	}

	// Standard field metadata (descriptions + examples).
	standardFields := map[string]struct {
		description string
		example     any
	}{
		"id":          {"The unique identifier for this resource.", "my-aep-resource-definition"},
		"path":        {"The full resource path (e.g. publishers/acme/books/1984).", "aep-resource-definitions/my-aep-resource-definition"},
		"create_time": {"The time this resource was created.", "2024-01-01T00:00:00Z"},
		"update_time": {"The time this resource was last updated.", "2024-06-15T12:00:00Z"},
	}

	// Add standard field descriptions and examples to all component schemas.
	for _, s := range schemas {
		schema, ok := s.(map[string]any)
		if !ok {
			continue
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			continue
		}
		for fieldName, meta := range standardFields {
			if prop, ok := props[fieldName].(map[string]any); ok {
				if _, hasDesc := prop["description"]; !hasDesc {
					prop["description"] = meta.description
				}
				if _, hasEx := prop["example"]; !hasEx {
					prop["example"] = meta.example
				}
			}
		}
	}

	// Add user-provided examples to schema properties and build resource-level examples.
	for name, s := range schemas {
		schema, ok := s.(map[string]any)
		if !ok {
			continue
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			continue
		}

		// Inject user-provided examples.
		resExamples := examples[name]
		for fieldName, example := range resExamples {
			if prop, ok := props[fieldName].(map[string]any); ok {
				prop["example"] = example
			}
		}

		// Build a resource-level example from all property examples.
		resourceExample := make(map[string]any)
		for fieldName, prop := range props {
			propMap, ok := prop.(map[string]any)
			if !ok {
				continue
			}
			if ex, ok := propMap["example"]; ok {
				resourceExample[fieldName] = ex
			}
		}
		if len(resourceExample) > 0 {
			schema["example"] = resourceExample
		}
	}

	// Build global tags list from resources (with descriptions).
	var tags []any
	for name := range singularToPlural {
		tag := map[string]any{"name": name}
		if desc, ok := descriptions[name]; ok && desc != "" {
			tag["description"] = desc
		}
		tags = append(tags, tag)
	}
	if len(tags) > 0 {
		doc["tags"] = tags
	}

	// Build resource-level examples keyed by singular name for use in responses.
	schemaExamples := make(map[string]map[string]any)
	for name, s := range schemas {
		schema, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if ex, ok := schema["example"].(map[string]any); ok {
			schemaExamples[name] = ex
		}
	}

	// Well-known parameter descriptions and examples.
	type paramMeta struct {
		description string
		example     any
	}
	paramInfo := map[string]paramMeta{
		"max_page_size": {"The maximum number of results to return per page.", 25},
		"page_token":    {"A token from a previous list response to fetch the next page.", "eyJsYXN0X2lkIjoiMTIzIn0="},
		"skip":          {"The number of results to skip before returning.", 0},
		"filter":        {"A filter expression to narrow results (e.g. \"author=Orwell\").", "author=Orwell"},
		"id":            {"The ID to use for the new resource definition. If omitted, one is generated automatically.", "my-aep-resource-definition"},
		"force":         {"If true, delete even if child resources exist.", false},
	}

	// Process paths: add tags, descriptions, parameters, request/response enrichments, and 4xx responses.
	paths, _ := doc["paths"].(map[string]any)
	for _, pathValue := range paths {
		pathItem, ok := pathValue.(map[string]any)
		if !ok {
			continue
		}
		for _, opValue := range pathItem {
			op, ok := opValue.(map[string]any)
			if !ok {
				continue
			}
			opID, _ := op["operationId"].(string)
			if opID == "" {
				continue
			}

			// Derive resource name and action from operationId (e.g. "ListPublisher" -> "List", "Publisher").
			action, singular := parseOperationID(opID)
			plural := singularToPlural[singular]
			if plural == "" {
				plural = singular + "s"
			}

			// Tags — group operations by resource.
			op["tags"] = []any{singular}

			// Operation descriptions.
			op["description"] = operationDescription(action, singular, plural)

			// Parameter descriptions and examples.
			if params, ok := op["parameters"].([]any); ok {
				for _, p := range params {
					param, ok := p.(map[string]any)
					if !ok {
						continue
					}
					name, _ := param["name"].(string)
					if meta, ok := paramInfo[name]; ok {
						param["description"] = meta.description
						param["example"] = meta.example
					}
					// Path parameters (e.g. publisher_id, book_id) — add description if missing.
					in, _ := param["in"].(string)
					if in == "path" {
						if _, hasDesc := param["description"]; !hasDesc {
							// Strip _id suffix to get resource name.
							resName := strings.TrimSuffix(name, "_id")
							param["description"] = fmt.Sprintf("The ID of the %s.", resName)
							param["example"] = fmt.Sprintf("my-%s", resName)
						}
					}
				}
			}

			// RequestBody descriptions.
			if rb, ok := op["requestBody"].(map[string]any); ok {
				rb["description"] = requestBodyDescription(action, singular)
			}

			// Enrich responses: examples for success responses, 4xx error responses with examples.
			resExample := schemaExamples[singular]
			responses, _ := op["responses"].(map[string]any)
			if responses == nil {
				continue
			}

			// Success response examples.
			if resExample != nil {
				switch action {
				case "Get", "Create", "Update", "Apply":
					addResponseExample(responses, "200", resExample)
				case "List":
					listExample := map[string]any{
						"results":         []any{resExample},
						"next_page_token": "",
					}
					addResponseExample(responses, "200", listExample)
				}
			}

			// Enrich list response schema: add descriptions to next_page_token and results.
			if action == "List" {
				if resp200, ok := responses["200"].(map[string]any); ok {
					if content, ok := resp200["content"].(map[string]any); ok {
						if jsonContent, ok := content["application/json"].(map[string]any); ok {
							if schema, ok := jsonContent["schema"].(map[string]any); ok {
								if props, ok := schema["properties"].(map[string]any); ok {
									if npt, ok := props["next_page_token"].(map[string]any); ok {
										npt["description"] = "A token to retrieve the next page of results. Empty if there are no more results."
										npt["example"] = ""
									}
									if results, ok := props["results"].(map[string]any); ok {
										results["description"] = fmt.Sprintf("The list of %s.", plural)
										if resExample != nil {
											results["example"] = []any{resExample}
										}
									}
								}
							}
						}
					}
				}
			}

			// Fix 204 responses: set type to object with empty object example.
			if resp204, ok := responses["204"].(map[string]any); ok {
				resp204["content"] = map[string]any{
					"application/json": map[string]any{
						"schema":  map[string]any{"type": "object"},
						"example": map[string]any{},
					},
				}
			}

			// Error response helpers.
			errResp := func(code int, desc, msg string) map[string]any {
				return map[string]any{
					"description": desc,
					"content": map[string]any{
						"application/json": map[string]any{
							"schema": errorResponseSchema(),
							"example": map[string]any{
								"error": map[string]any{
									"code":    code,
									"message": msg,
								},
							},
						},
					},
				}
			}

			notFoundMsg := fmt.Sprintf("%s not found.", singular)
			notFound := errResp(404, fmt.Sprintf("The %s was not found.", singular), notFoundMsg)
			badRequest := errResp(400, "The request body is invalid.", "invalid request body")
			conflict := errResp(409,
				fmt.Sprintf("A %s with the given ID already exists.", singular),
				fmt.Sprintf("definition with id %q already exists", "my-aep-resource-definition"),
			)

			switch action {
			case "Get":
				responses["404"] = notFound
			case "Create":
				responses["400"] = badRequest
				responses["409"] = conflict
			case "Update":
				responses["400"] = badRequest
				responses["404"] = notFound
			case "Apply":
				responses["400"] = badRequest
			case "Delete":
				responses["404"] = notFound
			case "List":
				responses["400"] = errResp(400, "The request parameters are invalid.", "invalid filter expression")
			}
		}
	}

	return json.MarshalIndent(doc, "", "  ")
}

// parseOperationID splits an operationId like "ListPublisher" into ("List", "publisher").
func parseOperationID(opID string) (action, singular string) {
	for _, prefix := range []string{"List", "Create", "Get", "Update", "Delete", "Apply"} {
		if len(opID) > len(prefix) && opID[:len(prefix)] == prefix {
			name := opID[len(prefix):]
			// Convert PascalCase resource name to lowercase.
			singular = strings.ToLower(name[:1]) + name[1:]
			return prefix, singular
		}
	}
	return "", strings.ToLower(opID)
}

// requestBodyDescription returns a description for a request body.
func requestBodyDescription(action, singular string) string {
	switch action {
	case "Create":
		return fmt.Sprintf("The %s to create.", singular)
	case "Update":
		return fmt.Sprintf("The fields to update on the %s.", singular)
	case "Apply":
		return fmt.Sprintf("The full %s to create or replace.", singular)
	default:
		return ""
	}
}

// addResponseExample injects an example into a response's application/json content.
func addResponseExample(responses map[string]any, code string, example any) {
	resp, ok := responses[code].(map[string]any)
	if !ok {
		return
	}
	content, ok := resp["content"].(map[string]any)
	if !ok {
		return
	}
	jsonContent, ok := content["application/json"].(map[string]any)
	if !ok {
		return
	}
	jsonContent["example"] = example
}

// operationDescription returns a human-readable description for a CRUD operation.
func operationDescription(action, singular, plural string) string {
	switch action {
	case "List":
		return fmt.Sprintf("Returns a paginated list of %s.", plural)
	case "Create":
		return fmt.Sprintf("Creates a new %s.", singular)
	case "Get":
		return fmt.Sprintf("Retrieves a single %s by ID.", singular)
	case "Update":
		return fmt.Sprintf("Updates an existing %s. Only the fields provided in the request body are modified.", singular)
	case "Delete":
		return fmt.Sprintf("Deletes a %s by ID.", singular)
	case "Apply":
		return fmt.Sprintf("Creates or replaces a %s.", singular)
	default:
		return ""
	}
}

// standardFields are managed by the DB layer as fixed columns.
var standardFields = map[string]bool{
	"id": true, "path": true, "create_time": true, "update_time": true,
}

// userColumnsFromSchema returns column defs for user-defined properties only.
func userColumnsFromSchema(schema *openapi.Schema) []db.ColumnDef {
	var cols []db.ColumnDef
	for name, prop := range schema.Properties {
		if standardFields[name] {
			continue
		}
		cols = append(cols, db.ColumnDef{
			Name:    name,
			SQLType: db.SchemaTypeToSQLite(prop.Type, prop.Format),
		})
	}
	return cols
}

type muxWrapper struct {
	state *State
}

func (w *muxWrapper) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	w.state.mu.RLock()
	mux := w.state.mux
	allowedOrigins := w.state.CORSAllowedOrigins
	w.state.mu.RUnlock()

	if handleCORS(rw, req, allowedOrigins) {
		return
	}

	mux.ServeHTTP(rw, req)
}

// handleCORS sets CORS headers if the request origin matches an allowed origin.
// Returns true if the request was a preflight OPTIONS request and has been handled.
func handleCORS(rw http.ResponseWriter, req *http.Request, allowedOrigins []string) bool {
	if origin := req.Header.Get("Origin"); origin != "" && len(allowedOrigins) > 0 {
		for _, allowed := range allowedOrigins {
			if allowed == "*" || allowed == origin {
				rw.Header().Set("Access-Control-Allow-Origin", origin)
				rw.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
				rw.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
				break
			}
		}
	}

	if req.Method == http.MethodOptions {
		rw.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

// AddCustomMethod registers a custom method on a resource.
// The method will be available at /{resource_path}:{methodName}.
// If the resource does not exist yet, the registration is deferred
// and will be applied automatically when the resource is added.
func (s *State) AddCustomMethod(resourceSingular, methodName string, config CustomMethodConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addCustomMethodLocked(resourceSingular, methodName, config)
}

func (s *State) addCustomMethodLocked(resourceSingular, methodName string, config CustomMethodConfig) error {
	if config.Method != "POST" && config.Method != "GET" {
		return fmt.Errorf("custom method must use POST or GET, got %q", config.Method)
	}
	if config.ResponseSchema == nil {
		return fmt.Errorf("ResponseSchema is required")
	}
	if config.Method == "POST" && config.RequestSchema == nil {
		return fmt.Errorf("RequestSchema is required for POST custom methods")
	}
	if config.Handler == nil {
		return fmt.Errorf("Handler is required")
	}

	// Wrap async handlers so callers get an Operation back.
	if config.Async {
		config.Handler = s.wrapAsyncHandler(config.Handler)
	}

	reg := customMethodRegistration{
		resourceSingular: resourceSingular,
		methodName:       methodName,
		config:           config,
	}

	r, ok := s.API.Resources[resourceSingular]
	if !ok {
		// Resource doesn't exist yet — defer until AddResource is called.
		s.pendingCustomMethods = append(s.pendingCustomMethods, reg)
		return nil
	}

	// For async methods, the OpenAPI response is the Operation schema wrapping
	// the original response, and the status code is 202.
	responseSchema := config.ResponseSchema
	if config.Async {
		responseSchema = asyncOperationSchema(config.ResponseSchema)
	}

	// Add to aep-lib-go's resource for OpenAPI generation.
	cm := &api.CustomMethod{
		Name:     methodName,
		Method:   config.Method,
		Request:  config.RequestSchema,
		Response: responseSchema,
	}
	r.CustomMethods = append(r.CustomMethods, cm)

	// Store the registration for route building.
	s.customMethods = append(s.customMethods, reg)

	s.rebuildMux()
	return nil
}

// asyncOperationSchema returns an Operation-shaped schema where the response
// property uses the provided inner schema.
func asyncOperationSchema(innerResponse *openapi.Schema) *openapi.Schema {
	return &openapi.Schema{
		Type: "object",
		Properties: openapi.Properties{
			"id":          {Type: "string", ReadOnly: true, Description: "The unique identifier for this operation."},
			"path":        {Type: "string", ReadOnly: true, Description: "The full resource path (e.g. operations/abc123)."},
			"done":        {Type: "boolean", ReadOnly: true, Description: "Whether the operation has completed."},
			"error":       {Type: "object", ReadOnly: true, Description: "Error details if the operation failed."},
			"response":    *innerResponse,
			"create_time": {Type: "string", Format: "date-time", ReadOnly: true, Description: "The time the operation was created."},
		},
	}
}

func (s *State) GetDB() *sql.DB {
	return s.DB
}

// wrapAsyncHandler wraps a custom method handler to run asynchronously.
// It creates an Operation, returns 202 immediately, and runs the handler
// in a background goroutine. When the handler completes, the operation is
// updated with the response or error.
func (s *State) wrapAsyncHandler(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		opID := operation.GenerateID()
		now := time.Now().UTC().Format(time.RFC3339)
		op := &operation.Operation{
			ID:         opID,
			Path:       "operations/" + opID,
			Done:       false,
			CreateTime: now,
		}
		if err := operation.Insert(s.DB, op); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":{"code":500,"message":"failed to create operation: %v"}}`, err), http.StatusInternalServerError)
			return
		}

		// Buffer the request body so the background goroutine can read it.
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":{"code":400,"message":"failed to read request body"}}`, http.StatusBadRequest)
			return
		}
		r.Body.Close()

		go func() {
			// Build a new request with the buffered body for the background handler.
			bgReq, _ := http.NewRequest(r.Method, r.URL.String(), bytes.NewReader(bodyBytes))
			bgReq.Header = r.Header
			// Copy path values by re-using the original request's pattern result.
			bgReq.SetPathValue("", "") // initialize the path values map
			for _, paramName := range extractPathValueNames(r) {
				bgReq.SetPathValue(paramName, r.PathValue(paramName))
			}

			recorder := httptest.NewRecorder()
			handler(recorder, bgReq)

			result := recorder.Result()
			var respBody any
			if result.Body != nil {
				defer result.Body.Close()
				respBytes, _ := io.ReadAll(result.Body)
				json.Unmarshal(respBytes, &respBody)
			}

			if result.StatusCode >= 400 {
				// Handler returned an error — store it as the operation error.
				errMap, ok := respBody.(map[string]any)
				if !ok {
					errMap = map[string]any{
						"code":    result.StatusCode,
						"message": "operation failed",
					}
				}
				// If the response has an "error" wrapper, unwrap it.
				if inner, ok := errMap["error"].(map[string]any); ok {
					errMap = inner
				}
				operation.MarkDone(s.DB, opID, nil, errMap)
			} else {
				operation.MarkDone(s.DB, opID, respBody, nil)
			}
		}()

		// Return 202 with the operation.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(op.ToMap())
	}
}

// extractPathValueNames returns the path value parameter names set on a request.
// Go's net/http doesn't expose path value names directly, so we track them
// based on the resource's pattern elements.
func extractPathValueNames(r *http.Request) []string {
	// Try common parameter names from the URL pattern.
	// The path value map is not exported, so we probe known suffixes.
	var names []string
	// Walk through the URL pattern to find {param} names.
	pattern := r.Pattern
	for {
		start := strings.Index(pattern, "{")
		if start < 0 {
			break
		}
		end := strings.Index(pattern[start:], "}")
		if end < 0 {
			break
		}
		name := pattern[start+1 : start+end]
		// Strip trailing ... for catch-all patterns.
		name = strings.TrimSuffix(name, "...")
		names = append(names, name)
		pattern = pattern[start+end+1:]
	}
	return names
}

// fixSingletonOpenAPI transforms the OpenAPI spec generated by aep-lib-go to
// use correct singleton paths. aep-lib-go generates collection-style paths
// (e.g., /users/{user_id}/configs/{config_id}) which need to be replaced with
// singleton paths (e.g., /users/{user_id}/config).
func fixSingletonOpenAPI(data []byte, a *api.API, singletons map[string]bool) ([]byte, error) {
	if len(singletons) == 0 {
		return data, nil
	}

	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	paths, _ := doc["paths"].(map[string]any)
	if paths == nil {
		return data, nil
	}
	components, _ := doc["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)

	for singular := range singletons {
		r, ok := a.Resources[singular]
		if !ok {
			continue
		}

		// Add singleton: true to x-aep-resource annotation.
		if schemas != nil {
			// aep-lib-go may use the singular name as-is or capitalized as the schema key.
			schemaName := singular
			if _, ok := schemas[schemaName]; !ok {
				schemaName = capitalize(singular)
			}
			if schema, ok := schemas[schemaName].(map[string]any); ok {
				if xaep, ok := schema["x-aep-resource"].(map[string]any); ok {
					xaep["singleton"] = true
				}
				// Remove "id" from required and properties since singletons have no ID.
				if props, ok := schema["properties"].(map[string]any); ok {
					delete(props, "id")
				}
				if required, ok := schema["required"].([]any); ok {
					var filtered []any
					for _, r := range required {
						if r != "id" {
							filtered = append(filtered, r)
						}
					}
					schema["required"] = filtered
				}
			}
		}

		// Build the old paths that aep-lib-go generated (collection + resource).
		elems := r.PatternElems()
		oldCollectionPath := "/" + strings.Join(elems[:len(elems)-1], "/")
		oldResourcePath := "/" + strings.Join(elems, "/")

		// Build the correct singleton path.
		var singletonPathParts []string
		if len(r.Parents) > 0 {
			parentRes := a.Resources[r.Parents[0]]
			if parentRes != nil {
				singletonPathParts = append(singletonPathParts, parentRes.PatternElems()...)
			}
		}
		singletonPathParts = append(singletonPathParts, r.Singular)
		singletonPath := "/" + strings.Join(singletonPathParts, "/")

		// Extract GET and PATCH operations from the old paths and move them to the singleton path.
		singletonOps := make(map[string]any)

		if resourceOps, ok := paths[oldResourcePath].(map[string]any); ok {
			if getOp, ok := resourceOps["get"]; ok {
				op := getOp.(map[string]any)
				// Fix operation: remove the resource ID parameter.
				fixSingletonOperation(op, r)
				singletonOps["get"] = op
			}
			if patchOp, ok := resourceOps["patch"]; ok {
				op := patchOp.(map[string]any)
				fixSingletonOperation(op, r)
				singletonOps["patch"] = op
			}
		}

		// Remove the old paths.
		delete(paths, oldCollectionPath)
		delete(paths, oldResourcePath)

		// Add the new singleton path.
		if len(singletonOps) > 0 {
			paths[singletonPath] = singletonOps
		}
	}

	return json.MarshalIndent(doc, "", "  ")
}

// fixSingletonOperation removes the resource ID parameter from an operation
// since singletons don't have their own ID in the path.
func fixSingletonOperation(op map[string]any, r *api.Resource) {
	params, ok := op["parameters"].([]any)
	if !ok {
		return
	}
	idParamName := strings.ReplaceAll(r.Singular, "-", "_") + "_id"
	var filtered []any
	for _, p := range params {
		param, ok := p.(map[string]any)
		if !ok {
			filtered = append(filtered, p)
			continue
		}
		if name, ok := param["name"].(string); ok && name == idParamName {
			continue
		}
		filtered = append(filtered, p)
	}
	op["parameters"] = filtered
}

// capitalize returns s with the first letter uppercased.
func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
