package aepbase

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/aep-dev/aep-lib-go/pkg/api"
	"github.com/aep-dev/aep-lib-go/pkg/openapi"

	"github.com/aep-dev/aepbase/pkg/db"
	"github.com/aep-dev/aepbase/pkg/meta"
	"github.com/aep-dev/aepbase/pkg/resource"
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
	resourceDescriptions  map[string]string         // singular -> description
	resourceExamples      map[string]map[string]any  // singular -> field -> example value
}

// metaResourceSingular is the singular name for the built-in meta resource.
// It is registered in the API for OpenAPI generation but routes are handled
// separately by the meta package.
const metaResourceSingular = "definition"

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
				URL:  "https://github.com/aep-dev/aepbase",
			},
		},
		resourceDescriptions: map[string]string{
			"definition": "A resource definition. Create these to dynamically add new API endpoints.",
		},
		resourceExamples: map[string]map[string]any{
			"definition": {
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
		Singular: "definition",
		Plural:   "definitions",
		Schema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"id":          {Type: "string", ReadOnly: true, Description: "The unique identifier for this resource definition."},
				"path":        {Type: "string", ReadOnly: true, Description: "The full resource path (e.g. definitions/book)."},
				"singular":    {Type: "string", Description: "The singular name of the resource (e.g. book)."},
				"plural":      {Type: "string", Description: "The plural name of the resource, used as the URL collection path (e.g. books)."},
				"description": {Type: "string", Description: "A human-readable description of the resource."},
				"examples":    {Type: "object", Description: "Example values for the resource's fields, keyed by field name."},
				"schema":      {Type: "object", Description: "The JSON Schema defining the resource's properties."},
				"parents":     {Type: "array", Items: &openapi.Schema{Type: "string"}, Description: "The singular names of parent resources for nested resources."},
				"create_time": {Type: "string", Format: "date-time", ReadOnly: true, Description: "The time this resource definition was created."},
				"update_time": {Type: "string", Format: "date-time", ReadOnly: true, Description: "The time this resource definition was last updated."},
			},
		},
		Children: []*api.Resource{},
		Methods: api.Methods{
			Get:    &api.GetMethod{},
			List:   &api.ListMethod{},
			Create: &api.CreateMethod{SupportsUserSettableCreate: true},
			Update: &api.UpdateMethod{},
			Delete: &api.DeleteMethod{},
		},
	}
	metaResource.API = s.API
	s.API.Resources[metaResourceSingular] = metaResource
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
	schema.Properties["id"] = openapi.Schema{Type: "string", ReadOnly: true}
	schema.Properties["path"] = openapi.Schema{Type: "string", ReadOnly: true}
	schema.Properties["create_time"] = openapi.Schema{Type: "string", Format: "date-time", ReadOnly: true}
	schema.Properties["update_time"] = openapi.Schema{Type: "string", Format: "date-time", ReadOnly: true}

	// Validate parents exist.
	for _, parentSingular := range def.Parents {
		if _, ok := s.API.Resources[parentSingular]; !ok {
			return fmt.Errorf("parent resource %q not found", parentSingular)
		}
	}

	r := &api.Resource{
		Singular: def.Singular,
		Plural:   def.Plural,
		Parents:  def.Parents,
		Children: []*api.Resource{},
		Schema:   &schema,
		Methods: api.Methods{
			Get:    &api.GetMethod{},
			List:   &api.ListMethod{SupportsFilter: true, SupportsSkip: true},
			Create: &api.CreateMethod{SupportsUserSettableCreate: true},
			Update: &api.UpdateMethod{},
			Delete: &api.DeleteMethod{},
			Apply:  &api.ApplyMethod{},
		},
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
	if def.Description != "" {
		s.resourceDescriptions[def.Singular] = def.Description
	}
	if len(def.Examples) > 0 {
		s.resourceExamples[def.Singular] = def.Examples
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
	delete(s.resourceDescriptions, singular)
	delete(s.resourceExamples, singular)
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
	schema.Properties["id"] = openapi.Schema{Type: "string", ReadOnly: true}
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
	mux.HandleFunc("GET /openapi.json", s.serveOpenAPI)
	for _, r := range s.API.Resources {
		// Skip the meta resource — its routes are registered by meta.RegisterRoutes above.
		if r.Singular == metaResourceSingular {
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
		resource.RegisterRoutes(mux, s.DB, r, cmHandlers)
	}
	s.mux = mux
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

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)
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
		"id":          {"The unique identifier for this resource.", "my-definition"},
		"path":        {"The full resource path (e.g. publishers/acme/books/1984).", "definitions/my-definition"},
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
		"id":            {"The ID to use for the new definition. If omitted, one is generated automatically.", "my-definition"},
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
				fmt.Sprintf("definition with id %q already exists", "my-definition"),
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

	// Add to aep-lib-go's resource for OpenAPI generation.
	cm := &api.CustomMethod{
		Name:     methodName,
		Method:   config.Method,
		Request:  config.RequestSchema,
		Response: config.ResponseSchema,
	}
	r.CustomMethods = append(r.CustomMethods, cm)

	// Store the registration for route building.
	s.customMethods = append(s.customMethods, reg)

	s.rebuildMux()
	return nil
}

func (s *State) GetDB() *sql.DB {
	return s.DB
}
