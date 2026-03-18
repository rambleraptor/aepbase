package apistate

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
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
	mu                   sync.RWMutex
	API                  *api.API
	mux                  *http.ServeMux
	DB                   *sql.DB
	ServerURL            string
	customMethods        []customMethodRegistration
	pendingCustomMethods []customMethodRegistration
}

// metaResourceSingular is the singular name for the built-in meta resource.
// It is registered in the API for OpenAPI generation but routes are handled
// separately by the meta package.
const metaResourceSingular = "resource"

func NewState(d *sql.DB, serverURL string) *State {
	s := &State{
		DB:        d,
		ServerURL: serverURL,
		API: &api.API{
			ServerURL: serverURL,
			Name:      "aepbase",
			Resources: make(map[string]*api.Resource),
		},
	}
	// Register the meta resource so it appears in the OpenAPI spec.
	metaResource := &api.Resource{
		Singular: "resource",
		Plural:   "resources",
		Schema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"id":          {Type: "string", ReadOnly: true},
				"path":        {Type: "string", ReadOnly: true},
				"singular":    {Type: "string"},
				"plural":      {Type: "string"},
				"schema":      {Type: "object"},
				"parents":     {Type: "array", Items: &openapi.Schema{Type: "string"}},
				"create_time": {Type: "string", Format: "date-time", ReadOnly: true},
				"update_time": {Type: "string", Format: "date-time", ReadOnly: true},
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
			List:   &api.ListMethod{},
			Create: &api.CreateMethod{SupportsUserSettableCreate: true},
			Update: &api.UpdateMethod{},
			Delete: &api.DeleteMethod{},
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
	s.mu.RUnlock()

	jsonBytes, err := a.ConvertToOpenAPIBytes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Post-process to add "type" field to x-aep-resource (required by linter, not yet in aep-lib-go).
	jsonBytes, err = injectResourceTypes(jsonBytes, a.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)
}

func injectResourceTypes(data []byte, apiName string) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	components, ok := doc["components"].(map[string]any)
	if !ok {
		return data, nil
	}
	schemas, ok := components["schemas"].(map[string]any)
	if !ok {
		return data, nil
	}
	for name, s := range schemas {
		schema, ok := s.(map[string]any)
		if !ok {
			continue
		}
		xaep, ok := schema["x-aep-resource"].(map[string]any)
		if !ok {
			continue
		}
		if _, hasType := xaep["type"]; !hasType {
			xaep["type"] = apiName + "/" + name
		}
	}
	return json.MarshalIndent(doc, "", "  ")
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
	w.state.mu.RUnlock()
	mux.ServeHTTP(rw, req)
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
