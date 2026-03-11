package apistate

import (
	"database/sql"
	"fmt"
	"net/http"
	"sync"

	"github.com/aep-dev/aep-lib-go/pkg/api"
	"github.com/aep-dev/aep-lib-go/pkg/openapi"

	"github.com/aep-dev/aepbase/pkg/db"
	"github.com/aep-dev/aepbase/pkg/meta"
	"github.com/aep-dev/aepbase/pkg/resource"
)

type State struct {
	mu        sync.RWMutex
	API       *api.API
	mux       *http.ServeMux
	DB        *sql.DB
	ServerURL string
}

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

	// Build pattern elems from parents.
	patternElems := buildPatternElems(def, s.API.Resources)

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

	// Resolve parent resource pointers.
	var parents []*api.Resource
	for _, parentSingular := range def.Parents {
		p, ok := s.API.Resources[parentSingular]
		if !ok {
			return fmt.Errorf("parent resource %q not found", parentSingular)
		}
		parents = append(parents, p)
	}

	r := &api.Resource{
		Singular:     def.Singular,
		Plural:       def.Plural,
		Parents:      parents,
		Children:     []*api.Resource{},
		PatternElems: patternElems,
		Schema:       &schema,
		GetMethod:    &api.GetMethod{},
		ListMethod:   &api.ListMethod{},
		CreateMethod: &api.CreateMethod{SupportsUserSettableCreate: true},
		UpdateMethod: &api.UpdateMethod{},
		DeleteMethod: &api.DeleteMethod{},
	}

	// Wire as child of parents.
	for _, p := range parents {
		p.Children = append(p.Children, r)
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
	for _, p := range r.Parents {
		for i, c := range p.Children {
			if c.Singular == singular {
				p.Children = append(p.Children[:i], p.Children[i+1:]...)
				break
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
		resource.RegisterRoutes(mux, s.DB, r)
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
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonBytes)
}

func buildPatternElems(def meta.ResourceDefinition, resources map[string]*api.Resource) []string {
	var elems []string
	for _, parentSingular := range def.Parents {
		if parent, ok := resources[parentSingular]; ok {
			elems = append(elems, parent.PatternElems...)
		}
	}
	elems = append(elems, def.Plural, "{"+def.Singular+"}")
	return elems
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

func (s *State) GetDB() *sql.DB {
	return s.DB
}
