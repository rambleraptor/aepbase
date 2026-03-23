package aepbase

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
	"github.com/rambleraptor/aepbase/pkg/db"
	"github.com/rambleraptor/aepbase/pkg/meta"
)

// ServerOptions configures an aepbase server.
// Zero-value fields use sensible defaults (port 8080, DB "aepbase.db").
type ServerOptions struct {
	Port               int
	DataDir            string
	DBFile             string
	InMemory           bool
	CORSAllowedOrigins []string
	CustomMethods      []CustomMethodOption
	corsRaw            string
}

// CustomMethodOption defines a custom method to register on a resource.
// The Handler factory receives the initialized *sql.DB so handlers can
// access the database without needing it at configuration time.
type CustomMethodOption struct {
	ResourceSingular string
	MethodName       string
	Method           string
	RequestSchema    *openapi.Schema
	ResponseSchema   *openapi.Schema
	Handler          func(*sql.DB) http.HandlerFunc
}

// RegisterFlags registers CLI flags for the server options.
// If Port or DBPath are already set, their values are used as the flag defaults.
// Call flag.Parse() after this method.
func (o *ServerOptions) RegisterFlags() {
	if o.Port == 0 {
		o.Port = 8080
	}
	if o.DataDir == "" {
		o.DataDir = "aepbase_data"
	}
	if o.DBFile == "" {
		o.DBFile = "aepbase.db"
	}
	flag.IntVar(&o.Port, "port", o.Port, "port to listen on")
	flag.StringVar(&o.DataDir, "data-dir", o.DataDir, "directory for database files")
	flag.StringVar(&o.DBFile, "db", o.DBFile, "database file name")
	flag.StringVar(&o.corsRaw, "cors-allowed-origins", "", "comma-separated list of allowed CORS origins (e.g. \"https://ui.aep.dev,http://localhost:3000\")")
}

// Run initializes the database, loads existing resources, registers custom
// methods, and starts the HTTP server. It blocks until the server exits.
func Run(opts ServerOptions) error {
	if opts.Port == 0 {
		opts.Port = 8080
	}
	if opts.DataDir == "" {
		opts.DataDir = "aepbase_data"
	}
	if opts.DBFile == "" {
		opts.DBFile = "aepbase.db"
	}
	// If RegisterFlags was used, merge the parsed CORS flag.
	if opts.corsRaw != "" && len(opts.CORSAllowedOrigins) == 0 {
		opts.CORSAllowedOrigins = strings.Split(opts.corsRaw, ",")
	}

	serverURL := fmt.Sprintf("http://localhost:%d", opts.Port)

	var dbPath string
	if opts.InMemory {
		dbPath = ":memory:"
	} else {
		dbPath = filepath.Join(opts.DataDir, opts.DBFile)
	}
	d, err := db.Init(dbPath)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %v", err)
	}
	defer d.Close()

	state := NewState(d, serverURL)
	state.CORSAllowedOrigins = opts.CORSAllowedOrigins

	// Load existing resource definitions from a previous run.
	defs, err := meta.LoadAll(d)
	if err != nil {
		return fmt.Errorf("failed to load resource definitions: %v", err)
	}
	defs = topoSortDefs(defs)
	for _, def := range defs {
		if err := state.AddResource(def); err != nil {
			return fmt.Errorf("failed to restore resource %q: %v", def.Singular, err)
		}
		log.Printf("restored resource: %s (%s)", def.Singular, def.Plural)
	}

	// Register custom methods.
	for _, cm := range opts.CustomMethods {
		if err := state.AddCustomMethod(cm.ResourceSingular, cm.MethodName, CustomMethodConfig{
			Method:         cm.Method,
			RequestSchema:  cm.RequestSchema,
			ResponseSchema: cm.ResponseSchema,
			Handler:        cm.Handler(d),
		}); err != nil {
			return fmt.Errorf("failed to register custom method %q: %v", cm.MethodName, err)
		}
	}

	log.Printf("aepbase listening on %s", serverURL)
	log.Printf("OpenAPI spec at %s/openapi.json", serverURL)
	return http.ListenAndServe(fmt.Sprintf(":%d", opts.Port), state.Handler())
}

// topoSortDefs orders resource definitions so that parents come before children.
func topoSortDefs(defs []meta.ResourceDefinition) []meta.ResourceDefinition {
	byName := make(map[string]meta.ResourceDefinition, len(defs))
	for _, d := range defs {
		byName[d.Singular] = d
	}

	visited := make(map[string]bool, len(defs))
	var sorted []meta.ResourceDefinition

	var visit func(string)
	visit = func(name string) {
		if visited[name] {
			return
		}
		visited[name] = true
		d, ok := byName[name]
		if !ok {
			return
		}
		for _, p := range d.Parents {
			visit(p)
		}
		sorted = append(sorted, d)
	}

	for _, d := range defs {
		visit(d.Singular)
	}
	return sorted
}
