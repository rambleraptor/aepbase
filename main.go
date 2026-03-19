package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/aep-dev/aepbase/pkg/aepbase"
	"github.com/aep-dev/aepbase/pkg/db"
	"github.com/aep-dev/aepbase/pkg/meta"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	dbPath := flag.String("db", "aepbase.db", "path to SQLite database file")
	corsOrigins := flag.String("cors-allowed-origins", "", "comma-separated list of allowed CORS origins (e.g. \"https://ui.aep.dev,http://localhost:3000\")")
	flag.Parse()

	serverURL := fmt.Sprintf("http://localhost:%d", *port)

	d, err := db.Init(*dbPath)
	if err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}
	defer d.Close()

	state := aepbase.NewState(d, serverURL)

	if *corsOrigins != "" {
		state.CORSAllowedOrigins = strings.Split(*corsOrigins, ",")
	}

	// Load existing resource definitions from a previous run.
	defs, err := meta.LoadAll(d)
	if err != nil {
		log.Fatalf("failed to load resource definitions: %v", err)
	}
	for _, def := range defs {
		if err := state.AddResource(def); err != nil {
			log.Fatalf("failed to restore resource %q: %v", def.Singular, err)
		}
		log.Printf("restored resource: %s (%s)", def.Singular, def.Plural)
	}

	log.Printf("aepbase listening on %s", serverURL)
	log.Printf("OpenAPI spec at %s/openapi.json", serverURL)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), state.Handler()); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
