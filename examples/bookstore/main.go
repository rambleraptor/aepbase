package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
	"github.com/aep-dev/aepbase/pkg/aepbase"
	"github.com/aep-dev/aepbase/pkg/db"
	"github.com/aep-dev/aepbase/pkg/meta"
	"github.com/aep-dev/aepbase/pkg/resource"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	dataDir := flag.String("data-dir", "aepbase_data", "directory for database files")
	dbFile := flag.String("db", "bookstore.db", "database file name")
	corsOrigins := flag.String("cors-allowed-origins", "", "comma-separated list of allowed CORS origins")
	flag.Parse()

	serverURL := fmt.Sprintf("http://localhost:%d", *port)

	d, err := db.Init(filepath.Join(*dataDir, *dbFile))
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	state := aepbase.NewState(d, serverURL)

	if *corsOrigins != "" {
		state.CORSAllowedOrigins = strings.Split(*corsOrigins, ",")
	}

	// Restore resources from previous runs.
	defs, _ := meta.LoadAll(d)
	for _, def := range defs {
		state.AddResource(def)
	}

	// Register custom methods on the "book" resource.
	// If the resource doesn't exist yet (first run), these are deferred
	// and applied automatically when "book" is created via the meta-API.

	// :publish — sets the book's published field to true.
	state.AddCustomMethod("book", "publish", aepbase.CustomMethodConfig{
		Method: "POST",
		RequestSchema: &openapi.Schema{
			Type:       "object",
			Properties: openapi.Properties{},
		},
		ResponseSchema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"id":        {Type: "string"},
				"published": {Type: "boolean"},
			},
		},
		Handler: makePublishHandler(d),
	})

	// :purchase — increments the purchase_count field on the book.
	state.AddCustomMethod("book", "purchase", aepbase.CustomMethodConfig{
		Method: "POST",
		RequestSchema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"quantity": {Type: "integer"},
			},
		},
		ResponseSchema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"id":             {Type: "string"},
				"purchase_count": {Type: "integer"},
				"quantity":       {Type: "integer"},
			},
		},
		Handler: makePurchaseHandler(d),
	})

	log.Printf("Bookstore server listening on %s", serverURL)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), state.Handler()))
}

func makePublishHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bookID := r.PathValue("book_id")
		publisherID := r.PathValue("publisher_id")

		path := fmt.Sprintf("publishers/%s/books/%s", publisherID, bookID)

		stored, err := resource.Get(d, "books", path, bookSchema())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if stored == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("book %q not found", path))
			return
		}

		stored.Fields["published"] = true
		if err := resource.Update(d, "books", path, stored.Fields, stored.UpdateTime, bookSchema()); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":        bookID,
			"published": true,
		})
	}
}

func makePurchaseHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bookID := r.PathValue("book_id")
		publisherID := r.PathValue("publisher_id")

		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		quantity := 1
		if q, ok := body["quantity"]; ok {
			if qf, ok := q.(float64); ok {
				quantity = int(qf)
			}
		}

		path := fmt.Sprintf("publishers/%s/books/%s", publisherID, bookID)

		stored, err := resource.Get(d, "books", path, bookSchema())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if stored == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("book %q not found", path))
			return
		}

		currentCount := 0
		if c, ok := stored.Fields["purchase_count"]; ok {
			if cf, ok := c.(float64); ok {
				currentCount = int(cf)
			} else if ci, ok := c.(int64); ok {
				currentCount = int(ci)
			}
		}
		newCount := currentCount + quantity
		stored.Fields["purchase_count"] = newCount

		if err := resource.Update(d, "books", path, stored.Fields, stored.UpdateTime, bookSchema()); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":             bookID,
			"purchase_count": newCount,
			"quantity":       quantity,
		})
	}
}

// bookSchema returns the schema used for book DB queries.
func bookSchema() *openapi.Schema {
	return &openapi.Schema{
		Type: "object",
		Properties: openapi.Properties{
			"id":             {Type: "string", ReadOnly: true},
			"path":           {Type: "string", ReadOnly: true},
			"create_time":    {Type: "string", Format: "date-time", ReadOnly: true},
			"update_time":    {Type: "string", Format: "date-time", ReadOnly: true},
			"title":          {Type: "string"},
			"author":         {Type: "string"},
			"published":      {Type: "boolean"},
			"purchase_count": {Type: "integer"},
		},
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": msg,
		},
	})
}
