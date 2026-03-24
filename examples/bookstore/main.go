package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
	"github.com/rambleraptor/aepbase/pkg/aepbase"
	"github.com/rambleraptor/aepbase/pkg/resource"
)

func main() {
	opts := aepbase.ServerOptions{
		DBFile: "bookstore.db",
		CustomMethods: []aepbase.CustomMethodOption{
			{
				ResourceSingular: "book",
				MethodName:       "publish",
				Method:           "POST",
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
				Handler: makePublishHandler,
			},
			{
				ResourceSingular: "book",
				MethodName:       "purchase",
				Method:           "POST",
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
				Handler: makePurchaseHandler,
			},
			{
				ResourceSingular: "book",
				MethodName:       "write",
				Method:           "POST",
				Async:            true,
				RequestSchema: &openapi.Schema{
					Type: "object",
					Properties: openapi.Properties{
						"chapters": {Type: "integer"},
					},
				},
				ResponseSchema: &openapi.Schema{
					Type: "object",
					Properties: openapi.Properties{
						"id":       {Type: "string"},
						"chapters": {Type: "integer"},
						"status":   {Type: "string"},
					},
				},
				Handler: makeWriteHandler,
			},
		},
	}
	opts.RegisterFlags()
	flag.Parse()

	if err := aepbase.Run(opts); err != nil {
		log.Fatal(err)
	}
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

// makeWriteHandler simulates a long-running book writing process.
// Because it's registered with Async: true, the framework runs this handler
// in the background and returns an Operation to the caller immediately.
func makeWriteHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bookID := r.PathValue("book_id")

		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		chapters := 10
		if c, ok := body["chapters"]; ok {
			if cf, ok := c.(float64); ok {
				chapters = int(cf)
			}
		}

		// Simulate long-running work (1 second per chapter, capped at 5s).
		duration := time.Duration(chapters) * time.Second
		if duration > 5*time.Second {
			duration = 5 * time.Second
		}
		time.Sleep(duration)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":       bookID,
			"chapters": chapters,
			"status":   "written",
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
