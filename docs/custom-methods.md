# Custom Methods

aepbase supports adding custom methods to resources, following [AEP-136](https://aep.dev/136).

Custom methods let you define operations beyond standard CRUD on a resource.
For example, you might add an `:archive` action to a publisher, or a `:search`
query to a collection.

## How it works

1. You import aepbase as a library instead of running it as a standalone binary
2. You register custom methods with `state.AddCustomMethod()`
3. aepbase handles routing, OpenAPI spec generation, and path parameter extraction

## Example

```go
package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"

    "github.com/aep-dev/aep-lib-go/pkg/openapi"
    "github.com/rambleraptor/aepbase/pkg/aepbase"
    "github.com/rambleraptor/aepbase/pkg/db"
    "github.com/rambleraptor/aepbase/pkg/meta"
)

func main() {
    port := flag.Int("port", 8080, "port to listen on")
    dbPath := flag.String("db", "app.db", "path to SQLite database")
    flag.Parse()

    serverURL := fmt.Sprintf("http://localhost:%d", *port)

    d, err := db.Init(*dbPath)
    if err != nil {
        log.Fatal(err)
    }
    defer d.Close()

    state := aepbase.NewState(d, serverURL)

    // Restore resources from previous runs.
    defs, _ := meta.LoadAll(d)
    for _, def := range defs {
        state.AddResource(def)
    }

    // Register a POST custom method on the "publisher" resource.
    // This will be available at POST /publishers/{publisher}:archive
    state.AddCustomMethod("publisher", "archive", aepbase.CustomMethodConfig{
        Method: "POST",
        RequestSchema: &openapi.Schema{
            Type: "object",
            Properties: openapi.Properties{
                "reason": {Type: "string"},
            },
        },
        ResponseSchema: &openapi.Schema{
            Type: "object",
            Properties: openapi.Properties{
                "archived": {Type: "boolean"},
                "reason":   {Type: "string"},
            },
        },
        Handler: func(w http.ResponseWriter, r *http.Request) {
            publisherID := r.PathValue("publisher_id")

            var body map[string]any
            json.NewDecoder(r.Body).Decode(&body)

            // Your custom logic here.
            // You have access to the full request, path values, and
            // can use state.GetDB() for database operations.

            w.Header().Set("Content-Type", "application/json")
            json.NewEncoder(w).Encode(map[string]any{
                "archived": true,
                "reason":   body["reason"],
                "id":       publisherID,
            })
        },
    })

    // Register a GET custom method (read-only, no request body).
    // This will be available at GET /publishers/{publisher}:stats
    state.AddCustomMethod("publisher", "stats", aepbase.CustomMethodConfig{
        Method: "GET",
        ResponseSchema: &openapi.Schema{
            Type: "object",
            Properties: openapi.Properties{
                "book_count":   {Type: "integer"},
                "total_revenue": {Type: "number"},
            },
        },
        Handler: func(w http.ResponseWriter, r *http.Request) {
            publisherID := r.PathValue("publisher_id")

            // Query your database, call external services, etc.
            _ = publisherID

            w.Header().Set("Content-Type", "application/json")
            json.NewEncoder(w).Encode(map[string]any{
                "book_count":    42,
                "total_revenue": 1234.56,
            })
        },
    })

    log.Printf("listening on %s", serverURL)
    http.ListenAndServe(fmt.Sprintf(":%d", *port), state.Handler())
}
```

## API Reference

### `state.AddCustomMethod(resourceSingular, methodName, config)`

Registers a custom method on a resource. Must be called after `AddResource` for
the target resource.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `resourceSingular` | `string` | The singular name of the resource (e.g., `"publisher"`) |
| `methodName` | `string` | The custom method name (e.g., `"archive"`). Used in the URL as `:{methodName}` |
| `config` | `CustomMethodConfig` | Configuration for the custom method |

**CustomMethodConfig fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Method` | `string` | Yes | HTTP method: `"POST"` or `"GET"` |
| `RequestSchema` | `*openapi.Schema` | Yes (POST) | OpenAPI schema for the request body |
| `ResponseSchema` | `*openapi.Schema` | Yes | OpenAPI schema for the response body |
| `Handler` | `http.HandlerFunc` | Yes | Your handler function |

**Returns:** `error` if the resource doesn't exist, the config is invalid, or
the HTTP method is not POST/GET.

## URL Pattern

Custom methods use the colon syntax defined by AEP-136:

```
POST /publishers/{publisher}:archive
GET  /publishers/{publisher}:stats
POST /publishers/{publisher}/books/{book}:translate
```

## What you get automatically

When you register a custom method, aepbase:

- Registers the HTTP route with proper colon syntax
- Adds the method to the OpenAPI spec at `/openapi.json` with the correct
  `operationId` (e.g., `:ArchivePublisher`)
- Extracts path parameters so `r.PathValue("publisher_id")` works in your handler
- Validates that only POST and GET are used (per AEP-136)

## Accessing path values in handlers

Your handler receives a standard `http.Request` with path values already
extracted. Use `r.PathValue()` to access them:

```go
Handler: func(w http.ResponseWriter, r *http.Request) {
    // For a resource at /publishers/{publisher_id}/books/{book_id}:archive
    publisherID := r.PathValue("publisher_id")
    bookID := r.PathValue("book_id")
    // ...
}
```

## POST vs GET

Per AEP-136:

- **POST**: Use for operations that have side effects (modify state, trigger
  actions). Must have a `RequestSchema`.
- **GET**: Use for read-only operations (search, stats, computed views).
  Must be idempotent with no side effects. `RequestSchema` is not used
  (GET methods cannot have a request body).
