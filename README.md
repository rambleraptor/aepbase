# aepbase

A dynamic, schema-driven REST API server built on [AEP](https://aep.dev) standards. Define resources at runtime and get fully compliant CRUD endpoints, parent-child hierarchies, custom methods, and auto-generated OpenAPI specs — no code generation or recompilation required.

## Features

- **Runtime resource definition** — Create, update, and delete API resources via a meta-API at `/resources`
- **AEP-compliant CRUD** — Automatically generated Create, Read, Update, Delete, List, and Apply endpoints
- **Parent-child resources** — Define hierarchical relationships (e.g., publishers → books → chapters)
- **Custom methods** — Register POST and GET custom methods using [AEP-136](https://aep.dev/136) colon syntax (e.g., `:publish`, `:archive`)
- **OpenAPI 3.1.0 generation** — Live spec available at `/openapi.json`, updated as resources change
- **Schema evolution** — Add or remove fields without downtime or data loss
- **Filtering & pagination** — Built-in support for list filtering and cursor-based pagination
- **SQLite persistence** — Lightweight storage with WAL mode; state recovers automatically on restart

## Quick start

### Build and run

```bash
go build -o aepbase .
./aepbase -port 8080 -db aepbase.db
```

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `-port` | `8080` | HTTP server port |
| `-db` | `aepbase.db` | SQLite database file path |
| `-cors-allowed-origins` | | Comma-separated allowed CORS origins |

### Define a resource

```bash
curl -X POST http://localhost:8080/resources -d '{
  "singular": "book",
  "plural": "books",
  "schema": {
    "properties": {
      "title": { "type": "string" },
      "author": { "type": "string" },
      "published": { "type": "boolean" }
    },
    "required": ["title"]
  }
}'
```

This creates the following endpoints automatically:

- `POST /books` — Create a book
- `GET /books/{book}` — Get a book
- `GET /books` — List books
- `PATCH /books/{book}` — Update a book
- `DELETE /books/{book}` — Delete a book

### Use the generated API

```bash
# Create
curl -X POST http://localhost:8080/books -d '{
  "title": "The Art of API Design",
  "author": "Jane Doe"
}'

# List
curl http://localhost:8080/books

# Get
curl http://localhost:8080/books/my-book-id

# Update
curl -X PATCH http://localhost:8080/books/my-book-id -d '{
  "published": true
}'

# Delete
curl -X DELETE http://localhost:8080/books/my-book-id
```

## Parent-child resources

Define hierarchical resources by specifying `parents`:

```bash
curl -X POST http://localhost:8080/resources -d '{
  "singular": "chapter",
  "plural": "chapters",
  "parents": ["publishers/*/books"],
  "schema": {
    "properties": {
      "title": { "type": "string" },
      "page_count": { "type": "integer" }
    }
  }
}'
```

This generates nested endpoints like `POST /publishers/{publisher}/books/{book}/chapters`.

## Custom methods

aepbase supports [AEP-136 custom methods](https://aep.dev/136) using colon syntax. See [docs/custom-methods.md](docs/custom-methods.md) for the full guide.

## Using as a library

Import aepbase into your own Go application for more control:

```go
package main

import (
	"github.com/aep-dev/aepbase/pkg/aepbase"
	"github.com/aep-dev/aepbase/pkg/db"
)

func main() {
	database, _ := db.NewDB("app.db")
	state := aepbase.NewState(database)

	// Register custom methods, add resources programmatically, etc.
	mux := state.BuildMux()
	http.ListenAndServe(":8080", mux)
}
```

See the [bookstore example](examples/bookstore/main.go) for a complete working application with custom methods.

## Running tests

```bash
go test ./...
```

## License

[MIT](LICENSE)
