# aepbase

A dynamic API backend that follows the [AEP (API Enhancement Proposals)](https://aep.dev) standard. Define resources at runtime via a meta-API and get fully functional CRUD endpoints, a SQLite database, and an OpenAPI 3.0 spec.

## Features

- **Dynamic resource definitions** — create, update, and delete resource types through the API itself
- **Automatic CRUD** — each resource gets Create, Read, Update (PATCH), Apply (PUT), Delete, and List endpoints
- **Nested resources** — define parent-child relationships (e.g. `/publishers/{id}/books/{id}`)
- **Custom methods** — add arbitrary POST/GET actions on resources following [AEP-136](https://aep.dev/136) (e.g. `/books/{id}:archive`)
- **OpenAPI 3.0** — auto-generated spec served at `/openapi.json`
- **SQLite with WAL** — lightweight, zero-config persistence
- **CORS support** — configurable allowed origins

## Quick Start

### Build and run

```bash
go build -o aepbase ./
./aepbase
```

Server starts at `http://localhost:8080`.

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8080` | Listen port |
| `-db` | `aepbase.db` | SQLite database path |
| `-cors-allowed-origins` | *(none)* | Comma-separated allowed origins (`*` for all) |

### Define a resource

aepbase exposes a standard REST API. Define a resource with a single POST:

```bash
curl -X POST http://localhost:8080/resources \
  -H "Content-Type: application/json" \
  -d '{
    "singular": "book",
    "plural": "books",
    "schema": {
      "type": "object",
      "properties": {
        "title": {"type": "string"},
        "author": {"type": "string"}
      }
    }
  }'
```

That's it — aepbase now serves full CRUD at `/books` and an updated OpenAPI spec at `/openapi.json`.

### Using aepcli

Since aepbase generates an OpenAPI spec, you can use [aepcli](https://github.com/aep-dev/aepcli) to interact with it instead of writing curl commands by hand. `aepcli` reads the spec and gives you typed commands for every resource automatically.

```bash
go install github.com/aep-dev/aepcli/cmd/aepcli@latest
```

Point it at the OpenAPI endpoint:

```bash
API="http://localhost:8080/openapi.json"
```

### CRUD operations

```bash
# Create
aepcli $API book create 1984 \
  --title "1984" \
  --author "George Orwell"

# List
aepcli $API book list

# Get
aepcli $API book get 1984

# Update
aepcli $API book update 1984 \
  --title "Nineteen Eighty-Four"

# Delete
aepcli $API book delete 1984
```

### Nested resources

```bash
# Define a child resource
aepcli $API resource create chapter \
  --singular chapter \
  --plural chapters \
  --parents book \
  --schema '{"type":"object","properties":{"title":{"type":"string"},"number":{"type":"integer"}}}'

# Create a chapter under a book
aepcli $API chapter --book 1984 create ch1 \
  --title "Part One" \
  --number 1

# List chapters
aepcli $API chapter --book 1984 list
```

### Custom methods

Custom methods use the `:method` syntax:

```bash
aepcli $API book :publish 1984

aepcli $API book :purchase 1984 \
  --quantity 3
```

## Web UI

You can browse and manage your resources through [AEP Explorer](https://github.com/aep-dev/aep-explorer), a web UI for AEP-compliant APIs. Start aepbase with CORS enabled:

```bash
./aepbase -cors-allowed-origins "https://ui.aep.dev"
```

Then open [ui.aep.dev](https://ui.aep.dev) and paste in your OpenAPI URL:

```
http://localhost:8080/openapi.json
```

The UI will discover all your resources and let you create, read, update, and delete them without writing any commands.

## Use as a library

aepbase can be embedded in your own Go server to add custom methods and logic:

```go
package main

import (
    "net/http"
    "github.com/aep-dev/aepbase/pkg/aepbase"
    "github.com/aep-dev/aepbase/pkg/db"
)

func main() {
    database, _ := db.Init("app.db")
    state := aepbase.NewState(database, "http://localhost:8080")

    state.AddCustomMethod("book", "archive", aepbase.CustomMethodConfig{
        Method: "POST",
        Handler: func(w http.ResponseWriter, r *http.Request) {
            // your logic here
        },
    })

    http.ListenAndServe(":8080", state.Handler())
}
```

See [`examples/bookstore/`](examples/bookstore/) for a full working example with custom methods and an end-to-end demo script.

## Project Structure

```
main.go              # Standalone server
pkg/
  aepbase/           # Core orchestration and state management
  db/                # SQLite schema management
  meta/              # Meta-API handlers (resource definitions CRUD)
  resource/          # Dynamic resource CRUD handlers and validation
examples/
  bookstore/         # Example app with custom methods and demo script
docs/
  custom-methods.md  # Custom methods guide
```

## Running Tests

```bash
go test ./...
```

## License

MIT
