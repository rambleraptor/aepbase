package aepbase_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
	"github.com/rambleraptor/aepbase/pkg/aepbase"
	"github.com/rambleraptor/aepbase/pkg/db"
	"github.com/rambleraptor/aepbase/pkg/meta"
)

// helper to create a fresh State with an in-memory SQLite DB.
func newTestState(t *testing.T) *aepbase.State {
	t.Helper()
	d, err := db.Init(":memory:")
	if err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return aepbase.NewState(d, "http://localhost:8080")
}

func doRequest(t *testing.T, handler http.Handler, method, path, body string) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, bodyReader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w.Result()
}

func readJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("parsing JSON %q: %v", string(b), err)
	}
	return m
}

func createResource(t *testing.T, h http.Handler, id, singular, plural string, parents []string, props map[string]any) map[string]any {
	t.Helper()
	schema := map[string]any{"properties": props}
	body := map[string]any{
		"singular": singular,
		"plural":   plural,
		"schema":   schema,
	}
	if len(parents) > 0 {
		body["parents"] = parents
	}
	b, _ := json.Marshal(body)
	path := "/aep-resource-definitions"
	if id != "" {
		path += "?id=" + id
	}
	resp := doRequest(t, h, "POST", path, string(b))
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("createResource %s: status %d: %v", singular, resp.StatusCode, m)
	}
	return readJSON(t, resp)
}

// --- Meta-API Tests ---

func TestCreateResource(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	m := createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	if m["id"] != "publisher" {
		t.Errorf("expected id=publisher, got %v", m["id"])
	}
	if m["path"] != "aep-resource-definitions/publisher" {
		t.Errorf("expected path=aep-resource-definitions/publisher, got %v", m["path"])
	}
	if m["create_time"] == nil || m["update_time"] == nil {
		t.Error("expected timestamps")
	}
}

func TestCreateResourceWithUserID(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	m := createResource(t, h, "my-custom-id", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	if m["id"] != "my-custom-id" {
		t.Errorf("expected id=my-custom-id, got %v", m["id"])
	}
}

func TestCreateResourceAutoID(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	m := createResource(t, h, "", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	// Auto ID defaults to singular name.
	if m["id"] != "publisher" {
		t.Errorf("expected id=publisher (auto), got %v", m["id"])
	}
}

func TestCreateResourceDuplicate(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	body := `{"singular":"publisher","plural":"publishers2","schema":{"properties":{"name":{"type":"string"}}}}`
	resp := doRequest(t, h, "POST", "/aep-resource-definitions?id=publisher", body)
	if resp.StatusCode != 409 {
		t.Errorf("expected 409, got %d", resp.StatusCode)
	}
}

func TestCreateResourceMissingFields(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	resp := doRequest(t, h, "POST", "/aep-resource-definitions", `{"singular":"","plural":""}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestCreateResourceInvalidParent(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	body := `{"singular":"book","plural":"books","parents":["nonexistent"],"schema":{"properties":{"title":{"type":"string"}}}}`
	resp := doRequest(t, h, "POST", "/aep-resource-definitions?id=book", body)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestGetResource(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "GET", "/aep-resource-definitions/publisher", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["singular"] != "publisher" {
		t.Errorf("expected singular=publisher, got %v", m["singular"])
	}
}

func TestGetResourceNotFound(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	resp := doRequest(t, h, "GET", "/aep-resource-definitions/nonexistent", "")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestListResources(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "author", "author", "authors", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "GET", "/aep-resource-definitions", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	results := m["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestDeleteResource(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "DELETE", "/aep-resource-definitions/publisher", "")
	if resp.StatusCode != 204 {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}
	resp = doRequest(t, h, "GET", "/aep-resource-definitions/publisher", "")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404 after delete, got %d", resp.StatusCode)
	}
}

func TestDeleteResourceWithChildren(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "book", "book", "books", []string{"publisher"}, map[string]any{
		"title": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "DELETE", "/aep-resource-definitions/publisher", "")
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 (has children), got %d", resp.StatusCode)
	}
}

func TestUpdateResourceAddColumn(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	// Create an instance first.
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	// Add a column.
	patch := `{"schema":{"properties":{"name":{"type":"string"},"location":{"type":"string"}}}}`
	resp := doRequest(t, h, "PATCH", "/aep-resource-definitions/publisher", patch)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}

	// Use the new column.
	resp = doRequest(t, h, "PATCH", "/publishers/acme", `{"location":"NYC"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 on update, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["location"] != "NYC" {
		t.Errorf("expected location=NYC, got %v", m["location"])
	}
	if m["name"] != "Acme" {
		t.Errorf("expected name=Acme preserved, got %v", m["name"])
	}
}

func TestUpdateResourceRemoveColumn(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":     map[string]any{"type": "string"},
		"location": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","location":"NYC"}`)

	// Remove location column.
	patch := `{"schema":{"properties":{"name":{"type":"string"}}}}`
	resp := doRequest(t, h, "PATCH", "/aep-resource-definitions/publisher", patch)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}

	// Verify existing data still has name.
	resp = doRequest(t, h, "GET", "/publishers/acme", "")
	m := readJSON(t, resp)
	if m["name"] != "Acme" {
		t.Errorf("expected name=Acme after column removal, got %v", m["name"])
	}
	// location should no longer appear.
	if _, ok := m["location"]; ok {
		t.Errorf("expected location to be gone, but it's still present: %v", m["location"])
	}
}

func TestUpdateResourceChangeType(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	patch := `{"schema":{"properties":{"name":{"type":"integer"}}}}`
	resp := doRequest(t, h, "PATCH", "/aep-resource-definitions/publisher", patch)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 on type change, got %d", resp.StatusCode)
	}
}

func TestUpdateResourceChangeParents(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "author", "author", "authors", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "book", "book", "books", []string{"publisher"}, map[string]any{
		"title": map[string]any{"type": "string"},
	})
	patch := `{"parents":["author"]}`
	resp := doRequest(t, h, "PATCH", "/aep-resource-definitions/book", patch)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 on parent change, got %d", resp.StatusCode)
	}
}

// --- Dynamic Resource CRUD Tests ---

func TestCreateInstance(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":     map[string]any{"type": "string"},
		"location": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","location":"NYC"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["id"] != "acme" {
		t.Errorf("expected id=acme, got %v", m["id"])
	}
	if m["path"] != "publishers/acme" {
		t.Errorf("expected path=publishers/acme, got %v", m["path"])
	}
	if m["name"] != "Acme" {
		t.Errorf("expected name=Acme, got %v", m["name"])
	}
	if m["create_time"] == nil || m["update_time"] == nil {
		t.Error("expected timestamps")
	}
}

func TestCreateInstanceAutoID(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "POST", "/publishers", `{"name":"Acme"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["id"] == nil || m["id"] == "" {
		t.Error("expected auto-generated id")
	}
}

func TestGetInstance(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	resp := doRequest(t, h, "GET", "/publishers/acme", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["name"] != "Acme" {
		t.Errorf("expected name=Acme, got %v", m["name"])
	}
}

func TestGetInstanceNotFound(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "GET", "/publishers/nonexistent", "")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestUpdateInstance(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	resp := doRequest(t, h, "PATCH", "/publishers/acme", `{"name":"Acme Corp"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["name"] != "Acme Corp" {
		t.Errorf("expected name=Acme Corp, got %v", m["name"])
	}
}

func TestUpdateInstancePartial(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":     map[string]any{"type": "string"},
		"location": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","location":"NYC"}`)

	// Only update name.
	resp := doRequest(t, h, "PATCH", "/publishers/acme", `{"name":"Acme Corp"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["name"] != "Acme Corp" {
		t.Errorf("expected name=Acme Corp, got %v", m["name"])
	}
	if m["location"] != "NYC" {
		t.Errorf("expected location=NYC preserved, got %v", m["location"])
	}
}

func TestDeleteInstance(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	resp := doRequest(t, h, "DELETE", "/publishers/acme", "")
	if resp.StatusCode != 204 {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}
}

func TestDeleteInstanceNotFound(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "DELETE", "/publishers/nonexistent", "")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestListInstances(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)
	doRequest(t, h, "POST", "/publishers?id=beta", `{"name":"Beta"}`)

	resp := doRequest(t, h, "GET", "/publishers", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	results := m["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

// --- Parent/Child Tests ---

func setupPublisherAndBook(t *testing.T, h http.Handler) {
	t.Helper()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "book", "book", "books", []string{"publisher"}, map[string]any{
		"title":      map[string]any{"type": "string"},
		"page_count": map[string]any{"type": "integer"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)
	doRequest(t, h, "POST", "/publishers?id=beta", `{"name":"Beta"}`)
}

func TestCreateChildInstance(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	setupPublisherAndBook(t, h)

	resp := doRequest(t, h, "POST", "/publishers/acme/books?id=go-guide", `{"title":"The Go Guide","page_count":350}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["path"] != "publishers/acme/books/go-guide" {
		t.Errorf("expected path=publishers/acme/books/go-guide, got %v", m["path"])
	}
	if m["title"] != "The Go Guide" {
		t.Errorf("expected title=The Go Guide, got %v", m["title"])
	}
}

func TestGetChildInstance(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	setupPublisherAndBook(t, h)
	doRequest(t, h, "POST", "/publishers/acme/books?id=go-guide", `{"title":"The Go Guide","page_count":350}`)

	resp := doRequest(t, h, "GET", "/publishers/acme/books/go-guide", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["title"] != "The Go Guide" {
		t.Errorf("expected title=The Go Guide, got %v", m["title"])
	}
}

func TestListChildrenScopedToParent(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	setupPublisherAndBook(t, h)
	doRequest(t, h, "POST", "/publishers/acme/books?id=book1", `{"title":"Book One","page_count":100}`)
	doRequest(t, h, "POST", "/publishers/acme/books?id=book2", `{"title":"Book Two","page_count":200}`)
	doRequest(t, h, "POST", "/publishers/beta/books?id=book3", `{"title":"Book Three","page_count":300}`)

	// List books under acme — should only return 2.
	resp := doRequest(t, h, "GET", "/publishers/acme/books", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	results := m["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 books under acme, got %d", len(results))
	}

	// List books under beta — should only return 1.
	resp = doRequest(t, h, "GET", "/publishers/beta/books", "")
	m = readJSON(t, resp)
	results = m["results"].([]any)
	if len(results) != 1 {
		t.Errorf("expected 1 book under beta, got %d", len(results))
	}
}

// --- Pagination Tests ---

func TestListPagination(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("pub%02d", i)
		doRequest(t, h, "POST", fmt.Sprintf("/publishers?id=%s", id), fmt.Sprintf(`{"name":"Publisher %d"}`, i))
	}

	// First page: 2 items.
	resp := doRequest(t, h, "GET", "/publishers?max_page_size=2", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	results := m["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 results on first page, got %d", len(results))
	}
	nextToken, ok := m["next_page_token"].(string)
	if !ok || nextToken == "" {
		t.Fatal("expected next_page_token on first page")
	}

	// Second page.
	resp = doRequest(t, h, "GET", fmt.Sprintf("/publishers?max_page_size=2&page_token=%s", nextToken), "")
	m = readJSON(t, resp)
	results = m["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 results on second page, got %d", len(results))
	}
	nextToken2, ok := m["next_page_token"].(string)
	if !ok || nextToken2 == "" {
		t.Fatal("expected next_page_token on second page")
	}

	// Third page: 1 item, no next token.
	resp = doRequest(t, h, "GET", fmt.Sprintf("/publishers?max_page_size=2&page_token=%s", nextToken2), "")
	m = readJSON(t, resp)
	results = m["results"].([]any)
	if len(results) != 1 {
		t.Errorf("expected 1 result on third page, got %d", len(results))
	}
	if _, ok := m["next_page_token"]; ok {
		t.Error("expected no next_page_token on last page")
	}
}

// --- OpenAPI Tests ---

func TestOpenAPIEmpty(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	resp := doRequest(t, h, "GET", "/openapi.json", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["openapi"] != "3.1.0" {
		t.Errorf("expected openapi=3.1.0, got %v", m["openapi"])
	}
}

func TestOpenAPIAfterCreate(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	paths := m["paths"].(map[string]any)
	if _, ok := paths["/publishers"]; !ok {
		t.Error("expected /publishers path in OpenAPI spec")
	}
	if _, ok := paths["/publishers/{publisher_id}"]; !ok {
		t.Error("expected /publishers/{publisher_id} path in OpenAPI spec")
	}
}

func TestOpenAPIAfterDelete(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "DELETE", "/aep-resource-definitions/publisher", "")

	resp := doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	paths, ok := m["paths"].(map[string]any)
	if !ok {
		paths = map[string]any{}
	}
	// After deleting the user resource, only the built-in meta and operation paths should remain.
	builtinPaths := map[string]bool{
		"/aep-resource-definitions":                          true,
		"/aep-resource-definitions/{aep_resource_definition_id}": true,
		"/operations":                  true,
		"/operations/{operation_id}":   true,
	}
	for p := range paths {
		if !builtinPaths[p] {
			t.Errorf("unexpected path %q after deleting resource", p)
		}
	}
}

func TestOpenAPIParentChild(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "book", "book", "books", []string{"publisher"}, map[string]any{
		"title": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	paths := m["paths"].(map[string]any)
	expected := []string{
		"/publishers",
		"/publishers/{publisher_id}",
		"/publishers/{publisher_id}/books",
		"/publishers/{publisher_id}/books/{book_id}",
	}
	for _, p := range expected {
		if _, ok := paths[p]; !ok {
			t.Errorf("expected path %s in OpenAPI spec", p)
		}
	}
}

// --- Restart Recovery Test ---

func TestRestartRecovery(t *testing.T) {
	dbPath := t.TempDir() + "/shared.db"

	// Phase 1: Create resources and data.
	d1, err := db.Init(dbPath)
	if err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	state1 := aepbase.NewState(d1, "http://localhost:8080")
	h1 := state1.Handler()
	createResource(t, h1, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h1, "POST", "/publishers?id=acme", `{"name":"Acme"}`)
	d1.Close()

	// Phase 2: Reopen and recover (simulates restart).
	d2, err := db.Init(dbPath)
	if err != nil {
		t.Fatalf("db.Init (reopen): %v", err)
	}
	defer d2.Close()

	state2 := aepbase.NewState(d2, "http://localhost:8080")
	defs, err := meta.LoadAll(d2)
	if err != nil {
		t.Fatalf("meta.LoadAll: %v", err)
	}
	if len(defs) != 1 {
		t.Fatalf("expected 1 definition, got %d", len(defs))
	}
	for _, def := range defs {
		if err := state2.AddResource(def); err != nil {
			t.Fatalf("AddResource on recovery: %v", err)
		}
	}

	h2 := state2.Handler()

	// Verify data survived.
	resp := doRequest(t, h2, "GET", "/publishers/acme", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 after recovery, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["name"] != "Acme" {
		t.Errorf("expected name=Acme after recovery, got %v", m["name"])
	}

	// OpenAPI should also be restored.
	resp = doRequest(t, h2, "GET", "/openapi.json", "")
	spec := readJSON(t, resp)
	paths := spec["paths"].(map[string]any)
	if _, ok := paths["/publishers"]; !ok {
		t.Error("expected /publishers path after recovery")
	}
}

// --- Custom Method Tests ---

func TestCustomMethodPOST(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":     map[string]any{"type": "string"},
		"archived": map[string]any{"type": "boolean"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","archived":false}`)

	err := state.AddCustomMethod("publisher", "archive", aepbase.CustomMethodConfig{
		Method:         "POST",
		RequestSchema:  &openapi.Schema{Type: "object", Properties: openapi.Properties{}},
		ResponseSchema: &openapi.Schema{Type: "object", Properties: openapi.Properties{"archived": {Type: "boolean"}}},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"archived": true})
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	// Must re-get handler after mux rebuild.
	h = state.Handler()

	resp := doRequest(t, h, "POST", "/publishers/acme:archive", `{}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["archived"] != true {
		t.Errorf("expected archived=true, got %v", m["archived"])
	}
}

func TestCustomMethodGET(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	err := state.AddCustomMethod("publisher", "stats", aepbase.CustomMethodConfig{
		Method:         "GET",
		ResponseSchema: &openapi.Schema{Type: "object", Properties: openapi.Properties{"book_count": {Type: "integer"}}},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"book_count": 42})
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	h = state.Handler()

	resp := doRequest(t, h, "GET", "/publishers/acme:stats", "")
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["book_count"] != float64(42) {
		t.Errorf("expected book_count=42, got %v", m["book_count"])
	}
}

func TestCustomMethodGETDoesNotBreakRegularGet(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	err := state.AddCustomMethod("publisher", "stats", aepbase.CustomMethodConfig{
		Method:         "GET",
		ResponseSchema: &openapi.Schema{Type: "object", Properties: openapi.Properties{"count": {Type: "integer"}}},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"count": 1})
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	h = state.Handler()

	// Regular GET should still work.
	resp := doRequest(t, h, "GET", "/publishers/acme", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 for regular GET, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	if m["name"] != "Acme" {
		t.Errorf("expected name=Acme, got %v", m["name"])
	}
}

func TestCustomMethodNotFound(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})

	resp := doRequest(t, h, "POST", "/publishers/acme:nonexistent", `{}`)
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestCustomMethodAppearsInOpenAPI(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})

	err := state.AddCustomMethod("publisher", "archive", aepbase.CustomMethodConfig{
		Method:         "POST",
		RequestSchema:  &openapi.Schema{Type: "object", Properties: openapi.Properties{}},
		ResponseSchema: &openapi.Schema{Type: "object", Properties: openapi.Properties{"archived": {Type: "boolean"}}},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	h = state.Handler()

	resp := doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	paths := m["paths"].(map[string]any)
	cmPath, ok := paths["/publishers/{publisher_id}:archive"]
	if !ok {
		t.Fatalf("expected /publishers/{publisher_id}:archive path in OpenAPI spec, got paths: %v", keys(paths))
	}
	pathItem := cmPath.(map[string]any)
	if _, ok := pathItem["post"]; !ok {
		t.Error("expected POST operation on custom method path")
	}
}

func TestCustomMethodOnChildResource(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	setupPublisherAndBook(t, h)
	doRequest(t, h, "POST", "/publishers/acme/books?id=go-guide", `{"title":"The Go Guide","page_count":350}`)

	err := state.AddCustomMethod("book", "archive", aepbase.CustomMethodConfig{
		Method:         "POST",
		RequestSchema:  &openapi.Schema{Type: "object"},
		ResponseSchema: &openapi.Schema{Type: "object", Properties: openapi.Properties{"status": {Type: "string"}}},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			bookID := r.PathValue("book_id")
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"status": "archived", "book": bookID})
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	h = state.Handler()

	resp := doRequest(t, h, "POST", "/publishers/acme/books/go-guide:archive", `{}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["status"] != "archived" {
		t.Errorf("expected status=archived, got %v", m["status"])
	}
	if m["book"] != "go-guide" {
		t.Errorf("expected book=go-guide, got %v", m["book"])
	}
}

func TestCustomMethodInvalidConfig(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})

	// Missing handler.
	err := state.AddCustomMethod("publisher", "archive", aepbase.CustomMethodConfig{
		Method:         "POST",
		RequestSchema:  &openapi.Schema{Type: "object"},
		ResponseSchema: &openapi.Schema{Type: "object"},
	})
	if err == nil {
		t.Error("expected error for missing handler")
	}

	// Invalid HTTP method.
	err = state.AddCustomMethod("publisher", "archive", aepbase.CustomMethodConfig{
		Method:         "PUT",
		RequestSchema:  &openapi.Schema{Type: "object"},
		ResponseSchema: &openapi.Schema{Type: "object"},
		Handler:        func(w http.ResponseWriter, r *http.Request) {},
	})
	if err == nil {
		t.Error("expected error for invalid HTTP method")
	}

	// Missing response schema.
	err = state.AddCustomMethod("publisher", "archive", aepbase.CustomMethodConfig{
		Method:  "POST",
		Handler: func(w http.ResponseWriter, r *http.Request) {},
	})
	if err == nil {
		t.Error("expected error for missing response schema")
	}

	// Nonexistent resource — deferred, no error.
	err = state.AddCustomMethod("nonexistent", "archive", aepbase.CustomMethodConfig{
		Method:         "POST",
		RequestSchema:  &openapi.Schema{Type: "object"},
		ResponseSchema: &openapi.Schema{Type: "object"},
		Handler:        func(w http.ResponseWriter, r *http.Request) {},
	})
	if err != nil {
		t.Errorf("expected deferred registration for nonexistent resource, got error: %v", err)
	}
}

func keys(m map[string]any) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// --- Apply Method Tests ---

func TestApplyCreatesNewResource(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})

	// Apply (PUT) to a resource that doesn't exist yet — should create it.
	resp := doRequest(t, h, "PUT", "/publishers/acme", `{"name":"Acme"}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["id"] != "acme" {
		t.Errorf("expected id=acme, got %v", m["id"])
	}
	if m["name"] != "Acme" {
		t.Errorf("expected name=Acme, got %v", m["name"])
	}

	// Verify it can be retrieved.
	resp = doRequest(t, h, "GET", "/publishers/acme", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 on GET, got %d", resp.StatusCode)
	}
}

func TestApplyReplacesExistingResource(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":     map[string]any{"type": "string"},
		"location": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","location":"NYC"}`)

	// Apply (PUT) replaces the entire resource.
	resp := doRequest(t, h, "PUT", "/publishers/acme", `{"name":"Acme Corp"}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["name"] != "Acme Corp" {
		t.Errorf("expected name=Acme Corp, got %v", m["name"])
	}
	// location should be nil/empty since Apply is a full replace, not a merge.
	if m["location"] != nil {
		t.Errorf("expected location=nil after full replace, got %v", m["location"])
	}
}

func TestApplyOnChildResource(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	setupPublisherAndBook(t, h)

	// Apply a book that doesn't exist yet.
	resp := doRequest(t, h, "PUT", "/publishers/acme/books/go-guide", `{"title":"The Go Guide","page_count":350}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["path"] != "publishers/acme/books/go-guide" {
		t.Errorf("expected path=publishers/acme/books/go-guide, got %v", m["path"])
	}
}

func TestApplyAppearsInOpenAPI(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	paths := m["paths"].(map[string]any)
	resourcePath := paths["/publishers/{publisher_id}"].(map[string]any)
	if _, ok := resourcePath["put"]; !ok {
		t.Error("expected PUT (Apply) operation in OpenAPI spec for /publishers/{publisher_id}")
	}
}

// --- Required Field Enforcement Tests ---

func TestCreateRejectsWhenRequiredFieldMissing(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	// Create a resource with required fields.
	schema := map[string]any{
		"properties": map[string]any{
			"name":     map[string]any{"type": "string"},
			"location": map[string]any{"type": "string"},
		},
		"required": []any{"name"},
	}
	body := map[string]any{
		"singular": "publisher",
		"plural":   "publishers",
		"schema":   schema,
	}
	b, _ := json.Marshal(body)
	resp := doRequest(t, h, "POST", "/aep-resource-definitions?id=publisher", string(b))
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("createResource: status %d: %v", resp.StatusCode, m)
	}

	// Try creating without the required "name" field.
	resp = doRequest(t, h, "POST", "/publishers?id=acme", `{"location":"NYC"}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for missing required field, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	errMsg := m["error"].(map[string]any)["message"].(string)
	if !strings.Contains(errMsg, "name") {
		t.Errorf("expected error to mention 'name', got: %s", errMsg)
	}
}

func TestCreateSucceedsWhenRequiredFieldPresent(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	schema := map[string]any{
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
		},
		"required": []any{"name"},
	}
	body := map[string]any{
		"singular": "publisher",
		"plural":   "publishers",
		"schema":   schema,
	}
	b, _ := json.Marshal(body)
	doRequest(t, h, "POST", "/aep-resource-definitions?id=publisher", string(b))

	resp := doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
}

func TestApplyRejectsWhenRequiredFieldMissing(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	schema := map[string]any{
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
		},
		"required": []any{"name"},
	}
	body := map[string]any{
		"singular": "publisher",
		"plural":   "publishers",
		"schema":   schema,
	}
	b, _ := json.Marshal(body)
	doRequest(t, h, "POST", "/aep-resource-definitions?id=publisher", string(b))

	resp := doRequest(t, h, "PUT", "/publishers/acme", `{}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for missing required field on Apply, got %d", resp.StatusCode)
	}
}

func TestOpenAPIIncludesRequiredFields(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	schema := map[string]any{
		"properties": map[string]any{
			"name":     map[string]any{"type": "string"},
			"location": map[string]any{"type": "string"},
		},
		"required": []any{"name"},
	}
	body := map[string]any{
		"singular": "publisher",
		"plural":   "publishers",
		"schema":   schema,
	}
	b, _ := json.Marshal(body)
	resp := doRequest(t, h, "POST", "/aep-resource-definitions?id=publisher", string(b))
	if resp.StatusCode != 200 {
		t.Fatalf("createResource: status %d", resp.StatusCode)
	}

	resp = doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	components := m["components"].(map[string]any)
	schemas := components["schemas"].(map[string]any)
	pub := schemas["publisher"].(map[string]any)
	req, ok := pub["required"]
	if !ok {
		t.Fatal("expected 'required' array in OpenAPI component schema for publisher")
	}
	arr := req.([]any)
	found := false
	for _, v := range arr {
		if v == "name" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'name' in required array, got %v", arr)
	}

	// Also verify the required array is returned in the resource definition GET.
	resp = doRequest(t, h, "GET", "/aep-resource-definitions/publisher", "")
	defn := readJSON(t, resp)
	defSchema := defn["schema"].(map[string]any)
	defReq, ok := defSchema["required"]
	if !ok {
		t.Fatal("expected 'required' in resource definition schema")
	}
	defArr := defReq.([]any)
	found = false
	for _, v := range defArr {
		if v == "name" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'name' in resource definition required array, got %v", defArr)
	}
}

func TestOpenAPIIncludesRequiredFieldsAfterRestart(t *testing.T) {
	dbPath := t.TempDir() + "/required.db"

	// Phase 1: Create resource with required field.
	d1, err := db.Init(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	s1 := aepbase.NewState(d1, "http://localhost:8080")
	h1 := s1.Handler()

	schema := map[string]any{
		"properties": map[string]any{
			"name":     map[string]any{"type": "string"},
			"location": map[string]any{"type": "string"},
		},
		"required": []any{"name"},
	}
	body := map[string]any{
		"singular": "publisher",
		"plural":   "publishers",
		"schema":   schema,
	}
	b, _ := json.Marshal(body)
	resp := doRequest(t, h1, "POST", "/aep-resource-definitions?id=publisher", string(b))
	if resp.StatusCode != 200 {
		t.Fatalf("createResource: status %d", resp.StatusCode)
	}
	d1.Close()

	// Phase 2: Reopen and recover.
	d2, err := db.Init(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	s2 := aepbase.NewState(d2, "http://localhost:8080")
	defs, err := meta.LoadAll(d2)
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range defs {
		if err := s2.AddResource(def); err != nil {
			t.Fatalf("AddResource on recovery: %v", err)
		}
	}
	h2 := s2.Handler()

	// Verify required survives restart in OpenAPI.
	resp = doRequest(t, h2, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	components := m["components"].(map[string]any)
	schemas := components["schemas"].(map[string]any)
	pub := schemas["publisher"].(map[string]any)
	req, ok := pub["required"]
	if !ok {
		t.Fatal("expected 'required' in OpenAPI schema after restart")
	}
	arr := req.([]any)
	found := false
	for _, v := range arr {
		if v == "name" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'name' in required array after restart, got %v", arr)
	}
}

// --- Data Type Validation Tests ---

func TestCreateRejectsWrongType(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":       map[string]any{"type": "string"},
		"book_count": map[string]any{"type": "integer"},
		"active":     map[string]any{"type": "boolean"},
	})

	// String field with integer value.
	resp := doRequest(t, h, "POST", "/publishers?id=acme", `{"name":123,"book_count":5,"active":true}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for wrong type (string got number), got %d", resp.StatusCode)
	}

	// Integer field with string value.
	resp = doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","book_count":"five","active":true}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for wrong type (integer got string), got %d", resp.StatusCode)
	}

	// Boolean field with string value.
	resp = doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","book_count":5,"active":"yes"}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for wrong type (boolean got string), got %d", resp.StatusCode)
	}
}

func TestCreateAcceptsCorrectTypes(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":       map[string]any{"type": "string"},
		"book_count": map[string]any{"type": "integer"},
		"active":     map[string]any{"type": "boolean"},
		"rating":     map[string]any{"type": "number"},
	})

	resp := doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","book_count":5,"active":true,"rating":4.5}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
}

func TestUpdateRejectsWrongType(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	resp := doRequest(t, h, "PATCH", "/publishers/acme", `{"name":42}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for wrong type on update, got %d", resp.StatusCode)
	}
}

func TestCreateRejectsFloatForInteger(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"book_count": map[string]any{"type": "integer"},
	})

	resp := doRequest(t, h, "POST", "/publishers?id=acme", `{"book_count":3.14}`)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for float in integer field, got %d", resp.StatusCode)
	}
}

// --- ReadOnly Field Enforcement Tests ---

func TestCreateStripsReadOnlyFields(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	// Create resource where "status" is read-only.
	schema := map[string]any{
		"properties": map[string]any{
			"name":   map[string]any{"type": "string"},
			"status": map[string]any{"type": "string", "readOnly": true},
		},
	}
	body := map[string]any{
		"singular": "publisher",
		"plural":   "publishers",
		"schema":   schema,
	}
	b, _ := json.Marshal(body)
	doRequest(t, h, "POST", "/aep-resource-definitions?id=publisher", string(b))

	// Try creating with the read-only field — should be silently stripped.
	resp := doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","status":"active"}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	// status should not be set (or be nil).
	if m["status"] != nil {
		t.Errorf("expected status to be nil (read-only stripped), got %v", m["status"])
	}
}

func TestUpdateStripsReadOnlyFields(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	schema := map[string]any{
		"properties": map[string]any{
			"name":   map[string]any{"type": "string"},
			"status": map[string]any{"type": "string", "readOnly": true},
		},
	}
	body := map[string]any{
		"singular": "publisher",
		"plural":   "publishers",
		"schema":   schema,
	}
	b, _ := json.Marshal(body)
	doRequest(t, h, "POST", "/aep-resource-definitions?id=publisher", string(b))

	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)

	// Try updating with a read-only field.
	resp := doRequest(t, h, "PATCH", "/publishers/acme", `{"name":"Acme Corp","status":"archived"}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["name"] != "Acme Corp" {
		t.Errorf("expected name=Acme Corp, got %v", m["name"])
	}
	// status should not have been updated.
	if m["status"] != nil {
		t.Errorf("expected status to remain nil (read-only stripped), got %v", m["status"])
	}
}

// --- List Method Options Tests ---

func TestListWithFilter(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name":     map[string]any{"type": "string"},
		"location": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme","location":"NYC"}`)
	doRequest(t, h, "POST", "/publishers?id=beta", `{"name":"Beta","location":"LA"}`)
	doRequest(t, h, "POST", "/publishers?id=gamma", `{"name":"Gamma","location":"NYC"}`)

	// Filter by location == "NYC" (CEL syntax).
	resp := doRequest(t, h, "GET", "/publishers?filter=location+%3D%3D+'NYC'", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	results := m["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 results for filter location=NYC, got %d", len(results))
	}
}

func TestListWithSkip(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("pub%02d", i)
		doRequest(t, h, "POST", fmt.Sprintf("/publishers?id=%s", id), fmt.Sprintf(`{"name":"Publisher %d"}`, i))
	}

	// Skip first 3 results.
	resp := doRequest(t, h, "GET", "/publishers?skip=3", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	results := m["results"].([]any)
	if len(results) != 2 {
		t.Errorf("expected 2 results after skipping 3 of 5, got %d", len(results))
	}
}

func TestListFilterAppearsInOpenAPI(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	resp := doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	paths := m["paths"].(map[string]any)
	listPath := paths["/publishers"].(map[string]any)
	getOp := listPath["get"].(map[string]any)
	params := getOp["parameters"].([]any)
	foundFilter := false
	foundSkip := false
	for _, p := range params {
		param := p.(map[string]any)
		if param["name"] == "filter" {
			foundFilter = true
		}
		if param["name"] == "skip" {
			foundSkip = true
		}
	}
	if !foundFilter {
		t.Error("expected 'filter' parameter in OpenAPI list operation")
	}
	if !foundSkip {
		t.Error("expected 'skip' parameter in OpenAPI list operation")
	}
}

// --- Parent-Child Relationship Depth Tests ---

func TestGrandchildResources(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	// Create 3-level hierarchy: publisher -> book -> chapter.
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "book", "book", "books", []string{"publisher"}, map[string]any{
		"title": map[string]any{"type": "string"},
	})
	createResource(t, h, "chapter", "chapter", "chapters", []string{"book"}, map[string]any{
		"heading": map[string]any{"type": "string"},
	})

	// Create instances at each level.
	doRequest(t, h, "POST", "/publishers?id=acme", `{"name":"Acme"}`)
	doRequest(t, h, "POST", "/publishers/acme/books?id=go-guide", `{"title":"The Go Guide"}`)
	resp := doRequest(t, h, "POST", "/publishers/acme/books/go-guide/chapters?id=ch1", `{"heading":"Getting Started"}`)
	if resp.StatusCode != 200 {
		m := readJSON(t, resp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, m)
	}
	m := readJSON(t, resp)
	if m["path"] != "publishers/acme/books/go-guide/chapters/ch1" {
		t.Errorf("expected full path for grandchild, got %v", m["path"])
	}

	// Get the grandchild.
	resp = doRequest(t, h, "GET", "/publishers/acme/books/go-guide/chapters/ch1", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 on GET grandchild, got %d", resp.StatusCode)
	}
	m = readJSON(t, resp)
	if m["heading"] != "Getting Started" {
		t.Errorf("expected heading=Getting Started, got %v", m["heading"])
	}
}

func TestGrandchildOpenAPIPaths(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "book", "book", "books", []string{"publisher"}, map[string]any{
		"title": map[string]any{"type": "string"},
	})
	createResource(t, h, "chapter", "chapter", "chapters", []string{"book"}, map[string]any{
		"heading": map[string]any{"type": "string"},
	})

	resp := doRequest(t, h, "GET", "/openapi.json", "")
	m := readJSON(t, resp)
	paths := m["paths"].(map[string]any)
	expected := []string{
		"/publishers/{publisher_id}/books/{book_id}/chapters",
		"/publishers/{publisher_id}/books/{book_id}/chapters/{chapter_id}",
	}
	for _, p := range expected {
		if _, ok := paths[p]; !ok {
			t.Errorf("expected path %s in OpenAPI spec, got paths: %v", p, keys(paths))
		}
	}
}

func TestDeleteResourceWithGrandchildren(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()
	createResource(t, h, "publisher", "publisher", "publishers", nil, map[string]any{
		"name": map[string]any{"type": "string"},
	})
	createResource(t, h, "book", "book", "books", []string{"publisher"}, map[string]any{
		"title": map[string]any{"type": "string"},
	})
	createResource(t, h, "chapter", "chapter", "chapters", []string{"book"}, map[string]any{
		"heading": map[string]any{"type": "string"},
	})

	// Cannot delete book because chapter depends on it.
	resp := doRequest(t, h, "DELETE", "/aep-resource-definitions/book", "")
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 (has children), got %d", resp.StatusCode)
	}

	// Delete chapter first, then book, then publisher.
	resp = doRequest(t, h, "DELETE", "/aep-resource-definitions/chapter", "")
	if resp.StatusCode != 204 {
		t.Errorf("expected 204 deleting chapter, got %d", resp.StatusCode)
	}
	resp = doRequest(t, h, "DELETE", "/aep-resource-definitions/book", "")
	if resp.StatusCode != 204 {
		t.Errorf("expected 204 deleting book, got %d", resp.StatusCode)
	}
	resp = doRequest(t, h, "DELETE", "/aep-resource-definitions/publisher", "")
	if resp.StatusCode != 204 {
		t.Errorf("expected 204 deleting publisher, got %d", resp.StatusCode)
	}
}

// --- Operations Tests ---

func TestOperationsListEmpty(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()

	resp := doRequest(t, h, "GET", "/operations", "")
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	m := readJSON(t, resp)
	results := m["results"].([]any)
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
	}
}

func TestOperationsGetNotFound(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()

	resp := doRequest(t, h, "GET", "/operations/nonexistent", "")
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestAsyncCustomMethod(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()

	// Create a resource.
	createResource(t, h, "book", "book", "books", nil, map[string]any{
		"title": map[string]any{"type": "string"},
	})

	// Register an async custom method.
	err := state.AddCustomMethod("book", "write", aepbase.CustomMethodConfig{
		Method: "POST",
		Async:  true,
		RequestSchema: &openapi.Schema{
			Type:       "object",
			Properties: openapi.Properties{},
		},
		ResponseSchema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"status": {Type: "string"},
			},
		},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"status": "written",
			})
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	// Create a book first.
	doRequest(t, h, "POST", "/books?id=book1", `{"title":"Test Book"}`)

	// Call the async custom method.
	resp := doRequest(t, h, "POST", "/books/book1:write", `{}`)
	if resp.StatusCode != 202 {
		m := readJSON(t, resp)
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, m)
	}

	op := readJSON(t, resp)
	if op["done"] != false {
		t.Errorf("expected done=false, got %v", op["done"])
	}
	opPath, ok := op["path"].(string)
	if !ok || !strings.HasPrefix(opPath, "operations/") {
		t.Fatalf("expected operation path, got %v", op["path"])
	}

	// Poll the operation until done.
	opID := op["id"].(string)
	var finalOp map[string]any
	for range 50 {
		time.Sleep(50 * time.Millisecond)
		pollResp := doRequest(t, h, "GET", "/operations/"+opID, "")
		if pollResp.StatusCode != 200 {
			t.Fatalf("polling operation: status %d", pollResp.StatusCode)
		}
		finalOp = readJSON(t, pollResp)
		if finalOp["done"] == true {
			break
		}
	}
	if finalOp["done"] != true {
		t.Fatalf("operation did not complete in time")
	}

	// Check the response.
	response, ok := finalOp["response"].(map[string]any)
	if !ok {
		t.Fatalf("expected response map, got %v", finalOp["response"])
	}
	if response["status"] != "written" {
		t.Errorf("expected status=written, got %v", response["status"])
	}

	// Verify the operation appears in list.
	listResp := doRequest(t, h, "GET", "/operations", "")
	listBody := readJSON(t, listResp)
	results := listBody["results"].([]any)
	if len(results) != 1 {
		t.Errorf("expected 1 operation in list, got %d", len(results))
	}
}

func TestAsyncCustomMethodError(t *testing.T) {
	state := newTestState(t)
	h := state.Handler()

	createResource(t, h, "book", "book", "books", nil, map[string]any{
		"title": map[string]any{"type": "string"},
	})

	err := state.AddCustomMethod("book", "fail", aepbase.CustomMethodConfig{
		Method: "POST",
		Async:  true,
		RequestSchema: &openapi.Schema{
			Type:       "object",
			Properties: openapi.Properties{},
		},
		ResponseSchema: &openapi.Schema{
			Type:       "object",
			Properties: openapi.Properties{},
		},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    500,
					"message": "something went wrong",
				},
			})
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	doRequest(t, h, "POST", "/books?id=book1", `{"title":"Test"}`)
	resp := doRequest(t, h, "POST", "/books/book1:fail", `{}`)
	if resp.StatusCode != 202 {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	op := readJSON(t, resp)
	opID := op["id"].(string)

	// Poll until done.
	var finalOp map[string]any
	for range 20 {
		time.Sleep(50 * time.Millisecond)
		pollResp := doRequest(t, h, "GET", "/operations/"+opID, "")
		finalOp = readJSON(t, pollResp)
		if finalOp["done"] == true {
			break
		}
	}
	if finalOp["done"] != true {
		t.Fatalf("operation did not complete")
	}

	// Should have an error, no response.
	if finalOp["error"] == nil {
		t.Errorf("expected error to be set")
	}
	errMap := finalOp["error"].(map[string]any)
	if errMap["message"] != "something went wrong" {
		t.Errorf("expected error message, got %v", errMap["message"])
	}
	if finalOp["response"] != nil {
		t.Errorf("expected nil response on error, got %v", finalOp["response"])
	}
}

func TestAsyncCustomMethodPending(t *testing.T) {
	// Test that async custom methods can be registered before the resource exists.
	state := newTestState(t)
	h := state.Handler()

	err := state.AddCustomMethod("book", "write", aepbase.CustomMethodConfig{
		Method: "POST",
		Async:  true,
		RequestSchema: &openapi.Schema{
			Type:       "object",
			Properties: openapi.Properties{},
		},
		ResponseSchema: &openapi.Schema{
			Type: "object",
			Properties: openapi.Properties{
				"status": {Type: "string"},
			},
		},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"status": "done"})
		},
	})
	if err != nil {
		t.Fatalf("AddCustomMethod: %v", err)
	}

	// Now create the resource — the pending method should be applied.
	createResource(t, h, "book", "book", "books", nil, map[string]any{
		"title": map[string]any{"type": "string"},
	})
	doRequest(t, h, "POST", "/books?id=book1", `{"title":"Test"}`)

	resp := doRequest(t, h, "POST", "/books/book1:write", `{}`)
	if resp.StatusCode != 202 {
		m := readJSON(t, resp)
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, m)
	}
}

// Ensure unused imports are consumed.
var _ = (*sql.DB)(nil)
var _ = meta.ResourceDefinition{}
