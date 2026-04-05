package resource

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"database/sql"

	"github.com/aep-dev/aep-lib-go/pkg/api"
	"github.com/aep-dev/aep-lib-go/pkg/openapi"

	"github.com/rambleraptor/aepbase/pkg/db"
)

// CustomMethodHandler holds the handler and HTTP method for a custom method.
type CustomMethodHandler struct {
	Method  string // "POST" or "GET"
	Handler http.HandlerFunc
}

// FieldEnums maps a field name to the set of allowed string values for that field.
// A nil or empty map means no enum constraints are applied.
type FieldEnums = map[string][]string

func RegisterRoutes(mux *http.ServeMux, d *sql.DB, r *api.Resource, customMethods map[string]CustomMethodHandler, enums FieldEnums) {
	elems := r.PatternElems()
	collectionPath := "/" + strings.Join(elems[:len(elems)-1], "/")
	resourcePath := "/" + strings.Join(elems, "/")

	// The last pattern element is the resource ID param, e.g. "{book_id}".
	idParam := strings.Trim(elems[len(elems)-1], "{}")

	mux.HandleFunc("POST "+collectionPath, makeCreateHandler(d, r, enums))
	mux.HandleFunc("GET "+collectionPath, makeListHandler(d, r))
	mux.HandleFunc("GET "+resourcePath, makeGetOrCustomHandler(d, r, customMethods, idParam))
	mux.HandleFunc("POST "+resourcePath, makePostCustomHandler(r, customMethods, idParam))
	mux.HandleFunc("PATCH "+resourcePath, makeUpdateHandler(d, r, idParam, enums))
	mux.HandleFunc("PUT "+resourcePath, makeApplyHandler(d, r, idParam, enums))
	mux.HandleFunc("DELETE "+resourcePath, makeDeleteHandler(d, r, idParam))
}

// RegisterSingletonRoutes registers GET and PATCH routes for a singleton resource.
// Singleton resources have a fixed path like /parents/{parent_id}/singular
// with no collection endpoints and no resource ID in the path.
func RegisterSingletonRoutes(mux *http.ServeMux, d *sql.DB, r *api.Resource, singletonPath string, enums FieldEnums) {
	mux.HandleFunc("GET "+singletonPath, makeSingletonGetHandler(d, r))
	mux.HandleFunc("PATCH "+singletonPath, makeSingletonUpdateHandler(d, r, enums))
}

// makeGetOrCustomHandler returns a handler that serves GET for both regular
// resources and GET-based custom methods. Go 1.22's {wildcard} matches
// "id:method" as a single value, so we check for a colon to dispatch.
func makeGetOrCustomHandler(d *sql.DB, r *api.Resource, customMethods map[string]CustomMethodHandler, idParam string) http.HandlerFunc {
	getHandler := makeGetHandler(d, r, idParam)
	return func(w http.ResponseWriter, req *http.Request) {
		rawID := req.PathValue(idParam)
		if id, methodName, ok := splitCustomMethod(rawID); ok {
			cm, exists := customMethods[methodName]
			if !exists || cm.Method != "GET" {
				writeError(w, http.StatusNotFound, fmt.Sprintf("custom method %q not found", methodName))
				return
			}
			// Re-set the path value so the handler sees the real ID.
			req.SetPathValue(idParam, id)
			cm.Handler(w, req)
			return
		}
		getHandler(w, req)
	}
}

// makePostCustomHandler returns a handler for POST-based custom methods.
// Regular POST to a resource path is not a standard CRUD operation, so any
// POST to /resource/{id} must be a custom method.
func makePostCustomHandler(r *api.Resource, customMethods map[string]CustomMethodHandler, idParam string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		rawID := req.PathValue(idParam)
		id, methodName, ok := splitCustomMethod(rawID)
		if !ok {
			writeError(w, http.StatusMethodNotAllowed, "POST is not allowed on individual resources; use PATCH to update")
			return
		}
		cm, exists := customMethods[methodName]
		if !exists || cm.Method != "POST" {
			writeError(w, http.StatusNotFound, fmt.Sprintf("custom method %q not found", methodName))
			return
		}
		req.SetPathValue(idParam, id)
		cm.Handler(w, req)
	}
}

// splitCustomMethod checks if rawID contains a ":" and splits it into
// (resourceID, methodName, true). Returns ("", "", false) if no colon.
func splitCustomMethod(rawID string) (string, string, bool) {
	idx := strings.Index(rawID, ":")
	if idx < 0 {
		return "", "", false
	}
	return rawID[:idx], rawID[idx+1:], true
}

func makeCreateHandler(d *sql.DB, r *api.Resource, enums FieldEnums) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		var fields map[string]any
		if err := json.Unmarshal(body, &fields); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}

		// Get or generate ID.
		id := req.URL.Query().Get("id")
		if id == "" {
			id = generateID()
		}

		// Extract parent IDs from path.
		allParentIDs := extractParentIDs(req, r)
		directParentIDs := extractDirectParentIDs(allParentIDs, r)

		// Build the AEP path.
		path := buildResourcePath(r, allParentIDs, id)
		now := time.Now().UTC().Format(time.RFC3339)

		// Remove standard fields from user data — they are managed by us.
		delete(fields, "id")
		delete(fields, "path")
		delete(fields, "create_time")
		delete(fields, "update_time")

		// Strip read-only fields — clients may not set them.
		stripReadOnlyFields(r.Schema, fields)

		// Validate required fields.
		if err := validateRequired(r.Schema, fields); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate field types.
		if err := validateTypes(r.Schema, fields); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate enum constraints.
		if err := validateEnums(enums, fields); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		stored := &StoredResource{
			ID:         id,
			Path:       path,
			CreateTime: now,
			UpdateTime: now,
			Fields:     fields,
		}

		if err := Insert(d, r.Plural, stored, directParentIDs, r.Schema); err != nil {
			if isUniqueConstraintError(err) {
				writeError(w, http.StatusConflict, fmt.Sprintf("resource %q already exists", path))
				return
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create resource: %v", err))
			return
		}

		writeResourceJSON(w, http.StatusOK, stored, r.Schema)
	}
}

func makeGetHandler(d *sql.DB, r *api.Resource, idParam string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		parentIDs := extractParentIDs(req, r)
		id := req.PathValue(idParam)
		path := buildResourcePath(r, parentIDs, id)

		stored, err := Get(d, r.Plural, path, r.Schema)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if stored == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("resource %q not found", path))
			return
		}

		writeResourceJSON(w, http.StatusOK, stored, r.Schema)
	}
}

func makeListHandler(d *sql.DB, r *api.Resource) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		allParentIDs := extractParentIDs(req, r)
		parentIDs := extractDirectParentIDs(allParentIDs, r)

		pageSize := 50
		if ps := req.URL.Query().Get("max_page_size"); ps != "" {
			if n, err := strconv.Atoi(ps); err == nil && n > 0 {
				pageSize = n
				if pageSize > 1000 {
					pageSize = 1000
				}
			}
		}
		pageToken := req.URL.Query().Get("page_token")

		// Skip support: skip N results before returning.
		skip := 0
		if r.Methods.List != nil && r.Methods.List.SupportsSkip {
			if s := req.URL.Query().Get("skip"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 {
					skip = n
				}
			}
		}

		// CEL filter support.
		filter := ""
		if r.Methods.List != nil && r.Methods.List.SupportsFilter {
			filter = req.URL.Query().Get("filter")
		}

		results, nextPageToken, err := List(d, r.Plural, parentIDs, r.Schema, pageSize, pageToken, skip, filter)
		if err != nil {
			if strings.Contains(err.Error(), "invalid filter") {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}

		items := make([]map[string]any, 0, len(results))
		for _, sr := range results {
			items = append(items, storedToMap(&sr, r.Schema))
		}

		resp := map[string]any{
			"results": items,
		}
		if nextPageToken != "" {
			resp["next_page_token"] = nextPageToken
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func makeUpdateHandler(d *sql.DB, r *api.Resource, idParam string, enums FieldEnums) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		parentIDs := extractParentIDs(req, r)
		id := req.PathValue(idParam)
		path := buildResourcePath(r, parentIDs, id)

		existing, err := Get(d, r.Plural, path, r.Schema)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if existing == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("resource %q not found", path))
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		var patch map[string]any
		if err := json.Unmarshal(body, &patch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}

		// Remove standard fields from patch.
		delete(patch, "id")
		delete(patch, "path")
		delete(patch, "create_time")
		delete(patch, "update_time")

		// Strip read-only fields — clients may not update them.
		stripReadOnlyFields(r.Schema, patch)

		// Validate field types on the patch values.
		if err := validateTypes(r.Schema, patch); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate enum constraints on the patch values.
		if err := validateEnums(enums, patch); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Merge patch onto existing fields.
		for k, v := range patch {
			existing.Fields[k] = v
		}

		now := time.Now().UTC().Format(time.RFC3339)
		existing.UpdateTime = now

		if err := Update(d, r.Plural, path, existing.Fields, now, r.Schema); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update resource: %v", err))
			return
		}

		writeResourceJSON(w, http.StatusOK, existing, r.Schema)
	}
}

// makeApplyHandler returns a handler for the Apply (PUT) method.
// Apply is a declarative create-or-update: if the resource exists it replaces it fully,
// if it doesn't exist it creates it.
func makeApplyHandler(d *sql.DB, r *api.Resource, idParam string, enums FieldEnums) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		allParentIDs := extractParentIDs(req, r)
		directParentIDs := extractDirectParentIDs(allParentIDs, r)
		id := req.PathValue(idParam)
		path := buildResourcePath(r, allParentIDs, id)

		body, err := io.ReadAll(req.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		var fields map[string]any
		if err := json.Unmarshal(body, &fields); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}

		// Remove standard fields — managed by us.
		delete(fields, "id")
		delete(fields, "path")
		delete(fields, "create_time")
		delete(fields, "update_time")

		// Strip read-only fields.
		stripReadOnlyFields(r.Schema, fields)

		// Validate required fields.
		if err := validateRequired(r.Schema, fields); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate field types.
		if err := validateTypes(r.Schema, fields); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate enum constraints.
		if err := validateEnums(enums, fields); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		now := time.Now().UTC().Format(time.RFC3339)

		// Check if the resource already exists.
		existing, err := Get(d, r.Plural, path, r.Schema)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}

		if existing != nil {
			// Replace: update all fields (full replacement, not merge).
			existing.Fields = fields
			existing.UpdateTime = now

			if err := Update(d, r.Plural, path, existing.Fields, now, r.Schema); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update resource: %v", err))
				return
			}
			writeResourceJSON(w, http.StatusOK, existing, r.Schema)
		} else {
			// Create new resource.
			stored := &StoredResource{
				ID:         id,
				Path:       path,
				CreateTime: now,
				UpdateTime: now,
				Fields:     fields,
			}
			if err := Insert(d, r.Plural, stored, directParentIDs, r.Schema); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create resource: %v", err))
				return
			}
			writeResourceJSON(w, http.StatusOK, stored, r.Schema)
		}
	}
}

func makeDeleteHandler(d *sql.DB, r *api.Resource, idParam string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		parentIDs := extractParentIDs(req, r)
		id := req.PathValue(idParam)
		path := buildResourcePath(r, parentIDs, id)

		deleted, err := Delete(d, r.Plural, path)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if !deleted {
			writeError(w, http.StatusNotFound, fmt.Sprintf("resource %q not found", path))
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// makeSingletonGetHandler returns a handler for GET on a singleton resource.
// If the singleton doesn't exist yet, it is implicitly created with default values.
func makeSingletonGetHandler(d *sql.DB, r *api.Resource) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		parentIDs := extractSingletonParentIDs(req, r)
		path := buildSingletonPath(r, parentIDs)

		stored, err := Get(d, r.Plural, path, r.Schema)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if stored == nil {
			// Implicit creation: singleton always exists.
			now := time.Now().UTC().Format(time.RFC3339)
			stored = &StoredResource{
				ID:         singletonID(parentIDs, r),
				Path:       path,
				CreateTime: now,
				UpdateTime: now,
				Fields:     make(map[string]any),
			}
			directParentIDs := extractDirectParentIDs(parentIDs, r)
			if err := Insert(d, r.Plural, stored, directParentIDs, r.Schema); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create singleton: %v", err))
				return
			}
		}

		writeSingletonJSON(w, http.StatusOK, stored, r.Schema)
	}
}

// makeSingletonUpdateHandler returns a handler for PATCH on a singleton resource.
// If the singleton doesn't exist yet, it is implicitly created with the patch values.
func makeSingletonUpdateHandler(d *sql.DB, r *api.Resource, enums FieldEnums) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		parentIDs := extractSingletonParentIDs(req, r)
		path := buildSingletonPath(r, parentIDs)

		body, err := io.ReadAll(req.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		var patch map[string]any
		if err := json.Unmarshal(body, &patch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}

		// Remove standard fields from patch.
		delete(patch, "id")
		delete(patch, "path")
		delete(patch, "create_time")
		delete(patch, "update_time")

		// Strip read-only fields.
		stripReadOnlyFields(r.Schema, patch)

		// Validate field types on the patch values.
		if err := validateTypes(r.Schema, patch); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate enum constraints on the patch values.
		if err := validateEnums(enums, patch); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		existing, err := Get(d, r.Plural, path, r.Schema)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}

		now := time.Now().UTC().Format(time.RFC3339)

		if existing == nil {
			// Implicit creation with patch values.
			existing = &StoredResource{
				ID:         singletonID(parentIDs, r),
				Path:       path,
				CreateTime: now,
				UpdateTime: now,
				Fields:     patch,
			}
			directParentIDs := extractDirectParentIDs(parentIDs, r)
			if err := Insert(d, r.Plural, existing, directParentIDs, r.Schema); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create singleton: %v", err))
				return
			}
		} else {
			// Merge patch onto existing fields.
			for k, v := range patch {
				existing.Fields[k] = v
			}
			existing.UpdateTime = now

			if err := Update(d, r.Plural, path, existing.Fields, now, r.Schema); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update singleton: %v", err))
				return
			}
		}

		writeSingletonJSON(w, http.StatusOK, existing, r.Schema)
	}
}

// extractSingletonParentIDs extracts parent IDs for a singleton resource.
// For singletons, the parent resource's PatternElems define the URL params.
func extractSingletonParentIDs(req *http.Request, r *api.Resource) map[string]string {
	parentIDs := make(map[string]string)
	// Walk through all ancestor resources to collect their ID params.
	for _, parentSingular := range r.Parents {
		parentRes := r.API.Resources[parentSingular]
		if parentRes == nil {
			continue
		}
		parentElems := parentRes.PatternElems()
		// Collect all ID params from parent pattern (includes grandparent IDs).
		for i := 1; i < len(parentElems); i += 2 {
			paramName := strings.Trim(parentElems[i], "{}")
			parentIDs[paramName] = req.PathValue(paramName)
		}
	}
	return parentIDs
}

// buildSingletonPath constructs the path for a singleton resource.
// E.g., "users/123/config" for a config singleton under a user.
func buildSingletonPath(r *api.Resource, parentIDs map[string]string) string {
	var parts []string
	// Get the direct parent's pattern elements to build the parent path.
	if len(r.Parents) > 0 {
		parentRes := r.API.Resources[r.Parents[0]]
		if parentRes != nil {
			parentElems := parentRes.PatternElems()
			for i := 0; i < len(parentElems); i += 2 {
				collection := parentElems[i]
				paramName := strings.Trim(parentElems[i+1], "{}")
				parts = append(parts, collection, parentIDs[paramName])
			}
		}
	}
	parts = append(parts, r.Singular)
	return strings.Join(parts, "/")
}

// singletonID generates a unique ID for a singleton instance.
// Uses the direct parent's ID since there's exactly one singleton per parent.
func singletonID(parentIDs map[string]string, r *api.Resource) string {
	if len(r.Parents) > 0 {
		paramName := r.Parents[0] + "_id"
		if id, ok := parentIDs[paramName]; ok {
			return id
		}
	}
	return r.Singular
}

// singletonToMap converts a StoredResource to a map for singleton JSON output.
// Singletons don't include an "id" field per the AEP spec.
func singletonToMap(s *StoredResource, schema *openapi.Schema) map[string]any {
	m := map[string]any{
		"path":        s.Path,
		"create_time": s.CreateTime,
		"update_time": s.UpdateTime,
	}
	for propName := range schema.Properties {
		if v, ok := s.Fields[propName]; ok {
			m[propName] = v
		}
	}
	return m
}

func writeSingletonJSON(w http.ResponseWriter, status int, s *StoredResource, schema *openapi.Schema) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(singletonToMap(s, schema))
}

// extractParentIDs pulls ALL ancestor IDs from the URL path values.
// PatternElems: ["publishers", "{publisher_id}", "books", "{book_id}"]
// Parents are at odd indices before the last two elements.
func extractParentIDs(req *http.Request, r *api.Resource) map[string]string {
	parentIDs := make(map[string]string)
	elems := r.PatternElems()
	// Everything before the last 2 elements is parent pairs.
	for i := 0; i+1 < len(elems)-2; i += 2 {
		paramName := strings.Trim(elems[i+1], "{}")
		parentIDs[paramName] = req.PathValue(paramName)
	}
	return parentIDs
}

// extractDirectParentIDs returns only the direct parent IDs (not grandparents).
// These correspond to the actual foreign key columns in the DB table.
func extractDirectParentIDs(allParentIDs map[string]string, r *api.Resource) map[string]string {
	directIDs := make(map[string]string)
	for _, parentSingular := range r.Parents {
		paramName := parentSingular + "_id"
		if v, ok := allParentIDs[paramName]; ok {
			directIDs[paramName] = v
		}
	}
	return directIDs
}

// buildResourcePath constructs the AEP path like "publishers/pub1/books/book1".
func buildResourcePath(r *api.Resource, parentIDs map[string]string, id string) string {
	var parts []string
	elems := r.PatternElems()
	for i := 0; i < len(elems)-2; i += 2 {
		collection := elems[i]
		paramName := strings.Trim(elems[i+1], "{}")
		parts = append(parts, collection, parentIDs[paramName])
	}
	parts = append(parts, elems[len(elems)-2], id)
	return strings.Join(parts, "/")
}

func storedToMap(s *StoredResource, schema *openapi.Schema) map[string]any {
	m := map[string]any{
		"id":          s.ID,
		"path":        s.Path,
		"create_time": s.CreateTime,
		"update_time": s.UpdateTime,
	}
	for propName := range schema.Properties {
		if v, ok := s.Fields[propName]; ok {
			m[propName] = v
		}
	}
	return m
}

func writeResourceJSON(w http.ResponseWriter, status int, s *StoredResource, schema *openapi.Schema) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(storedToMap(s, schema))
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

func generateID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func isUniqueConstraintError(err error) bool {
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func columnsFromSchema(schema *openapi.Schema) []db.ColumnDef {
	var cols []db.ColumnDef
	for name, prop := range schema.Properties {
		cols = append(cols, db.ColumnDef{
			Name:    name,
			SQLType: db.SchemaTypeToSQLite(prop.Type, prop.Format),
		})
	}
	return cols
}
