package resource

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"database/sql"

	"github.com/aep-dev/aep-lib-go/pkg/api"
	"github.com/aep-dev/aep-lib-go/pkg/openapi"

	"github.com/rambleraptor/aepbase/pkg/db"
	"github.com/rambleraptor/aepbase/pkg/filestore"
	"github.com/rambleraptor/aepbase/pkg/user"
)

// CustomMethodHandler holds the handler and HTTP method for a custom method.
type CustomMethodHandler struct {
	Method  string // "POST" or "GET"
	Handler http.HandlerFunc
}

// FieldEnums maps a field name to the set of allowed string values for that field.
// A nil or empty map means no enum constraints are applied.
type FieldEnums = map[string][]string

// UserScopeConfig controls user-based access scoping for resources that are
// children of the user resource. When Enabled is true, non-superuser requests
// are restricted to resources belonging to the authenticated user.
type UserScopeConfig struct {
	Enabled bool
}

// checkUserScope verifies that the authenticated user is allowed to access
// the resource at the given user_id. Superusers are always allowed.
// Returns true if the request should be rejected (error already written).
func checkUserScope(w http.ResponseWriter, req *http.Request, scope UserScopeConfig) bool {
	if !scope.Enabled {
		return false
	}
	caller := user.FromContext(req.Context())
	if caller == nil {
		return false // no auth context means users aren't enabled
	}
	if caller.Type == user.TypeSuperuser {
		return false
	}
	userID := req.PathValue("user_id")
	if userID != "" && userID != caller.ID {
		writeError(w, http.StatusForbidden, "you do not have access to this resource")
		return true
	}
	return false
}

func RegisterRoutes(mux *http.ServeMux, d *sql.DB, r *api.Resource, customMethods map[string]CustomMethodHandler, enums FieldEnums, files FileFieldConfig, scope UserScopeConfig) {
	elems := r.PatternElems()
	collectionPath := "/" + strings.Join(elems[:len(elems)-1], "/")
	resourcePath := "/" + strings.Join(elems, "/")

	// The last pattern element is the resource ID param, e.g. "{book_id}".
	idParam := strings.Trim(elems[len(elems)-1], "{}")

	mux.HandleFunc("POST "+collectionPath, makeCreateHandler(d, r, enums, files, scope))
	mux.HandleFunc("GET "+collectionPath, makeListHandler(d, r, files, scope))
	mux.HandleFunc("GET "+resourcePath, makeGetOrCustomHandler(d, r, customMethods, idParam, files, scope))
	mux.HandleFunc("POST "+resourcePath, makePostCustomHandler(r, customMethods, idParam))
	mux.HandleFunc("PATCH "+resourcePath, makeUpdateHandler(d, r, idParam, enums, files, scope))
	mux.HandleFunc("PUT "+resourcePath, makeApplyHandler(d, r, idParam, enums, files, scope))
	mux.HandleFunc("DELETE "+resourcePath, makeDeleteHandler(d, r, idParam, files, scope))
}

// RegisterSingletonRoutes registers GET and PATCH routes for a singleton resource.
// Singleton resources have a fixed path like /parents/{parent_id}/singular
// with no collection endpoints and no resource ID in the path.
func RegisterSingletonRoutes(mux *http.ServeMux, d *sql.DB, r *api.Resource, singletonPath string, enums FieldEnums, scope UserScopeConfig) {
	mux.HandleFunc("GET "+singletonPath, makeSingletonGetHandler(d, r, scope))
	mux.HandleFunc("PATCH "+singletonPath, makeSingletonUpdateHandler(d, r, enums, scope))
}

// makeGetOrCustomHandler returns a handler that serves GET for both regular
// resources and GET-based custom methods. Go 1.22's {wildcard} matches
// "id:method" as a single value, so we check for a colon to dispatch.
func makeGetOrCustomHandler(d *sql.DB, r *api.Resource, customMethods map[string]CustomMethodHandler, idParam string, files FileFieldConfig, scope UserScopeConfig) http.HandlerFunc {
	getHandler := makeGetHandler(d, r, idParam, files, scope)
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

func makeCreateHandler(d *sql.DB, r *api.Resource, enums FieldEnums, files FileFieldConfig, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
		// Get or generate ID (needed early to build the resource path for file storage).
		id := req.URL.Query().Get("id")
		if id == "" {
			id = generateID()
		}
		allParentIDs := extractParentIDs(req, r)
		directParentIDs := extractDirectParentIDs(allParentIDs, r)
		path := buildResourcePath(r, allParentIDs, id)

		fields, uploaded, err := readCreateOrApplyBody(req, files, path)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		now := time.Now().UTC().Format(time.RFC3339)

		// Remove standard fields from user data — they are managed by us.
		delete(fields, "id")
		delete(fields, "path")
		delete(fields, "create_time")
		delete(fields, "update_time")

		// Strip read-only fields — clients may not set them.
		stripReadOnlyFields(r.Schema, fields)

		// Record uploaded file fields as on-disk sentinel values so they
		// persist in the DB and the read path knows they exist.
		for name := range uploaded {
			fields[name] = fileFieldSentinel
		}
		// Reject attempts to set file-field values via JSON (they must come
		// from multipart parts).
		for name := range files.Fields {
			if _, ok := uploaded[name]; ok {
				continue
			}
			if v, set := fields[name]; set && v != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("file field %q must be uploaded as a multipart file part, not a JSON value", name))
				return
			}
			delete(fields, name)
		}

		// Validate required fields (file fields are considered present if uploaded).
		if err := validateRequiredWithFiles(r.Schema, fields, files.Fields, uploaded); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate field types (skipping file fields — sentinel strings are internal).
		if err := validateTypesSkipping(r.Schema, fields, files.Fields); err != nil {
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

		writeResourceJSONWithFiles(w, http.StatusOK, stored, r.Schema, files)
	}
}

func makeGetHandler(d *sql.DB, r *api.Resource, idParam string, files FileFieldConfig, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
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

		writeResourceJSONWithFiles(w, http.StatusOK, stored, r.Schema, files)
	}
}

func makeListHandler(d *sql.DB, r *api.Resource, files FileFieldConfig, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
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
			items = append(items, storedToMapWithFiles(&sr, r.Schema, files))
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

func makeUpdateHandler(d *sql.DB, r *api.Resource, idParam string, enums FieldEnums, files FileFieldConfig, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
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

		patch, uploaded, err := readCreateOrApplyBody(req, files, path)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Remove standard fields from patch.
		delete(patch, "id")
		delete(patch, "path")
		delete(patch, "create_time")
		delete(patch, "update_time")

		// Strip read-only fields — clients may not update them.
		stripReadOnlyFields(r.Schema, patch)

		// Record uploaded files as sentinels; reject JSON values for file fields.
		for name := range uploaded {
			patch[name] = fileFieldSentinel
		}
		for name := range files.Fields {
			if _, ok := uploaded[name]; ok {
				continue
			}
			if v, set := patch[name]; set && v != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("file field %q must be uploaded as a multipart file part, not a JSON value", name))
				return
			}
			delete(patch, name)
		}

		// Validate field types on the patch values (skipping file fields).
		if err := validateTypesSkipping(r.Schema, patch, files.Fields); err != nil {
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

		writeResourceJSONWithFiles(w, http.StatusOK, existing, r.Schema, files)
	}
}

// makeApplyHandler returns a handler for the Apply (PUT) method.
// Apply is a declarative create-or-update: if the resource exists it replaces it fully,
// if it doesn't exist it creates it.
func makeApplyHandler(d *sql.DB, r *api.Resource, idParam string, enums FieldEnums, files FileFieldConfig, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
		allParentIDs := extractParentIDs(req, r)
		directParentIDs := extractDirectParentIDs(allParentIDs, r)
		id := req.PathValue(idParam)
		path := buildResourcePath(r, allParentIDs, id)

		fields, uploaded, err := readCreateOrApplyBody(req, files, path)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Remove standard fields — managed by us.
		delete(fields, "id")
		delete(fields, "path")
		delete(fields, "create_time")
		delete(fields, "update_time")

		// Strip read-only fields.
		stripReadOnlyFields(r.Schema, fields)

		for name := range uploaded {
			fields[name] = fileFieldSentinel
		}
		for name := range files.Fields {
			if _, ok := uploaded[name]; ok {
				continue
			}
			if v, set := fields[name]; set && v != nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("file field %q must be uploaded as a multipart file part, not a JSON value", name))
				return
			}
			delete(fields, name)
		}

		// Validate required fields (file fields considered present if uploaded).
		if err := validateRequiredWithFiles(r.Schema, fields, files.Fields, uploaded); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		// Validate field types.
		if err := validateTypesSkipping(r.Schema, fields, files.Fields); err != nil {
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
			writeResourceJSONWithFiles(w, http.StatusOK, existing, r.Schema, files)
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
			writeResourceJSONWithFiles(w, http.StatusOK, stored, r.Schema, files)
		}
	}
}

func makeDeleteHandler(d *sql.DB, r *api.Resource, idParam string, files FileFieldConfig, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
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

		// Best-effort cleanup of any stored file-field contents for this resource.
		if files.HasFileFields() {
			_ = filestore.DeleteAll(files.FilesDir, path)
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// makeSingletonGetHandler returns a handler for GET on a singleton resource.
// If the singleton doesn't exist yet, it is implicitly created with default values.
func makeSingletonGetHandler(d *sql.DB, r *api.Resource, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
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
func makeSingletonUpdateHandler(d *sql.DB, r *api.Resource, enums FieldEnums, scope UserScopeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if checkUserScope(w, req, scope) {
			return
		}
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

// storedToMapWithFiles converts a stored resource to a response map, rewriting
// any file-field values into absolute download URLs. If the on-disk file is
// missing, the field is omitted from the output.
func storedToMapWithFiles(s *StoredResource, schema *openapi.Schema, files FileFieldConfig) map[string]any {
	m := storedToMap(s, schema)
	if !files.HasFileFields() {
		return m
	}
	for name := range files.Fields {
		if !filestore.Exists(files.FilesDir, s.Path, name) {
			delete(m, name)
			continue
		}
		m[name] = fileFieldDownloadURL(files.ServerURL, s.Path, name)
	}
	return m
}

func writeResourceJSONWithFiles(w http.ResponseWriter, status int, s *StoredResource, schema *openapi.Schema, files FileFieldConfig) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(storedToMapWithFiles(s, schema, files))
}

// fileFieldSentinel is the value stored in the DB column for a file field
// to indicate that content has been uploaded. The real bytes live on disk.
const fileFieldSentinel = "1"

// fileFieldDownloadURL builds the URL clients can POST to download a file.
func fileFieldDownloadURL(serverURL, resourcePath, field string) string {
	return fmt.Sprintf("%s/%s:download?field=%s", strings.TrimRight(serverURL, "/"), resourcePath, field)
}

// readCreateOrApplyBody parses either a JSON body or a multipart/form-data
// body into a fields map. For multipart requests it additionally streams any
// file parts matching declared file fields onto disk and returns the set of
// fields that were uploaded.
//
// Multipart layout:
//   - A part named "resource" (or "body") contains a JSON object with the
//     non-file fields. It is optional — if omitted, the fields map is empty.
//   - Any remaining file parts whose form name matches a declared file field
//     are streamed to disk.
func readCreateOrApplyBody(req *http.Request, files FileFieldConfig, resourcePath string) (map[string]any, map[string]bool, error) {
	ct := req.Header.Get("Content-Type")
	mediaType, _, _ := mime.ParseMediaType(ct)

	if mediaType != "multipart/form-data" {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid request body")
		}
		fields := make(map[string]any)
		if len(body) > 0 {
			if err := json.Unmarshal(body, &fields); err != nil {
				return nil, nil, fmt.Errorf("invalid JSON")
			}
		}
		return fields, nil, nil
	}

	if !files.HasFileFields() {
		return nil, nil, fmt.Errorf("resource does not accept multipart uploads")
	}

	mr, err := req.MultipartReader()
	if err != nil {
		return nil, nil, fmt.Errorf("invalid multipart body: %v", err)
	}

	fields := make(map[string]any)
	uploaded := make(map[string]bool)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("reading multipart: %v", err)
		}
		name := part.FormName()
		filename := part.FileName()

		// The JSON body of non-file fields arrives as a form field named
		// "resource" or "body".
		if filename == "" && (name == "resource" || name == "body") {
			data, err := io.ReadAll(part)
			part.Close()
			if err != nil {
				return nil, nil, fmt.Errorf("reading JSON part: %v", err)
			}
			if len(data) > 0 {
				if err := json.Unmarshal(data, &fields); err != nil {
					return nil, nil, fmt.Errorf("invalid JSON in %q part: %v", name, err)
				}
			}
			continue
		}

		// File part matching a declared file field — stream to disk.
		if files.Fields[name] {
			if _, err := filestore.Write(files.FilesDir, resourcePath, name, part); err != nil {
				part.Close()
				return nil, nil, fmt.Errorf("storing file field %q: %v", name, err)
			}
			part.Close()
			uploaded[name] = true
			continue
		}

		// Unknown part — skip to avoid accidental writes.
		part.Close()
	}
	return fields, uploaded, nil
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
