package meta

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"encoding/base64"
)

// StateManager is the interface that the central state must implement.
// This avoids a circular import between meta and aepbase.
type StateManager interface {
	GetDB() *sql.DB
	AddResource(def ResourceDefinition) error
	RemoveResource(singular string) error
	UpdateResourceSchema(def ResourceDefinition, oldDef ResourceDefinition) error
}

func RegisterRoutes(mux *http.ServeMux, state StateManager) {
	mux.HandleFunc("POST /aep-resource-definitions", makeCreateHandler(state))
	mux.HandleFunc("GET /aep-resource-definitions", makeListHandler(state))
	mux.HandleFunc("GET /aep-resource-definitions/{id}", makeGetHandler(state))
	mux.HandleFunc("PATCH /aep-resource-definitions/{id}", makeUpdateHandler(state))
	mux.HandleFunc("DELETE /aep-resource-definitions/{id}", makeDeleteHandler(state))
}

func makeCreateHandler(state StateManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		var def ResourceDefinition
		if err := json.Unmarshal(body, &def); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}

		// Auto-detect file fields from schema extensions in the raw JSON.
		// openapi.Schema drops unknown extensions, so we must re-parse the
		// body to find `x-aepbase-file-field: true` markers on properties.
		if detected := extractFileFieldsFromRaw(body); len(detected) > 0 {
			def.FileFields = mergeStringSets(def.FileFields, detected)
		}

		if def.Singular == "" || def.Plural == "" {
			writeError(w, http.StatusBadRequest, "singular and plural are required")
			return
		}
		if strings.HasPrefix(def.Plural, "_") {
			writeError(w, http.StatusBadRequest, "plural must not start with underscore")
			return
		}
		if def.Singleton && len(def.Parents) == 0 {
			writeError(w, http.StatusBadRequest, "singleton resources must have at least one parent")
			return
		}

		// User-settable ID or generate one.
		id := r.URL.Query().Get("id")
		if id == "" {
			id = def.Singular
		}
		def.ID = id
		def.Path = "aep-resource-definitions/" + id
		now := time.Now().UTC().Format(time.RFC3339)
		def.CreateTime = now
		def.UpdateTime = now

		// Validate parents exist.
		for _, parentSingular := range def.Parents {
			existing, err := getDefinition(state.GetDB(), parentSingular)
			if err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("checking parent: %v", err))
				return
			}
			if existing == nil {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("parent resource %q not found", parentSingular))
				return
			}
		}

		// Check uniqueness.
		existing, _ := getDefinitionByID(state.GetDB(), id)
		if existing != nil {
			writeError(w, http.StatusConflict, fmt.Sprintf("definition with id %q already exists", id))
			return
		}

		// Persist to _aep_resource_definitions.
		if err := insertDefinition(state.GetDB(), &def); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("saving definition: %v", err))
			return
		}

		// Register routes and create table.
		if err := state.AddResource(def); err != nil {
			// Rollback the meta row.
			deleteDefinition(state.GetDB(), id)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("registering resource: %v", err))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(def)
	}
}

func makeGetHandler(state StateManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		def, err := getDefinitionByID(state.GetDB(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if def == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("definition %q not found", id))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(def)
	}
}

func makeListHandler(state StateManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pageSize := 50
		if ps := r.URL.Query().Get("max_page_size"); ps != "" {
			if n, err := strconv.Atoi(ps); err == nil && n > 0 {
				pageSize = n
				if pageSize > 1000 {
					pageSize = 1000
				}
			}
		}
		pageToken := r.URL.Query().Get("page_token")

		cursor := ""
		if pageToken != "" {
			decoded, err := base64.StdEncoding.DecodeString(pageToken)
			if err == nil {
				cursor = string(decoded)
			}
		}

		defs, err := listDefinitions(state.GetDB(), pageSize+1, cursor)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}

		nextPageToken := ""
		if len(defs) > pageSize {
			lastID := defs[pageSize-1].ID
			nextPageToken = base64.StdEncoding.EncodeToString([]byte(lastID))
			defs = defs[:pageSize]
		}

		resp := map[string]any{
			"results": defs,
		}
		if nextPageToken != "" {
			resp["next_page_token"] = nextPageToken
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func makeUpdateHandler(state StateManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		existing, err := getDefinitionByID(state.GetDB(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if existing == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("definition %q not found", id))
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		var patch ResourceDefinition
		if err := json.Unmarshal(body, &patch); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}

		// Disallow changing parents.
		if patch.Parents != nil && !stringSliceEqual(patch.Parents, existing.Parents) {
			writeError(w, http.StatusBadRequest, "changing parents is not supported")
			return
		}

		// Disallow renaming singular/plural.
		if patch.Singular != "" && patch.Singular != existing.Singular {
			writeError(w, http.StatusBadRequest, "changing singular name is not supported")
			return
		}
		if patch.Plural != "" && patch.Plural != existing.Plural {
			writeError(w, http.StatusBadRequest, "changing plural name is not supported")
			return
		}

		oldDef := *existing

		// Merge description if provided.
		if patch.Description != "" {
			existing.Description = patch.Description
		}

		// Merge examples if provided.
		if patch.Examples != nil {
			if existing.Examples == nil {
				existing.Examples = make(map[string]any)
			}
			for k, v := range patch.Examples {
				existing.Examples[k] = v
			}
		}

		// Merge schema if provided.
		if patch.Schema.Properties != nil {
			existing.Schema = patch.Schema
		}

		// Replace enums wholesale if provided.
		if patch.Enums != nil {
			existing.Enums = patch.Enums
		}

		// Re-detect file fields from the patched schema body and merge with
		// any explicit file_fields entry from the client.
		if detected := extractFileFieldsFromRaw(body); len(detected) > 0 || patch.FileFields != nil {
			existing.FileFields = mergeStringSets(patch.FileFields, detected)
		}

		now := time.Now().UTC().Format(time.RFC3339)
		existing.UpdateTime = now

		// Update schema in DB and SQLite table.
		if err := state.UpdateResourceSchema(*existing, oldDef); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := updateDefinition(state.GetDB(), existing); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("saving definition: %v", err))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(existing)
	}
}

func makeDeleteHandler(state StateManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		existing, err := getDefinitionByID(state.GetDB(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if existing == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("definition %q not found", id))
			return
		}

		if err := state.RemoveResource(existing.Singular); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := deleteDefinition(state.GetDB(), id); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("deleting definition: %v", err))
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// --- SQL helpers for _aep_resource_definitions ---

func insertDefinition(db *sql.DB, def *ResourceDefinition) error {
	schemaJSON, _ := json.Marshal(def.Schema)
	parentsJSON, _ := json.Marshal(def.Parents)
	examplesJSON, _ := json.Marshal(def.Examples)
	enumsJSON, _ := json.Marshal(def.Enums)
	fileFieldsJSON, _ := json.Marshal(def.FileFields)
	singletonInt := 0
	if def.Singleton {
		singletonInt = 1
	}
	_, err := db.Exec(
		"INSERT INTO _aep_resource_definitions (id, singular, plural, description, examples_json, schema_json, parents_json, enums_json, file_fields_json, singleton, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		def.ID, def.Singular, def.Plural, def.Description, string(examplesJSON), string(schemaJSON), string(parentsJSON), string(enumsJSON), string(fileFieldsJSON), singletonInt, def.CreateTime, def.UpdateTime,
	)
	return err
}

// extractFileFieldsFromRaw scans a raw resource definition JSON body and
// returns the names of all properties marked with x-aepbase-file-field: true.
// openapi.Schema does not preserve unknown extensions during unmarshal, so
// file fields must be discovered from the raw bytes.
func extractFileFieldsFromRaw(body []byte) []string {
	var raw struct {
		Schema struct {
			Properties map[string]map[string]any `json:"properties"`
		} `json:"schema"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	var names []string
	for name, prop := range raw.Schema.Properties {
		if v, ok := prop["x-aepbase-file-field"]; ok {
			if b, ok := v.(bool); ok && b {
				names = append(names, name)
			}
		}
	}
	return names
}

// mergeStringSets returns the union of a and b preserving the order of a
// followed by new elements from b.
func mergeStringSets(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range a {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, s := range b {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func getDefinitionByID(db *sql.DB, id string) (*ResourceDefinition, error) {
	row := db.QueryRow("SELECT id, singular, plural, description, examples_json, schema_json, parents_json, enums_json, file_fields_json, singleton, create_time, update_time FROM _aep_resource_definitions WHERE id = ?", id)
	return scanDefinition(row)
}

func getDefinition(db *sql.DB, singular string) (*ResourceDefinition, error) {
	row := db.QueryRow("SELECT id, singular, plural, description, examples_json, schema_json, parents_json, enums_json, file_fields_json, singleton, create_time, update_time FROM _aep_resource_definitions WHERE singular = ?", singular)
	return scanDefinition(row)
}

func scanDefinition(row *sql.Row) (*ResourceDefinition, error) {
	var def ResourceDefinition
	var schemaJSON, parentsJSON, examplesJSON, enumsJSON, fileFieldsJSON string
	var singletonInt int
	err := row.Scan(&def.ID, &def.Singular, &def.Plural, &def.Description, &examplesJSON, &schemaJSON, &parentsJSON, &enumsJSON, &fileFieldsJSON, &singletonInt, &def.CreateTime, &def.UpdateTime)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(schemaJSON), &def.Schema)
	json.Unmarshal([]byte(parentsJSON), &def.Parents)
	json.Unmarshal([]byte(examplesJSON), &def.Examples)
	if enumsJSON != "" {
		json.Unmarshal([]byte(enumsJSON), &def.Enums)
	}
	if fileFieldsJSON != "" {
		json.Unmarshal([]byte(fileFieldsJSON), &def.FileFields)
	}
	def.Singleton = singletonInt != 0
	def.Path = "aep-resource-definitions/" + def.ID
	return &def, nil
}

func listDefinitions(db *sql.DB, limit int, cursor string) ([]ResourceDefinition, error) {
	var rows *sql.Rows
	var err error
	if cursor != "" {
		rows, err = db.Query(
			"SELECT id, singular, plural, description, examples_json, schema_json, parents_json, enums_json, file_fields_json, singleton, create_time, update_time FROM _aep_resource_definitions WHERE id > ? ORDER BY id LIMIT ?",
			cursor, limit,
		)
	} else {
		rows, err = db.Query(
			"SELECT id, singular, plural, description, examples_json, schema_json, parents_json, enums_json, file_fields_json, singleton, create_time, update_time FROM _aep_resource_definitions ORDER BY id LIMIT ?",
			limit,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var defs []ResourceDefinition
	for rows.Next() {
		var def ResourceDefinition
		var schemaJSON, parentsJSON, examplesJSON, enumsJSON, fileFieldsJSON string
		var singletonInt int
		if err := rows.Scan(&def.ID, &def.Singular, &def.Plural, &def.Description, &examplesJSON, &schemaJSON, &parentsJSON, &enumsJSON, &fileFieldsJSON, &singletonInt, &def.CreateTime, &def.UpdateTime); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(schemaJSON), &def.Schema)
		json.Unmarshal([]byte(parentsJSON), &def.Parents)
		json.Unmarshal([]byte(examplesJSON), &def.Examples)
		if enumsJSON != "" {
			json.Unmarshal([]byte(enumsJSON), &def.Enums)
		}
		if fileFieldsJSON != "" {
			json.Unmarshal([]byte(fileFieldsJSON), &def.FileFields)
		}
		def.Singleton = singletonInt != 0
		def.Path = "aep-resource-definitions/" + def.ID
		defs = append(defs, def)
	}
	return defs, nil
}

func updateDefinition(db *sql.DB, def *ResourceDefinition) error {
	schemaJSON, _ := json.Marshal(def.Schema)
	examplesJSON, _ := json.Marshal(def.Examples)
	enumsJSON, _ := json.Marshal(def.Enums)
	fileFieldsJSON, _ := json.Marshal(def.FileFields)
	_, err := db.Exec(
		"UPDATE _aep_resource_definitions SET description = ?, examples_json = ?, schema_json = ?, enums_json = ?, file_fields_json = ?, update_time = ? WHERE id = ?",
		def.Description, string(examplesJSON), string(schemaJSON), string(enumsJSON), string(fileFieldsJSON), def.UpdateTime, def.ID,
	)
	return err
}

func deleteDefinition(db *sql.DB, id string) error {
	_, err := db.Exec("DELETE FROM _aep_resource_definitions WHERE id = ?", id)
	return err
}

func LoadAll(db *sql.DB) ([]ResourceDefinition, error) {
	return listDefinitions(db, 10000, "")
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

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
