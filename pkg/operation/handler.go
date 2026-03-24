package operation

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// RegisterRoutes registers the GET /operations and GET /operations/{id} endpoints.
func RegisterRoutes(mux *http.ServeMux, d *sql.DB) {
	mux.HandleFunc("GET /operations", makeListHandler(d))
	mux.HandleFunc("GET /operations/{operation_id}", makeGetHandler(d))
}

func makeGetHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("operation_id")
		path := "operations/" + id

		op, err := Get(d, path)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if op == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("operation %q not found", path))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(op.ToMap())
	}
}

func makeListHandler(d *sql.DB) http.HandlerFunc {
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

		results, nextPageToken, err := List(d, pageSize, pageToken)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}

		items := make([]map[string]any, 0, len(results))
		for _, op := range results {
			items = append(items, op.ToMap())
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
