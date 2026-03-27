package file

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// RegisterRoutes registers the file content download endpoint.
func RegisterRoutes(mux *http.ServeMux, d *sql.DB) {
	mux.HandleFunc("GET /files/{file_id}", makeDownloadHandler(d))
}

func makeDownloadHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("file_id")

		f, err := Get(d, id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if f == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("file %q not found", id))
			return
		}

		w.Header().Set("Content-Type", f.ContentType)
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, f.Filename))
		w.Header().Set("Content-Length", strconv.FormatInt(f.Size, 10))
		w.Write(f.Content)
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
