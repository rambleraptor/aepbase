package resource

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/aep-dev/aep-lib-go/pkg/api"

	"github.com/rambleraptor/aepbase/pkg/filestore"
)

// FileFieldConfig carries per-resource file-field configuration through
// route registration. Fields is nil/empty when the resource has no file
// fields, in which case file-field behavior is skipped entirely.
type FileFieldConfig struct {
	// Fields is the set of property names that are file fields.
	Fields map[string]bool
	// FilesDir is the on-disk root where file contents are stored.
	FilesDir string
	// ServerURL is the fully-qualified server base URL used to build
	// download URLs in resource responses.
	ServerURL string
}

// HasFileFields reports whether the config has any file fields declared.
func (c FileFieldConfig) HasFileFields() bool {
	return len(c.Fields) > 0
}

// MakeDownloadHandler returns an HTTP handler for the :download custom method.
// Clients POST a JSON body {"field": "<name>"} naming a file field on the
// resource identified by the URL; the handler streams the file bytes back.
func MakeDownloadHandler(d *sql.DB, r *api.Resource, fileFields map[string]bool, filesDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		var payload struct {
			Field string `json:"field"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if payload.Field == "" {
			writeError(w, http.StatusBadRequest, "field is required")
			return
		}
		if !fileFields[payload.Field] {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("field %q is not a file field", payload.Field))
			return
		}

		parentIDs := extractParentIDs(req, r)
		elems := r.PatternElems()
		idParam := strings.Trim(elems[len(elems)-1], "{}")
		id := req.PathValue(idParam)
		path := buildResourcePath(r, parentIDs, id)

		// Verify the resource exists.
		stored, err := Get(d, r.Plural, path, r.Schema)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("database error: %v", err))
			return
		}
		if stored == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("resource %q not found", path))
			return
		}

		diskPath, err := filestore.Path(filesDir, path, payload.Field)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		f, err := os.Open(diskPath)
		if err != nil {
			if os.IsNotExist(err) {
				writeError(w, http.StatusNotFound, fmt.Sprintf("file field %q has no content", payload.Field))
				return
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("opening file: %v", err))
			return
		}
		defer f.Close()

		stat, err := f.Stat()
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("stat file: %v", err))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", payload.Field))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()))
		io.Copy(w, f)
	}
}
