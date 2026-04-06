package user

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// HashPassword returns a bcrypt hash of the given password.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// RegisterRoutes registers user CRUD and auth endpoints on the mux.
func RegisterRoutes(mux *http.ServeMux, d *sql.DB) {
	mux.HandleFunc("POST /users", makeCreateHandler(d))
	mux.HandleFunc("GET /users", makeListHandler(d))
	mux.HandleFunc("GET /users/{user_id}", makeGetOrCustomHandler(d))
	mux.HandleFunc("PATCH /users/{user_id}", makeUpdateHandler(d))
	mux.HandleFunc("DELETE /users/{user_id}", makeDeleteHandler(d))
	// Custom methods use POST with colon syntax routed through the user_id param.
	mux.HandleFunc("POST /users/{user_id}", makePostCustomHandler(d))
}

// --- Login / Logout (custom methods) ---

func makePostCustomHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw := r.PathValue("user_id")
		// Custom methods use :methodName in the path value.
		if idx := findColon(raw); idx >= 0 {
			method := raw[idx+1:]
			switch method {
			case "login":
				handleLogin(d, w, r)
			case "logout":
				handleLogout(d, w, r)
			default:
				writeError(w, http.StatusNotFound, fmt.Sprintf("unknown method %q", method))
			}
			return
		}
		writeError(w, http.StatusMethodNotAllowed, "POST not allowed on individual user resource")
	}
}

func makeGetOrCustomHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw := r.PathValue("user_id")
		if idx := findColon(raw); idx >= 0 {
			writeError(w, http.StatusNotFound, fmt.Sprintf("unknown GET method %q", raw[idx+1:]))
			return
		}
		handleGet(d, w, r, raw)
	}
}

func handleLogin(d *sql.DB, w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Email == "" || body.Password == "" {
		writeError(w, http.StatusBadRequest, "email and password are required")
		return
	}

	u, hash, err := GetUserByEmail(d, body.Email)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up user")
		return
	}
	if u == nil {
		writeError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(body.Password)); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}

	token, err := GenerateToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}
	if err := InsertToken(d, token, u.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to store token")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"token": token,
		"user":  u,
	})
}

func handleLogout(d *sql.DB, w http.ResponseWriter, r *http.Request) {
	token := extractBearerToken(r)
	if token == "" {
		writeError(w, http.StatusUnauthorized, "missing Authorization header")
		return
	}
	if err := DeleteToken(d, token); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to revoke token")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{})
}

// --- CRUD handlers ---

func makeCreateHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller := FromContext(r.Context())
		if caller == nil || caller.Type != TypeSuperuser {
			writeError(w, http.StatusForbidden, "only superusers can create users")
			return
		}

		var body struct {
			Email       string `json:"email"`
			DisplayName string `json:"display_name"`
			Password    string `json:"password"`
			Type        string `json:"type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if body.Email == "" {
			writeError(w, http.StatusBadRequest, "email is required")
			return
		}
		if body.Password == "" {
			writeError(w, http.StatusBadRequest, "password is required")
			return
		}
		if body.Type == "" {
			body.Type = TypeRegular
		}
		if body.Type != TypeSuperuser && body.Type != TypeRegular {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("type must be %q or %q", TypeSuperuser, TypeRegular))
			return
		}

		hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to hash password")
			return
		}

		id := r.URL.Query().Get("id")
		if id == "" {
			id = GenerateID()
		}
		now := time.Now().UTC().Format(time.RFC3339)
		u := &User{
			ID:          id,
			Path:        "users/" + id,
			Email:       body.Email,
			DisplayName: body.DisplayName,
			Type:        body.Type,
			CreateTime:  now,
			UpdateTime:  now,
		}

		if err := InsertUser(d, u, string(hash)); err != nil {
			if isUniqueConstraintError(err) {
				writeError(w, http.StatusConflict, "a user with that email or id already exists")
				return
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create user: %v", err))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(u)
	}
}

func handleGet(d *sql.DB, w http.ResponseWriter, r *http.Request, id string) {
	caller := FromContext(r.Context())
	if caller == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	// Regular users can only get their own record.
	if caller.Type != TypeSuperuser && caller.ID != id {
		writeError(w, http.StatusForbidden, "you can only view your own user")
		return
	}

	u, _, err := GetUserByID(d, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get user")
		return
	}
	if u == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("user %q not found", id))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(u)
}

func makeListHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller := FromContext(r.Context())
		if caller == nil || caller.Type != TypeSuperuser {
			writeError(w, http.StatusForbidden, "only superusers can list users")
			return
		}

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

		users, nextToken, err := ListUsers(d, pageSize, pageToken)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list users: %v", err))
			return
		}

		resp := map[string]any{"results": users}
		if nextToken != "" {
			resp["next_page_token"] = nextToken
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func makeUpdateHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller := FromContext(r.Context())
		if caller == nil {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}

		id := r.PathValue("user_id")

		// Regular users can only update their own record.
		if caller.Type != TypeSuperuser && caller.ID != id {
			writeError(w, http.StatusForbidden, "only superusers can update other users")
			return
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		fields := make(map[string]string)
		if v, ok := body["email"].(string); ok && v != "" {
			fields["email"] = v
		}
		if v, ok := body["display_name"].(string); ok {
			fields["display_name"] = v
		}
		// Only superusers can change type.
		if v, ok := body["type"].(string); ok && v != "" {
			if caller.Type != TypeSuperuser {
				writeError(w, http.StatusForbidden, "only superusers can change user type")
				return
			}
			if v != TypeSuperuser && v != TypeRegular {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("type must be %q or %q", TypeSuperuser, TypeRegular))
				return
			}
			fields["type"] = v
		}
		if v, ok := body["password"].(string); ok && v != "" {
			hash, err := bcrypt.GenerateFromPassword([]byte(v), bcrypt.DefaultCost)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "failed to hash password")
				return
			}
			fields["password_hash"] = string(hash)
		}

		if len(fields) == 0 {
			writeError(w, http.StatusBadRequest, "no updatable fields provided")
			return
		}

		now := time.Now().UTC().Format(time.RFC3339)
		if err := UpdateUser(d, id, fields, now); err != nil {
			if isUniqueConstraintError(err) {
				writeError(w, http.StatusConflict, "a user with that email already exists")
				return
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update user: %v", err))
			return
		}

		u, _, err := GetUserByID(d, id)
		if err != nil || u == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("user %q not found", id))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(u)
	}
}

func makeDeleteHandler(d *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller := FromContext(r.Context())
		if caller == nil || caller.Type != TypeSuperuser {
			writeError(w, http.StatusForbidden, "only superusers can delete users")
			return
		}

		id := r.PathValue("user_id")

		// Delete all tokens for this user first.
		if err := DeleteTokensByUser(d, id); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to revoke user tokens")
			return
		}

		deleted, err := DeleteUser(d, id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete user: %v", err))
			return
		}
		if !deleted {
			writeError(w, http.StatusNotFound, fmt.Sprintf("user %q not found", id))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{})
	}
}

// --- helpers ---

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}

func findColon(s string) int {
	for i, c := range s {
		if c == ':' {
			return i
		}
	}
	return -1
}

func isUniqueConstraintError(err error) bool {
	return err != nil && (contains(err.Error(), "UNIQUE constraint failed") || contains(err.Error(), "unique constraint"))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
