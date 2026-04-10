package user

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
)

type contextKey struct{}

// FromContext extracts the authenticated User from the request context.
// Returns nil if no user is present.
func FromContext(ctx context.Context) *User {
	u, _ := ctx.Value(contextKey{}).(*User)
	return u
}

// WithContext returns a new context carrying the authenticated user.
func WithContext(ctx context.Context, u *User) context.Context {
	return context.WithValue(ctx, contextKey{}, u)
}

// Middleware returns an http.Handler that validates the Authorization header,
// looks up the token in the database, and injects the User into the request
// context. Requests without a valid token receive a 401 response.
//
// The login endpoint (POST /users/:login) and the OpenAPI spec endpoint
// (GET /openapi.json) are exempt from auth.
func Middleware(d *sql.DB) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Allow the login endpoint through without auth.
			if r.Method == http.MethodPost && r.URL.Path == "/users/:login" {
				next.ServeHTTP(w, r)
				return
			}
			// Allow the OpenAPI spec endpoint through without auth.
			if r.Method == http.MethodGet && r.URL.Path == "/openapi.json" {
				next.ServeHTTP(w, r)
				return
			}
			// Allow CORS preflight through.
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}

			token := extractBearerToken(r)
			if token == "" {
				writeAuthError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
				return
			}

			u, err := GetUserByToken(d, token)
			if err != nil {
				writeAuthError(w, http.StatusInternalServerError, "failed to validate token")
				return
			}
			if u == nil {
				writeAuthError(w, http.StatusUnauthorized, "invalid token")
				return
			}

			ctx := WithContext(r.Context(), u)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	const prefix = "Bearer "
	if len(auth) < len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(auth[len(prefix):])
}

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}
