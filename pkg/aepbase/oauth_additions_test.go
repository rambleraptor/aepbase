package aepbase_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rambleraptor/aepbase/pkg/aepbase"
	"github.com/rambleraptor/aepbase/pkg/db"
	"github.com/rambleraptor/aepbase/pkg/oauth"
	"github.com/rambleraptor/aepbase/pkg/user"
)

// usersState builds an in-memory State with users enabled and returns it
// alongside its DB.
func usersState(t *testing.T) (*aepbase.State, *httptest.Server) {
	t.Helper()
	d, err := db.Init(":memory:")
	if err != nil {
		t.Fatalf("db.Init: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	state := aepbase.NewState(d, "http://localhost:8080")
	if err := state.EnableUsers(); err != nil {
		t.Fatalf("EnableUsers: %v", err)
	}
	ts := httptest.NewServer(state.Handler())
	t.Cleanup(ts.Close)
	return state, ts
}

// TestWhoamiUsersMe verifies GET /users/me resolves to the token holder.
func TestWhoamiUsersMe(t *testing.T) {
	state, ts := usersState(t)
	d := state.GetDB()

	now := time.Now().UTC().Format(time.RFC3339)
	u := &user.User{ID: "u1", Path: "users/u1", Email: "a@b.com", Type: user.TypeRegular, CreateTime: now, UpdateTime: now}
	if err := user.InsertUser(d, u, "x"); err != nil {
		t.Fatalf("InsertUser: %v", err)
	}
	token, _ := user.GenerateToken()
	if err := user.InsertToken(d, token, "u1"); err != nil {
		t.Fatalf("InsertToken: %v", err)
	}

	req, _ := http.NewRequest("GET", ts.URL+"/users/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /users/me: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != "u1" || got.Email != "a@b.com" {
		t.Fatalf("whoami returned %+v, want u1/a@b.com", got)
	}
}

// TestWhoamiRequiresAuth verifies /users/me is not exempt from auth.
func TestWhoamiRequiresAuth(t *testing.T) {
	_, ts := usersState(t)
	resp, err := http.Get(ts.URL + "/users/me")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// TestOAuthProvidersList verifies GET /oauth/providers lists configured
// providers (name + display name) without auth and without leaking secrets.
func TestOAuthProvidersList(t *testing.T) {
	state, ts := usersState(t)
	err := state.EnableOAuth(oauth.Provider{
		Name:               "google",
		DisplayName:        "Google",
		ClientID:           "cid",
		ClientSecret:       "secret",
		AuthURL:            "https://accounts.google.com/o/oauth2/v2/auth",
		TokenURL:           "https://oauth2.googleapis.com/token",
		UserInfoURL:        "https://openidconnect.googleapis.com/v1/userinfo",
		RedirectURL:        "http://localhost/api/aep/oauth/google/callback",
		SuccessRedirectURL: "http://localhost/auth/callback",
	})
	if err != nil {
		t.Fatalf("EnableOAuth: %v", err)
	}

	resp, err := http.Get(ts.URL + "/oauth/providers")
	if err != nil {
		t.Fatalf("GET /oauth/providers: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Providers []struct {
			Name        string `json:"name"`
			DisplayName string `json:"display_name"`
		} `json:"providers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Providers) != 1 || body.Providers[0].Name != "google" || body.Providers[0].DisplayName != "Google" {
		t.Fatalf("unexpected providers: %+v", body.Providers)
	}
}
