package aepbase_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rambleraptor/aepbase/pkg/aepbase"
	"github.com/rambleraptor/aepbase/pkg/db"
	"github.com/rambleraptor/aepbase/pkg/oauth"
	"github.com/rambleraptor/aepbase/pkg/user"
)

// mockProvider stands in for an external OAuth provider's token and userinfo
// endpoints.
type mockProvider struct {
	server         *httptest.Server
	tokenResponse  string
	userInfoResp   string
	tokenStatus    int
	userInfoStatus int
}

func newMockProvider(t *testing.T) *mockProvider {
	t.Helper()
	mp := &mockProvider{
		tokenResponse:  `{"access_token":"fake-access-token","token_type":"Bearer"}`,
		userInfoResp:   `{"sub":"provider-user-123","email":"alice@example.com","name":"Alice"}`,
		tokenStatus:    http.StatusOK,
		userInfoStatus: http.StatusOK,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(mp.tokenStatus)
		w.Write([]byte(mp.tokenResponse))
	})
	mux.HandleFunc("GET /userinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(mp.userInfoStatus)
		w.Write([]byte(mp.userInfoResp))
	})
	mp.server = httptest.NewServer(mux)
	t.Cleanup(mp.server.Close)
	return mp
}

func (mp *mockProvider) provider(name string) oauth.Provider {
	return oauth.Provider{
		Name:               name,
		ClientID:           "fake-client",
		ClientSecret:       "fake-secret",
		RedirectURL:        "http://localhost:8080/oauth/" + name + "/callback",
		SuccessRedirectURL: "http://app.example.com/auth/done",
		TokenURL:           mp.server.URL + "/token",
		UserInfoURL:        mp.server.URL + "/userinfo",
	}
}

func (mp *mockProvider) providerAllowingRegistration(name string) oauth.Provider {
	p := mp.provider(name)
	p.AllowRegistration = true
	return p
}

func newTestStateWithOAuth(t *testing.T, providers ...oauth.Provider) (*aepbase.State, http.Handler) {
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
	if len(providers) > 0 {
		if err := state.EnableOAuth(providers...); err != nil {
			t.Fatalf("EnableOAuth: %v", err)
		}
	}
	return state, state.Handler()
}

func tokenFromFragment(t *testing.T, location string) string {
	t.Helper()
	idx := strings.Index(location, "#")
	if idx < 0 {
		t.Fatalf("no fragment in Location: %q", location)
	}
	for _, kv := range strings.Split(location[idx+1:], "&") {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 && parts[0] == "token" {
			return parts[1]
		}
	}
	t.Fatalf("token not in fragment: %q", location)
	return ""
}

// --- Tier 1: wire-up / config ---

func TestEnableOAuthRequiresEnableUsers(t *testing.T) {
	state := newTestState(t)
	mp := newMockProvider(t)
	err := state.EnableOAuth(mp.provider("google"))
	if err == nil || !strings.Contains(err.Error(), "EnableUsers") {
		t.Fatalf("expected EnableUsers error, got %v", err)
	}
	if state.OAuthEnabled() {
		t.Fatal("OAuthEnabled should remain false after a failed EnableOAuth")
	}
}

func TestEnableOAuthValidatesFields(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(p *oauth.Provider)
		wantMsg string
	}{
		{"empty name", func(p *oauth.Provider) { p.Name = "" }, "Name is required"},
		{"empty client id", func(p *oauth.Provider) { p.ClientID = "" }, "ClientID"},
		{"empty client secret", func(p *oauth.Provider) { p.ClientSecret = "" }, "ClientSecret"},
		{"empty token url", func(p *oauth.Provider) { p.TokenURL = "" }, "TokenURL"},
		{"empty userinfo url", func(p *oauth.Provider) { p.UserInfoURL = "" }, "UserInfoURL"},
		{"empty redirect url", func(p *oauth.Provider) { p.RedirectURL = "" }, "RedirectURL"},
		{"empty success url", func(p *oauth.Provider) { p.SuccessRedirectURL = "" }, "SuccessRedirectURL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, _ := newTestStateWithUsers(t)
			mp := newMockProvider(t)
			p := mp.provider("google")
			tc.mutate(&p)
			err := state.EnableOAuth(p)
			if err == nil || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("expected error containing %q, got %v", tc.wantMsg, err)
			}
		})
	}
}

func TestOAuthRoutesNotRegisteredWhenDisabled(t *testing.T) {
	state, h := newTestStateWithUsers(t)
	if state.OAuthEnabled() {
		t.Fatal("OAuth should be disabled by default")
	}
	resp := doRequest(t, h, "GET", "/oauth/google/callback?code=x", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 (no route registered), got %d", resp.StatusCode)
	}
}

func TestOAuthRouteRegisteredWhenEnabled(t *testing.T) {
	mp := newMockProvider(t)
	state, h := newTestStateWithOAuth(t, mp.provider("google"))
	if !state.OAuthEnabled() {
		t.Fatal("OAuthEnabled should be true after EnableOAuth")
	}
	// No code: handler returns 400, not the mux's 404.
	resp := doRequest(t, h, "GET", "/oauth/google/callback", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing code, got %d", resp.StatusCode)
	}
}

func TestOAuthMultipleProviders(t *testing.T) {
	mp := newMockProvider(t)
	_, h := newTestStateWithOAuth(t, mp.provider("google"), mp.provider("github"))
	for _, name := range []string{"google", "github"} {
		resp := doRequest(t, h, "GET", "/oauth/"+name+"/callback", "")
		if resp.StatusCode == http.StatusNotFound {
			t.Fatalf("provider %q: expected route registered, got 404", name)
		}
	}
	resp := doRequest(t, h, "GET", "/oauth/notconfigured/callback?code=x", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown provider: expected 404, got %d", resp.StatusCode)
	}
	body := readJSON(t, resp)
	if errMap, _ := body["error"].(map[string]any); errMap == nil {
		t.Fatalf("expected JSON error body, got %v", body)
	}
}

func TestOAuthCallbackExemptFromAuth(t *testing.T) {
	mp := newMockProvider(t)
	_, h := newTestStateWithOAuth(t, mp.provider("google"))
	resp := doAuthRequest(t, h, "GET", "/oauth/google/callback?code=abc", "", "")
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatalf("callback should be exempt from auth, got 401")
	}
}

// --- Tier 2: callback behavior ---

func TestOAuthCallbackCreatesNewUser(t *testing.T) {
	mp := newMockProvider(t)
	state, h := newTestStateWithOAuth(t, mp.providerAllowingRegistration("google"))

	resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc&state=xyz", "")
	if resp.StatusCode != http.StatusFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 302, got %d: %s", resp.StatusCode, body)
	}
	loc := resp.Header.Get("Location")
	if !strings.HasPrefix(loc, "http://app.example.com/auth/done#") {
		t.Fatalf("expected redirect to success URL with fragment, got %q", loc)
	}
	if !strings.Contains(loc, "token=") {
		t.Fatalf("expected token in fragment, got %q", loc)
	}

	u, _, err := user.GetUserByEmail(state.GetDB(), "alice@example.com")
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if u == nil {
		t.Fatal("expected user to be created")
	}
	if u.DisplayName != "Alice" {
		t.Errorf("DisplayName: want Alice, got %q", u.DisplayName)
	}
	if u.Type != user.TypeRegular {
		t.Errorf("Type: want regular, got %q", u.Type)
	}
}

func TestOAuthCallbackReturningUser(t *testing.T) {
	mp := newMockProvider(t)
	state, h := newTestStateWithOAuth(t, mp.providerAllowingRegistration("google"))

	if resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("first callback: expected 302, got %d", resp.StatusCode)
	}
	if resp := doRequest(t, h, "GET", "/oauth/google/callback?code=def", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("second callback: expected 302, got %d", resp.StatusCode)
	}

	n, err := user.CountUsers(state.GetDB())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 users, got %d", n)
	}
}

func TestOAuthAutoLinkByEmail(t *testing.T) {
	mp := newMockProvider(t)
	state, h := newTestStateWithOAuth(t, mp.provider("google"))

	d := state.GetDB()
	hash, err := user.HashPassword("alice-pw")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	existing := &user.User{
		ID:          "alice-existing",
		Path:        "users/alice-existing",
		Email:       "alice@example.com",
		DisplayName: "Alice (password)",
		Type:        user.TypeRegular,
		CreateTime:  "2024-01-01T00:00:00Z",
		UpdateTime:  "2024-01-01T00:00:00Z",
	}
	if err := user.InsertUser(d, existing, hash); err != nil {
		t.Fatalf("insert password user: %v", err)
	}

	if resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("callback: expected 302, got %d", resp.StatusCode)
	}

	u, _, _ := user.GetUserByEmail(d, "alice@example.com")
	if u == nil {
		t.Fatal("user disappeared")
	}
	if u.ID != "alice-existing" {
		t.Fatalf("expected auto-link to existing user, got new user id %q", u.ID)
	}
	if u.DisplayName != "Alice (password)" {
		t.Errorf("expected existing display name preserved, got %q", u.DisplayName)
	}

	n, _ := user.CountUsers(d)
	if n != 2 {
		t.Fatalf("expected 2 users, got %d", n)
	}
}

func TestOAuthStatePassThrough(t *testing.T) {
	mp := newMockProvider(t)
	_, h := newTestStateWithOAuth(t, mp.providerAllowingRegistration("google"))

	resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc&state=my-csrf-token", "")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302, got %d", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, "state=my-csrf-token") {
		t.Fatalf("state not passed through to fragment: %q", loc)
	}

	resp2 := doRequest(t, h, "GET", "/oauth/google/callback?code=abc", "")
	if loc := resp2.Header.Get("Location"); strings.Contains(loc, "state=") {
		t.Fatalf("no state expected in fragment when omitted, got %q", loc)
	}
}

func TestOAuthMintedTokenUsableAsBearer(t *testing.T) {
	mp := newMockProvider(t)
	_, h := newTestStateWithOAuth(t, mp.providerAllowingRegistration("google"))

	resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc", "")
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("callback: expected 302, got %d", resp.StatusCode)
	}
	token := tokenFromFragment(t, resp.Header.Get("Location"))

	// /users requires superuser; alice is regular. 403 means the token
	// authenticated successfully — a 401 would mean it was rejected.
	resp2 := doAuthRequest(t, h, "GET", "/users", "", token)
	if resp2.StatusCode == http.StatusUnauthorized {
		t.Fatalf("minted token rejected as invalid (401)")
	}
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", resp2.StatusCode)
	}
}

func TestOAuthRegistrationDisabledRejectsNewUser(t *testing.T) {
	mp := newMockProvider(t)
	state, h := newTestStateWithOAuth(t, mp.provider("google"))

	resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc", "")
	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403, got %d: %s", resp.StatusCode, body)
	}

	n, err := user.CountUsers(state.GetDB())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected only bootstrap admin, got %d users", n)
	}
}

func TestOAuthRegistrationDisabledStillAllowsLinking(t *testing.T) {
	mp := newMockProvider(t)
	state, h := newTestStateWithOAuth(t, mp.provider("google"))

	d := state.GetDB()
	hash, err := user.HashPassword("alice-pw")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	existing := &user.User{
		ID:          "alice-existing",
		Path:        "users/alice-existing",
		Email:       "alice@example.com",
		DisplayName: "Alice",
		Type:        user.TypeRegular,
		CreateTime:  "2024-01-01T00:00:00Z",
		UpdateTime:  "2024-01-01T00:00:00Z",
	}
	if err := user.InsertUser(d, existing, hash); err != nil {
		t.Fatalf("insert: %v", err)
	}

	resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc", "")
	if resp.StatusCode != http.StatusFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 302 (link to existing user), got %d: %s", resp.StatusCode, body)
	}

	u, _, _ := user.GetUserByEmail(d, "alice@example.com")
	if u == nil || u.ID != "alice-existing" {
		t.Fatalf("expected linked to alice-existing, got %+v", u)
	}

	// Second callback now resolves via identity, not email.
	resp2 := doRequest(t, h, "GET", "/oauth/google/callback?code=def", "")
	if resp2.StatusCode != http.StatusFound {
		t.Fatalf("returning user via identity: expected 302, got %d", resp2.StatusCode)
	}
}

func TestOAuthOnlyUserCannotPasswordLogin(t *testing.T) {
	mp := newMockProvider(t)
	_, h := newTestStateWithOAuth(t, mp.providerAllowingRegistration("google"))

	if resp := doRequest(t, h, "GET", "/oauth/google/callback?code=abc", ""); resp.StatusCode != http.StatusFound {
		t.Fatalf("callback: expected 302, got %d", resp.StatusCode)
	}

	// The sentinel "!" isn't a valid bcrypt hash, so no password attempt
	// — including the sentinel itself — can succeed.
	for _, pw := range []string{"", "!", "alice-pw", "password"} {
		body := fmt.Sprintf(`{"email":"alice@example.com","password":%q}`, pw)
		resp := doAuthRequest(t, h, "POST", "/users/:login", body, "")
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("password login succeeded with %q — must always fail for OAuth-only user", pw)
		}
	}
}
