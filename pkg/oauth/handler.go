package oauth

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rambleraptor/aepbase/pkg/user"
)

// noPasswordSentinel is an invalid bcrypt hash; password login always fails
// for OAuth-only users.
const noPasswordSentinel = "!"

// ErrRegistrationDisabled is returned when AllowRegistration is false and no
// existing user matches the provider's identity or email.
var ErrRegistrationDisabled = errors.New("registration disabled for this provider")

func RegisterRoutes(mux *http.ServeMux, d *sql.DB, providers map[string]Provider) {
	mux.HandleFunc("GET /oauth/{provider}/callback", makeCallbackHandler(d, providers))
}

func makeCallbackHandler(d *sql.DB, providers map[string]Provider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("provider")
		provider, ok := providers[name]
		if !ok {
			writeError(w, http.StatusNotFound, fmt.Sprintf("unknown provider %q", name))
			return
		}

		if errParam := r.URL.Query().Get("error"); errParam != "" {
			desc := r.URL.Query().Get("error_description")
			writeError(w, http.StatusBadRequest, fmt.Sprintf("provider returned error: %s: %s", errParam, desc))
			return
		}

		code := r.URL.Query().Get("code")
		state := r.URL.Query().Get("state")
		if code == "" {
			writeError(w, http.StatusBadRequest, "missing code parameter")
			return
		}

		accessToken, err := exchangeCode(provider, code)
		if err != nil {
			writeError(w, http.StatusBadGateway, fmt.Sprintf("code exchange failed: %v", err))
			return
		}

		info, err := fetchUserInfo(provider, accessToken)
		if err != nil {
			writeError(w, http.StatusBadGateway, fmt.Sprintf("userinfo fetch failed: %v", err))
			return
		}
		if info.Sub == "" || info.Email == "" {
			writeError(w, http.StatusBadGateway, "provider userinfo missing required fields (sub, email)")
			return
		}

		u, err := findOrCreateUser(d, provider, info)
		if errors.Is(err, ErrRegistrationDisabled) {
			writeError(w, http.StatusForbidden, "registration is not enabled for this provider; the email is not linked to an existing account")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("user lookup failed: %v", err))
			return
		}

		token, err := user.GenerateToken()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to generate token")
			return
		}
		if err := user.InsertToken(d, token, u.ID); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to store token")
			return
		}

		params := url.Values{}
		params.Set("token", token)
		if state != "" {
			params.Set("state", state)
		}
		http.Redirect(w, r, provider.SuccessRedirectURL+"#"+params.Encode(), http.StatusFound)
	}
}

type userInfo struct {
	Sub   string `json:"sub"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

func exchangeCode(p Provider, code string) (string, error) {
	body := url.Values{}
	body.Set("grant_type", "authorization_code")
	body.Set("code", code)
	body.Set("redirect_uri", p.RedirectURL)
	body.Set("client_id", p.ClientID)
	body.Set("client_secret", p.ClientSecret)

	req, err := http.NewRequest("POST", p.TokenURL, strings.NewReader(body.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("token endpoint %d: %s", resp.StatusCode, string(rb))
	}
	var tr struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(rb, &tr); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("no access_token in response")
	}
	return tr.AccessToken, nil
}

func fetchUserInfo(p Provider, accessToken string) (*userInfo, error) {
	req, err := http.NewRequest("GET", p.UserInfoURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("userinfo endpoint %d: %s", resp.StatusCode, string(rb))
	}
	var info userInfo
	if err := json.Unmarshal(rb, &info); err != nil {
		return nil, fmt.Errorf("decoding userinfo: %w", err)
	}
	// Some providers (e.g. GitHub) return "id" instead of "sub".
	if info.Sub == "" {
		var alt struct {
			ID any `json:"id"`
		}
		if err := json.Unmarshal(rb, &alt); err == nil && alt.ID != nil {
			info.Sub = fmt.Sprint(alt.ID)
		}
	}
	return &info, nil
}

func findOrCreateUser(d *sql.DB, p Provider, info *userInfo) (*user.User, error) {
	ident, err := GetIdentity(d, p.Name, info.Sub)
	if err != nil {
		return nil, fmt.Errorf("get identity: %w", err)
	}
	if ident != nil {
		u, _, err := user.GetUserByID(d, ident.UserID)
		if err != nil {
			return nil, err
		}
		if u == nil {
			return nil, fmt.Errorf("identity references missing user %q", ident.UserID)
		}
		return u, nil
	}

	now := time.Now().UTC().Format(time.RFC3339)

	existing, _, err := user.GetUserByEmail(d, info.Email)
	if err != nil {
		return nil, fmt.Errorf("get user by email: %w", err)
	}
	if existing != nil {
		if err := InsertIdentity(d, &Identity{
			Provider:       p.Name,
			ProviderUserID: info.Sub,
			UserID:         existing.ID,
			Email:          info.Email,
			CreateTime:     now,
		}); err != nil {
			return nil, fmt.Errorf("link identity: %w", err)
		}
		return existing, nil
	}

	if !p.AllowRegistration {
		return nil, ErrRegistrationDisabled
	}

	id := user.GenerateID()
	u := &user.User{
		ID:          id,
		Path:        "users/" + id,
		Email:       info.Email,
		DisplayName: info.Name,
		Type:        user.TypeRegular,
		CreateTime:  now,
		UpdateTime:  now,
	}
	if err := user.InsertUser(d, u, noPasswordSentinel); err != nil {
		return nil, fmt.Errorf("insert user: %w", err)
	}
	if err := InsertIdentity(d, &Identity{
		Provider:       p.Name,
		ProviderUserID: info.Sub,
		UserID:         u.ID,
		Email:          info.Email,
		CreateTime:     now,
	}); err != nil {
		return nil, fmt.Errorf("insert identity: %w", err)
	}
	return u, nil
}

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
