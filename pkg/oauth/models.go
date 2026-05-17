package oauth

// Provider is the per-provider configuration supplied via State.EnableOAuth.
// The library reads nothing from the environment.
type Provider struct {
	// Name appears in the callback URL: /oauth/{Name}/callback.
	Name string

	ClientID     string
	ClientSecret string

	// RedirectURL must resolve to /oauth/{Name}/callback on this server
	// and match what was registered with the provider.
	RedirectURL string

	// SuccessRedirectURL receives the minted token in the URL fragment
	// (#token=...&state=...) so it never appears in access logs.
	SuccessRedirectURL string

	TokenURL    string
	UserInfoURL string

	// AllowRegistration permits the callback to create a new _users row
	// when neither identity nor email matches an existing account.
	AllowRegistration bool
}

// Identity links a provider's user ID to a local _users row.
type Identity struct {
	Provider       string
	ProviderUserID string
	UserID         string
	Email          string
	CreateTime     string
}
