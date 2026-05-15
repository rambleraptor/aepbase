package oauth

// Provider is the per-provider configuration the library consumer supplies
// via State.EnableOAuth. URLs and credentials are not read from environment
// variables by the library — the consumer is responsible for sourcing them.
type Provider struct {
	// Name appears in the callback URL (/oauth/{name}/callback) and as the
	// provider key in the _oauth_identities table. Must be non-empty.
	Name string

	ClientID     string
	ClientSecret string

	// RedirectURL is the URL registered with the provider. It must resolve
	// to /oauth/{Name}/callback on this server.
	RedirectURL string

	// SuccessRedirectURL is where the user is sent after a successful
	// callback. The minted token is appended in the URL fragment as
	// #token=...&state=... (fragments are not sent to servers, so the
	// token does not appear in access logs).
	SuccessRedirectURL string

	Scopes []string

	AuthURL     string
	TokenURL    string
	UserInfoURL string
}

// Identity is a row in _oauth_identities — a link between a provider's
// user ID and a local _users row.
type Identity struct {
	Provider       string
	ProviderUserID string
	UserID         string
	Email          string
	CreateTime     string
}
