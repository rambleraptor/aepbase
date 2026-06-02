# Users

```notice
User support is **off by default**. Please use aepbase as a library
to enable it.
```

aepbase has built-in support for **user authentication**.

Users can be created with basic usernames + passwords.

## Enabling users

User support is a library-only opt-in. Enable it via `ServerOptions`:

```go
err := aepbase.Run(aepbase.ServerOptions{
    Port:        8080,
    DataDir:     "aepbase_data",
    EnableUsers: true,
})
```

Or when using the library directly:

```go
state := aepbase.NewState(d, serverURL)
if err := state.EnableUsers(); err != nil {
    log.Fatal(err)
}
```

On first run, if no users exist, a default superuser is created and its
credentials are printed to stdout:

```
=== DEFAULT SUPERUSER CREATED ===
  Email:    admin@example.com
  Password: 7d337c645cb70980
  Change this password immediately.
=================================
```

## User types

There are two user types:

| Type | Description |
|------|-------------|
| `superuser` | Full access. Can create, list, update, and delete any user. Can access all user-scoped child resources. |
| `regular` | Can only view and update their own user record. Can only access their own child resources. |

## Authentication

### Login

```sh
curl -X POST http://localhost:8080/users/:login \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@example.com","password":"7d337c645cb70980"}'
```

Response:

```json
{
  "token": "a]b4c5d6e7f8...",
  "user": {
    "id": "19abc...",
    "path": "users/19abc...",
    "email": "admin@example.com",
    "display_name": "Admin",
    "type": "superuser",
    "create_time": "2025-01-01T00:00:00Z",
    "update_time": "2025-01-01T00:00:00Z"
  }
}
```

### Using the token

Include the token as a Bearer token in the `Authorization` header on all
subsequent requests:

```sh
curl http://localhost:8080/widgets \
  -H 'Authorization: Bearer a4b4c5d6e7f8...'
```

Requests without a valid token receive `401 Unauthorized`.

### Whoami

A client that holds only a token (for example after an OAuth login, where
the callback hands back a bare token) can resolve the current user with the
`me` alias on the user resource:

```sh
curl http://localhost:8080/users/me \
  -H 'Authorization: Bearer a4b4c5d6e7f8...'
```

This returns the authenticated user's record — identical to
`GET /users/{id}` for that user's own id. Like every other endpoint except
login, it requires a valid token.

### Logout

```sh
curl -X POST http://localhost:8080/users/:logout \
  -H 'Authorization: Bearer a4b4c5d6e7f8...'
```

This revokes the token. The user can log in again to get a new one.

## OAuth

aepbase can mint the same bearer token from an OAuth 2.0 provider
(Google, GitHub, etc.). The library reads no credentials from the
environment — the consumer supplies them via `State.EnableOAuth`.

### Enabling OAuth

OAuth requires users to be enabled first. Register one or more providers:

```go
state := aepbase.NewState(d, "https://yourapi.example.com")
if err := state.EnableUsers(); err != nil { log.Fatal(err) }

if err := state.EnableOAuth(oauth.Provider{
    Name:               "google",
    DisplayName:        "Google",
    ClientID:           os.Getenv("GOOGLE_CLIENT_ID"),
    ClientSecret:       os.Getenv("GOOGLE_CLIENT_SECRET"),
    RedirectURL:        "https://yourapi.example.com/oauth/google/callback",
    SuccessRedirectURL: "https://yourapp.example.com/auth/callback",
    Scopes:             []string{"openid", "email", "profile"},
    AuthURL:            "https://accounts.google.com/o/oauth2/v2/auth",
    TokenURL:           "https://oauth2.googleapis.com/token",
    UserInfoURL:        "https://openidconnect.googleapis.com/v1/userinfo",
    AllowRegistration:  true,
}); err != nil {
    log.Fatal(err)
}
```

`RedirectURL` must resolve to `/oauth/{Name}/callback` on this server and
match what was registered with the provider. `SuccessRedirectURL` is
where the user is sent after a successful login.

### The flow

These routes are exposed (only when at least one provider is registered):

| Route | Purpose |
|-------|---------|
| `GET /oauth/providers` | Lists configured providers as `{name, display_name}` so a frontend can render buttons without hard-coding them (no secrets exposed; unauthenticated) |
| `GET /oauth/{provider}/start` | Sets a CSRF cookie, 302s to the provider's authorize URL |
| `GET /oauth/{provider}/callback` | Verifies the cookie, exchanges the code, mints a token, 302s to `SuccessRedirectURL` |

The frontend just needs a link:

```html
<a href="https://yourapi.example.com/oauth/google/start">Sign in with Google</a>
```

`GET /oauth/providers` returns the configured providers, using each
provider's `DisplayName` (falling back to `Name`) as the button label:

```json
{ "providers": [{ "name": "google", "display_name": "Google" }] }
```

After the callback completes, the user lands at
`{SuccessRedirectURL}#token=…`. The token is in the URL **fragment** so
it never appears in server access logs. Read it client-side:

```js
const fragment = new URLSearchParams(window.location.hash.slice(1));
const token = fragment.get("token");
localStorage.setItem("api_token", token);
history.replaceState(null, "", window.location.pathname);
```

The token is the same Bearer token that `POST /users/:login` returns —
use it identically on subsequent requests. Because the callback returns
only a token (not the user record), fetch the signed-in user with
[`GET /users/me`](#whoami) once you have it.

### Account creation and linking

When a callback completes, aepbase resolves the user in three steps:

1. **Identity match.** If `_oauth_identities` has a row for
   `(provider, sub)`, that user signs in.
2. **Email auto-link.** Otherwise, if a local user with the same email
   exists, the new identity is linked to that user. (Useful when a user
   originally signed up with a password and later clicks "Sign in with
   Google".)
3. **New user.** Only if `AllowRegistration: true`. A new local user is
   created with the provider's email and display name; the password
   hash is set to a sentinel that rejects every password attempt.

When `AllowRegistration` is false (the default), step 3 returns **403**.
This is the safer default for deployments that gate account creation —
new users must be provisioned by a superuser first.

### Multiple providers

Call `EnableOAuth` with as many providers as you like. Each gets its
own pair of routes and its own row in `_oauth_identities`. A single
local user can be linked to multiple providers (e.g. Google + GitHub
for the same email).

## User CRUD

The user resource is a standard AEP-resource. There is some authorization baked in:

- Regular users can only update themselves.
- Superusers can create new users and list all users.
- Superusers can update any user.


## User-scoped child resources

Any resource created with `"user"` as a parent is automatically scoped to
the owning user. This means:

- **Regular users** can only access resources under their own user ID
- **Superusers** can access resources under any user

### Example: user preferences

Create a preferences resource as a child of user:

```sh
curl -X POST http://localhost:8080/aep-resource-definitions \
  -H 'Authorization: Bearer <admin-token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "singular": "preference",
    "plural": "preferences",
    "parents": ["user"],
    "schema": {
      "properties": {
        "theme": {"type": "string"},
        "language": {"type": "string"}
      }
    }
  }'
```

Now each user has their own preferences at `/users/{user_id}/preferences`:

```sh
# Alice creates her preferences (using her own token)
curl -X POST http://localhost:8080/users/alice-id/preferences \
  -H 'Authorization: Bearer <alice-token>' \
  -H 'Content-Type: application/json' \
  -d '{"theme": "dark", "language": "en"}'

# Alice can list her own preferences
curl http://localhost:8080/users/alice-id/preferences \
  -H 'Authorization: Bearer <alice-token>'

# Alice CANNOT see Bob's preferences (403 Forbidden)
curl http://localhost:8080/users/bob-id/preferences \
  -H 'Authorization: Bearer <alice-token>'

# An admin CAN see anyone's preferences
curl http://localhost:8080/users/bob-id/preferences \
  -H 'Authorization: Bearer <admin-token>'
```
