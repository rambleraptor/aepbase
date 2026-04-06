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

### Logout

```sh
curl -X POST http://localhost:8080/users/:logout \
  -H 'Authorization: Bearer a4b4c5d6e7f8...'
```

This revokes the token. The user can log in again to get a new one.

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
