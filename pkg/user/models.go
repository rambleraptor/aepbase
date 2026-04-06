package user

// User represents an authenticated user of the API.
type User struct {
	ID          string `json:"id"`
	Path        string `json:"path"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name,omitempty"`
	Type        string `json:"type"`
	CreateTime  string `json:"create_time"`
	UpdateTime  string `json:"update_time"`
}

const (
	TypeSuperuser = "superuser"
	TypeRegular   = "regular"
)

const (
	// UserResourceSingular is the singular name for the built-in user resource.
	UserResourceSingular = "user"
	// UserResourcePlural is the plural name for the built-in user resource.
	UserResourcePlural = "users"
)
