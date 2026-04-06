package user

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// CreateUsersTable creates the _users table if it does not exist.
func CreateUsersTable(d *sql.DB) error {
	_, err := d.Exec(`CREATE TABLE IF NOT EXISTS _users (
		id TEXT PRIMARY KEY,
		path TEXT UNIQUE NOT NULL,
		email TEXT UNIQUE NOT NULL,
		display_name TEXT NOT NULL DEFAULT '',
		type TEXT NOT NULL,
		password_hash TEXT NOT NULL,
		create_time TEXT NOT NULL,
		update_time TEXT NOT NULL
	)`)
	return err
}

// CreateTokensTable creates the _tokens table if it does not exist.
func CreateTokensTable(d *sql.DB) error {
	_, err := d.Exec(`CREATE TABLE IF NOT EXISTS _tokens (
		token TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		create_time TEXT NOT NULL
	)`)
	return err
}

// InsertUser inserts a new user into the database.
func InsertUser(d *sql.DB, u *User, passwordHash string) error {
	_, err := d.Exec(
		"INSERT INTO _users (id, path, email, display_name, type, password_hash, create_time, update_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		u.ID, u.Path, u.Email, u.DisplayName, u.Type, passwordHash, u.CreateTime, u.UpdateTime,
	)
	return err
}

// GetUserByID retrieves a user by ID.
func GetUserByID(d *sql.DB, id string) (*User, string, error) {
	return scanUser(d.QueryRow("SELECT id, path, email, display_name, type, password_hash, create_time, update_time FROM _users WHERE id = ?", id))
}

// GetUserByPath retrieves a user by path.
func GetUserByPath(d *sql.DB, path string) (*User, string, error) {
	return scanUser(d.QueryRow("SELECT id, path, email, display_name, type, password_hash, create_time, update_time FROM _users WHERE path = ?", path))
}

// GetUserByEmail retrieves a user by email.
func GetUserByEmail(d *sql.DB, email string) (*User, string, error) {
	return scanUser(d.QueryRow("SELECT id, path, email, display_name, type, password_hash, create_time, update_time FROM _users WHERE email = ?", email))
}

func scanUser(row *sql.Row) (*User, string, error) {
	var u User
	var passwordHash string
	err := row.Scan(&u.ID, &u.Path, &u.Email, &u.DisplayName, &u.Type, &passwordHash, &u.CreateTime, &u.UpdateTime)
	if err == sql.ErrNoRows {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return &u, passwordHash, nil
}

// ListUsers returns a page of users ordered by ID.
func ListUsers(d *sql.DB, pageSize int, pageToken string) ([]User, string, error) {
	var whereClauses []string
	var args []any

	if pageToken != "" {
		cursor, err := base64.StdEncoding.DecodeString(pageToken)
		if err == nil && len(cursor) > 0 {
			whereClauses = append(whereClauses, "id > ?")
			args = append(args, string(cursor))
		}
	}

	where := ""
	if len(whereClauses) > 0 {
		where = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	fetchCount := pageSize + 1
	query := fmt.Sprintf("SELECT id, path, email, display_name, type, create_time, update_time FROM _users%s ORDER BY id LIMIT ?", where)
	args = append(args, fetchCount)

	rows, err := d.Query(query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var results []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Path, &u.Email, &u.DisplayName, &u.Type, &u.CreateTime, &u.UpdateTime); err != nil {
			return nil, "", err
		}
		results = append(results, u)
	}

	nextPageToken := ""
	if len(results) > pageSize {
		lastID := results[pageSize-1].ID
		nextPageToken = base64.StdEncoding.EncodeToString([]byte(lastID))
		results = results[:pageSize]
	}

	return results, nextPageToken, nil
}

// UpdateUser updates mutable user fields.
func UpdateUser(d *sql.DB, id string, fields map[string]string, updateTime string) error {
	var setClauses []string
	var args []any

	setClauses = append(setClauses, "update_time = ?")
	args = append(args, updateTime)

	for col, val := range fields {
		setClauses = append(setClauses, col+" = ?")
		args = append(args, val)
	}

	args = append(args, id)
	query := fmt.Sprintf("UPDATE _users SET %s WHERE id = ?", strings.Join(setClauses, ", "))
	_, err := d.Exec(query, args...)
	return err
}

// DeleteUser deletes a user by ID. Returns true if a row was deleted.
func DeleteUser(d *sql.DB, id string) (bool, error) {
	result, err := d.Exec("DELETE FROM _users WHERE id = ?", id)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

// CountUsers returns the number of users.
func CountUsers(d *sql.DB) (int, error) {
	var count int
	err := d.QueryRow("SELECT COUNT(*) FROM _users").Scan(&count)
	return count, err
}

// InsertToken stores a new auth token.
func InsertToken(d *sql.DB, token, userID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := d.Exec("INSERT INTO _tokens (token, user_id, create_time) VALUES (?, ?, ?)", token, userID, now)
	return err
}

// GetUserByToken looks up the user associated with a token.
func GetUserByToken(d *sql.DB, token string) (*User, error) {
	row := d.QueryRow(
		"SELECT u.id, u.path, u.email, u.display_name, u.type, u.create_time, u.update_time FROM _users u INNER JOIN _tokens t ON u.id = t.user_id WHERE t.token = ?",
		token,
	)
	var u User
	err := row.Scan(&u.ID, &u.Path, &u.Email, &u.DisplayName, &u.Type, &u.CreateTime, &u.UpdateTime)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// DeleteToken removes a specific token.
func DeleteToken(d *sql.DB, token string) error {
	_, err := d.Exec("DELETE FROM _tokens WHERE token = ?", token)
	return err
}

// DeleteTokensByUser removes all tokens for a user.
func DeleteTokensByUser(d *sql.DB, userID string) error {
	_, err := d.Exec("DELETE FROM _tokens WHERE user_id = ?", userID)
	return err
}

// GenerateToken creates a cryptographically random token string.
func GenerateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// GenerateID creates a hex-encoded nanosecond timestamp ID (matches aepbase pattern).
func GenerateID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
