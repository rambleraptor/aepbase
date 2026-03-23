package operation

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Operation represents a long-running operation.
type Operation struct {
	ID         string         `json:"id"`
	Path       string         `json:"path"`
	Done       bool           `json:"done"`
	Error      map[string]any `json:"error"`
	Response   any            `json:"response"`
	CreateTime string         `json:"create_time"`
}

// Insert creates a new operation in the database.
func Insert(d *sql.DB, op *Operation) error {
	var errJSON, respJSON sql.NullString
	if op.Error != nil {
		b, err := json.Marshal(op.Error)
		if err != nil {
			return fmt.Errorf("marshaling error: %w", err)
		}
		errJSON = sql.NullString{String: string(b), Valid: true}
	}
	if op.Response != nil {
		b, err := json.Marshal(op.Response)
		if err != nil {
			return fmt.Errorf("marshaling response: %w", err)
		}
		respJSON = sql.NullString{String: string(b), Valid: true}
	}
	done := 0
	if op.Done {
		done = 1
	}
	_, err := d.Exec(
		`INSERT INTO _operations (id, path, done, error_json, response_json, create_time) VALUES (?, ?, ?, ?, ?, ?)`,
		op.ID, op.Path, done, errJSON, respJSON, op.CreateTime,
	)
	return err
}

// Get retrieves an operation by path.
func Get(d *sql.DB, path string) (*Operation, error) {
	row := d.QueryRow(`SELECT id, path, done, error_json, response_json, create_time FROM _operations WHERE path = ?`, path)
	return scanOperation(row)
}

// GetByID retrieves an operation by ID.
func GetByID(d *sql.DB, id string) (*Operation, error) {
	row := d.QueryRow(`SELECT id, path, done, error_json, response_json, create_time FROM _operations WHERE id = ?`, id)
	return scanOperation(row)
}

func scanOperation(row *sql.Row) (*Operation, error) {
	var op Operation
	var done int
	var errJSON, respJSON sql.NullString
	if err := row.Scan(&op.ID, &op.Path, &done, &errJSON, &respJSON, &op.CreateTime); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	op.Done = done != 0
	if errJSON.Valid {
		json.Unmarshal([]byte(errJSON.String), &op.Error)
	}
	if respJSON.Valid {
		json.Unmarshal([]byte(respJSON.String), &op.Response)
	}
	return &op, nil
}

// MarkDone updates an operation as completed with a response or error.
func MarkDone(d *sql.DB, id string, response any, opErr map[string]any) error {
	var respJSON sql.NullString
	if response != nil {
		b, err := json.Marshal(response)
		if err != nil {
			return fmt.Errorf("marshaling response: %w", err)
		}
		respJSON = sql.NullString{String: string(b), Valid: true}
	}
	var errJSON sql.NullString
	if opErr != nil {
		b, err := json.Marshal(opErr)
		if err != nil {
			return fmt.Errorf("marshaling error: %w", err)
		}
		errJSON = sql.NullString{String: string(b), Valid: true}
	}
	_, err := d.Exec(
		`UPDATE _operations SET done = 1, response_json = ?, error_json = ? WHERE id = ?`,
		respJSON, errJSON, id,
	)
	return err
}

// List returns a paginated list of operations.
func List(d *sql.DB, pageSize int, pageToken string) ([]Operation, string, error) {
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
	query := fmt.Sprintf(`SELECT id, path, done, error_json, response_json, create_time FROM _operations%s ORDER BY id LIMIT ?`, where)
	args = append(args, fetchCount)

	rows, err := d.Query(query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var results []Operation
	for rows.Next() {
		var op Operation
		var done int
		var errJSON, respJSON sql.NullString
		if err := rows.Scan(&op.ID, &op.Path, &done, &errJSON, &respJSON, &op.CreateTime); err != nil {
			return nil, "", err
		}
		op.Done = done != 0
		if errJSON.Valid {
			json.Unmarshal([]byte(errJSON.String), &op.Error)
		}
		if respJSON.Valid {
			json.Unmarshal([]byte(respJSON.String), &op.Response)
		}
		results = append(results, op)
	}

	nextPageToken := ""
	if len(results) > pageSize {
		lastID := results[pageSize-1].ID
		nextPageToken = base64.StdEncoding.EncodeToString([]byte(lastID))
		results = results[:pageSize]
	}

	return results, nextPageToken, nil
}

// GenerateID creates a unique operation ID.
func GenerateID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

// ToMap converts an Operation to a map for JSON serialization.
func (op *Operation) ToMap() map[string]any {
	m := map[string]any{
		"id":          op.ID,
		"path":        op.Path,
		"done":        op.Done,
		"create_time": op.CreateTime,
	}
	if op.Error != nil {
		m["error"] = op.Error
	}
	if op.Response != nil {
		m["response"] = op.Response
	}
	return m
}
