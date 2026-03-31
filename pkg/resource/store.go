package resource

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
	"github.com/rambleraptor/aepbase/pkg/cel2sql"
	"github.com/rambleraptor/aepbase/pkg/db"
	"github.com/google/cel-go/cel"
)

type StoredResource struct {
	ID         string
	Path       string
	CreateTime string
	UpdateTime string
	Fields     map[string]any
}

func Insert(d *sql.DB, plural string, r *StoredResource, parentIDs map[string]string, schema *openapi.Schema) error {
	tableName := db.SanitizeTableName(plural)
	colNames := []string{"id", "path", "create_time", "update_time"}
	placeholders := []string{"?", "?", "?", "?"}
	values := []any{r.ID, r.Path, r.CreateTime, r.UpdateTime}

	for parentParam, parentID := range parentIDs {
		colNames = append(colNames, db.SanitizeTableName(parentParam))
		placeholders = append(placeholders, "?")
		values = append(values, parentID)
	}

	for _, propName := range schemaPropertyNames(schema) {
		colNames = append(colNames, propName)
		placeholders = append(placeholders, "?")
		values = append(values, r.Fields[propName])
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, strings.Join(colNames, ", "), strings.Join(placeholders, ", "))
	_, err := d.Exec(query, values...)
	return err
}

func Get(d *sql.DB, plural string, path string, schema *openapi.Schema) (*StoredResource, error) {
	tableName := db.SanitizeTableName(plural)
	propNames := schemaPropertyNames(schema)
	selectCols := append([]string{"id", "path", "create_time", "update_time"}, propNames...)

	query := fmt.Sprintf("SELECT %s FROM %s WHERE path = ?", strings.Join(selectCols, ", "), tableName)
	row := d.QueryRow(query, path)

	r := &StoredResource{Fields: make(map[string]any)}
	scanDest := make([]any, len(selectCols))
	scanDest[0] = &r.ID
	scanDest[1] = &r.Path
	scanDest[2] = &r.CreateTime
	scanDest[3] = &r.UpdateTime
	fieldPtrs := make([]any, len(propNames))
	for i := range propNames {
		fieldPtrs[i] = new(any)
		scanDest[4+i] = fieldPtrs[i]
	}

	if err := row.Scan(scanDest...); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	for i, name := range propNames {
		val := *(fieldPtrs[i].(*any))
		r.Fields[name] = coerceFieldValue(val, schema, name)
	}
	return r, nil
}

func List(d *sql.DB, plural string, parentIDs map[string]string, schema *openapi.Schema, pageSize int, pageToken string, skip int, filter string) ([]StoredResource, string, error) {
	tableName := db.SanitizeTableName(plural)
	propNames := schemaPropertyNames(schema)
	selectCols := append([]string{"id", "path", "create_time", "update_time"}, propNames...)

	var whereClauses []string
	var args []any

	for parentParam, parentID := range parentIDs {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", db.SanitizeTableName(parentParam)))
		args = append(args, parentID)
	}

	if pageToken != "" {
		cursor, err := base64.StdEncoding.DecodeString(pageToken)
		if err == nil && len(cursor) > 0 {
			whereClauses = append(whereClauses, "id > ?")
			args = append(args, string(cursor))
		}
	}

	// CEL filter support: parse CEL expression and convert to SQL.
	if filter != "" {
		sqlFilter, err := celFilterToSQL(filter, schema)
		if err != nil {
			return nil, "", fmt.Errorf("invalid filter: %w", err)
		}
		if sqlFilter != "" {
			whereClauses = append(whereClauses, sqlFilter)
		}
	}

	where := ""
	if len(whereClauses) > 0 {
		where = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Fetch enough to handle skip + page + one extra for next-page detection.
	fetchCount := skip + pageSize + 1
	query := fmt.Sprintf("SELECT %s FROM %s%s ORDER BY id LIMIT ?",
		strings.Join(selectCols, ", "), tableName, where)
	args = append(args, fetchCount)

	rows, err := d.Query(query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var results []StoredResource
	for rows.Next() {
		r := StoredResource{Fields: make(map[string]any)}
		scanDest := make([]any, len(selectCols))
		scanDest[0] = &r.ID
		scanDest[1] = &r.Path
		scanDest[2] = &r.CreateTime
		scanDest[3] = &r.UpdateTime
		fieldPtrs := make([]any, len(propNames))
		for i := range propNames {
			fieldPtrs[i] = new(any)
			scanDest[4+i] = fieldPtrs[i]
		}
		if err := rows.Scan(scanDest...); err != nil {
			return nil, "", err
		}
		for i, name := range propNames {
			val := *(fieldPtrs[i].(*any))
			r.Fields[name] = coerceFieldValue(val, schema, name)
		}
		results = append(results, r)
	}

	// Apply skip: drop the first N results.
	if skip > 0 && skip < len(results) {
		results = results[skip:]
	} else if skip >= len(results) {
		return []StoredResource{}, "", nil
	}

	nextPageToken := ""
	if len(results) > pageSize {
		lastID := results[pageSize-1].ID
		nextPageToken = base64.StdEncoding.EncodeToString([]byte(lastID))
		results = results[:pageSize]
	}

	return results, nextPageToken, nil
}

// celFilterToSQL compiles a CEL filter expression and converts it to a SQL
// WHERE clause. The CEL environment is built from the schema properties so
// only known fields can be referenced.
func celFilterToSQL(filter string, schema *openapi.Schema) (string, error) {
	var opts []cel.EnvOption
	for name, prop := range schema.Properties {
		if standardFields[name] {
			continue
		}
		opts = append(opts, cel.Variable(name, schemaTypeToCEL(prop.Type)))
	}
	env, err := cel.NewEnv(opts...)
	if err != nil {
		return "", fmt.Errorf("creating CEL environment: %w", err)
	}
	return cel2sql.Convert(env, filter)
}

// schemaTypeToCEL maps OpenAPI schema types to CEL types.
func schemaTypeToCEL(schemaType string) *cel.Type {
	switch schemaType {
	case "integer":
		return cel.IntType
	case "number":
		return cel.DoubleType
	case "boolean":
		return cel.BoolType
	default:
		return cel.StringType
	}
}

// coerceFieldValue converts SQLite-stored values back to their schema types.
// For example, SQLite stores booleans as integers (0/1), so this converts
// them back to Go bools when the schema type is "boolean".
func coerceFieldValue(val any, schema *openapi.Schema, name string) any {
	prop, ok := schema.Properties[name]
	if !ok {
		return val
	}
	switch prop.Type {
	case "boolean":
		switch v := val.(type) {
		case int64:
			return v != 0
		case float64:
			return v != 0
		}
	case "object", "array":
		if s, ok := val.(string); ok && s != "" {
			return json.RawMessage(s)
		}
	}
	return val
}

func Update(d *sql.DB, plural string, path string, fields map[string]any, updateTime string, schema *openapi.Schema) error {
	tableName := db.SanitizeTableName(plural)
	var setClauses []string
	var args []any

	setClauses = append(setClauses, "update_time = ?")
	args = append(args, updateTime)

	for _, propName := range schemaPropertyNames(schema) {
		if v, ok := fields[propName]; ok {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", propName))
			args = append(args, v)
		}
	}

	args = append(args, path)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE path = ?", tableName, strings.Join(setClauses, ", "))
	_, err := d.Exec(query, args...)
	return err
}

func Delete(d *sql.DB, plural string, path string) (bool, error) {
	tableName := db.SanitizeTableName(plural)
	result, err := d.Exec(fmt.Sprintf("DELETE FROM %s WHERE path = ?", tableName), path)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

var standardFields = map[string]bool{
	"id": true, "path": true, "create_time": true, "update_time": true,
}

func schemaPropertyNames(schema *openapi.Schema) []string {
	var names []string
	for name := range schema.Properties {
		if standardFields[name] {
			continue
		}
		names = append(names, name)
	}
	return names
}
