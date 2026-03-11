package resource

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aep-dev/aep-lib-go/pkg/openapi"
	"github.com/aep-dev/aepbase/pkg/db"
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

	for parentSingular, parentID := range parentIDs {
		colNames = append(colNames, db.SanitizeTableName(parentSingular)+"_id")
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
		r.Fields[name] = *(fieldPtrs[i].(*any))
	}
	return r, nil
}

func List(d *sql.DB, plural string, parentIDs map[string]string, schema *openapi.Schema, pageSize int, pageToken string) ([]StoredResource, string, error) {
	tableName := db.SanitizeTableName(plural)
	propNames := schemaPropertyNames(schema)
	selectCols := append([]string{"id", "path", "create_time", "update_time"}, propNames...)

	var whereClauses []string
	var args []any

	for parentSingular, parentID := range parentIDs {
		whereClauses = append(whereClauses, fmt.Sprintf("%s_id = ?", db.SanitizeTableName(parentSingular)))
		args = append(args, parentID)
	}

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

	// Fetch one extra to determine if there's a next page.
	query := fmt.Sprintf("SELECT %s FROM %s%s ORDER BY id LIMIT ?",
		strings.Join(selectCols, ", "), tableName, where)
	args = append(args, pageSize+1)

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
			r.Fields[name] = *(fieldPtrs[i].(*any))
		}
		results = append(results, r)
	}

	nextPageToken := ""
	if len(results) > pageSize {
		lastID := results[pageSize-1].ID
		nextPageToken = base64.StdEncoding.EncodeToString([]byte(lastID))
		results = results[:pageSize]
	}

	return results, nextPageToken, nil
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
