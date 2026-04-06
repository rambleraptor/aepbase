package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

func Init(dbPath string) (*sql.DB, error) {
	if dbPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return nil, fmt.Errorf("creating data directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("enabling WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return nil, fmt.Errorf("setting busy timeout: %w", err)
	}
	if err := createMetaTables(db); err != nil {
		return nil, fmt.Errorf("creating meta tables: %w", err)
	}
	return db, nil
}

func createMetaTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS _aep_resource_definitions (
			id TEXT PRIMARY KEY,
			singular TEXT NOT NULL UNIQUE,
			plural TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL DEFAULT '',
			examples_json TEXT NOT NULL DEFAULT '{}',
			schema_json TEXT NOT NULL,
			parents_json TEXT NOT NULL DEFAULT '[]',
			enums_json TEXT NOT NULL DEFAULT '{}',
			file_fields_json TEXT NOT NULL DEFAULT '[]',
			create_time TEXT NOT NULL,
			update_time TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}
	// Migrate: add columns if missing (existing databases).
	// These are no-ops for new databases since the columns are in the CREATE TABLE.
	db.Exec(`ALTER TABLE _aep_resource_definitions ADD COLUMN description TEXT NOT NULL DEFAULT ''`)
	db.Exec(`ALTER TABLE _aep_resource_definitions ADD COLUMN examples_json TEXT NOT NULL DEFAULT '{}'`)
	db.Exec(`ALTER TABLE _aep_resource_definitions ADD COLUMN singleton INTEGER NOT NULL DEFAULT 0`)
	db.Exec(`ALTER TABLE _aep_resource_definitions ADD COLUMN enums_json TEXT NOT NULL DEFAULT '{}'`)
	db.Exec(`ALTER TABLE _aep_resource_definitions ADD COLUMN file_fields_json TEXT NOT NULL DEFAULT '[]'`)

	// Operations table for long-running operations.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS _operations (
			id TEXT PRIMARY KEY,
			path TEXT NOT NULL UNIQUE,
			done INTEGER NOT NULL DEFAULT 0,
			error_json TEXT,
			response_json TEXT,
			create_time TEXT NOT NULL
		)
	`)
	return err
}

type ParentRef struct {
	Singular string
}

type ColumnDef struct {
	Name    string
	SQLType string
}

func SchemaTypeToSQLite(oasType, oasFormat string) string {
	switch oasType {
	case "integer":
		return "INTEGER"
	case "number":
		return "REAL"
	case "boolean":
		return "INTEGER"
	case "string":
		return "TEXT"
	case "binary":
		// File fields are stored as text (a sentinel marker); the contents
		// live on disk and are served via a custom download method.
		return "TEXT"
	case "object", "array":
		return "TEXT"
	default:
		return "TEXT"
	}
}

func CreateResourceTable(db *sql.DB, plural string, parents []ParentRef, columns []ColumnDef) error {
	tableName := SanitizeTableName(plural)
	cols := []string{
		"id TEXT PRIMARY KEY",
		"path TEXT NOT NULL UNIQUE",
		"create_time TEXT NOT NULL",
		"update_time TEXT NOT NULL",
	}
	for _, p := range parents {
		colName := SanitizeTableName(p.Singular) + "_id"
		cols = append(cols, colName+" TEXT NOT NULL")
	}
	for _, c := range columns {
		cols = append(cols, fmt.Sprintf("%s %s", c.Name, c.SQLType))
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)", tableName, strings.Join(cols, ",\n  "))
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("creating table %s: %w", tableName, err)
	}
	for _, p := range parents {
		colName := SanitizeTableName(p.Singular) + "_id"
		idx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s(%s)", tableName, colName, tableName, colName)
		if _, err := db.Exec(idx); err != nil {
			return fmt.Errorf("creating index: %w", err)
		}
	}
	return nil
}

func DropResourceTable(db *sql.DB, plural string) error {
	tableName := SanitizeTableName(plural)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	return err
}

func AddColumn(db *sql.DB, plural string, col ColumnDef) error {
	tableName := SanitizeTableName(plural)
	_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, col.Name, col.SQLType))
	return err
}

func RemoveColumns(db *sql.DB, plural string, parents []ParentRef, keepColumns []ColumnDef) error {
	tableName := SanitizeTableName(plural)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Build the new column list.
	cols := []string{
		"id TEXT PRIMARY KEY",
		"path TEXT NOT NULL UNIQUE",
		"create_time TEXT NOT NULL",
		"update_time TEXT NOT NULL",
	}
	colNames := []string{"id", "path", "create_time", "update_time"}
	for _, p := range parents {
		colName := SanitizeTableName(p.Singular) + "_id"
		cols = append(cols, colName+" TEXT NOT NULL")
		colNames = append(colNames, colName)
	}
	for _, c := range keepColumns {
		cols = append(cols, fmt.Sprintf("%s %s", c.Name, c.SQLType))
		colNames = append(colNames, c.Name)
	}

	tmpTable := tableName + "_new"
	createSQL := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)", tmpTable, strings.Join(cols, ",\n  "))
	if _, err := tx.Exec(createSQL); err != nil {
		return fmt.Errorf("creating temp table: %w", err)
	}

	copySQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
		tmpTable, strings.Join(colNames, ", "), strings.Join(colNames, ", "), tableName)
	if _, err := tx.Exec(copySQL); err != nil {
		return fmt.Errorf("copying data: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
		return fmt.Errorf("dropping old table: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tmpTable, tableName)); err != nil {
		return fmt.Errorf("renaming table: %w", err)
	}

	// Recreate parent indexes.
	for _, p := range parents {
		colName := SanitizeTableName(p.Singular) + "_id"
		idx := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s(%s)", tableName, colName, tableName, colName)
		if _, err := tx.Exec(idx); err != nil {
			return fmt.Errorf("recreating index: %w", err)
		}
	}

	return tx.Commit()
}

func SanitizeTableName(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}
