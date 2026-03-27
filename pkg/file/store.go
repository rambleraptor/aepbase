package file

import (
	"database/sql"
	"fmt"
	"time"
)

// File represents an uploaded file stored in the _files table.
type File struct {
	ID          string
	Filename    string
	ContentType string
	Size        int64
	Content     []byte
	CreateTime  string
}

// Insert stores a new file in the database.
func Insert(d *sql.DB, f *File) error {
	_, err := d.Exec(
		`INSERT INTO _files (id, filename, content_type, size, content, create_time) VALUES (?, ?, ?, ?, ?, ?)`,
		f.ID, f.Filename, f.ContentType, f.Size, f.Content, f.CreateTime,
	)
	return err
}

// Get retrieves a file by ID, including its content.
func Get(d *sql.DB, id string) (*File, error) {
	row := d.QueryRow(`SELECT id, filename, content_type, size, content, create_time FROM _files WHERE id = ?`, id)
	var f File
	err := row.Scan(&f.ID, &f.Filename, &f.ContentType, &f.Size, &f.Content, &f.CreateTime)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// GetMetadata retrieves file metadata by ID without loading the content.
func GetMetadata(d *sql.DB, id string) (*File, error) {
	row := d.QueryRow(`SELECT id, filename, content_type, size, create_time FROM _files WHERE id = ?`, id)
	var f File
	err := row.Scan(&f.ID, &f.Filename, &f.ContentType, &f.Size, &f.CreateTime)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// Delete removes a file by ID.
func Delete(d *sql.DB, id string) error {
	_, err := d.Exec(`DELETE FROM _files WHERE id = ?`, id)
	return err
}

// GenerateID creates a unique file ID.
func GenerateID() string {
	return fmt.Sprintf("f-%x", time.Now().UnixNano())
}

// ToMetadataMap returns file metadata as a map (used in resource JSON responses).
func (f *File) ToMetadataMap() map[string]any {
	return map[string]any{
		"id":           f.ID,
		"filename":     f.Filename,
		"content_type": f.ContentType,
		"size":         f.Size,
	}
}
