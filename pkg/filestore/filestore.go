// Package filestore provides on-disk storage for resource file fields.
//
// File field support is an experimental, non-AEP-spec extension of aepbase.
// Files are stored under a root directory (typically {DataDir}/files) in a
// layout that mirrors the resource path:
//
//	{root}/{resource_path}/{field_name}
//
// e.g. data/files/publishers/pub1/books/book1/cover
package filestore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// sanitizeSegment rejects path segments that could escape the files root.
func sanitizeSegment(s string) error {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, "/\\") {
		return fmt.Errorf("invalid path segment %q", s)
	}
	return nil
}

// resolve returns the absolute on-disk path for a field of a resource.
// It validates every path component to prevent directory traversal.
func resolve(root, resourcePath, field string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("filestore root is empty")
	}
	if err := sanitizeSegment(field); err != nil {
		return "", err
	}
	segments := strings.Split(resourcePath, "/")
	for _, seg := range segments {
		if err := sanitizeSegment(seg); err != nil {
			return "", err
		}
	}
	parts := append([]string{root}, segments...)
	parts = append(parts, field)
	return filepath.Join(parts...), nil
}

// Path returns the on-disk path where a resource's file field is stored.
// It does not check whether the file exists.
func Path(root, resourcePath, field string) (string, error) {
	return resolve(root, resourcePath, field)
}

// Write streams the contents of r into the field's file, creating parent
// directories as needed. Any existing file is overwritten.
func Write(root, resourcePath, field string, r io.Reader) (int64, error) {
	p, err := resolve(root, resourcePath, field)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return 0, fmt.Errorf("creating file directory: %w", err)
	}
	f, err := os.Create(p)
	if err != nil {
		return 0, fmt.Errorf("creating file: %w", err)
	}
	defer f.Close()
	n, err := io.Copy(f, r)
	if err != nil {
		return n, fmt.Errorf("writing file: %w", err)
	}
	return n, nil
}

// Exists reports whether a file field exists on disk.
func Exists(root, resourcePath, field string) bool {
	p, err := resolve(root, resourcePath, field)
	if err != nil {
		return false
	}
	_, err = os.Stat(p)
	return err == nil
}

// DeleteAll removes every stored file for a resource. Best-effort: returns the
// first error encountered but does not stop on missing files.
func DeleteAll(root, resourcePath string) error {
	if root == "" {
		return nil
	}
	segments := strings.Split(resourcePath, "/")
	for _, seg := range segments {
		if err := sanitizeSegment(seg); err != nil {
			return err
		}
	}
	dir := filepath.Join(append([]string{root}, segments...)...)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(dir)
}
