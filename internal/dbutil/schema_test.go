package dbutil

import (
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
)

func openTestDB(t *testing.T) *bbolt.DB {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "test.db"), 0o644, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCheckSchema_FreshDB(t *testing.T) {
	db := openTestDB(t)

	// Fresh DB should succeed and set the version.
	if err := CheckSchema(db, "test.db", 1); err != nil {
		t.Fatalf("unexpected error on fresh DB: %v", err)
	}

	// Verify version was written.
	got, err := getSchemaVersion(db)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("expected version 1, got %d", got)
	}
}

func TestCheckSchema_SameVersion(t *testing.T) {
	db := openTestDB(t)

	// Set version 3.
	if err := CheckSchema(db, "test.db", 3); err != nil {
		t.Fatal(err)
	}

	// Same version should pass.
	if err := CheckSchema(db, "test.db", 3); err != nil {
		t.Fatalf("unexpected error on same version: %v", err)
	}
}

func TestCheckSchema_OlderVersion(t *testing.T) {
	db := openTestDB(t)

	// Set version 1.
	if err := CheckSchema(db, "test.db", 1); err != nil {
		t.Fatal(err)
	}

	// Opening with code version 2 should migrate (bump to 2).
	if err := CheckSchema(db, "test.db", 2); err != nil {
		t.Fatalf("unexpected error migrating from 1 to 2: %v", err)
	}

	got, err := getSchemaVersion(db)
	if err != nil {
		t.Fatal(err)
	}
	if got != 2 {
		t.Fatalf("expected version 2 after migration, got %d", got)
	}
}

func TestCheckSchema_NewerVersion(t *testing.T) {
	db := openTestDB(t)

	// Set version 5 (as if written by a newer binary).
	if err := CheckSchema(db, "test.db", 5); err != nil {
		t.Fatal(err)
	}

	// Opening with code version 3 should fail — refuse downgrade.
	err := CheckSchema(db, "test.db", 3)
	if err == nil {
		t.Fatal("expected error when DB version is newer than code")
	}
}

func TestGetSchemaVersion_EmptyDB(t *testing.T) {
	db := openTestDB(t)

	// No _meta bucket → version 0.
	got, err := getSchemaVersion(db)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("expected version 0 for empty DB, got %d", got)
	}
}
