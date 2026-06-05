package dbutil

import (
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

var (
	bucketMeta = []byte("_meta")
	keyVersion = []byte("schema_version")
)

// CheckSchema verifies the database schema version is compatible with the
// current code. On a fresh database, writes the current version. On an
// older version, migrates forward (currently just bumps the version since
// there are no breaking schema changes yet). On a newer version, returns
// an error — the database was written by a newer binary and must not be
// opened by an older one.
//
// dbName is used only in error messages (e.g. "objects.db").
func CheckSchema(db *bbolt.DB, dbName string, codeVersion uint64) error {
	return db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return fmt.Errorf("create _meta bucket: %w", err)
		}

		data := b.Get(keyVersion)
		if data == nil {
			// Fresh database — write current version.
			return putVersion(b, codeVersion)
		}

		if len(data) < 8 {
			return fmt.Errorf("%s: corrupt schema_version (got %d bytes, need 8)", dbName, len(data))
		}
		dbVersion := binary.BigEndian.Uint64(data)

		if dbVersion == codeVersion {
			return nil // exact match
		}

		if dbVersion > codeVersion {
			return fmt.Errorf(
				"%s: schema version %d is newer than this binary supports (max %d); "+
					"upgrade ziggurat or use the binary that created this database",
				dbName, dbVersion, codeVersion,
			)
		}

		// dbVersion < codeVersion — migrate forward.
		// Currently no breaking changes exist, so just bump the version.
		// Future migrations would go here as version-gated switch cases.
		return putVersion(b, codeVersion)
	})
}

// getSchemaVersion reads the schema version from a database. Returns 0
// if the _meta bucket or schema_version key does not exist.
func getSchemaVersion(db *bbolt.DB) (uint64, error) {
	var version uint64
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if b == nil {
			return nil
		}
		data := b.Get(keyVersion)
		if data == nil {
			return nil
		}
		version = binary.BigEndian.Uint64(data)
		return nil
	})
	return version, err
}

func putVersion(b *bbolt.Bucket, v uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return b.Put(keyVersion, buf)
}
