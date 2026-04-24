package store

import (
	"archive/tar"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// CreateDeterministicTar writes a deterministic tar archive of dir to w.
// Files are sorted lexicographically, metadata is normalized (uid/gid 0,
// mtime epoch, mode 0644/0755) to ensure identical content produces
// identical hashes.
func CreateDeterministicTar(dir string, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	var paths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	sort.Strings(paths)
	epoch := time.Unix(0, 0)

	for _, rel := range paths {
		abs := filepath.Join(dir, rel)
		info, err := os.Lstat(abs)
		if err != nil {
			return fmt.Errorf("stat %s: %w", rel, err)
		}

		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("header %s: %w", rel, err)
		}

		// Normalize for determinism. Use forward slashes so the tar
		// is identical regardless of OS (filepath.Rel returns backslashes
		// on Windows, but tar entries must use forward slashes per spec).
		hdr.Name = filepath.ToSlash(rel)
		hdr.Uid = 0
		hdr.Gid = 0
		hdr.Uname = ""
		hdr.Gname = ""
		hdr.ModTime = epoch
		hdr.AccessTime = epoch
		hdr.ChangeTime = epoch

		if info.IsDir() {
			hdr.Mode = 0o755
			hdr.Name += "/"
		} else {
			hdr.Mode = 0o644
		}

		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("write header %s: %w", rel, err)
		}

		if !info.IsDir() {
			f, err := os.Open(abs)
			if err != nil {
				return fmt.Errorf("open %s: %w", rel, err)
			}
			if _, err := io.Copy(tw, f); err != nil {
				f.Close()
				return fmt.Errorf("copy %s: %w", rel, err)
			}
			f.Close()
		}
	}

	return nil
}

// ExtractTar extracts a tar archive from r into dest.
func ExtractTar(r io.Reader, dest string) error {
	tr := tar.NewReader(r)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}

		target := filepath.Join(dest, hdr.Name)

		// Prevent path traversal.
		if !filepath.IsLocal(hdr.Name) {
			return fmt.Errorf("invalid tar path: %s", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", hdr.Name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return fmt.Errorf("create %s: %w", hdr.Name, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("write %s: %w", hdr.Name, err)
			}
			f.Close()
		}
	}
	return nil
}
