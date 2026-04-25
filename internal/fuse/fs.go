package fuse

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// ZigFS implements a FUSE filesystem backed by the Ziggurat object store API.
// Files are accessed via their namespace keys. The directory tree is derived
// from key prefixes (keys containing "/" create virtual directories).
type ZigFS struct {
	fs.Inode
	apiBase string
	client  *http.Client
	log     *slog.Logger
}

// ZigFSConfig holds the configuration for mounting.
type ZigFSConfig struct {
	APIBase    string // e.g. "http://127.0.0.1:7100"
	MountPoint string
	Log        *slog.Logger
}

// Mount mounts the Ziggurat filesystem at the given path. Returns the
// server (call server.Unmount() to clean up) and any error.
func Mount(cfg ZigFSConfig) (*fuse.Server, error) {
	root := &ZigFS{
		apiBase: cfg.APIBase,
		client:  &http.Client{Timeout: 30 * time.Second},
		log:     cfg.Log,
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "ziggurat",
			Name:   "zig",
			Debug:  false,
		},
		AttrTimeout:  &oneSecond,
		EntryTimeout: &oneSecond,
	}

	server, err := fs.Mount(cfg.MountPoint, root, opts)
	if err != nil {
		return nil, err
	}
	return server, nil
}

var oneSecond = 1 * time.Second

// --- Directory operations ---

var _ = (fs.NodeReaddirer)((*ZigFS)(nil))
var _ = (fs.NodeLookuper)((*ZigFS)(nil))

// Readdir lists entries at this level of the namespace hierarchy.
func (z *ZigFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	prefix := z.pathPrefix()
	keys, err := z.listKeys(prefix)
	if err != nil {
		z.log.Warn("readdir failed", "prefix", prefix, "err", err)
		return nil, syscall.EIO
	}

	seen := make(map[string]bool)
	var entries []fuse.DirEntry
	for _, key := range keys {
		// Strip prefix to get relative path.
		rel := strings.TrimPrefix(key, prefix)
		if rel == "" {
			continue
		}

		// If there's a "/" in the remainder, this is a subdirectory.
		if idx := strings.Index(rel, "/"); idx >= 0 {
			dirName := rel[:idx]
			if !seen[dirName] {
				seen[dirName] = true
				entries = append(entries, fuse.DirEntry{
					Name: dirName,
					Mode: syscall.S_IFDIR,
				})
			}
		} else {
			entries = append(entries, fuse.DirEntry{
				Name: rel,
				Mode: syscall.S_IFREG,
			})
		}
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup resolves a child name into a node.
func (z *ZigFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	prefix := z.pathPrefix() + name
	keys, err := z.listKeys(prefix)
	if err != nil {
		return nil, syscall.EIO
	}
	if len(keys) == 0 {
		return nil, syscall.ENOENT
	}

	// Check if this is a file (exact match) or directory (prefix with children).
	isDir := false
	isFile := false
	for _, k := range keys {
		if k == prefix {
			isFile = true
		}
		if strings.HasPrefix(k, prefix+"/") {
			isDir = true
		}
	}

	if isDir {
		child := &ZigFS{
			apiBase: z.apiBase,
			client:  z.client,
			log:     z.log,
		}
		out.Mode = syscall.S_IFDIR | 0o755
		return z.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}

	if isFile {
		child := &zigFile{
			apiBase: z.apiBase,
			key:     prefix,
			client:  z.client,
			log:     z.log,
		}
		out.Mode = syscall.S_IFREG | 0o644
		// We don't cache file sizes — set to 0 and let Read handle it.
		return z.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	}

	// Key exists as a prefix only (directory).
	child := &ZigFS{
		apiBase: z.apiBase,
		client:  z.client,
		log:     z.log,
	}
	out.Mode = syscall.S_IFDIR | 0o755
	return z.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
}

// pathPrefix returns the namespace prefix for this directory node.
func (z *ZigFS) pathPrefix() string {
	path := z.Path(nil)
	if path == "" {
		return ""
	}
	return path + "/"
}

// listKeys calls the store list API with the given prefix.
func (z *ZigFS) listKeys(prefix string) ([]string, error) {
	url := z.apiBase + "/api/v1/store"
	if prefix != "" {
		url += "?prefix=" + prefix
	}
	resp, err := z.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// Parse JSON array of strings.
	return parseStringArray(body)
}

// --- File operations ---

type zigFile struct {
	fs.Inode
	apiBase string
	key     string
	client  *http.Client
	log     *slog.Logger
}

var _ = (fs.NodeOpener)((*zigFile)(nil))
var _ = (fs.NodeReader)((*zigFile)(nil))
var _ = (fs.NodeGetattrer)((*zigFile)(nil))

func (f *zigFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0o644
	// Attempt to get content length.
	size, err := f.contentSize()
	if err == nil {
		out.Size = uint64(size)
	}
	return 0
}

func (f *zigFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *zigFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data, err := f.fetchContent()
	if err != nil {
		f.log.Warn("read failed", "key", f.key, "err", err)
		return nil, syscall.EIO
	}

	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), 0
	}

	return fuse.ReadResultData(data[off:end]), 0
}

func (f *zigFile) fetchContent() ([]byte, error) {
	url := f.apiBase + "/api/v1/store/" + f.key
	resp, err := f.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (f *zigFile) contentSize() (int64, error) {
	url := f.apiBase + "/api/v1/store/" + f.key
	resp, err := f.client.Head(url)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.ContentLength, nil
}

// --- Write support ---

var _ = (fs.NodeCreater)((*ZigFS)(nil))

// Create creates a new file in the store namespace.
func (z *ZigFS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	key := z.pathPrefix() + name
	wf := &zigWriteFile{
		apiBase: z.apiBase,
		key:     key,
		client:  z.client,
		log:     z.log,
	}
	child := &zigFile{
		apiBase: z.apiBase,
		key:     key,
		client:  z.client,
		log:     z.log,
	}
	out.Mode = syscall.S_IFREG | 0o644
	inode := z.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG})
	return inode, wf, fuse.FOPEN_DIRECT_IO, 0
}

type zigWriteFile struct {
	apiBase string
	key     string
	client  *http.Client
	log     *slog.Logger
	buf     bytes.Buffer
}

var _ = (fs.FileWriter)((*zigWriteFile)(nil))
var _ = (fs.FileFlusher)((*zigWriteFile)(nil))

func (wf *zigWriteFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, _ := wf.buf.Write(data)
	return uint32(n), 0
}

func (wf *zigWriteFile) Flush(ctx context.Context) syscall.Errno {
	if wf.buf.Len() == 0 {
		return 0
	}
	url := wf.apiBase + "/api/v1/store/" + wf.key
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, &wf.buf)
	if err != nil {
		wf.log.Warn("flush: create request failed", "key", wf.key, "err", err)
		return syscall.EIO
	}
	resp, err := wf.client.Do(req)
	if err != nil {
		wf.log.Warn("flush: upload failed", "key", wf.key, "err", err)
		return syscall.EIO
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		wf.log.Warn("flush: server error", "key", wf.key, "status", resp.StatusCode)
		return syscall.EIO
	}
	return 0
}

// --- Delete support ---

var _ = (fs.NodeUnlinker)((*ZigFS)(nil))

func (z *ZigFS) Unlink(ctx context.Context, name string) syscall.Errno {
	key := z.pathPrefix() + name
	url := z.apiBase + "/api/v1/store/" + key
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return syscall.EIO
	}
	resp, err := z.client.Do(req)
	if err != nil {
		return syscall.EIO
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return syscall.ENOENT
	}
	return 0
}

// parseStringArray is a minimal JSON string-array parser.
func parseStringArray(data []byte) ([]string, error) {
	s := strings.TrimSpace(string(data))
	if s == "null" || s == "[]" {
		return nil, nil
	}
	// Trim brackets.
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "\"")
		if p != "" {
			result = append(result, p)
		}
	}
	return result, nil
}
