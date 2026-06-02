// Package version holds the binary's build metadata, injected at build time
// via -ldflags. It is the single source of truth for the version reported by
// the CLI and advertised as the ziggurat.version node capability.
package version

var (
	// Version is the build version, e.g. "v0.3.0" or, for untagged builds,
	// a git-describe string like "v0.3.0-5-gabc1234-dirty" or a commit hash.
	Version = "dev"
	// Commit is the short git commit hash.
	Commit = "unknown"
)
