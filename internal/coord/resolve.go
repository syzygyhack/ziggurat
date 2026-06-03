package coord

import (
	"fmt"
	"path"

	"github.com/syzygyhack/ziggurat/internal/model"
	"github.com/syzygyhack/ziggurat/internal/store"
)

// ResolveRefs resolves all namespace keys in a task's InputRefs and Artifacts
// to content hashes at submission time. This makes tasks immune to subsequent
// namespace key reassignment. On failure, all already-incremented refcounts
// are rolled back.
func ResolveRefs(task *model.Task, s *store.Store) error {
	// Track all refs we increment so we can roll back on failure.
	var incrd []string
	rollback := func() {
		for _, h := range incrd {
			s.DecrRef(h)
		}
	}

	// Resolve InputRefs: name -> namespace key becomes name -> content hash.
	resolved := make(map[string]string, len(task.InputRefs))
	for name, nsKey := range task.InputRefs {
		hash, err := s.Resolve(nsKey)
		if err != nil {
			rollback()
			return fmt.Errorf("input %q: %w", name, err)
		}
		if err := s.IncrRef(hash); err != nil {
			rollback()
			return fmt.Errorf("incr ref for input %q: %w", name, err)
		}
		incrd = append(incrd, hash)
		resolved[name] = hash
	}
	task.InputRefs = resolved

	// Resolve Artifacts: namespace key -> content hash. The basename of each
	// namespace key is preserved (parallel slice) so the worker can stage a
	// raw single-file artifact under its original filename rather than a
	// hash-derived name.
	arts := make([]string, 0, len(task.Artifacts))
	names := make([]string, 0, len(task.Artifacts))
	for _, nsKey := range task.Artifacts {
		hash, err := s.Resolve(nsKey)
		if err != nil {
			rollback()
			return fmt.Errorf("artifact %q: %w", nsKey, err)
		}
		if err := s.IncrRef(hash); err != nil {
			rollback()
			return fmt.Errorf("incr ref for artifact %q: %w", nsKey, err)
		}
		incrd = append(incrd, hash)
		arts = append(arts, hash)
		names = append(names, path.Base(nsKey))
	}
	task.Artifacts = arts
	task.ArtifactNames = names

	return nil
}

// ReleaseRefs decrements refcounts for all objects referenced by a task.
// Called after a task reaches a terminal state.
func ReleaseRefs(task *model.Task, s *store.Store) {
	for _, hash := range task.InputRefs {
		s.DecrRef(hash)
	}
	for _, hash := range task.Artifacts {
		s.DecrRef(hash)
	}
}
