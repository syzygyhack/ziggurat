package scheduler

import (
	"strconv"

	"github.com/syzygyhack/ziggurat/internal/model"
)

// ObjectLocator knows which nodes hold a given content hash.
// Returns the node IDs that have the object locally. Nil means unknown.
type ObjectLocator interface {
	NodesForHash(hash string) []string
}

// NodeLoad reports how busy a node is. Returns running task count and
// concurrency limit. A node with running == limit is full.
type NodeLoad interface {
	Load(nodeID string) (running, limit int)
}

// Candidate represents a node that could execute a task.
type Candidate struct {
	NodeID string
	Tags   []string
	Caps   map[string]string
}

// Score computes the scheduling score for assigning a task to a candidate node.
//
// From the spec:
//
//	locality_score = local_shards / total_input_shards  (0 if no inputs)
//	score = locality_score * (1 - load_factor)
//
// This always returns locality * (1 - loadFactor). Zero-locality candidates
// score 0 here; the Select function falls back to least-loaded when all
// candidates score 0.
func Score(task *model.Task, candidate Candidate, locator ObjectLocator, load NodeLoad) float64 {
	locality := localityScore(task, candidate.NodeID, locator)
	lf := loadFactor(candidate.NodeID, load)
	return locality * (1 - lf)
}

// Select picks the best candidate for a task. Returns the best candidate's
// index, or -1 if no candidates are provided.
//
// If the task has an affinity and a candidate matches, that candidate wins
// immediately (provided it is not at capacity).
//
// If all candidates score 0 (no locality data, or all nodes are full),
// falls back to the least-loaded node.
func Select(task *model.Task, candidates []Candidate, locator ObjectLocator, load NodeLoad) int {
	if len(candidates) == 0 {
		return -1
	}

	// Affinity: if the task requests a specific node and that node is a
	// candidate with remaining capacity, use it directly.
	if task.Config.Affinity != "" {
		for i, c := range candidates {
			if c.NodeID == task.Config.Affinity {
				if lf := loadFactor(c.NodeID, load); lf < 1.0 {
					return i
				}
				break // affinity node is full, fall through to scoring
			}
		}
	}

	bestIdx := 0
	bestScore := Score(task, candidates[0], locator, load)

	for i := 1; i < len(candidates); i++ {
		s := Score(task, candidates[i], locator, load)
		if s > bestScore {
			bestScore = s
			bestIdx = i
		}
	}

	// If all scores are zero, fall back to least-loaded node.
	if bestScore == 0 {
		bestLF := loadFactor(candidates[0].NodeID, load)
		for i := 1; i < len(candidates); i++ {
			lf := loadFactor(candidates[i].NodeID, load)
			if lf < bestLF {
				bestLF = lf
				bestIdx = i
			}
		}
	}

	return bestIdx
}

// localityScore computes what fraction of a task's input objects are local
// to the given node. Returns 0 if the task has no inputs or the locator is nil.
func localityScore(task *model.Task, nodeID string, locator ObjectLocator) float64 {
	if locator == nil {
		return 0
	}

	// Collect all input hashes: input_refs values + artifacts.
	var hashes []string
	for _, h := range task.InputRefs {
		hashes = append(hashes, h)
	}
	hashes = append(hashes, task.Artifacts...)

	if len(hashes) == 0 {
		return 0
	}

	local := 0
	for _, h := range hashes {
		nodes := locator.NodesForHash(h)
		for _, n := range nodes {
			if n == nodeID {
				local++
				break
			}
		}
	}

	return float64(local) / float64(len(hashes))
}

// loadFactor returns a value in [0, 1] indicating how loaded a node is.
// 0 = idle, 1 = at capacity. If load info is unavailable, returns 0 (assume idle).
func loadFactor(nodeID string, load NodeLoad) float64 {
	if load == nil {
		return 0
	}
	running, limit := load.Load(nodeID)
	if limit <= 0 {
		return 0
	}
	f := float64(running) / float64(limit)
	if f > 1 {
		return 1
	}
	return f
}

// AllocatedResources tracks resources currently allocated to running tasks
// on a node.
type AllocatedResources struct {
	Memory   int64 // bytes currently allocated
	CPUCores int   // cores currently allocated
	GPUs     int   // GPU devices currently allocated
}

// ResourceTracker reports allocated resources per node. Used by Fits()
// to determine if a node has enough remaining capacity for a task.
type ResourceTracker interface {
	Allocated(nodeID string) AllocatedResources
}

// Fits checks whether a candidate node has enough remaining resources to
// run the task. Returns true if:
//   - The task has no resource requirements, OR
//   - For each non-zero requirement, the node's total capacity minus
//     currently allocated resources is >= the task's request.
//
// If tracker is nil, only static capacity (caps) is checked.
func Fits(task *model.Task, candidate Candidate, tracker ResourceTracker) bool {
	req := task.Resources
	if req.Memory == 0 && req.CPUCores == 0 && req.GPUs == 0 {
		return true
	}

	var alloc AllocatedResources
	if tracker != nil {
		alloc = tracker.Allocated(candidate.NodeID)
	}

	if req.Memory > 0 {
		total, ok := parseCapInt(candidate.Caps, "mem.total")
		if !ok {
			return false
		}
		remaining := total - alloc.Memory
		if req.Memory > remaining {
			return false
		}
	}

	if req.CPUCores > 0 {
		total, ok := parseCapInt(candidate.Caps, "cpu.cores")
		if !ok {
			return false
		}
		remaining := int(total) - alloc.CPUCores
		if req.CPUCores > remaining {
			return false
		}
	}

	if req.GPUs > 0 {
		total, ok := parseCapInt(candidate.Caps, "gpu.count")
		if !ok {
			return false
		}
		remaining := int(total) - alloc.GPUs
		if req.GPUs > remaining {
			return false
		}
	}

	return true
}

// parseCapInt extracts an integer value from a capabilities map.
func parseCapInt(caps map[string]string, key string) (int64, bool) {
	v, ok := caps[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
