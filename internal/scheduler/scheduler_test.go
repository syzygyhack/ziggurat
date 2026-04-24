package scheduler

import (
	"testing"

	"github.com/syzygyhack/ziggurat/internal/model"
)

// mockLocator implements ObjectLocator for tests.
type mockLocator struct {
	// hash -> list of node IDs that have it
	data map[string][]string
}

func (m *mockLocator) NodesForHash(hash string) []string {
	return m.data[hash]
}

// mockLoad implements NodeLoad for tests.
type mockLoad struct {
	// nodeID -> (running, limit)
	loads map[string][2]int
}

func (m *mockLoad) Load(nodeID string) (int, int) {
	if l, ok := m.loads[nodeID]; ok {
		return l[0], l[1]
	}
	return 0, 4 // default: idle with 4 slots
}

func TestScore_NoInputs(t *testing.T) {
	task := &model.Task{Command: []string{"echo"}}
	c := Candidate{NodeID: "node-1"}
	load := &mockLoad{loads: map[string][2]int{
		"node-1": {0, 4},
	}}

	s := Score(task, c, nil, load)
	// No inputs → locality=0, score = 0 * (1 - 0) = 0.
	// Select handles the fallback to least-loaded when all scores are 0.
	if s != 0.0 {
		t.Fatalf("expected 0.0, got %f", s)
	}
}

func TestScore_WithLocality(t *testing.T) {
	task := &model.Task{
		Command:   []string{"process"},
		InputRefs: map[string]string{"a": "hash1", "b": "hash2"},
	}

	locator := &mockLocator{data: map[string][]string{
		"hash1": {"node-1", "node-2"},
		"hash2": {"node-1"},
	}}
	load := &mockLoad{loads: map[string][2]int{
		"node-1": {0, 4},
		"node-2": {0, 4},
	}}

	// node-1 has both hashes → locality = 2/2 = 1.0
	s1 := Score(task, Candidate{NodeID: "node-1"}, locator, load)
	// node-2 has only hash1 → locality = 1/2 = 0.5
	s2 := Score(task, Candidate{NodeID: "node-2"}, locator, load)

	if s1 != 1.0 {
		t.Fatalf("node-1 score: expected 1.0, got %f", s1)
	}
	if s2 != 0.5 {
		t.Fatalf("node-2 score: expected 0.5, got %f", s2)
	}
}

func TestScore_LoadFactor(t *testing.T) {
	task := &model.Task{
		Command:   []string{"process"},
		InputRefs: map[string]string{"a": "hash1"},
	}

	locator := &mockLocator{data: map[string][]string{
		"hash1": {"node-1", "node-2"},
	}}
	load := &mockLoad{loads: map[string][2]int{
		"node-1": {3, 4}, // 75% loaded
		"node-2": {1, 4}, // 25% loaded
	}}

	// Both have the data (locality = 1.0)
	// node-1: 1.0 * (1 - 0.75) = 0.25
	// node-2: 1.0 * (1 - 0.25) = 0.75
	s1 := Score(task, Candidate{NodeID: "node-1"}, locator, load)
	s2 := Score(task, Candidate{NodeID: "node-2"}, locator, load)

	if s1 != 0.25 {
		t.Fatalf("node-1 score: expected 0.25, got %f", s1)
	}
	if s2 != 0.75 {
		t.Fatalf("node-2 score: expected 0.75, got %f", s2)
	}
}

func TestSelect_BestCandidate(t *testing.T) {
	task := &model.Task{
		Command:   []string{"process"},
		InputRefs: map[string]string{"a": "hash1", "b": "hash2"},
	}

	candidates := []Candidate{
		{NodeID: "node-1"},
		{NodeID: "node-2"},
		{NodeID: "node-3"},
	}

	locator := &mockLocator{data: map[string][]string{
		"hash1": {"node-1", "node-3"},
		"hash2": {"node-3"},
	}}
	load := &mockLoad{loads: map[string][2]int{
		"node-1": {0, 4},
		"node-2": {0, 4},
		"node-3": {0, 4},
	}}

	// node-1: locality = 1/2 = 0.5, load = 0 → score = 0.5
	// node-2: locality = 0/2 = 0, load = 0 → score = 0 (no data!)
	// node-3: locality = 2/2 = 1.0, load = 0 → score = 1.0
	// node-3 has the highest score and should win — zero-locality node-2
	// no longer outranks partial-locality node-1.
	idx := Select(task, candidates, locator, load)
	if idx != 2 {
		t.Fatalf("expected node-3 (idx=2), got idx=%d", idx)
	}
}

func TestSelect_ZeroLocality_NoOutrank(t *testing.T) {
	// Verify that a zero-locality idle node does NOT outrank a
	// partial-locality node (the bug this fix addresses).
	task := &model.Task{
		Command:   []string{"process"},
		InputRefs: map[string]string{"a": "hash1"},
	}

	candidates := []Candidate{
		{NodeID: "node-with-data"},
		{NodeID: "node-no-data"},
	}

	locator := &mockLocator{data: map[string][]string{
		"hash1": {"node-with-data"},
	}}
	load := &mockLoad{loads: map[string][2]int{
		"node-with-data": {0, 4},
		"node-no-data":   {0, 4},
	}}

	// node-with-data: locality=1.0, score = 1.0
	// node-no-data:   locality=0,   score = 0
	idx := Select(task, candidates, locator, load)
	if idx != 0 {
		t.Fatalf("expected node-with-data (idx=0), got idx=%d", idx)
	}
}

func TestSelect_NoLocator_PreferLeastLoaded(t *testing.T) {
	task := &model.Task{Command: []string{"echo"}}
	candidates := []Candidate{
		{NodeID: "node-1"},
		{NodeID: "node-2"},
		{NodeID: "node-3"},
	}

	load := &mockLoad{loads: map[string][2]int{
		"node-1": {3, 4}, // 75% loaded
		"node-2": {1, 4}, // 25% loaded
		"node-3": {2, 4}, // 50% loaded
	}}

	idx := Select(task, candidates, nil, load)
	if idx != 1 {
		t.Fatalf("expected node-2 (idx=1, least loaded), got idx=%d", idx)
	}
}

func TestSelect_Empty(t *testing.T) {
	task := &model.Task{Command: []string{"echo"}}
	idx := Select(task, nil, nil, nil)
	if idx != -1 {
		t.Fatalf("expected -1 for empty candidates, got %d", idx)
	}
}

func TestScore_FullNode(t *testing.T) {
	task := &model.Task{Command: []string{"echo"}}
	c := Candidate{NodeID: "node-1"}
	load := &mockLoad{loads: map[string][2]int{
		"node-1": {4, 4}, // 100% loaded
	}}

	s := Score(task, c, nil, load)
	// locality=0, so 0 * (1 - 1.0) = 0
	if s != 0.0 {
		t.Fatalf("expected 0.0 for full node, got %f", s)
	}
}

func TestScore_ArtifactLocality(t *testing.T) {
	task := &model.Task{
		Command:   []string{"process"},
		Artifacts: []string{"art1", "art2"},
	}

	locator := &mockLocator{data: map[string][]string{
		"art1": {"node-1"},
		"art2": {"node-1"},
	}}
	load := &mockLoad{loads: map[string][2]int{
		"node-1": {0, 4},
	}}

	s := Score(task, Candidate{NodeID: "node-1"}, locator, load)
	if s != 1.0 {
		t.Fatalf("expected 1.0 for all artifacts local, got %f", s)
	}
}
