package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// maxSweepTasks bounds parameter-space expansion to guard against accidental
// combinatorial explosions.
const maxSweepTasks = 10000

// sweepRequest expands a task template over a parameter space into N independent
// tasks (parametric fan-out). The template's command/args, input_refs values,
// artifacts, env values, and params may reference parameters as ${name}; each
// expanded task substitutes one point from the space. Provide either a grid
// (cartesian product of named axes) or an explicit list of points.
type sweepRequest struct {
	Template submitTaskRequest   `json:"template"`
	Grid     map[string][]string `json:"grid,omitempty"`
	Points   []map[string]string `json:"points,omitempty"`
}

// submitSweep expands a sweep request and submits the resulting tasks (atomic,
// like batch). Returns a sweep id and the created task ids.
func (s *Server) submitSweep(w http.ResponseWriter, r *http.Request) {
	if !s.requireCoordinator(w) {
		return
	}
	var req sweepRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxControlPlaneBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}

	reqs, err := expandSweep(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	results, badIdx, err := s.submitTaskBatch(r.Context(), reqs)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("task[%d]: %v", badIdx, err))
		return
	}

	ids := make([]string, len(results))
	for i, t := range results {
		ids[i] = t.ID
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"sweep_id": uuid.NewString(),
		"count":    len(ids),
		"task_ids": ids,
	})
}

// expandSweep produces one task request per point in the parameter space.
func expandSweep(req sweepRequest) ([]submitTaskRequest, error) {
	if len(req.Template.Command) == 0 {
		return nil, fmt.Errorf("sweep template command is required")
	}
	if len(req.Grid) > 0 && len(req.Points) > 0 {
		return nil, fmt.Errorf("specify either grid or points, not both")
	}

	points := req.Points
	if len(points) == 0 {
		// Validate and size the grid BEFORE materializing the product, so a
		// small request cannot expand into a huge point set and exhaust memory.
		size, err := gridSize(req.Grid)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return nil, fmt.Errorf("sweep requires a non-empty grid or points")
		}
		if size > maxSweepTasks {
			return nil, fmt.Errorf("sweep expands to %d tasks, exceeds limit %d", size, maxSweepTasks)
		}
		points = cartesian(req.Grid)
	} else if len(points) > maxSweepTasks {
		return nil, fmt.Errorf("sweep has %d points, exceeds limit %d", len(points), maxSweepTasks)
	}

	out := make([]submitTaskRequest, 0, len(points))
	for _, p := range points {
		t := req.Template // shallow copy; parameterized fields replaced below
		t.Command = substSlice(req.Template.Command, p)
		t.Artifacts = substSlice(req.Template.Artifacts, p)
		t.InputRefs = substMapVals(req.Template.InputRefs, p)
		t.Env = substMapVals(req.Template.Env, p)
		t.Params = mergeParams(req.Template.Params, p)
		out = append(out, t)
	}
	return out, nil
}

// gridSize returns the number of points in the cartesian product, requiring
// every axis to have at least one value (an empty axis would otherwise be
// silently dropped, leaving its ${name} unsubstituted). It stops multiplying
// once the running product exceeds maxSweepTasks, so it never overflows or
// pre-counts an oversized space.
func gridSize(grid map[string][]string) (int, error) {
	if len(grid) == 0 {
		return 0, nil
	}
	size := 1
	for k, vals := range grid {
		if len(vals) == 0 {
			return 0, fmt.Errorf("grid axis %q has no values", k)
		}
		size *= len(vals)
		if size > maxSweepTasks {
			return size, nil // caller rejects; don't keep multiplying
		}
	}
	return size, nil
}

// cartesian returns the cartesian product of the named axes as a list of points.
// Axis order is sorted by key for deterministic, reproducible task ordering.
// Callers must validate axes are non-empty first (see gridSize).
func cartesian(grid map[string][]string) []map[string]string {
	if len(grid) == 0 {
		return nil
	}
	keys := make([]string, 0, len(grid))
	for k := range grid {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	points := []map[string]string{{}}
	for _, k := range keys {
		vals := grid[k]
		if len(vals) == 0 {
			continue
		}
		next := make([]map[string]string, 0, len(points)*len(vals))
		for _, base := range points {
			for _, v := range vals {
				m := make(map[string]string, len(base)+1)
				for bk, bv := range base {
					m[bk] = bv
				}
				m[k] = v
				next = append(next, m)
			}
		}
		points = next
	}
	return points
}

func substSlice(in []string, p map[string]string) []string {
	if len(in) == 0 {
		return in
	}
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = substStr(s, p)
	}
	return out
}

func substMapVals(in map[string]string, p map[string]string) map[string]string {
	if len(in) == 0 {
		return in
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = substStr(v, p)
	}
	return out
}

// substStr replaces each ${name} occurrence with the point's value. Tokens are
// distinct (wrapped in ${}), so replacement order is irrelevant.
func substStr(s string, p map[string]string) string {
	for k, v := range p {
		s = strings.ReplaceAll(s, "${"+k+"}", v)
	}
	return s
}

// mergeParams overlays a point's parameters on the template's params.
func mergeParams(base, point map[string]string) map[string]string {
	if len(base) == 0 && len(point) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(point))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range point {
		out[k] = v
	}
	return out
}
