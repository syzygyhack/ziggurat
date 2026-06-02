package api

import (
	"strconv"
	"testing"
)

func TestExpandSweep_Grid(t *testing.T) {
	req := sweepRequest{
		Template: submitTaskRequest{
			Command:   []string{"python", "train.py", "--seed", "${seed}", "--lr", "${lr}"},
			InputRefs: map[string]string{"data": "ds_${seed}"},
			Params:    map[string]string{"base": "1"},
		},
		Grid: map[string][]string{"seed": {"1", "2"}, "lr": {"0.1", "0.01"}},
	}
	got, err := expandSweep(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 { // 2 x 2 cartesian
		t.Fatalf("expected 4 tasks, got %d", len(got))
	}
	// Deterministic order: keys sorted (lr, seed) -> lr outer, seed inner.
	first := got[0]
	if first.Command[3] != "1" || first.Command[5] != "0.1" {
		t.Errorf("substitution wrong: %v", first.Command)
	}
	if first.InputRefs["data"] != "ds_1" {
		t.Errorf("input_refs not substituted: %v", first.InputRefs)
	}
	if first.Params["base"] != "1" || first.Params["seed"] != "1" || first.Params["lr"] != "0.1" {
		t.Errorf("params not merged: %v", first.Params)
	}
	// No ${...} placeholders should remain anywhere.
	for _, r := range got {
		for _, a := range r.Command {
			if a == "${seed}" || a == "${lr}" {
				t.Errorf("unsubstituted placeholder: %v", r.Command)
			}
		}
	}
}

func TestExpandSweep_Points(t *testing.T) {
	req := sweepRequest{
		Template: submitTaskRequest{Command: []string{"render", "--frame", "${f}"}},
		Points:   []map[string]string{{"f": "1"}, {"f": "2"}, {"f": "3"}},
	}
	got, err := expandSweep(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	if got[2].Command[2] != "3" {
		t.Errorf("point substitution wrong: %v", got[2].Command)
	}
}

func TestExpandSweep_Errors(t *testing.T) {
	if _, err := expandSweep(sweepRequest{Grid: map[string][]string{"x": {"1"}}}); err == nil {
		t.Error("expected error for missing template command")
	}
	if _, err := expandSweep(sweepRequest{Template: submitTaskRequest{Command: []string{"x"}}}); err == nil {
		t.Error("expected error for empty grid/points")
	}
}

func TestCartesian_Deterministic(t *testing.T) {
	a := cartesian(map[string][]string{"x": {"1", "2"}, "y": {"a"}})
	b := cartesian(map[string][]string{"x": {"1", "2"}, "y": {"a"}})
	if len(a) != 2 {
		t.Fatalf("expected 2 points, got %d", len(a))
	}
	for i := range a {
		if a[i]["x"] != b[i]["x"] || a[i]["y"] != b[i]["y"] {
			t.Error("cartesian product is not deterministic across calls")
		}
	}
}

func TestExpandSweep_GuardsAndValidation(t *testing.T) {
	cmd := submitTaskRequest{Command: []string{"x", "${a}"}}

	// Oversized grid (101 x 101 = 10201 > maxSweepTasks) is rejected by size
	// check BEFORE materializing the product.
	big := make([]string, 101)
	for i := range big {
		big[i] = strconv.Itoa(i)
	}
	if _, err := expandSweep(sweepRequest{Template: cmd, Grid: map[string][]string{"a": big, "b": big}}); err == nil {
		t.Error("expected error for oversized grid")
	}

	// Empty axis is invalid (would leave ${a} unsubstituted), not silently dropped.
	if _, err := expandSweep(sweepRequest{Template: cmd, Grid: map[string][]string{"a": {}, "b": {"1"}}}); err == nil {
		t.Error("expected error for empty grid axis")
	}

	// grid and points are mutually exclusive.
	if _, err := expandSweep(sweepRequest{
		Template: cmd,
		Grid:     map[string][]string{"a": {"1"}},
		Points:   []map[string]string{{"a": "1"}},
	}); err == nil {
		t.Error("expected error when both grid and points are set")
	}

	// Oversized explicit points list is rejected.
	pts := make([]map[string]string, maxSweepTasks+1)
	for i := range pts {
		pts[i] = map[string]string{"a": "1"}
	}
	if _, err := expandSweep(sweepRequest{Template: cmd, Points: pts}); err == nil {
		t.Error("expected error for oversized points")
	}
}

func TestGridSize(t *testing.T) {
	n, err := gridSize(map[string][]string{"a": {"1", "2"}, "b": {"x", "y", "z"}})
	if err != nil || n != 6 {
		t.Errorf("gridSize = %d, %v; want 6, nil", n, err)
	}
	if _, err := gridSize(map[string][]string{"a": {}}); err == nil {
		t.Error("expected error for empty axis")
	}
	if n, _ := gridSize(nil); n != 0 {
		t.Errorf("empty grid size = %d, want 0", n)
	}
}
