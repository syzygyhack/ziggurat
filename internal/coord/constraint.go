package coord

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/syzygyhack/ziggurat/internal/util"
)

// Constraint represents a parsed capability constraint expression.
type Constraint struct {
	Key   string // capability key (e.g. "gpu.vram")
	Op    string // operator: ==, !=, >=, <=, >, <
	Value string // right-hand value (e.g. "16GB", "3.11", "linux")
}

var validOps = map[string]bool{
	"==": true, "!=": true,
	">=": true, "<=": true,
	">": true, "<": true,
}

// ParseConstraint parses an expression like "gpu.vram >= 16GB" into a Constraint.
func ParseConstraint(expr string) (Constraint, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return Constraint{}, fmt.Errorf("empty constraint expression")
	}

	// Try two-char operators first, then single-char.
	for _, op := range []string{">=", "<=", "!=", "==", ">", "<"} {
		idx := strings.Index(expr, " "+op+" ")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(expr[:idx])
		val := strings.TrimSpace(expr[idx+len(op)+2:])
		if key == "" || val == "" {
			return Constraint{}, fmt.Errorf("incomplete constraint: %q", expr)
		}
		return Constraint{Key: key, Op: op, Value: val}, nil
	}

	return Constraint{}, fmt.Errorf("no valid operator found in constraint: %q", expr)
}

// EvalConstraint evaluates a single constraint against a capabilities map.
// Returns false if the key is missing or the comparison fails.
func EvalConstraint(c Constraint, caps map[string]string) bool {
	actual, ok := caps[c.Key]
	if !ok {
		return false
	}

	// Try integer comparison (with byte suffix support).
	if aInt, err := util.ParseByteSize(actual); err == nil {
		if bInt, err := util.ParseByteSize(c.Value); err == nil {
			return compareInt(aInt, c.Op, bInt)
		}
	}

	// Try version comparison (dot-separated numeric segments).
	if isVersionString(actual) && isVersionString(c.Value) {
		cmp := compareVersions(actual, c.Value)
		return applyOp(cmp, c.Op)
	}

	// Fall back to string comparison.
	cmp := strings.Compare(actual, c.Value)
	return applyOp(cmp, c.Op)
}

// MatchesConstraints evaluates all constraints against capabilities.
// Returns true if all pass (or if constraints is empty).
func MatchesConstraints(constraints []string, caps map[string]string) bool {
	for _, expr := range constraints {
		c, err := ParseConstraint(expr)
		if err != nil {
			return false // malformed constraint = no match
		}
		if !EvalConstraint(c, caps) {
			return false
		}
	}
	return true
}


func compareInt(a int64, op string, b int64) bool {
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	case ">":
		return a > b
	case "<":
		return a < b
	}
	return false
}

// isVersionString returns true if s looks like a dot-separated version (e.g. "3.12", "1.10.2").
func isVersionString(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return false
	}
	for _, p := range parts {
		if _, err := strconv.Atoi(p); err != nil {
			return false
		}
	}
	return true
}

// compareVersions does segment-by-segment numeric comparison.
// Returns -1, 0, or 1 like strings.Compare.
func compareVersions(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	maxLen := len(aParts)
	if len(bParts) > maxLen {
		maxLen = len(bParts)
	}

	for i := 0; i < maxLen; i++ {
		av, bv := 0, 0
		if i < len(aParts) {
			av, _ = strconv.Atoi(aParts[i])
		}
		if i < len(bParts) {
			bv, _ = strconv.Atoi(bParts[i])
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
	}
	return 0
}

func applyOp(cmp int, op string) bool {
	switch op {
	case "==":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	}
	return false
}
