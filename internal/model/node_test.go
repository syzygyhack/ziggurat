package model

import (
	"encoding/json"
	"testing"
)

func TestRole_String(t *testing.T) {
	tests := []struct {
		role Role
		want string
	}{
		{RoleHybrid, "hybrid"},
		{RoleCoordinator, "coordinator"},
		{RoleWorker, "worker"},
		{Role(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.role.String(); got != tt.want {
			t.Errorf("Role(%d).String() = %q, want %q", tt.role, got, tt.want)
		}
	}
}

func TestRole_MarshalJSON(t *testing.T) {
	tests := []struct {
		role Role
		want string
	}{
		{RoleHybrid, `"hybrid"`},
		{RoleCoordinator, `"coordinator"`},
		{RoleWorker, `"worker"`},
	}
	for _, tt := range tests {
		got, err := json.Marshal(tt.role)
		if err != nil {
			t.Fatalf("Marshal(%v) error: %v", tt.role, err)
		}
		if string(got) != tt.want {
			t.Errorf("Marshal(%v) = %s, want %s", tt.role, got, tt.want)
		}
	}
}

func TestRole_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Role
		wantErr bool
	}{
		{"hybrid string", `"hybrid"`, RoleHybrid, false},
		{"coordinator string", `"coordinator"`, RoleCoordinator, false},
		{"worker string", `"worker"`, RoleWorker, false},
		{"empty string defaults hybrid", `""`, RoleHybrid, false},
		{"numeric 0", `0`, RoleHybrid, false},
		{"numeric 1", `1`, RoleCoordinator, false},
		{"numeric 2", `2`, RoleWorker, false},
		{"unknown string", `"bogus"`, 0, true},
		{"invalid type", `true`, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Role
			err := json.Unmarshal([]byte(tt.input), &got)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Unmarshal(%s) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("Unmarshal(%s) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestRole_JSONRoundTrip(t *testing.T) {
	for _, role := range []Role{RoleHybrid, RoleCoordinator, RoleWorker} {
		data, err := json.Marshal(role)
		if err != nil {
			t.Fatalf("Marshal(%v): %v", role, err)
		}
		var got Role
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("Unmarshal(%s): %v", data, err)
		}
		if got != role {
			t.Errorf("roundtrip: got %v, want %v", got, role)
		}
	}
}

func TestRole_UnmarshalJSON_InStruct(t *testing.T) {
	// Verify Role works correctly when embedded in a struct (like Node).
	type wrapper struct {
		Role Role `json:"role"`
	}

	input := `{"role":"worker"}`
	var w wrapper
	if err := json.Unmarshal([]byte(input), &w); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if w.Role != RoleWorker {
		t.Errorf("got role %v, want %v", w.Role, RoleWorker)
	}

	// Numeric backwards compat in struct context.
	input = `{"role":1}`
	if err := json.Unmarshal([]byte(input), &w); err != nil {
		t.Fatalf("Unmarshal numeric: %v", err)
	}
	if w.Role != RoleCoordinator {
		t.Errorf("got role %v, want %v", w.Role, RoleCoordinator)
	}
}
