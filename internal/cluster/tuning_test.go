package cluster

import (
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func TestTuneFailureDetector(t *testing.T) {
	// Zero values leave the memberlist LAN defaults untouched.
	def := memberlist.DefaultLANConfig()
	mc := memberlist.DefaultLANConfig()
	tuneFailureDetector(mc, 0, 0)
	if mc.ProbeInterval != def.ProbeInterval || mc.SuspicionMult != def.SuspicionMult {
		t.Fatalf("zero tuning changed defaults: probe=%v mult=%d", mc.ProbeInterval, mc.SuspicionMult)
	}

	// heartbeat sets ProbeInterval; suspicion derives SuspicionMult ≈ susp/probe.
	mc = memberlist.DefaultLANConfig()
	tuneFailureDetector(mc, 2*time.Second, 10*time.Second)
	if mc.ProbeInterval != 2*time.Second {
		t.Errorf("ProbeInterval = %v, want 2s", mc.ProbeInterval)
	}
	if mc.SuspicionMult != 5 {
		t.Errorf("SuspicionMult = %d, want 5", mc.SuspicionMult)
	}

	// Rounded: 2500ms / 1000ms = 2.5 → 3.
	mc = memberlist.DefaultLANConfig()
	tuneFailureDetector(mc, 1*time.Second, 2500*time.Millisecond)
	if mc.SuspicionMult != 3 {
		t.Errorf("SuspicionMult = %d, want 3 (rounded)", mc.SuspicionMult)
	}

	// suspicion < heartbeat clamps the multiplier to 1.
	mc = memberlist.DefaultLANConfig()
	tuneFailureDetector(mc, 5*time.Second, 1*time.Second)
	if mc.SuspicionMult != 1 {
		t.Errorf("SuspicionMult = %d, want 1 (clamped)", mc.SuspicionMult)
	}

	// suspicion alone (no heartbeat) leaves the multiplier at the default,
	// since it is relative to the probe interval.
	mc = memberlist.DefaultLANConfig()
	tuneFailureDetector(mc, 0, 10*time.Second)
	if mc.ProbeInterval != def.ProbeInterval {
		t.Errorf("ProbeInterval changed without heartbeat: %v", mc.ProbeInterval)
	}
}
