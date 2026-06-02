package cluster

import "testing"

func TestInterfaceForIP(t *testing.T) {
	// Loopback exists on every host and owns 127.0.0.1.
	if interfaceForIP("127.0.0.1") == nil {
		t.Error("expected an interface owning 127.0.0.1")
	}
	// An address owned by no local interface resolves to nil (→ default iface).
	if interfaceForIP("203.0.113.7") != nil {
		t.Error("expected nil for a non-local IP")
	}
	// Garbage / empty → nil.
	if interfaceForIP("") != nil || interfaceForIP("not-an-ip") != nil {
		t.Error("expected nil for invalid IP input")
	}
	if ifaceName(nil) != "default" {
		t.Error("nil iface should log as 'default'")
	}
}
