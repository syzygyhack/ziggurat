package cluster

import (
	"encoding/json"
	"testing"
)

func sampleMeta() *NodeMeta {
	return &NodeMeta{
		ID:       "4ee0f8ec-7897-4958-96bc-ac384db1b8c4",
		HTTPPort: 7100, GRPCPort: 7101,
		Tags:        []string{"gpu", "cuda"},
		Role:        "hybrid",
		ClusterName: "default",
		TokenHMAC:   "0000000000000000000000000000000000000000000000000000000000000000",
		Caps: map[string]string{
			"os": "windows", "arch": "amd64", "cpu.cores": "16", "hostname": "Maomao",
			"mem.total": "68603351040", "disk.avail": "22020493312", "storage.class": "ssd",
			"compute.concurrency": "16", "gpu.count": "1", "gpu.cuda": "12.8",
			"gpu.driver": "591.86", "gpu.model": "NVIDIA GeForce RTX 4090 D", "gpu.vram": "51527024640",
			"python.version": "3.12.1", "node.version": "20.11.0", "go.version": "1.24.0",
			"java.version": "17.0.2", "ruby.version": "3.2.2", "rust.version": "1.75.0",
		},
	}
}

func TestEncodeMeta_RoundTrip(t *testing.T) {
	m := sampleMeta()
	enc, err := encodeMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeMeta(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != m.ID || got.Role != m.Role || got.HTTPPort != m.HTTPPort {
		t.Errorf("scalar fields not preserved: %+v", got)
	}
	if len(got.Caps) != len(m.Caps) || got.Caps["gpu.model"] != "NVIDIA GeForce RTX 4090 D" || got.Caps["python.version"] != "3.12.1" {
		t.Errorf("caps not preserved: %v", got.Caps)
	}
}

// A fully-loaded node (GPU + join token + all runtime versions) must fit within
// memberlist's 512-byte meta limit after compression.
func TestEncodeMeta_FitsMemberlistLimit(t *testing.T) {
	enc, err := encodeMeta(sampleMeta())
	if err != nil {
		t.Fatal(err)
	}
	const memberlistMetaMaxSize = 512
	if len(enc) > memberlistMetaMaxSize {
		t.Fatalf("encoded meta is %d bytes, exceeds memberlist limit %d", len(enc), memberlistMetaMaxSize)
	}
	t.Logf("encoded meta: %d/%d bytes", len(enc), memberlistMetaMaxSize)
}

// decodeMeta must still accept legacy uncompressed JSON for rolling upgrades.
func TestDecodeMeta_LegacyJSON(t *testing.T) {
	m := sampleMeta()
	raw, _ := json.Marshal(m)
	got, err := decodeMeta(raw)
	if err != nil {
		t.Fatalf("legacy JSON decode failed: %v", err)
	}
	if got.ID != m.ID || got.Caps["go.version"] != "1.24.0" {
		t.Errorf("legacy decode mismatch: %+v", got)
	}
}
