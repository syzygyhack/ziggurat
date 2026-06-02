package cluster

import (
	"encoding/json"
	"fmt"
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

// When meta exceeds the limit, encodeMetaFitting must drop non-essential caps
// (largest-first), keep essentials, and always produce valid, decodable bytes.
func TestEncodeMetaFitting_DropsNonEssential(t *testing.T) {
	m := sampleMeta()
	// Add bulky non-essential caps with unique keys AND values (random-looking
	// so they don't just compress away) to force the encoding over the limit.
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("pkg%02d.version", i)
		m.Caps[k] = fmt.Sprintf("%d.%d.%d-build.%x", i*7%19, i*13%23, i*31%41, i*2654435761)
	}

	const limit = 512
	enc, dropped, err := encodeMetaFitting(m, limit)
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) > limit {
		t.Fatalf("encoded %d bytes still exceeds limit %d", len(enc), limit)
	}
	if len(dropped) == 0 {
		t.Fatal("expected some capabilities to be dropped")
	}
	// Must remain valid/decodable and retain essential scheduling caps.
	got, err := decodeMeta(enc)
	if err != nil {
		t.Fatalf("dropped-cap meta failed to decode: %v", err)
	}
	for _, k := range []string{"os", "arch", "cpu.cores", "gpu.count", "compute.concurrency"} {
		if got.Caps[k] == "" {
			t.Errorf("essential cap %q was dropped", k)
		}
	}
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
