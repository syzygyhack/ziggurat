package benchmark

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"
)

// PeerResult holds latency measurements for a single peer node.
type PeerResult struct {
	NodeID  string  `json:"node_id"`
	Name    string  `json:"name,omitempty"`
	Address string  `json:"address"`
	RTTMin  float64 `json:"rtt_min_ms"` // minimum RTT in milliseconds
	RTTAvg  float64 `json:"rtt_avg_ms"` // average RTT in milliseconds
	RTTP50  float64 `json:"rtt_p50_ms"` // median RTT
	RTTP99  float64 `json:"rtt_p99_ms"` // 99th percentile RTT
	RTTMax  float64 `json:"rtt_max_ms"` // maximum RTT in milliseconds
	Loss    float64 `json:"loss_pct"`   // packet loss percentage
	Error   string  `json:"error,omitempty"`
}

// NetworkResult holds the full network benchmark.
type NetworkResult struct {
	Peers []PeerResult `json:"peers"`
}

// PeerInfo describes a node to probe.
type PeerInfo struct {
	NodeID  string
	Name    string
	Address string // HTTP address (host:port)
}

const (
	probeCount   = 10
	probeTimeout = 3 * time.Second
)

// ProbePeers measures HTTP RTT to each peer by hitting GET /api/v1/health.
// Returns results sorted by average RTT (fastest first).
func ProbePeers(peers []PeerInfo) *NetworkResult {
	results := make([]PeerResult, len(peers))

	// Probe all peers concurrently.
	type indexed struct {
		idx int
		res PeerResult
	}
	ch := make(chan indexed, len(peers))

	for i, p := range peers {
		go func(idx int, peer PeerInfo) {
			ch <- indexed{idx: idx, res: probePeer(peer)}
		}(i, p)
	}

	for range peers {
		r := <-ch
		results[r.idx] = r.res
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].RTTAvg < results[j].RTTAvg
	})

	return &NetworkResult{Peers: results}
}

func probePeer(peer PeerInfo) PeerResult {
	result := PeerResult{
		NodeID:  peer.NodeID,
		Name:    peer.Name,
		Address: peer.Address,
	}

	url := fmt.Sprintf("http://%s/api/v1/health", peer.Address)
	client := &http.Client{Timeout: probeTimeout}

	var samples []float64
	failures := 0

	for i := 0; i < probeCount; i++ {
		start := time.Now()
		resp, err := client.Get(url)
		elapsed := time.Since(start)

		if err != nil {
			failures++
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 500 {
			failures++
			continue
		}

		samples = append(samples, float64(elapsed.Microseconds())/1000.0)
	}

	result.Loss = float64(failures) / float64(probeCount) * 100.0

	if len(samples) == 0 {
		result.Error = "all probes failed"
		return result
	}

	sort.Float64s(samples)

	result.RTTMin = samples[0]
	result.RTTMax = samples[len(samples)-1]
	result.RTTAvg = avg(samples)
	result.RTTP50 = percentile(samples, 0.50)
	result.RTTP99 = percentile(samples, 0.99)

	return result
}

func avg(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := pct * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
