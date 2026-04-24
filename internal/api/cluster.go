package api

import (
	"net/http"
	"sort"
	"time"
)

// nodeStats returns (total, healthy) node counts. In single-node mode
// (no NodeLister), always returns (1, 1).
func (s *Server) nodeStats() (int, int) {
	if s.nodes == nil {
		return 1, 1
	}
	count := s.nodes.Count()
	// Phase 0b: all memberlist-visible nodes are considered healthy.
	// Suspicion-based health will come with heartbeat failure detection.
	return count, count
}

func (s *Server) underReplicatedCount() int {
	if s.underReplicated != nil {
		return s.underReplicated()
	}
	return 0
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	running := 0
	queued := 0
	for _, t := range s.coord.List(nil) {
		switch t.Status.String() {
		case "running":
			running++
		case "queued":
			queued++
		}
	}

	stats := s.store.Stats()

	nodeCount, healthyCount := s.nodeStats()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":                   "healthy",
		"nodes":                    nodeCount,
		"nodes_healthy":            healthyCount,
		"tasks_running":            running,
		"tasks_queued":             queued,
		"storage_used_bytes":       stats.UsedBytes,
		"storage_capacity_bytes":   stats.Capacity,
		"objects_total":            stats.Objects,
		"objects_under_replicated": s.underReplicatedCount(),
		"uptime_seconds":           int(time.Since(s.startTime).Seconds()),
	})
}

func (s *Server) clusterStatus(w http.ResponseWriter, r *http.Request) {
	tasks := s.coord.List(nil)
	stats := s.store.Stats()

	counts := map[string]int{}
	var active []map[string]any
	for _, t := range tasks {
		counts[t.Status.String()]++
		status := t.Status.String()
		if status == "running" || status == "scheduled" {
			wall := ""
			if !t.Metrics.StartedAt.IsZero() {
				wall = time.Since(t.Metrics.StartedAt).Truncate(time.Second).String()
			}
			active = append(active, map[string]any{
				"id":       t.ID,
				"status":   status,
				"command":  t.Command,
				"wall":     wall,
				"priority": t.Config.Priority,
				"worker":   t.Worker,
			})
		}
	}

	// Sort active tasks by start time (newest first).
	sort.Slice(active, func(i, j int) bool {
		wi, _ := active[i]["wall"].(string)
		wj, _ := active[j]["wall"].(string)
		return wi > wj
	})

	nodeCount, healthyCount := s.nodeStats()

	// Build worker load snapshot.
	var workerLoadSnap []map[string]any
	for id, rl := range s.coord.WorkerLoad().Snapshot() {
		workerLoadSnap = append(workerLoadSnap, map[string]any{
			"node_id": id,
			"running": rl[0],
			"limit":   rl[1],
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":               "healthy",
		"nodes":                nodeCount,
		"nodes_healthy":        healthyCount,
		"uptime_seconds":       int(time.Since(s.startTime).Seconds()),
		"tasks_total":          len(tasks),
		"tasks_running":        counts["running"],
		"tasks_queued":         counts["queued"],
		"tasks_completed":      counts["completed"],
		"tasks_failed":         counts["failed"],
		"tasks_cancelled":      counts["cancelled"],
		"tasks_dead_letter":    counts["dead_letter"],
		"active_tasks":         active,
		"worker_load":          workerLoadSnap,
		"storage_objects":      stats.Objects,
		"storage_used_bytes":   stats.UsedBytes,
		"storage_capacity":     stats.Capacity,
	})
}
