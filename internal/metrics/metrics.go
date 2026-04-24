package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Compute metrics.
var (
	TasksSubmitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ziggurat_tasks_submitted_total",
		Help: "Total number of tasks submitted.",
	})

	TasksCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ziggurat_tasks_completed_total",
		Help: "Total number of tasks completed, by terminal status.",
	}, []string{"status"})

	TaskDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ziggurat_task_duration_seconds",
		Help:    "Task wall-clock duration in seconds.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14), // 1s to ~4.5h
	})

	TaskQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ziggurat_task_queue_depth",
		Help: "Current number of tasks in the queue.",
	})

	WorkersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ziggurat_workers_active",
		Help: "Number of tasks currently executing on this node.",
	})
)

// Storage metrics.
var (
	StoreObjects = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ziggurat_store_objects_total",
		Help: "Total number of objects in the local store.",
	})

	StoreBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ziggurat_store_bytes_total",
		Help: "Total bytes used by the local store.",
	})
)

// Cluster metrics.
var (
	NodesTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ziggurat_nodes_total",
		Help: "Number of nodes in the cluster, by role.",
	}, []string{"role"})
)
