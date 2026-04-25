package benchmark

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/blake3"
)

// LocalResult holds the results of local machine benchmarks.
type LocalResult struct {
	CPU    CPUResult    `json:"cpu"`
	Memory MemResult    `json:"memory"`
	Disk   DiskResult   `json:"disk"`
	System SystemInfo   `json:"system"`
}

// CPUResult captures CPU benchmark metrics.
type CPUResult struct {
	BLAKE3SingleMBps  float64 `json:"blake3_single_mbps"`  // single-core BLAKE3 throughput
	BLAKE3ParallelMBps float64 `json:"blake3_parallel_mbps"` // all-core BLAKE3 throughput
	Cores             int     `json:"cores"`
	ScalingEfficiency float64 `json:"scaling_efficiency"`  // parallel / (single * cores), 0-1
}

// MemResult captures memory bandwidth benchmark metrics.
type MemResult struct {
	SeqWriteMBps float64 `json:"seq_write_mbps"` // sequential write bandwidth
	SeqReadMBps  float64 `json:"seq_read_mbps"`  // sequential read bandwidth
}

// DiskResult captures disk I/O benchmark metrics.
type DiskResult struct {
	WriteMBps float64 `json:"write_mbps"` // sequential write throughput
	ReadMBps  float64 `json:"read_mbps"`  // sequential read throughput
	FsyncUs   float64 `json:"fsync_us"`   // average fsync latency in microseconds
}

// SystemInfo captures static system info for context.
type SystemInfo struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Cores    int    `json:"cores"`
	Hostname string `json:"hostname"`
}

// RunLocal executes all local benchmarks. tmpDir is used for disk I/O tests;
// if empty, os.TempDir() is used.
func RunLocal(tmpDir string) (*LocalResult, error) {
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	hostname, _ := os.Hostname()
	result := &LocalResult{
		System: SystemInfo{
			OS:       runtime.GOOS,
			Arch:     runtime.GOARCH,
			Cores:    runtime.NumCPU(),
			Hostname: hostname,
		},
	}

	result.CPU = benchCPU()
	result.Memory = benchMemory()

	disk, err := benchDisk(tmpDir)
	if err != nil {
		return result, fmt.Errorf("disk benchmark: %w", err)
	}
	result.Disk = disk

	return result, nil
}

// --- CPU benchmark: BLAKE3 hashing throughput ---

const cpuBenchSize = 64 * 1024 * 1024 // 64 MB per core

func benchCPU() CPUResult {
	cores := runtime.NumCPU()
	data := make([]byte, cpuBenchSize)
	rand.Read(data)

	// Single-core.
	single := hashThroughput(data, 1)

	// All-core parallel.
	parallel := hashThroughput(data, cores)

	efficiency := 0.0
	if single > 0 && cores > 0 {
		efficiency = parallel / (single * float64(cores))
	}

	return CPUResult{
		BLAKE3SingleMBps:   single,
		BLAKE3ParallelMBps: parallel,
		Cores:              cores,
		ScalingEfficiency:  efficiency,
	}
}

// hashThroughput runs BLAKE3 on data across n goroutines and returns
// aggregate throughput in MB/s.
func hashThroughput(data []byte, n int) float64 {
	var wg sync.WaitGroup
	wg.Add(n)

	start := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			h := blake3.New()
			h.Write(data)
			h.Sum(nil)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalBytes := float64(len(data)) * float64(n)
	return (totalBytes / (1024 * 1024)) / elapsed.Seconds()
}

// --- Memory benchmark: sequential write/read bandwidth ---

const memBenchSize = 128 * 1024 * 1024 // 128 MB

func benchMemory() MemResult {
	buf := make([]byte, memBenchSize)
	src := make([]byte, 4096)
	rand.Read(src)

	// Sequential write: fill buf from src in 4K strides.
	start := time.Now()
	for off := 0; off+len(src) <= len(buf); off += len(src) {
		copy(buf[off:], src)
	}
	writeElapsed := time.Since(start)

	// Sequential read: XOR all bytes into an atomic to prevent the
	// compiler from optimizing the loop away.
	var sink atomic.Uint64
	start = time.Now()
	var acc byte
	for i := 0; i < len(buf); i++ {
		acc ^= buf[i]
	}
	sink.Store(uint64(acc))
	readElapsed := time.Since(start)

	mb := float64(memBenchSize) / (1024 * 1024)
	return MemResult{
		SeqWriteMBps: mb / writeElapsed.Seconds(),
		SeqReadMBps:  mb / readElapsed.Seconds(),
	}
}

// --- Disk benchmark: sequential write, read, fsync latency ---

const (
	diskBenchSize  = 64 * 1024 * 1024 // 64 MB total
	diskChunkSize  = 1024 * 1024       // 1 MB per write
	fsyncIterCount = 50                // number of fsync samples
)

func benchDisk(dir string) (DiskResult, error) {
	path := filepath.Join(dir, ".ziggurat-bench-"+fmt.Sprintf("%d", time.Now().UnixNano()))
	defer os.Remove(path)

	chunk := make([]byte, diskChunkSize)
	rand.Read(chunk)
	iterations := diskBenchSize / diskChunkSize

	// Sequential write.
	f, err := os.Create(path)
	if err != nil {
		return DiskResult{}, err
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		if _, err := f.Write(chunk); err != nil {
			f.Close()
			return DiskResult{}, err
		}
	}
	f.Sync()
	writeElapsed := time.Since(start)
	f.Close()

	// Sequential read.
	f, err = os.Open(path)
	if err != nil {
		return DiskResult{}, err
	}

	readBuf := make([]byte, diskChunkSize)
	start = time.Now()
	for {
		_, err := f.Read(readBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return DiskResult{}, err
		}
	}
	readElapsed := time.Since(start)
	f.Close()

	// Fsync latency: write a small block and fsync repeatedly.
	fsyncPath := path + ".fsync"
	defer os.Remove(fsyncPath)

	tiny := make([]byte, 512)
	var totalFsync time.Duration
	for i := 0; i < fsyncIterCount; i++ {
		ff, err := os.Create(fsyncPath)
		if err != nil {
			break
		}
		ff.Write(tiny)
		t := time.Now()
		ff.Sync()
		totalFsync += time.Since(t)
		ff.Close()
	}
	avgFsyncUs := float64(totalFsync.Microseconds()) / float64(fsyncIterCount)

	mb := float64(diskBenchSize) / (1024 * 1024)
	return DiskResult{
		WriteMBps: mb / writeElapsed.Seconds(),
		ReadMBps:  mb / readElapsed.Seconds(),
		FsyncUs:   avgFsyncUs,
	}, nil
}
