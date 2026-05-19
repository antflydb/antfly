// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type config struct {
	dir                    string
	samples                int
	docs                   int
	dims                   int
	batchSize              int
	seed                   uint64
	syncWrites             bool
	flushAfter             bool
	keepDir                bool
	disableAutoCompactions bool
	memTableSize           int
}

type diskStats struct {
	TotalBytes    int64
	SSTBytes      int64
	WALBytes      int64
	ManifestBytes int64
	OtherBytes    int64
	Files         int
	SSTFiles      int
	WALFiles      int
	ManifestFiles int
	OtherFiles    int
}

type metricSnap struct {
	FlushCount                int64
	CompactionCount           int64
	CompactionDurationNs      int64
	CompactionDebt            uint64
	CompactionInProgressBytes int64
	L0Runs                    int64
	L0Bytes                   int64
	L0Sublevels               int32
	RunBytes                  uint64
	RunCount                  int64
	ObsoleteTableBytes        uint64
	ObsoleteTableCount        uint64
	TablesFlushed             uint64
	TableBytesFlushed         uint64
	TablesCompacted           uint64
	TableBytesCompacted       uint64
	TableBytesRead            uint64
	WALBytesIn                uint64
	WALBytesWritten           uint64
	WALFiles                  int64
	ReadAmp                   int
}

type batchRecord struct {
	Engine            string `json:"engine"`
	Scenario          string `json:"scenario"`
	Storage           string `json:"storage"`
	Sample            int    `json:"sample"`
	Phase             string `json:"phase"`
	BatchIndex        int    `json:"batch_index"`
	DocStart          int    `json:"doc_start"`
	DocEnd            int    `json:"doc_end"`
	Docs              int    `json:"docs"`
	TotalDocs         int    `json:"total_docs"`
	Dims              int    `json:"dims"`
	BatchSize         int    `json:"batch_size"`
	Sync              bool   `json:"sync"`
	WallNS            int64  `json:"wall_ns"`
	LogicalValueBytes int    `json:"logical_value_bytes"`
}

type summaryRecord struct {
	Engine                               string  `json:"engine"`
	Scenario                             string  `json:"scenario"`
	Storage                              string  `json:"storage"`
	Sample                               int     `json:"sample"`
	Phase                                string  `json:"phase"`
	Docs                                 int     `json:"docs"`
	Dims                                 int     `json:"dims"`
	BatchSize                            int     `json:"batch_size"`
	Batches                              int     `json:"batches"`
	Sync                                 bool    `json:"sync"`
	WriteNS                              int64   `json:"write_ns"`
	WriteNSPerDoc                        float64 `json:"write_ns_per_doc"`
	MaxBatchNS                           int64   `json:"max_batch_ns"`
	FinalFlushNS                         int64   `json:"final_flush_ns"`
	LogicalValueWriteBytes               int64   `json:"logical_value_write_bytes"`
	DiskTotalBytes                       int64   `json:"disk_total_bytes"`
	SSTBytes                             int64   `json:"sst_bytes"`
	WALBytes                             int64   `json:"wal_bytes"`
	ManifestBytes                        int64   `json:"manifest_bytes"`
	OtherBytes                           int64   `json:"other_bytes"`
	Files                                int     `json:"files"`
	SSTFiles                             int     `json:"sst_files"`
	WALFiles                             int     `json:"wal_files"`
	ManifestFiles                        int     `json:"manifest_files"`
	OtherFiles                           int     `json:"other_files"`
	LSMFlushes                           int64   `json:"lsm_flushes"`
	LSMFlushOutputRuns                   uint64  `json:"lsm_flush_output_runs"`
	LSMFlushOutputBytes                  uint64  `json:"lsm_flush_output_bytes"`
	LSMTableFileWrites                   uint64  `json:"lsm_table_file_writes"`
	LSMTableFileBytes                    uint64  `json:"lsm_table_file_bytes"`
	Compactions                          int64   `json:"compactions"`
	CompactionInputBytes                 uint64  `json:"compaction_input_bytes"`
	CompactionOutputBytes                uint64  `json:"compaction_output_bytes"`
	CompactionNS                         int64   `json:"compaction_ns"`
	L0RunsAfter                          int64   `json:"l0_runs_after"`
	L0BytesAfter                         int64   `json:"l0_bytes_after"`
	L0SublevelsAfter                     int32   `json:"l0_sublevels_after"`
	RunBytesAfter                        uint64  `json:"run_bytes_after"`
	RunCountAfter                        int64   `json:"runs_after"`
	ObsoleteTableBytesAfter              uint64  `json:"obsolete_table_bytes_after"`
	ObsoletePathsAfter                   uint64  `json:"obsolete_paths_after"`
	PebbleCompactionDebtAfter            uint64  `json:"pebble_compaction_debt_after"`
	PebbleCompactionInProgressBytesAfter int64   `json:"pebble_compaction_in_progress_bytes_after"`
	PebbleReadAmpAfter                   int     `json:"pebble_read_amp_after"`
	PebbleWALBytesIn                     uint64  `json:"pebble_wal_bytes_in"`
	PebbleWALBytesWritten                uint64  `json:"pebble_wal_bytes_written"`
	PebbleWALFilesAfter                  int64   `json:"pebble_wal_files_after"`
	Dir                                  string  `json:"dir"`
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.dir, "dir", "", "database directory; a temp dir is used when empty")
	flag.IntVar(&cfg.samples, "samples", 1, "fresh Pebble database samples to run")
	flag.IntVar(&cfg.docs, "docs", 50000, "document count")
	flag.IntVar(&cfg.dims, "dims", 1536, "embedding dimensions")
	flag.IntVar(&cfg.batchSize, "batch-size", 500, "documents per Pebble batch")
	flag.Uint64Var(&cfg.seed, "seed", 42, "deterministic data seed")
	flag.BoolVar(&cfg.syncWrites, "sync", false, "commit Pebble batches with Sync")
	flag.BoolVar(&cfg.flushAfter, "flush-after", true, "flush Pebble memtables after ingest")
	flag.BoolVar(&cfg.keepDir, "keep-dir", false, "keep the auto-created temp database directory")
	flag.BoolVar(&cfg.disableAutoCompactions, "disable-auto-compactions", false, "disable Pebble automatic compactions")
	flag.IntVar(&cfg.memTableSize, "mem-table-size", 0, "Pebble memtable size in bytes; 0 uses Pebble default")
	flag.Parse()

	if cfg.samples <= 0 || cfg.docs <= 0 || cfg.dims <= 0 || cfg.batchSize <= 0 {
		panic("all numeric args must be > 0")
	}

	for sample := 0; sample < cfg.samples; sample++ {
		if err := runSample(cfg, sample); err != nil {
			panic(err)
		}
	}
}

func runSample(cfg config, sample int) error {
	dir, cleanup, err := sampleDir(cfg, sample)
	if err != nil {
		return err
	}
	if cleanup {
		defer os.RemoveAll(dir)
	}

	opts := &pebble.Options{
		DisableAutomaticCompactions: cfg.disableAutoCompactions,
	}
	if cfg.memTableSize > 0 {
		opts.MemTableSize = uint64(cfg.memTableSize)
	}

	db, err := pebble.Open(filepath.Clean(dir), opts)
	if err != nil {
		return err
	}
	defer db.Close()

	writeOptions := pebble.NoSync
	if cfg.syncWrites {
		writeOptions = pebble.Sync
	}

	before := captureMetrics(db)
	var writeNS int64
	var maxBatchNS int64
	var logicalValueBytes int64
	batches := 0

	for start := 0; start < cfg.docs; start += cfg.batchSize {
		end := minInt(start+cfg.batchSize, cfg.docs)
		keys, values, batchLogicalBytes := buildBatchDocs(cfg, start, end)
		logicalValueBytes += int64(batchLogicalBytes)

		started := time.Now()
		batch := db.NewBatch()
		for i := range keys {
			if err := batch.Set(keys[i], values[i], nil); err != nil {
				_ = batch.Close()
				return err
			}
		}
		if err := batch.Commit(writeOptions); err != nil {
			_ = batch.Close()
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
		elapsed := time.Since(started).Nanoseconds()
		writeNS += elapsed
		if elapsed > maxBatchNS {
			maxBatchNS = elapsed
		}
		batches++

		if err := json.NewEncoder(os.Stdout).Encode(batchRecord{
			Engine:            "pebble",
			Scenario:          "pebble_dense_ingest",
			Storage:           "pebble",
			Sample:            sample,
			Phase:             "write_batch",
			BatchIndex:        batches - 1,
			DocStart:          start,
			DocEnd:            end,
			Docs:              end - start,
			TotalDocs:         cfg.docs,
			Dims:              cfg.dims,
			BatchSize:         cfg.batchSize,
			Sync:              cfg.syncWrites,
			WallNS:            elapsed,
			LogicalValueBytes: batchLogicalBytes,
		}); err != nil {
			return err
		}
	}

	var finalFlushNS int64
	if cfg.flushAfter {
		started := time.Now()
		if err := db.Flush(); err != nil {
			return err
		}
		finalFlushNS = time.Since(started).Nanoseconds()
	}
	after := captureMetrics(db)
	disk, err := collectDiskStats(dir)
	if err != nil {
		return err
	}

	tableWriteBytes := deltaU64(after.TableBytesFlushed, before.TableBytesFlushed) + deltaU64(after.TableBytesCompacted, before.TableBytesCompacted)
	writeNSPerDoc := 0.0
	if cfg.docs > 0 {
		writeNSPerDoc = float64(writeNS) / float64(cfg.docs)
	}

	return json.NewEncoder(os.Stdout).Encode(summaryRecord{
		Engine:                               "pebble",
		Scenario:                             "pebble_dense_ingest",
		Storage:                              "pebble",
		Sample:                               sample,
		Phase:                                "ingest_summary",
		Docs:                                 cfg.docs,
		Dims:                                 cfg.dims,
		BatchSize:                            cfg.batchSize,
		Batches:                              batches,
		Sync:                                 cfg.syncWrites,
		WriteNS:                              writeNS,
		WriteNSPerDoc:                        writeNSPerDoc,
		MaxBatchNS:                           maxBatchNS,
		FinalFlushNS:                         finalFlushNS,
		LogicalValueWriteBytes:               logicalValueBytes,
		DiskTotalBytes:                       disk.TotalBytes,
		SSTBytes:                             disk.SSTBytes,
		WALBytes:                             disk.WALBytes,
		ManifestBytes:                        disk.ManifestBytes,
		OtherBytes:                           disk.OtherBytes,
		Files:                                disk.Files,
		SSTFiles:                             disk.SSTFiles,
		WALFiles:                             disk.WALFiles,
		ManifestFiles:                        disk.ManifestFiles,
		OtherFiles:                           disk.OtherFiles,
		LSMFlushes:                           after.FlushCount - before.FlushCount,
		LSMFlushOutputRuns:                   deltaU64(after.TablesFlushed, before.TablesFlushed),
		LSMFlushOutputBytes:                  deltaU64(after.TableBytesFlushed, before.TableBytesFlushed),
		LSMTableFileWrites:                   deltaU64(after.TablesFlushed, before.TablesFlushed) + deltaU64(after.TablesCompacted, before.TablesCompacted),
		LSMTableFileBytes:                    tableWriteBytes,
		Compactions:                          after.CompactionCount - before.CompactionCount,
		CompactionInputBytes:                 deltaU64(after.TableBytesRead, before.TableBytesRead),
		CompactionOutputBytes:                deltaU64(after.TableBytesCompacted, before.TableBytesCompacted),
		CompactionNS:                         after.CompactionDurationNs - before.CompactionDurationNs,
		L0RunsAfter:                          after.L0Runs,
		L0BytesAfter:                         after.L0Bytes,
		L0SublevelsAfter:                     after.L0Sublevels,
		RunBytesAfter:                        after.RunBytes,
		RunCountAfter:                        after.RunCount,
		ObsoleteTableBytesAfter:              after.ObsoleteTableBytes,
		ObsoletePathsAfter:                   after.ObsoleteTableCount,
		PebbleCompactionDebtAfter:            after.CompactionDebt,
		PebbleCompactionInProgressBytesAfter: after.CompactionInProgressBytes,
		PebbleReadAmpAfter:                   after.ReadAmp,
		PebbleWALBytesIn:                     deltaU64(after.WALBytesIn, before.WALBytesIn),
		PebbleWALBytesWritten:                deltaU64(after.WALBytesWritten, before.WALBytesWritten),
		PebbleWALFilesAfter:                  after.WALFiles,
		Dir:                                  dir,
	})
}

func buildBatchDocs(cfg config, start, end int) ([][]byte, [][]byte, int) {
	keys := make([][]byte, 0, end-start)
	values := make([][]byte, 0, end-start)
	logicalBytes := 0
	for docIdx := start; docIdx < end; docIdx++ {
		key := []byte(fmt.Sprintf("doc:%08d", docIdx))
		value := encodeVectorDocJSON(cfg, docIdx)
		keys = append(keys, key)
		values = append(values, value)
		logicalBytes += len(value)
	}
	return keys, values, logicalBytes
}

func encodeVectorDocJSON(cfg config, docIdx int) []byte {
	out := make([]byte, 0, 16+cfg.dims*12)
	out = append(out, `{"embedding":[`...)
	base := float32(docIdx%8) * 0.25
	for dimIdx := 0; dimIdx < cfg.dims; dimIdx++ {
		if dimIdx != 0 {
			out = append(out, ',')
		}
		value := base + deterministicNoise(cfg.seed, docIdx, dimIdx)
		out = strconv.AppendFloat(out, float64(value), 'g', -1, 32)
	}
	out = append(out, ']', '}')
	return out
}

func deterministicNoise(seed uint64, docIdx, dimIdx int) float32 {
	x := seed ^
		(uint64(docIdx+1) * 0x9E3779B97F4A7C15) ^
		(uint64(dimIdx+1) * 0xC2B2AE3D27D4EB4F)
	x ^= x >> 33
	x *= 0xFF51AFD7ED558CCD
	x ^= x >> 33
	x *= 0xC4CEB9FE1A85EC53
	x ^= x >> 33
	return (float32(x&1023) / 1024.0) * 0.01
}

func sampleDir(cfg config, sample int) (string, bool, error) {
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "antfly-pebble-dense-ingest-")
		return dir, !cfg.keepDir, err
	}
	dir := filepath.Clean(cfg.dir)
	if cfg.samples > 1 {
		dir = filepath.Join(dir, fmt.Sprintf("sample-%d", sample))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", false, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false, err
	}
	if len(entries) > 0 {
		return "", false, fmt.Errorf("database directory %q is not empty; omit --dir or pass an empty directory", dir)
	}
	return dir, false, nil
}

func captureMetrics(db *pebble.DB) metricSnap {
	metrics := db.Metrics()
	snap := metricSnap{
		FlushCount:                metrics.Flush.Count,
		CompactionCount:           metrics.Compact.Count,
		CompactionDurationNs:      metrics.Compact.Duration.Nanoseconds(),
		CompactionDebt:            metrics.Compact.EstimatedDebt,
		CompactionInProgressBytes: metrics.Compact.InProgressBytes,
		ObsoleteTableBytes:        metrics.Table.Local.ObsoleteSize,
		ObsoleteTableCount:        metrics.Table.Local.ObsoleteCount,
		WALBytesIn:                metrics.WAL.BytesIn,
		WALBytesWritten:           metrics.WAL.BytesWritten,
		WALFiles:                  metrics.WAL.Files,
		ReadAmp:                   metrics.ReadAmp(),
	}
	if len(metrics.Levels) > 0 {
		snap.L0Runs = metrics.Levels[0].TablesCount
		snap.L0Bytes = metrics.Levels[0].TablesSize
		snap.L0Sublevels = metrics.Levels[0].Sublevels
	}
	for _, level := range metrics.Levels {
		snap.RunBytes += nonNegativeInt64(level.TablesSize)
		snap.RunCount += level.TablesCount
		snap.TablesFlushed += level.TablesFlushed
		snap.TableBytesFlushed += level.TableBytesFlushed
		snap.TablesCompacted += level.TablesCompacted
		snap.TableBytesCompacted += level.TableBytesCompacted
		snap.TableBytesRead += level.TableBytesRead
	}
	return snap
}

func collectDiskStats(root string) (diskStats, error) {
	var stats diskStats
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		size := info.Size()
		stats.TotalBytes += size
		stats.Files++
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, ".sst"):
			stats.SSTBytes += size
			stats.SSTFiles++
		case strings.HasSuffix(name, ".log"):
			stats.WALBytes += size
			stats.WALFiles++
		case strings.HasPrefix(name, "MANIFEST"):
			stats.ManifestBytes += size
			stats.ManifestFiles++
		default:
			stats.OtherBytes += size
			stats.OtherFiles++
		}
		return nil
	})
	return stats, err
}

func deltaU64(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func nonNegativeInt64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func _normalize(v []float32) {
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(norm))
	for i := range v {
		v[i] *= inv
	}
}
