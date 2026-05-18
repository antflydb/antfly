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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type config struct {
	dir                    string
	samples                int
	keys                   int
	hotKeys                int
	overwriteRounds        int
	valueSize              int
	valuePattern           string
	batchSize              int
	syncWrites             bool
	compact                bool
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

type benchRecord struct {
	Engine                               string  `json:"engine"`
	Scenario                             string  `json:"scenario"`
	Storage                              string  `json:"storage"`
	Mode                                 string  `json:"mode"`
	Sample                               int     `json:"sample"`
	Phase                                string  `json:"phase"`
	Workload                             string  `json:"workload"`
	Keys                                 int     `json:"keys"`
	HotKeys                              int     `json:"hot_keys"`
	OverwriteRounds                      int     `json:"overwrite_rounds"`
	ValueSize                            int     `json:"value_size"`
	ValuePattern                         string  `json:"value_pattern"`
	BatchSize                            int     `json:"batch_size"`
	Ops                                  int     `json:"ops"`
	NS                                   int64   `json:"ns"`
	OpsPerSec                            float64 `json:"ops_per_sec"`
	NSPerOp                              float64 `json:"ns_per_op"`
	LogicalValueWriteBytes               int     `json:"logical_value_write_bytes"`
	CumulativeValueWriteBytes            int     `json:"cumulative_value_write_bytes"`
	LiveValueBytes                       int     `json:"live_value_bytes"`
	WriteAmpTable                        float64 `json:"write_amp_table"`
	SpaceAmpLiveTable                    float64 `json:"space_amp_live_table"`
	SpaceAmpDisk                         float64 `json:"space_amp_disk"`
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
	flag.IntVar(&cfg.keys, "keys", 20000, "initial key count")
	flag.IntVar(&cfg.hotKeys, "hot-keys", 1000, "hot key count overwritten repeatedly")
	flag.IntVar(&cfg.overwriteRounds, "overwrite-rounds", 20, "number of hot-key overwrite rounds")
	flag.IntVar(&cfg.valueSize, "value-size", 128, "value bytes per write")
	flag.StringVar(&cfg.valuePattern, "value-pattern", "repeat", "value pattern: repeat, deterministic, or keyed")
	flag.IntVar(&cfg.batchSize, "batch-size", 1000, "writes per Pebble batch")
	flag.BoolVar(&cfg.syncWrites, "sync", false, "commit Pebble batches with Sync")
	flag.BoolVar(&cfg.compact, "compact", true, "run a full manual compaction after overwrites")
	flag.BoolVar(&cfg.keepDir, "keep-dir", false, "keep the auto-created temp database directory")
	flag.BoolVar(&cfg.disableAutoCompactions, "disable-auto-compactions", false, "disable Pebble automatic compactions")
	flag.IntVar(&cfg.memTableSize, "mem-table-size", 0, "Pebble memtable size in bytes; 0 uses Pebble default")
	flag.Parse()

	if cfg.samples <= 0 || cfg.keys <= 0 || cfg.hotKeys <= 0 || cfg.overwriteRounds <= 0 || cfg.valueSize <= 0 || cfg.batchSize <= 0 {
		log.Fatal("all numeric args must be > 0")
	}
	if cfg.hotKeys > cfg.keys {
		cfg.hotKeys = cfg.keys
	}
	if err := validateValuePattern(cfg.valuePattern); err != nil {
		log.Fatal(err)
	}

	for sample := 0; sample < cfg.samples; sample++ {
		if err := runSample(cfg, sample); err != nil {
			log.Fatal(err)
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

	value, err := makeValue(cfg.valueSize, cfg.valuePattern, 0x12345678)
	if err != nil {
		return err
	}
	updateValue, err := makeValue(cfg.valueSize, cfg.valuePattern, 0x9abcdef0)
	if err != nil {
		return err
	}
	writeOptions := pebble.NoSync
	if cfg.syncWrites {
		writeOptions = pebble.Sync
	}

	before := captureMetrics(db)
	loadElapsed, err := writeKeys(db, cfg, value, writeOptions)
	if err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}
	after := captureMetrics(db)
	if err := emit("load_base", cfg, sample, dir, cfg.keys, cfg.keys*cfg.valueSize, cfg.keys*cfg.valueSize, cfg.keys*cfg.valueSize, loadElapsed, before, after); err != nil {
		return err
	}

	overwriteOps := cfg.hotKeys * cfg.overwriteRounds
	before = after
	overwriteElapsed, err := overwriteHotSet(db, cfg, updateValue, writeOptions)
	if err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}
	after = captureMetrics(db)
	if err := emit("overwrite_hotset", cfg, sample, dir, overwriteOps, overwriteOps*cfg.valueSize, (cfg.keys+overwriteOps)*cfg.valueSize, cfg.keys*cfg.valueSize, overwriteElapsed, before, after); err != nil {
		return err
	}

	if cfg.compact {
		before = after
		start := time.Now()
		if err := db.Compact(context.Background(), []byte("doc:"), []byte("doc;"), true); err != nil {
			return err
		}
		if err := db.Flush(); err != nil {
			return err
		}
		after = captureMetrics(db)
		if err := emit("maintenance_hotset", cfg, sample, dir, 1, 0, (cfg.keys+overwriteOps)*cfg.valueSize, cfg.keys*cfg.valueSize, time.Since(start), before, after); err != nil {
			return err
		}
	}
	return nil
}

func sampleDir(cfg config, sample int) (string, bool, error) {
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "antfly-pebble-overwrite-")
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

func writeKeys(db *pebble.DB, cfg config, value []byte, options *pebble.WriteOptions) (time.Duration, error) {
	start := time.Now()
	scratch := make([]byte, cfg.valueSize)
	opIndex := 0
	for i := 0; i < cfg.keys; {
		batch := db.NewBatch()
		written := 0
		for i < cfg.keys && written < cfg.batchSize {
			key := keyFor(i)
			writeValue := valueForWrite(cfg, value, scratch, key, opIndex, 0x12345678)
			if err := batch.Set(key, writeValue, nil); err != nil {
				_ = batch.Close()
				return 0, err
			}
			i++
			written++
			opIndex++
		}
		if err := batch.Commit(options); err != nil {
			_ = batch.Close()
			return 0, err
		}
		if err := batch.Close(); err != nil {
			return 0, err
		}
	}
	return time.Since(start), nil
}

func overwriteHotSet(db *pebble.DB, cfg config, value []byte, options *pebble.WriteOptions) (time.Duration, error) {
	start := time.Now()
	scratch := make([]byte, cfg.valueSize)
	keyIndex := 0
	opIndex := 0
	remaining := cfg.hotKeys * cfg.overwriteRounds
	for remaining > 0 {
		batch := db.NewBatch()
		written := 0
		for remaining > 0 && written < cfg.batchSize {
			key := keyFor(keyIndex)
			writeValue := valueForWrite(cfg, value, scratch, key, opIndex, 0x9abcdef0)
			if err := batch.Set(key, writeValue, nil); err != nil {
				_ = batch.Close()
				return 0, err
			}
			keyIndex++
			if keyIndex == cfg.hotKeys {
				keyIndex = 0
			}
			remaining--
			written++
			opIndex++
		}
		if err := batch.Commit(options); err != nil {
			_ = batch.Close()
			return 0, err
		}
		if err := batch.Close(); err != nil {
			return 0, err
		}
	}
	return time.Since(start), nil
}

func keyFor(i int) []byte {
	return []byte(fmt.Sprintf("doc:%012d", i))
}

func emit(workload string, cfg config, sample int, dir string, ops int, logicalWriteBytes int, cumulativeWriteBytes int, liveValueBytes int, elapsed time.Duration, before metricSnap, after metricSnap) error {
	disk, err := collectDiskStats(dir)
	if err != nil {
		return err
	}
	seconds := elapsed.Seconds()
	if seconds == 0 {
		seconds = 1e-9
	}
	tableWriteBytes := deltaU64(after.TableBytesFlushed, before.TableBytesFlushed) + deltaU64(after.TableBytesCompacted, before.TableBytesCompacted)
	record := benchRecord{
		Engine:                               "pebble",
		Scenario:                             "pebble_default_hot_overwrite",
		Storage:                              "pebble",
		Mode:                                 "default",
		Sample:                               sample,
		Phase:                                workload,
		Workload:                             workload,
		Keys:                                 cfg.keys,
		HotKeys:                              cfg.hotKeys,
		OverwriteRounds:                      cfg.overwriteRounds,
		ValueSize:                            cfg.valueSize,
		ValuePattern:                         cfg.valuePattern,
		BatchSize:                            cfg.batchSize,
		Ops:                                  ops,
		NS:                                   elapsed.Nanoseconds(),
		OpsPerSec:                            float64(ops) / seconds,
		NSPerOp:                              float64(elapsed.Nanoseconds()) / float64(maxInt(ops, 1)),
		LogicalValueWriteBytes:               logicalWriteBytes,
		CumulativeValueWriteBytes:            cumulativeWriteBytes,
		LiveValueBytes:                       liveValueBytes,
		WriteAmpTable:                        ratioWork(tableWriteBytes, logicalWriteBytes),
		SpaceAmpLiveTable:                    ratioU64(after.RunBytes, uint64(maxInt(liveValueBytes, 1))),
		SpaceAmpDisk:                         ratioU64(uint64(maxInt64(disk.TotalBytes, 0)), uint64(maxInt(liveValueBytes, 1))),
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
	}
	return json.NewEncoder(os.Stdout).Encode(record)
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

func deltaU64(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func ratioWork(numerator uint64, logicalWriteBytes int) float64 {
	if logicalWriteBytes <= 0 {
		return 0
	}
	return float64(numerator) / float64(logicalWriteBytes)
}

func ratioU64(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func nonNegativeInt64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func makeValue(size int, pattern string, seed uint64) ([]byte, error) {
	value := make([]byte, size)
	switch pattern {
	case "repeat":
		for i := range value {
			value[i] = byte(seed)
		}
	case "deterministic":
		fillDeterministicValue(value, seed)
	case "keyed":
		fillDeterministicValue(value, seed)
	default:
		return nil, fmt.Errorf("unknown value pattern %q", pattern)
	}
	return value, nil
}

func valueForWrite(cfg config, fallback []byte, scratch []byte, key []byte, opIndex int, seed uint64) []byte {
	if cfg.valuePattern != "keyed" {
		return fallback
	}
	keyedSeed := fnv64(seed, key)
	keyedSeed += uint64(opIndex) * 0x9e3779b97f4a7c15
	fillDeterministicValue(scratch, keyedSeed)
	return scratch
}

func fillDeterministicValue(value []byte, seed uint64) {
	state := seed | 1
	for i := range value {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		value[i] = byte(state)
	}
}

func validateValuePattern(pattern string) error {
	switch pattern {
	case "repeat", "deterministic", "keyed":
		return nil
	default:
		return fmt.Errorf("unknown value pattern %q", pattern)
	}
}

func fnv64(seed uint64, key []byte) uint64 {
	h := uint64(1469598103934665603) ^ seed
	for _, b := range key {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
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
