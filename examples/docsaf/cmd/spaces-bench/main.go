package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type objectInfo struct {
	key  string
	size int64
}

type record struct {
	id    string
	value json.RawMessage
}

type batch struct {
	id      int64
	records []record
	bytes   int64
}

type sample struct {
	name string
	d    time.Duration
	n    int
}

type sampleSink struct {
	mu      sync.Mutex
	samples []sample
}

type diagSnapshot struct {
	Elapsed                              string         `json:"elapsed"`
	GitCommit                            string         `json:"git_commit,omitempty"`
	RunLabel                             string         `json:"run_label,omitempty"`
	MetricsEnabled                       bool           `json:"metrics_enabled"`
	LoadElapsed                          string         `json:"load_elapsed,omitempty"`
	CatchupElapsed                       string         `json:"catchup_elapsed,omitempty"`
	CatchupComplete                      bool           `json:"catchup_complete,omitempty"`
	Objects                              int64          `json:"objects,omitempty"`
	ObjectBytes                          int64          `json:"object_bytes,omitempty"`
	RecordsRead                          int64          `json:"records_read,omitempty"`
	RecordsWritten                       int64          `json:"records_written,omitempty"`
	InvalidLines                         int64          `json:"invalid_lines,omitempty"`
	BytesRead                            int64          `json:"bytes_read,omitempty"`
	WritePayloadBytes                    int64          `json:"write_payload_bytes,omitempty"`
	BatchesSent                          int64          `json:"batches_sent,omitempty"`
	RecordsPerSec                        float64        `json:"records_per_sec,omitempty"`
	MiBPerSec                            float64        `json:"mib_per_sec,omitempty"`
	RSSBytes                             int64          `json:"rss_bytes,omitempty"`
	HealthMetricsAvailable               bool           `json:"health_metrics_available"`
	ProcessResidentBytes                 int64          `json:"process_resident_bytes,omitempty"`
	ProcessFootprintBytes                int64          `json:"process_footprint_bytes,omitempty"`
	MallocAllocatedBytes                 int64          `json:"malloc_allocated_bytes,omitempty"`
	MallocZoneBytes                      int64          `json:"malloc_zone_bytes,omitempty"`
	FullTextPendingBytes                 int64          `json:"full_text_pending_bytes,omitempty"`
	DerivedBacklogBytes                  int64          `json:"derived_backlog_bytes,omitempty"`
	TextMergeBufferBytes                 int64          `json:"text_merge_buffer_bytes,omitempty"`
	FullTextIndexes                      int64          `json:"full_text_indexes,omitempty"`
	FullTextSegments                     int64          `json:"full_text_segments,omitempty"`
	FullTextSegmentBytes                 int64          `json:"full_text_segment_bytes,omitempty"`
	FullTextMmapSegmentBytes             int64          `json:"full_text_mmap_segment_bytes,omitempty"`
	FullTextHeapSegmentBytes             int64          `json:"full_text_heap_segment_bytes,omitempty"`
	FullTextMaxSegmentBytes              int64          `json:"full_text_max_segment_bytes,omitempty"`
	FullTextStoredFieldsBytes            int64          `json:"full_text_stored_fields_bytes,omitempty"`
	FullTextInvertedBytes                int64          `json:"full_text_inverted_bytes,omitempty"`
	FullTextInvertedHeaderBytes          int64          `json:"full_text_inverted_header_bytes,omitempty"`
	FullTextInvertedFSTBytes             int64          `json:"full_text_inverted_fst_bytes,omitempty"`
	FullTextInvertedBloomBytes           int64          `json:"full_text_inverted_bloom_bytes,omitempty"`
	FullTextInvertedPostingsHeaderBytes  int64          `json:"full_text_inverted_postings_header_bytes,omitempty"`
	FullTextInvertedBlockMaxBytes        int64          `json:"full_text_inverted_block_max_bytes,omitempty"`
	FullTextInvertedChunkMetaBytes       int64          `json:"full_text_inverted_chunk_meta_bytes,omitempty"`
	FullTextInvertedPostingsPayloadBytes int64          `json:"full_text_inverted_postings_payload_bytes,omitempty"`
	FullTextInvertedPositionsBytes       int64          `json:"full_text_inverted_positions_bytes,omitempty"`
	FullTextInvertedSkipBytes            int64          `json:"full_text_inverted_skip_bytes,omitempty"`
	FullTextInvertedOneHitTerms          int64          `json:"full_text_inverted_one_hit_terms,omitempty"`
	FullTextInvertedPostingsTerms        int64          `json:"full_text_inverted_postings_terms,omitempty"`
	FullTextTypedDocValuesBytes          int64          `json:"full_text_typed_doc_values_bytes,omitempty"`
	FullTextDocOrdinalsBytes             int64          `json:"full_text_doc_ordinals_bytes,omitempty"`
	FullTextSectionIndexBytes            int64          `json:"full_text_section_index_bytes,omitempty"`
	TextMergePendingIndexes              int64          `json:"text_merge_pending_indexes,omitempty"`
	TextMergePendingSegments             int64          `json:"text_merge_pending_segments,omitempty"`
	TextMergePendingBytes                int64          `json:"text_merge_pending_bytes,omitempty"`
	TextMergeInFlightMerges              int64          `json:"text_merge_in_flight_merges,omitempty"`
	TextMergeInFlightSegments            int64          `json:"text_merge_in_flight_segments,omitempty"`
	TextMergeCompletedTotal              int64          `json:"text_merge_completed_total,omitempty"`
	TextMergeSkippedStaleTotal           int64          `json:"text_merge_skipped_stale_total,omitempty"`
	TextMergeFailedTotal                 int64          `json:"text_merge_failed_total,omitempty"`
	TextMergeDeferredForPressureTotal    int64          `json:"text_merge_deferred_for_pressure_total,omitempty"`
	TextMergeBackpressureEventsTotal     int64          `json:"text_merge_backpressure_events_total,omitempty"`
	TextMergeBackpressureNsTotal         int64          `json:"text_merge_backpressure_ns_total,omitempty"`
	TextMergeMaxPendingSegments          int64          `json:"text_merge_max_pending_segments,omitempty"`
	TextMergeMaxPendingBytes             int64          `json:"text_merge_max_pending_bytes,omitempty"`
	FullTextBuildUsedBytes               int64          `json:"full_text_build_used_bytes,omitempty"`
	FullTextBuildPeakBytes               int64          `json:"full_text_build_peak_bytes,omitempty"`
	FullTextPendingPeakBytes             int64          `json:"full_text_pending_peak_bytes,omitempty"`
	TextMergeBufferPeakBytes             int64          `json:"text_merge_buffer_peak_bytes,omitempty"`
	LSMCacheUsedBytes                    int64          `json:"lsm_cache_used_bytes,omitempty"`
	LSMCachePeakBytes                    int64          `json:"lsm_cache_peak_bytes,omitempty"`
	LSMCompactionUsedBytes               int64          `json:"lsm_compaction_used_bytes,omitempty"`
	LSMCompactionPeakBytes               int64          `json:"lsm_compaction_peak_bytes,omitempty"`
	LSMStateUsedBytes                    int64          `json:"lsm_state_used_bytes,omitempty"`
	LSMStatePeakBytes                    int64          `json:"lsm_state_peak_bytes,omitempty"`
	SegmentFiles                         int64          `json:"segment_files,omitempty"`
	SegmentBytes                         int64          `json:"segment_bytes,omitempty"`
	VMMapPhysicalFootprintBytes          int64          `json:"vmmap_physical_footprint_bytes,omitempty"`
	VMMapPhysicalPeakBytes               int64          `json:"vmmap_physical_peak_bytes,omitempty"`
	VMMapMappedFileResidentBytes         int64          `json:"vmmap_mapped_file_resident_bytes,omitempty"`
	VMMapMallocAllocatedBytes            int64          `json:"vmmap_malloc_allocated_bytes,omitempty"`
	Extra                                map[string]any `json:"extra,omitempty"`
}

type vmmapStats struct {
	PhysicalFootprintBytes  int64
	PhysicalPeakBytes       int64
	MappedFileResidentBytes int64
	MallocAllocatedBytes    int64
}

type monitor struct {
	start   time.Time
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	samples []diagSnapshot
}

func (s *sampleSink) add(name string, d time.Duration, n int) {
	s.mu.Lock()
	s.samples = append(s.samples, sample{name: name, d: d, n: n})
	s.mu.Unlock()
}

func (s *sampleSink) print(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var values []float64
	for _, sample := range s.samples {
		if sample.name == name {
			values = append(values, float64(sample.d.Microseconds())/1000.0)
		}
	}
	if len(values) == 0 {
		return
	}
	sort.Float64s(values)
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	pick := func(p float64) float64 {
		if len(values) == 1 {
			return values[0]
		}
		idx := int(float64(len(values)-1) * p)
		return values[idx]
	}
	fmt.Printf("%s_ms count=%d min=%.2f p50=%.2f p95=%.2f max=%.2f avg=%.2f\n",
		name, len(values), values[0], pick(0.50), pick(0.95), values[len(values)-1], sum/float64(len(values)))
}

func main() {
	var (
		localDir       = flag.String("local-dir", "", "local directory containing downloaded JSONL objects")
		contains       = flag.String("contains", string(filepath.Separator)+"codex"+string(filepath.Separator), "substring filter for local file paths")
		table          = flag.String("table", "codex_sessions", "Antfly table name")
		antflyURL      = flag.String("antfly-url", "http://127.0.0.1:8080/api/v1", "Antfly API base URL")
		shards         = flag.Int("shards", 8, "table shard count")
		batchSize      = flag.Int("batch-size", 200, "records per Antfly batch")
		batchWorkers   = flag.Int("batch-workers", 16, "concurrent Antfly batch writers")
		objectWorkers  = flag.Int("object-workers", 8, "concurrent local file readers")
		limitObjects   = flag.Int("limit-objects", 0, "optional limit on objects to load")
		skipCreate     = flag.Bool("skip-create", false, "skip table creation")
		samplePID      = flag.Int("sample-pid", 0, "optional Antfly server pid to sample with ps/vmmap")
		healthURL      = flag.String("health-url", "http://127.0.0.1:4200/metrics", "optional Antfly Prometheus metrics URL")
		sampleEvery    = flag.Duration("sample-every", 0, "sample process/health diagnostics during load; disabled when 0")
		waitCatchup    = flag.Duration("wait-catchup", 0, "after load, wait until health metrics show pending index/backlog bytes are drained; disabled when 0")
		dataDir        = flag.String("data-dir", "", "optional Antfly data dir to scan for full-text .seg bytes at the end")
		vmmapOut       = flag.String("vmmap-out", "", "optional file for final vmmap -summary output for --sample-pid")
		summaryOut     = flag.String("summary-out", "", "optional JSON file for final benchmark diagnostics")
		gitCommit      = flag.String("git-commit", "", "optional git commit stamped into --summary-out")
		runLabel       = flag.String("run-label", "", "optional run label stamped into --summary-out")
		metricsEnabled = flag.Bool("metrics-enabled", false, "stamp whether server metrics instrumentation was enabled")
	)
	flag.Parse()
	if *localDir == "" {
		log.Fatal("--local-dir is required")
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 120 * time.Second}
	var mon *monitor
	if *sampleEvery > 0 || *summaryOut != "" {
		mon = startMonitor(ctx, *samplePID, *healthURL, *dataDir, *sampleEvery)
		defer mon.stop()
	}
	if !*skipCreate {
		if err := createTable(ctx, httpClient, *antflyURL, *table, *shards); err != nil {
			log.Fatalf("create table: %v", err)
		}
	}

	fmt.Printf("listing local_dir=%s filter=%s\n", *localDir, *contains)
	listStart := time.Now()
	objects, totalBytes, err := listObjects(*localDir, *contains, *limitObjects)
	if err != nil {
		log.Fatalf("list files: %v", err)
	}
	fmt.Printf("objects=%d bytes=%d mib=%.2f list_elapsed=%s\n", len(objects), totalBytes, float64(totalBytes)/1048576.0, time.Since(listStart))
	if len(objects) == 0 {
		return
	}

	recordsCh := make(chan record, *batchSize**batchWorkers)
	batchesCh := make(chan batch, *batchWorkers)
	samples := &sampleSink{}
	var recordsRead atomic.Int64
	var bytesRead atomic.Int64
	var invalidLines atomic.Int64
	var batchesSent atomic.Int64
	var recordsWritten atomic.Int64
	var writeBytes atomic.Int64
	var firstIDsMu sync.Mutex
	var firstIDs []string

	ingestStart := time.Now()
	var writerWG sync.WaitGroup
	for i := 0; i < *batchWorkers; i++ {
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			for b := range batchesCh {
				start := time.Now()
				if err := postBatchWithRetry(ctx, httpClient, *antflyURL, *table, b); err != nil {
					log.Fatalf("post batch %d: %v", b.id, err)
				}
				samples.add("batch_write", time.Since(start), len(b.records))
				batchesSent.Add(1)
				recordsWritten.Add(int64(len(b.records)))
				writeBytes.Add(b.bytes)
				if sent := batchesSent.Load(); sent%50 == 0 {
					fmt.Printf("progress batches=%d records=%d elapsed=%s\n", sent, recordsWritten.Load(), time.Since(ingestStart))
				}
			}
		}()
	}

	var batcherWG sync.WaitGroup
	batcherWG.Add(1)
	go func() {
		defer batcherWG.Done()
		defer close(batchesCh)
		var current batch
		var batchID int64
		flush := func() {
			if len(current.records) == 0 {
				return
			}
			batchID++
			current.id = batchID
			batchesCh <- current
			current = batch{}
		}
		for rec := range recordsCh {
			if len(current.records) == 0 {
				current.records = make([]record, 0, *batchSize)
			}
			current.records = append(current.records, rec)
			current.bytes += int64(len(rec.value))
			if len(current.records) >= *batchSize {
				flush()
			}
		}
		flush()
	}()

	objectCh := make(chan objectInfo)
	var readerWG sync.WaitGroup
	for i := 0; i < *objectWorkers; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for obj := range objectCh {
				start := time.Now()
				n, readBytes, bad, ids, err := readObject(obj, recordsCh)
				if err != nil {
					log.Fatalf("read object %s: %v", obj.key, err)
				}
				samples.add("object_read", time.Since(start), n)
				recordsRead.Add(int64(n))
				bytesRead.Add(readBytes)
				invalidLines.Add(int64(bad))
				if len(ids) > 0 {
					firstIDsMu.Lock()
					for _, id := range ids {
						if len(firstIDs) < 5 {
							firstIDs = append(firstIDs, id)
						}
					}
					firstIDsMu.Unlock()
				}
			}
		}()
	}

	for _, obj := range objects {
		objectCh <- obj
	}
	close(objectCh)
	readerWG.Wait()
	close(recordsCh)
	batcherWG.Wait()
	writerWG.Wait()

	loadElapsed := time.Since(ingestStart)
	fmt.Printf("load_elapsed=%s records_read=%d records_written=%d invalid_lines=%d bytes_read=%d write_payload_bytes=%d\n",
		loadElapsed, recordsRead.Load(), recordsWritten.Load(), invalidLines.Load(), bytesRead.Load(), writeBytes.Load())
	recordsPerSec := float64(recordsWritten.Load()) / loadElapsed.Seconds()
	mibPerSec := float64(bytesRead.Load()) / 1048576.0 / loadElapsed.Seconds()
	fmt.Printf("throughput records_per_sec=%.2f mib_per_sec=%.2f batches=%d\n",
		recordsPerSec, mibPerSec, batchesSent.Load())
	samples.print("object_read")
	samples.print("batch_write")

	catchupStart := time.Now()
	var catchupElapsed time.Duration
	var catchupComplete bool
	if *waitCatchup > 0 {
		ok, diag, err := waitForCatchup(ctx, *samplePID, *healthURL, *dataDir, *waitCatchup)
		catchupElapsed = time.Since(catchupStart)
		catchupComplete = ok
		if err != nil {
			fmt.Printf("catchup_wait_error elapsed=%s err=%v\n", catchupElapsed, err)
		} else {
			fmt.Printf("catchup_wait elapsed=%s complete=%v full_text_pending_bytes=%d derived_backlog_bytes=%d text_merge_buffer_bytes=%d\n",
				catchupElapsed, ok, diag.FullTextPendingBytes, diag.DerivedBacklogBytes, diag.TextMergeBufferBytes)
		}
	}

	finalDiag := collectDiagnostics(*samplePID, *healthURL, *dataDir, time.Since(ingestStart))
	finalDiag.GitCommit = *gitCommit
	finalDiag.RunLabel = *runLabel
	finalDiag.MetricsEnabled = *metricsEnabled
	finalDiag.LoadElapsed = loadElapsed.String()
	finalDiag.CatchupElapsed = catchupElapsed.String()
	finalDiag.CatchupComplete = catchupComplete
	finalDiag.Objects = int64(len(objects))
	finalDiag.ObjectBytes = totalBytes
	finalDiag.RecordsRead = recordsRead.Load()
	finalDiag.RecordsWritten = recordsWritten.Load()
	finalDiag.InvalidLines = invalidLines.Load()
	finalDiag.BytesRead = bytesRead.Load()
	finalDiag.WritePayloadBytes = writeBytes.Load()
	finalDiag.BatchesSent = batchesSent.Load()
	finalDiag.RecordsPerSec = recordsPerSec
	finalDiag.MiBPerSec = mibPerSec
	fmt.Printf("baseline_diagnostics rss_bytes=%d footprint_bytes=%d malloc_allocated_bytes=%d malloc_zone_bytes=%d full_text_pending_bytes=%d derived_backlog_bytes=%d text_merge_buffer_bytes=%d segment_files=%d segment_bytes=%d\n",
		finalDiag.RSSBytes,
		finalDiag.ProcessFootprintBytes,
		finalDiag.MallocAllocatedBytes,
		finalDiag.MallocZoneBytes,
		finalDiag.FullTextPendingBytes,
		finalDiag.DerivedBacklogBytes,
		finalDiag.TextMergeBufferBytes,
		finalDiag.SegmentFiles,
		finalDiag.SegmentBytes,
	)
	fmt.Printf("full_text_layout segments=%d segment_bytes=%d mmap_segment_bytes=%d heap_segment_bytes=%d stored_fields_bytes=%d inverted_bytes=%d postings_payload_bytes=%d positions_bytes=%d term_dict_bytes=%d typed_doc_values_bytes=%d doc_ordinals_bytes=%d section_index_bytes=%d\n",
		finalDiag.FullTextSegments,
		finalDiag.FullTextSegmentBytes,
		finalDiag.FullTextMmapSegmentBytes,
		finalDiag.FullTextHeapSegmentBytes,
		finalDiag.FullTextStoredFieldsBytes,
		finalDiag.FullTextInvertedBytes,
		finalDiag.FullTextInvertedPostingsPayloadBytes,
		finalDiag.FullTextInvertedPositionsBytes,
		finalDiag.FullTextInvertedFSTBytes,
		finalDiag.FullTextTypedDocValuesBytes,
		finalDiag.FullTextDocOrdinalsBytes,
		finalDiag.FullTextSectionIndexBytes,
	)
	fmt.Printf("merge_and_resource_diagnostics text_merge_pending_segments=%d text_merge_pending_bytes=%d text_merge_completed_total=%d text_merge_failed_total=%d full_text_build_peak_bytes=%d full_text_pending_peak_bytes=%d text_merge_buffer_peak_bytes=%d lsm_cache_peak_bytes=%d lsm_compaction_peak_bytes=%d lsm_state_peak_bytes=%d\n",
		finalDiag.TextMergePendingSegments,
		finalDiag.TextMergePendingBytes,
		finalDiag.TextMergeCompletedTotal,
		finalDiag.TextMergeFailedTotal,
		finalDiag.FullTextBuildPeakBytes,
		finalDiag.FullTextPendingPeakBytes,
		finalDiag.TextMergeBufferPeakBytes,
		finalDiag.LSMCachePeakBytes,
		finalDiag.LSMCompactionPeakBytes,
		finalDiag.LSMStatePeakBytes,
	)
	if *vmmapOut != "" && *samplePID > 0 {
		if vmmapStats, err := writeVMMapSummary(*samplePID, *vmmapOut); err != nil {
			fmt.Printf("vmmap_error err=%v\n", err)
		} else {
			finalDiag.VMMapPhysicalFootprintBytes = vmmapStats.PhysicalFootprintBytes
			finalDiag.VMMapPhysicalPeakBytes = vmmapStats.PhysicalPeakBytes
			finalDiag.VMMapMappedFileResidentBytes = vmmapStats.MappedFileResidentBytes
			finalDiag.VMMapMallocAllocatedBytes = vmmapStats.MallocAllocatedBytes
			fmt.Printf("vmmap_summary=%s\n", *vmmapOut)
		}
	}
	if *summaryOut != "" {
		if mon != nil {
			finalDiag.Extra = map[string]any{"samples": mon.snapshots()}
		}
		if err := writeSummary(*summaryOut, finalDiag); err != nil {
			fmt.Printf("summary_write_error err=%v\n", err)
		} else {
			fmt.Printf("summary=%s\n", *summaryOut)
		}
	}

	firstIDsMu.Lock()
	ids := append([]string(nil), firstIDs...)
	firstIDsMu.Unlock()
	if len(ids) > 0 {
		fmt.Printf("sample_lookups=%d\n", len(ids))
		for _, id := range ids {
			start := time.Now()
			size, err := lookup(ctx, httpClient, *antflyURL, *table, id)
			if err != nil {
				log.Printf("lookup %s failed: %v", id, err)
				continue
			}
			fmt.Printf("lookup id=%s bytes=%d elapsed=%s\n", id, size, time.Since(start))
		}
	}
}

func startMonitor(parent context.Context, pid int, healthURL, dataDir string, every time.Duration) *monitor {
	ctx, cancel := context.WithCancel(parent)
	m := &monitor{start: time.Now(), cancel: cancel, done: make(chan struct{})}
	if every <= 0 {
		close(m.done)
		return m
	}
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				snap := collectDiagnostics(pid, healthURL, dataDir, time.Since(m.start))
				m.mu.Lock()
				m.samples = append(m.samples, snap)
				m.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
	return m
}

func (m *monitor) stop() {
	m.cancel()
	<-m.done
}

func (m *monitor) snapshots() []diagSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]diagSnapshot(nil), m.samples...)
}

func collectDiagnostics(pid int, healthURL, dataDir string, elapsed time.Duration) diagSnapshot {
	out := diagSnapshot{Elapsed: elapsed.String()}
	if pid > 0 {
		out.RSSBytes = psRSSBytes(pid)
	}
	if healthURL != "" {
		if metrics, err := fetchMetrics(healthURL); err == nil {
			out.HealthMetricsAvailable = true
			out.ProcessResidentBytes = int64(promValue(metrics, "antfly_process_resident_bytes", nil))
			out.ProcessFootprintBytes = int64(promValue(metrics, "antfly_process_footprint_bytes", nil))
			out.MallocAllocatedBytes = int64(promValue(metrics, "antfly_process_malloc_allocated_bytes", nil))
			out.MallocZoneBytes = int64(promValue(metrics, "antfly_process_malloc_zone_bytes", nil))
			out.FullTextPendingBytes = int64(promValue(metrics, "antfly_resource_used_bytes", map[string]string{"slice": "full_text.pending_segments"}))
			out.DerivedBacklogBytes = int64(promValue(metrics, "antfly_resource_used_bytes", map[string]string{"slice": "derived.backlog"}))
			out.TextMergeBufferBytes = int64(promValue(metrics, "antfly_resource_used_bytes", map[string]string{"slice": "text_merge.buffers"}))
			out.FullTextIndexes = int64(promValue(metrics, "antfly_full_text_indexes", nil))
			out.FullTextSegments = int64(promValue(metrics, "antfly_full_text_segments", nil))
			out.FullTextSegmentBytes = int64(promValue(metrics, "antfly_full_text_segment_bytes", nil))
			out.FullTextMmapSegmentBytes = int64(promValue(metrics, "antfly_full_text_mmap_segment_bytes", nil))
			out.FullTextHeapSegmentBytes = int64(promValue(metrics, "antfly_full_text_heap_segment_bytes", nil))
			out.FullTextMaxSegmentBytes = int64(promValue(metrics, "antfly_full_text_max_segment_bytes", nil))
			out.FullTextStoredFieldsBytes = int64(promValue(metrics, "antfly_full_text_stored_fields_bytes", nil))
			out.FullTextInvertedBytes = int64(promValue(metrics, "antfly_full_text_inverted_bytes", nil))
			out.FullTextInvertedHeaderBytes = int64(promValue(metrics, "antfly_full_text_inverted_header_bytes", nil))
			out.FullTextInvertedFSTBytes = int64(promValue(metrics, "antfly_full_text_inverted_fst_bytes", nil))
			out.FullTextInvertedBloomBytes = int64(promValue(metrics, "antfly_full_text_inverted_bloom_bytes", nil))
			out.FullTextInvertedPostingsHeaderBytes = int64(promValue(metrics, "antfly_full_text_inverted_postings_header_bytes", nil))
			out.FullTextInvertedBlockMaxBytes = int64(promValue(metrics, "antfly_full_text_inverted_block_max_bytes", nil))
			out.FullTextInvertedChunkMetaBytes = int64(promValue(metrics, "antfly_full_text_inverted_chunk_meta_bytes", nil))
			out.FullTextInvertedPostingsPayloadBytes = int64(promValue(metrics, "antfly_full_text_inverted_postings_payload_bytes", nil))
			out.FullTextInvertedPositionsBytes = int64(promValue(metrics, "antfly_full_text_inverted_positions_bytes", nil))
			out.FullTextInvertedSkipBytes = int64(promValue(metrics, "antfly_full_text_inverted_skip_bytes", nil))
			out.FullTextInvertedOneHitTerms = int64(promValue(metrics, "antfly_full_text_inverted_one_hit_terms", nil))
			out.FullTextInvertedPostingsTerms = int64(promValue(metrics, "antfly_full_text_inverted_postings_terms", nil))
			out.FullTextTypedDocValuesBytes = int64(promValue(metrics, "antfly_full_text_typed_doc_values_bytes", nil))
			out.FullTextDocOrdinalsBytes = int64(promValue(metrics, "antfly_full_text_doc_ordinals_bytes", nil))
			out.FullTextSectionIndexBytes = int64(promValue(metrics, "antfly_full_text_section_index_bytes", nil))
			out.TextMergePendingIndexes = int64(promValue(metrics, "antfly_text_merge_pending_indexes", nil))
			out.TextMergePendingSegments = int64(promValue(metrics, "antfly_text_merge_pending_segments", nil))
			out.TextMergePendingBytes = int64(promValue(metrics, "antfly_text_merge_pending_bytes", nil))
			out.TextMergeInFlightMerges = int64(promValue(metrics, "antfly_text_merge_in_flight_merges", nil))
			out.TextMergeInFlightSegments = int64(promValue(metrics, "antfly_text_merge_in_flight_segments", nil))
			out.TextMergeCompletedTotal = int64(promValue(metrics, "antfly_text_merge_completed_total", nil))
			out.TextMergeSkippedStaleTotal = int64(promValue(metrics, "antfly_text_merge_skipped_stale_total", nil))
			out.TextMergeFailedTotal = int64(promValue(metrics, "antfly_text_merge_failed_total", nil))
			out.TextMergeDeferredForPressureTotal = int64(promValue(metrics, "antfly_text_merge_deferred_for_pressure_total", nil))
			out.TextMergeBackpressureEventsTotal = int64(promValue(metrics, "antfly_text_merge_backpressure_events_total", nil))
			out.TextMergeBackpressureNsTotal = int64(promValue(metrics, "antfly_text_merge_backpressure_ns_total", nil))
			out.TextMergeMaxPendingSegments = int64(promValue(metrics, "antfly_text_merge_max_pending_segments", nil))
			out.TextMergeMaxPendingBytes = int64(promValue(metrics, "antfly_text_merge_max_pending_bytes", nil))
			out.FullTextBuildUsedBytes = int64(promValue(metrics, "antfly_resource_used_bytes", map[string]string{"slice": "full_text.build_working_set"}))
			out.FullTextBuildPeakBytes = int64(promValue(metrics, "antfly_resource_peak_bytes", map[string]string{"slice": "full_text.build_working_set"}))
			out.FullTextPendingPeakBytes = int64(promValue(metrics, "antfly_resource_peak_bytes", map[string]string{"slice": "full_text.pending_segments"}))
			out.TextMergeBufferPeakBytes = int64(promValue(metrics, "antfly_resource_peak_bytes", map[string]string{"slice": "text_merge.buffers"}))
			out.LSMCacheUsedBytes = int64(promValue(metrics, "antfly_resource_used_bytes", map[string]string{"slice": "lsm.block_table_cache"}))
			out.LSMCachePeakBytes = int64(promValue(metrics, "antfly_resource_peak_bytes", map[string]string{"slice": "lsm.block_table_cache"}))
			out.LSMCompactionUsedBytes = int64(promValue(metrics, "antfly_resource_used_bytes", map[string]string{"slice": "lsm.compaction_work"}))
			out.LSMCompactionPeakBytes = int64(promValue(metrics, "antfly_resource_peak_bytes", map[string]string{"slice": "lsm.compaction_work"}))
			out.LSMStateUsedBytes = int64(promValue(metrics, "antfly_resource_used_bytes", map[string]string{"slice": "lsm.in_memory_state"}))
			out.LSMStatePeakBytes = int64(promValue(metrics, "antfly_resource_peak_bytes", map[string]string{"slice": "lsm.in_memory_state"}))
		}
	}
	if dataDir != "" {
		files, bytes := scanSegmentFiles(dataDir)
		out.SegmentFiles = files
		out.SegmentBytes = bytes
	}
	return out
}

func waitForCatchup(ctx context.Context, pid int, healthURL, dataDir string, timeout time.Duration) (bool, diagSnapshot, error) {
	deadline := time.Now().Add(timeout)
	var last diagSnapshot
	stable := 0
	for {
		last = collectDiagnostics(pid, healthURL, dataDir, 0)
		drained := last.HealthMetricsAvailable &&
			last.FullTextPendingBytes == 0 &&
			last.DerivedBacklogBytes == 0 &&
			last.TextMergeBufferBytes == 0
		if drained {
			stable++
			if stable >= 2 {
				return true, last, nil
			}
		} else {
			stable = 0
		}
		if time.Now().After(deadline) {
			return false, last, nil
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return false, last, ctx.Err()
		}
	}
}

func psRSSBytes(pid int) int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return kb * 1024
}

func fetchMetrics(rawURL string) (string, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(rawURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics status=%d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return string(body), err
}

func promValue(metrics, name string, labels map[string]string) float64 {
	for _, line := range strings.Split(metrics, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		metric := fields[0]
		metricName := metric
		if idx := strings.IndexByte(metric, '{'); idx >= 0 {
			metricName = metric[:idx]
		}
		if metricName != name || !promLabelsMatch(metric, labels) {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err == nil {
			return value
		}
	}
	return 0
}

func promLabelsMatch(metric string, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}
	for key, value := range labels {
		needle := key + "=\"" + value + "\""
		if !strings.Contains(metric, needle) {
			return false
		}
	}
	return true
}

func scanSegmentFiles(root string) (int64, int64) {
	var files int64
	var bytes int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if !strings.Contains(path, string(filepath.Separator)+"full_text_index") || !strings.HasSuffix(path, ".seg") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		files++
		bytes += info.Size()
		return nil
	})
	return files, bytes
}

func writeVMMapSummary(pid int, path string) (vmmapStats, error) {
	out, err := exec.Command("vmmap", "-summary", strconv.Itoa(pid)).CombinedOutput()
	if err != nil {
		return vmmapStats{}, fmt.Errorf("%w: %s", err, string(out))
	}
	if err := os.WriteFile(path, out, 0644); err != nil {
		return vmmapStats{}, err
	}
	return parseVMMapSummary(string(out)), nil
}

func parseVMMapSummary(out string) vmmapStats {
	var stats vmmapStats
	for _, raw := range strings.Split(out, "\n") {
		line := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(line, "Physical footprint:"):
			stats.PhysicalFootprintBytes = parseVMMapSize(strings.TrimSpace(strings.TrimPrefix(line, "Physical footprint:")))
		case strings.HasPrefix(line, "Physical footprint (peak):"):
			stats.PhysicalPeakBytes = parseVMMapSize(strings.TrimSpace(strings.TrimPrefix(line, "Physical footprint (peak):")))
		case strings.HasPrefix(line, "mapped file"):
			fields := strings.Fields(line)
			if len(fields) >= 4 {
				stats.MappedFileResidentBytes = parseVMMapSize(fields[3])
			}
		case strings.HasPrefix(line, "TOTAL"):
			fields := strings.Fields(line)
			if len(fields) >= 7 {
				if allocated := parseVMMapSize(fields[6]); allocated > 0 {
					stats.MallocAllocatedBytes = allocated
				}
			}
		}
	}
	return stats
}

func parseVMMapSize(raw string) int64 {
	value := strings.TrimSpace(raw)
	value = strings.TrimSuffix(value, "(peak)")
	if value == "" {
		return 0
	}
	multiplier := float64(1)
	last := value[len(value)-1]
	switch last {
	case 'K', 'k':
		multiplier = 1024
		value = value[:len(value)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		value = value[:len(value)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		value = value[:len(value)-1]
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0
	}
	return int64(parsed * multiplier)
}

func writeSummary(path string, diag diagSnapshot) error {
	body, err := json.MarshalIndent(diag, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	return os.WriteFile(path, body, 0644)
}

func listObjects(root, contains string, limit int) ([]objectInfo, int64, error) {
	var objects []objectInfo
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".jsonl") {
			return nil
		}
		if contains != "" && !strings.Contains(path, contains) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		objects = append(objects, objectInfo{key: path, size: info.Size()})
		total += info.Size()
		if limit > 0 && len(objects) >= limit {
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].key < objects[j].key })
	return objects, total, nil
}

func readObject(obj objectInfo, out chan<- record) (int, int64, int, []string, error) {
	reader, err := os.Open(obj.key)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	defer reader.Close()

	br := bufio.NewReaderSize(reader, 1024*1024)
	keyHash := sha1.Sum([]byte(obj.key))
	keyID := hex.EncodeToString(keyHash[:])[:16]
	lineNo := 0
	valid := 0
	bad := 0
	var bytesRead int64
	var ids []string
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			bytesRead += int64(len(line))
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				if err == io.EOF {
					break
				}
				if err != nil && err != io.EOF {
					return valid, bytesRead, bad, ids, err
				}
				continue
			}
			lineNo++
			if !json.Valid(line) {
				bad++
			} else {
				id := fmt.Sprintf("codex:%s:%08d", keyID, lineNo)
				value, marshalErr := json.Marshal(map[string]any{
					"source_key": obj.key,
					"line":       lineNo,
					"size":       obj.size,
					"payload":    json.RawMessage(line),
				})
				if marshalErr != nil {
					return valid, bytesRead, bad, ids, marshalErr
				}
				out <- record{id: id, value: value}
				if len(ids) < 2 {
					ids = append(ids, id)
				}
				valid++
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return valid, bytesRead, bad, ids, err
		}
	}
	return valid, bytesRead, bad, ids, nil
}

func createTable(ctx context.Context, client *http.Client, baseURL, table string, shards int) error {
	body, _ := json.Marshal(map[string]any{"num_shards": shards})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/tables/"+url.PathEscape(table), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		fmt.Printf("created table=%s shards=%d\n", table, shards)
		return nil
	}
	if resp.StatusCode == http.StatusBadRequest && bytes.Contains(respBody, []byte("exists")) {
		fmt.Printf("table=%s already exists\n", table)
		return nil
	}
	return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(respBody))
}

func postBatch(ctx context.Context, client *http.Client, baseURL, table string, b batch) error {
	inserts := make(map[string]json.RawMessage, len(b.records))
	for _, rec := range b.records {
		inserts[rec.id] = rec.value
	}
	body, err := json.Marshal(map[string]any{"inserts": inserts})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/tables/"+url.PathEscape(table)+"/batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(respBody))
}

func postBatchWithRetry(ctx context.Context, client *http.Client, baseURL, table string, b batch) error {
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		if attempt > 0 {
			sleep := time.Duration(100*(1<<min(attempt, 5))) * time.Millisecond
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err := postBatch(ctx, client, baseURL, table, b)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func lookup(ctx context.Context, client *http.Client, baseURL, table, key string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/tables/"+url.PathEscape(table)+"/lookup/"+url.PathEscape(key), nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return 0, errors.New(string(body))
	}
	return len(body), nil
}
