package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCatchupDrainedScopes(t *testing.T) {
	diag := diagSnapshot{
		HealthMetricsAvailable: true,
		FullTextPendingBytes:   0,
		DerivedBacklogBytes:    7020,
		TextMergeBufferBytes:   0,
	}
	if catchupDrained(diag, catchupScopeAll) {
		t.Fatalf("all scope should wait for derived backlog bytes")
	}
	if !catchupDrained(diag, catchupScopeFullText) {
		t.Fatalf("full-text scope should only wait for full-text pending and merge buffers")
	}
}

func TestParseCatchupScope(t *testing.T) {
	if _, err := parseCatchupScope("all"); err != nil {
		t.Fatalf("parse all: %v", err)
	}
	if _, err := parseCatchupScope("full-text"); err != nil {
		t.Fatalf("parse full-text: %v", err)
	}
	if _, err := parseCatchupScope("derived"); err == nil {
		t.Fatalf("expected invalid catch-up scope to fail")
	}
}

func TestApplySamplePeaks(t *testing.T) {
	diag := diagSnapshot{
		RSSBytes:                 10,
		ProcessFootprintBytes:    20,
		MallocAllocatedBytes:     30,
		FullTextMmapSegmentBytes: 40,
	}
	applySamplePeaks(&diag, []diagSnapshot{
		{
			RSSBytes:                 100,
			ProcessResidentBytes:     90,
			ProcessFootprintBytes:    15,
			MallocAllocatedBytes:     25,
			FullTextMmapSegmentBytes: 50,
			SegmentBytes:             45,
		},
		{
			RSSBytes:              80,
			ProcessFootprintBytes: 120,
			MallocAllocatedBytes:  35,
			SegmentBytes:          70,
		},
	})
	if diag.PeakRSSBytes != 100 {
		t.Fatalf("PeakRSSBytes=%d", diag.PeakRSSBytes)
	}
	if diag.PeakProcessResidentBytes != 90 {
		t.Fatalf("PeakProcessResidentBytes=%d", diag.PeakProcessResidentBytes)
	}
	if diag.PeakProcessFootprintBytes != 120 {
		t.Fatalf("PeakProcessFootprintBytes=%d", diag.PeakProcessFootprintBytes)
	}
	if diag.PeakMallocAllocatedBytes != 35 {
		t.Fatalf("PeakMallocAllocatedBytes=%d", diag.PeakMallocAllocatedBytes)
	}
	if diag.PeakFullTextMmapSegmentBytes != 50 {
		t.Fatalf("PeakFullTextMmapSegmentBytes=%d", diag.PeakFullTextMmapSegmentBytes)
	}
	if diag.PeakSegmentBytes != 70 {
		t.Fatalf("PeakSegmentBytes=%d", diag.PeakSegmentBytes)
	}
}

func TestCollectDiagnosticsIncludesLSMMutableSnapshotCloneStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
# TYPE antfly_lsm_mutable_snapshot_clone_calls_total counter
antfly_lsm_mutable_snapshot_clone_calls_total 7
# TYPE antfly_lsm_mutable_snapshot_clone_bytes_total counter
antfly_lsm_mutable_snapshot_clone_bytes_total 8192
# TYPE antfly_lsm_mutable_snapshot_clone_peak_bytes gauge
antfly_lsm_mutable_snapshot_clone_peak_bytes 4096
# TYPE antfly_lsm_read_snapshot_mutable_rotations_total counter
antfly_lsm_read_snapshot_mutable_rotations_total 3
# TYPE antfly_lsm_read_snapshot_mutable_rotation_bytes_total counter
antfly_lsm_read_snapshot_mutable_rotation_bytes_total 16384
# TYPE antfly_lsm_read_snapshot_mutable_rotation_peak_bytes gauge
antfly_lsm_read_snapshot_mutable_rotation_peak_bytes 12288
# TYPE antfly_lsm_mutable_snapshot_clone_reason_calls_total counter
antfly_lsm_mutable_snapshot_clone_reason_calls_total{reason="bound_read_txn"} 5
antfly_lsm_mutable_snapshot_clone_reason_calls_total{reason="namespace_read_txn"} 2
antfly_lsm_mutable_snapshot_clone_reason_calls_total{reason="other"} 0
# TYPE antfly_lsm_mutable_snapshot_clone_reason_bytes_total counter
antfly_lsm_mutable_snapshot_clone_reason_bytes_total{reason="bound_read_txn"} 6144
antfly_lsm_mutable_snapshot_clone_reason_bytes_total{reason="namespace_read_txn"} 2048
antfly_lsm_mutable_snapshot_clone_reason_bytes_total{reason="other"} 0
`))
	}))
	defer server.Close()

	diag := collectDiagnostics(0, server.URL, "", time.Second)
	if !diag.HealthMetricsAvailable {
		t.Fatalf("expected health metrics to be available")
	}
	if diag.LSMMutableSnapshotCloneCalls != 7 {
		t.Fatalf("LSMMutableSnapshotCloneCalls=%d", diag.LSMMutableSnapshotCloneCalls)
	}
	if diag.LSMMutableSnapshotCloneBytesTotal != 8192 {
		t.Fatalf("LSMMutableSnapshotCloneBytesTotal=%d", diag.LSMMutableSnapshotCloneBytesTotal)
	}
	if diag.LSMMutableSnapshotClonePeakBytes != 4096 {
		t.Fatalf("LSMMutableSnapshotClonePeakBytes=%d", diag.LSMMutableSnapshotClonePeakBytes)
	}
	if diag.LSMReadSnapshotMutableRotations != 3 {
		t.Fatalf("LSMReadSnapshotMutableRotations=%d", diag.LSMReadSnapshotMutableRotations)
	}
	if diag.LSMReadSnapshotMutableRotationBytes != 16384 {
		t.Fatalf("LSMReadSnapshotMutableRotationBytes=%d", diag.LSMReadSnapshotMutableRotationBytes)
	}
	if diag.LSMReadSnapshotMutableRotationPeak != 12288 {
		t.Fatalf("LSMReadSnapshotMutableRotationPeak=%d", diag.LSMReadSnapshotMutableRotationPeak)
	}
	if diag.LSMMutableSnapshotBoundReadTxnCalls != 5 || diag.LSMMutableSnapshotBoundReadTxnBytes != 6144 {
		t.Fatalf("bound read txn clone stats calls=%d bytes=%d", diag.LSMMutableSnapshotBoundReadTxnCalls, diag.LSMMutableSnapshotBoundReadTxnBytes)
	}
	if diag.LSMMutableSnapshotNamespaceTxnCalls != 2 || diag.LSMMutableSnapshotNamespaceTxnBytes != 2048 {
		t.Fatalf("namespace txn clone stats calls=%d bytes=%d", diag.LSMMutableSnapshotNamespaceTxnCalls, diag.LSMMutableSnapshotNamespaceTxnBytes)
	}
}
