package main

import "testing"

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
