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
