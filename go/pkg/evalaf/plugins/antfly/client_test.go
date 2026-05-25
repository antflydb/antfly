package antflyevalaf

import (
	"testing"
)

func TestCreateRetrievalAgentTargetFunc(t *testing.T) {
	client, err := NewClient("http://localhost:0")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	fn := client.CreateRetrievalAgentTargetFunc([]string{"test-table"})
	if fn == nil {
		t.Fatal("expected non-nil target func")
	}
}

func TestCreateRetrievalAgentClassificationTargetFunc(t *testing.T) {
	client, err := NewClient("http://localhost:0")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	fn := client.CreateRetrievalAgentClassificationTargetFunc([]string{"test-table"})
	if fn == nil {
		t.Fatal("expected non-nil target func")
	}
}
