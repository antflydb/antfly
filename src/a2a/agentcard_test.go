// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package a2a

import (
	"context"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"go.uber.org/zap"
)

func TestCardProducerBasic(t *testing.T) {
	d := NewDispatcher(zap.NewNop())
	d.Register(&stubHandler{id: "retrieval"})
	d.Register(&stubHandler{id: "query-builder"})

	cp := NewCardProducer(d, "http://localhost:8080")
	card, err := cp.Card(context.Background())
	if err != nil {
		t.Fatalf("Card() returned error: %v", err)
	}

	if card.Name != "Antfly" {
		t.Errorf("expected name 'Antfly', got %q", card.Name)
	}
	if card.URL != "http://localhost:8080/a2a" {
		t.Errorf("expected URL 'http://localhost:8080/a2a', got %q", card.URL)
	}
	if card.Version != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", card.Version)
	}
	if card.ProtocolVersion != string(a2a.Version) {
		t.Errorf("expected protocol version %q, got %q", a2a.Version, card.ProtocolVersion)
	}
	if card.PreferredTransport != a2a.TransportProtocolJSONRPC {
		t.Errorf("expected JSONRPC transport, got %v", card.PreferredTransport)
	}
}

func TestCardProducerCapabilities(t *testing.T) {
	d := NewDispatcher(zap.NewNop())
	d.Register(&stubHandler{id: "retrieval"})

	cp := NewCardProducer(d, "http://localhost:8080")
	card, err := cp.Card(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !card.Capabilities.Streaming {
		t.Error("expected streaming capability to be true")
	}
	if !card.Capabilities.StateTransitionHistory {
		t.Error("expected state transition history capability to be true")
	}
}

func TestCardProducerSkills(t *testing.T) {
	d := NewDispatcher(zap.NewNop())
	d.Register(&stubHandler{id: "retrieval"})
	d.Register(&stubHandler{id: "query-builder"})

	cp := NewCardProducer(d, "http://localhost:8080")
	card, err := cp.Card(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(card.Skills) != 2 {
		t.Fatalf("expected 2 skills, got %d", len(card.Skills))
	}

	ids := map[string]bool{}
	for _, s := range card.Skills {
		ids[s.ID] = true
	}
	if !ids["retrieval"] || !ids["query-builder"] {
		t.Errorf("expected retrieval and query-builder skills, got %v", ids)
	}
}

func TestCardProducerNoSkills(t *testing.T) {
	d := NewDispatcher(zap.NewNop())

	cp := NewCardProducer(d, "http://example.com")
	card, err := cp.Card(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(card.Skills) != 0 {
		t.Errorf("expected 0 skills, got %d", len(card.Skills))
	}
}

func TestCardProducerURLConstruction(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantURL string
	}{
		{
			name:    "http localhost",
			baseURL: "http://localhost:8080",
			wantURL: "http://localhost:8080/a2a",
		},
		{
			name:    "https production",
			baseURL: "https://api.example.com",
			wantURL: "https://api.example.com/a2a",
		},
		{
			name:    "trailing slash stripped by caller",
			baseURL: "http://localhost:8080",
			wantURL: "http://localhost:8080/a2a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDispatcher(zap.NewNop())
			cp := NewCardProducer(d, tt.baseURL)
			card, err := cp.Card(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if card.URL != tt.wantURL {
				t.Errorf("got URL %q, want %q", card.URL, tt.wantURL)
			}
		})
	}
}

func TestCardProducerInputOutputModes(t *testing.T) {
	d := NewDispatcher(zap.NewNop())
	cp := NewCardProducer(d, "http://localhost:8080")
	card, err := cp.Card(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	wantInputModes := []string{"text", "data"}
	if len(card.DefaultInputModes) != len(wantInputModes) {
		t.Fatalf("expected %d input modes, got %d", len(wantInputModes), len(card.DefaultInputModes))
	}
	for i, mode := range wantInputModes {
		if card.DefaultInputModes[i] != mode {
			t.Errorf("input mode %d: got %q, want %q", i, card.DefaultInputModes[i], mode)
		}
	}

	wantOutputModes := []string{"text", "data"}
	if len(card.DefaultOutputModes) != len(wantOutputModes) {
		t.Fatalf("expected %d output modes, got %d", len(wantOutputModes), len(card.DefaultOutputModes))
	}
	for i, mode := range wantOutputModes {
		if card.DefaultOutputModes[i] != mode {
			t.Errorf("output mode %d: got %q, want %q", i, card.DefaultOutputModes[i], mode)
		}
	}
}

func TestCardProducerDescription(t *testing.T) {
	d := NewDispatcher(zap.NewNop())
	cp := NewCardProducer(d, "http://localhost:8080")
	card, err := cp.Card(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if card.Description == "" {
		t.Error("expected non-empty description")
	}
	if !strings.Contains(card.Description, "search") {
		t.Errorf("expected description to mention search, got %q", card.Description)
	}
}
