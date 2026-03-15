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

package pebbleutils

import (
	"io"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// stubMerger is a test ValueMerger that records which prefix matched.
type stubMerger struct {
	tag   string
	value []byte
}

func (s *stubMerger) MergeNewer(value []byte) error { return nil }
func (s *stubMerger) MergeOlder(value []byte) error { return nil }
func (s *stubMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return s.value, nil, nil
}

func TestRegistryDispatchByPrefix(t *testing.T) {
	reg := NewRegistry()

	var matched string
	reg.Register([]byte("alpha:"), func(key, value []byte) (pebble.ValueMerger, error) {
		matched = "alpha"
		return &stubMerger{tag: "alpha", value: value}, nil
	})
	reg.Register([]byte("beta:"), func(key, value []byte) (pebble.ValueMerger, error) {
		matched = "beta"
		return &stubMerger{tag: "beta", value: value}, nil
	})

	merger := reg.NewMerger("test.v1")

	// alpha: prefix → alpha handler
	matched = ""
	_, err := merger.Merge([]byte("alpha:key1"), []byte("val"))
	if err != nil {
		t.Fatal(err)
	}
	if matched != "alpha" {
		t.Errorf("expected alpha, got %q", matched)
	}

	// beta: prefix → beta handler
	matched = ""
	_, err = merger.Merge([]byte("beta:key2"), []byte("val"))
	if err != nil {
		t.Fatal(err)
	}
	if matched != "beta" {
		t.Errorf("expected beta, got %q", matched)
	}
}

func TestRegistryFallbackLastWriteWins(t *testing.T) {
	reg := NewRegistry()
	reg.Register([]byte("known:"), func(key, value []byte) (pebble.ValueMerger, error) {
		return &stubMerger{tag: "known", value: value}, nil
	})

	merger := reg.NewMerger("test.v1")

	// Unknown prefix → last-write-wins fallback
	vm, err := merger.Merge([]byte("unknown:key"), []byte("first"))
	if err != nil {
		t.Fatal(err)
	}

	// MergeNewer replaces value
	if err := vm.MergeNewer([]byte("second")); err != nil {
		t.Fatal(err)
	}

	result, _, err := vm.Finish(true)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "second" {
		t.Errorf("expected 'second', got %q", result)
	}
}

func TestRegistryLongestPrefixMatch(t *testing.T) {
	reg := NewRegistry()

	var matched string
	reg.Register([]byte("inv:"), func(key, value []byte) (pebble.ValueMerger, error) {
		matched = "short"
		return &stubMerger{tag: "short", value: value}, nil
	})
	reg.Register([]byte("inv:42:chunk"), func(key, value []byte) (pebble.ValueMerger, error) {
		matched = "long"
		return &stubMerger{tag: "long", value: value}, nil
	})

	merger := reg.NewMerger("test.v1")

	// Key matching longer prefix should use longer handler
	matched = ""
	_, err := merger.Merge([]byte("inv:42:chunk0"), []byte("val"))
	if err != nil {
		t.Fatal(err)
	}
	if matched != "long" {
		t.Errorf("expected long, got %q", matched)
	}

	// Key matching only shorter prefix should use shorter handler
	matched = ""
	_, err = merger.Merge([]byte("inv:42:meta"), []byte("val"))
	if err != nil {
		t.Fatal(err)
	}
	if matched != "short" {
		t.Errorf("expected short, got %q", matched)
	}
}

func TestLastWriteWinsMerger(t *testing.T) {
	m := NewLastWriteWins([]byte("initial"))

	// Newer value replaces
	if err := m.MergeNewer([]byte("newer")); err != nil {
		t.Fatal(err)
	}
	result, _, err := m.Finish(true)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "newer" {
		t.Errorf("expected 'newer', got %q", result)
	}

	// Older value is discarded
	m2 := NewLastWriteWins([]byte("current"))
	if err := m2.MergeOlder([]byte("old")); err != nil {
		t.Fatal(err)
	}
	result, _, err = m2.Finish(true)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "current" {
		t.Errorf("expected 'current', got %q", result)
	}
}

func TestMergerName(t *testing.T) {
	reg := NewRegistry()
	merger := reg.NewMerger("antfly.merge.v1")
	if merger.Name != "antfly.merge.v1" {
		t.Errorf("expected name 'antfly.merge.v1', got %q", merger.Name)
	}
}
