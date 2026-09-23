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

//go:build cgo && libantfly

package lite

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestThreadingModeIsSerialized(t *testing.T) {
	if got := ThreadingMode(); got != ThreadingSerialized {
		t.Fatalf("ThreadingMode() = %d, want %d", got, ThreadingSerialized)
	}

	db, err := CreateWithOptions(filepath.Join(t.TempDir(), "threading.aflite"), OpenOptions{NoSync: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Close()
	caps, err := db.Capabilities()
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if caps.Threading != "serialized" {
		t.Fatalf("capabilities threading = %q, want serialized", caps.Threading)
	}
}

// One handle shared by goroutines doing writes, reads, drains, and exclusive
// schema calls at once. Writes must queue rather than fail with Busy, and
// every write must be visible afterwards. Run with -race to also check the
// Go wrapper.
func TestConcurrentCallsOnOneHandle(t *testing.T) {
	db, err := CreateWithOptions(filepath.Join(t.TempDir(), "concurrent.aflite"), OpenOptions{NoSync: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Close()

	const writers, writesPerWriter, readers = 4, 25, 4
	var timestamp atomic.Uint64
	var wg sync.WaitGroup
	errs := make(chan error, writers*writesPerWriter+readers+2)
	stop := make(chan struct{})

	for w := range writers {
		wg.Go(func() {
			for i := range writesPerWriter {
				key := fmt.Sprintf("doc:w%d:%d", w, i)
				value := fmt.Appendf(nil, `{"body":"concurrent writer %d item %d"}`, w, i)
				if err := db.Batch([]WriteIntent{{Key: key, Value: value}}, timestamp.Add(1)); err != nil {
					errs <- fmt.Errorf("batch %s: %w", key, err)
					return
				}
			}
		})
	}

	var readerWG sync.WaitGroup
	search := []byte(`{"full_text_search":{"match":{"field":"body","text":"concurrent writer"}},"limit":5}`)
	scan := []byte(`{"from":"doc:w","to":"doc:x","include_documents":true,"limit":20}`)
	for range readers {
		readerWG.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := db.SearchJSON(search); err != nil {
					errs <- fmt.Errorf("search: %w", err)
					return
				}
				if _, err := db.StatsJSON(); err != nil {
					errs <- fmt.Errorf("stats: %w", err)
					return
				}
				if _, err := db.ScanJSON(scan); err != nil {
					errs <- fmt.Errorf("scan: %w", err)
					return
				}
				if _, err := db.LookupJSON("doc:w0:0"); err != nil && err != NotFound {
					errs <- fmt.Errorf("lookup: %w", err)
					return
				}
			}
		})
	}

	// A drain and an exclusive schema-path call alongside the traffic.
	readerWG.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := db.RunUntilIdle(); err != nil {
				errs <- fmt.Errorf("run until idle: %w", err)
				return
			}
			if _, err := db.DeleteIndex("no_such_index"); err != nil {
				errs <- fmt.Errorf("delete missing index: %w", err)
				return
			}
		}
	})

	wg.Wait()
	close(stop)
	readerWG.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	if err := db.RunUntilIdle(); err != nil {
		t.Fatalf("final run until idle: %v", err)
	}
	for w := range writers {
		for i := range writesPerWriter {
			key := fmt.Sprintf("doc:w%d:%d", w, i)
			got, err := db.LookupJSON(key)
			if err != nil {
				t.Fatalf("lookup %s after concurrent writes: %v", key, err)
			}
			if !bytes.Contains(got, fmt.Appendf(nil, "item %d", i)) {
				t.Fatalf("lookup %s = %s", key, got)
			}
		}
	}
}

// Close must wait for in-flight calls, and calls racing or following it must
// fail cleanly rather than touch a freed handle.
func TestCloseRacesInFlightCalls(t *testing.T) {
	db, err := CreateWithOptions(filepath.Join(t.TempDir(), "close-race.aflite"), OpenOptions{NoSync: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Batch([]WriteIntent{{Key: "doc:close", Value: []byte(`{"body":"close race"}`)}}, 1); err != nil {
		t.Fatalf("batch: %v", err)
	}

	var wg sync.WaitGroup
	var closedSeen atomic.Int32
	for range 8 {
		wg.Go(func() {
			for {
				_, err := db.LookupJSON("doc:close")
				switch err {
				case nil:
				case InvalidArgument:
					closedSeen.Add(1)
					return
				default:
					t.Errorf("lookup during close: %v", err)
					return
				}
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	var closers sync.WaitGroup
	for range 3 {
		closers.Go(func() {
			if err := db.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		})
	}
	closers.Wait()
	wg.Wait()
	if closedSeen.Load() != 8 {
		t.Fatalf("%d of 8 readers observed the closed handle", closedSeen.Load())
	}
	if _, err := db.StatsJSON(); err != InvalidArgument {
		t.Fatalf("call after close = %v, want %v", err, InvalidArgument)
	}
}

func TestBusyTimeoutWaitsForWriterLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy-timeout.aflite")
	first, err := CreateWithOptions(path, OpenOptions{NoSync: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Without a timeout the second writer fails immediately.
	if _, err := OpenWithOptions(path, OpenOptions{NoSync: true}); err != Busy {
		t.Fatalf("second writer without timeout = %v, want %v", err, Busy)
	}

	// With a short timeout it fails with Busy only after waiting.
	start := time.Now()
	if _, err := OpenWithOptions(path, OpenOptions{NoSync: true, BusyTimeout: 150 * time.Millisecond}); err != Busy {
		t.Fatalf("second writer with short timeout = %v, want %v", err, Busy)
	}
	if waited := time.Since(start); waited < 140*time.Millisecond {
		t.Fatalf("busy timeout returned after %v, want about 150ms", waited)
	}

	// With a longer timeout it succeeds once the first writer closes.
	go func() {
		time.Sleep(100 * time.Millisecond)
		first.Close()
	}()
	second, err := OpenWithOptions(path, OpenOptions{NoSync: true, BusyTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("second writer after first closed: %v", err)
	}
	second.Close()
}

// Handle values are opaque ids that the Go binding keeps in an
// unsafe.Pointer. Reopening reuses the same registry slot under a new
// generation each time; forcing GC between iterations makes the collector
// inspect every generation's value, which must never look like a bad Go
// heap pointer.
func TestHandleValuesSurviveGCAcrossSlotReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gc-handles.aflite")
	db, err := CreateWithOptions(path, OpenOptions{NoSync: true})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	db.Close()
	var stale []*DB
	for i := range 300 {
		db, err := OpenWithOptions(path, OpenOptions{NoSync: true})
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if _, err := db.StatsJSON(); err != nil {
			t.Fatalf("stats %d: %v", i, err)
		}
		db.Close()
		stale = append(stale, db)
		runtime.GC()
	}
	runtime.KeepAlive(stale)
}
