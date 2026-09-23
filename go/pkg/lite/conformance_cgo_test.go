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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Runs the shared libantfly conformance cases (see
// zig/pkg/antfly/capi-conformance/README.md) through the Go binding.
func TestConformance(t *testing.T) {
	casesDir := filepath.Join("..", "..", "..", "zig", "pkg", "antfly", "capi-conformance", "cases")
	files, err := filepath.Glob(filepath.Join(casesDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no conformance cases in %s", casesDir)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var c conformanceCase
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		t.Run(c.Name, func(t *testing.T) {
			r := &conformanceRunner{dir: t.TempDir()}
			defer r.closeCurrent()
			if err := r.openCurrent(c.Open); err != nil {
				t.Fatalf("open: %v", err)
			}
			for i, step := range c.Steps {
				if err := r.runStep(step); err != nil {
					t.Fatalf("step %d (%s): %v", i, step.Op, err)
				}
			}
		})
	}
}

type conformanceCase struct {
	Name  string            `json:"name"`
	Open  conformanceOpen   `json:"open"`
	Steps []conformanceStep `json:"steps"`
}

type conformanceOpen struct {
	Storage       string `json:"storage"`
	Create        bool   `json:"create"`
	Mode          string `json:"mode"`
	Profile       string `json:"profile"`
	NoSync        bool   `json:"no_sync"`
	BusyTimeoutMS uint64 `json:"busy_timeout_ms"`
	Path          string `json:"path"`
}

type conformanceWrite struct {
	Key    string          `json:"key"`
	Value  json.RawMessage `json:"value"`
	Delete bool            `json:"delete"`
}

type conformanceStep struct {
	conformanceOpen
	Op            string             `json:"op"`
	Writes        []conformanceWrite `json:"writes"`
	Timestamp     uint64             `json:"timestamp"`
	Key           string             `json:"key"`
	Request       json.RawMessage    `json:"request"`
	Config        json.RawMessage    `json:"config"`
	Schema        json.RawMessage    `json:"schema"`
	Name          string             `json:"name"`
	Kind          string             `json:"kind"`
	Index         string             `json:"index"`
	EdgeType      string             `json:"edge_type"`
	Direction     string             `json:"direction"`
	TxnID         string             `json:"txn_id"`
	Status        string             `json:"status"`
	CommitVersion uint64             `json:"commit_version"`
	Expect        *conformanceExpect `json:"expect"`
}

type conformanceExpect struct {
	Error       string          `json:"error"`
	JSONSubset  json.RawMessage `json:"json_subset"`
	Contains    []string        `json:"contains"`
	NotContains []string        `json:"not_contains"`
	Equals      json.RawMessage `json:"equals"`
}

type conformanceRunner struct {
	dir    string
	db     *DB
	path   string
	backup []byte
}

func (r *conformanceRunner) resolvePath(name string) string {
	if name == "" {
		name = "db.aflite"
	}
	return filepath.Join(r.dir, name)
}

func conformanceOpenOptions(o conformanceOpen) (OpenOptions, error) {
	opts := OpenOptions{NoSync: o.NoSync, BusyTimeout: time.Duration(o.BusyTimeoutMS) * time.Millisecond}
	storage, err := conformanceStorage(o.Storage)
	if err != nil {
		return opts, err
	}
	opts.Storage = storage
	switch o.Mode {
	case "", "writer":
		opts.Mode = OpenModeWriter
	case "readonly":
		opts.Mode = OpenModeReadonly
	case "status_only":
		opts.Mode = OpenModeStatusOnly
	default:
		return opts, fmt.Errorf("unknown mode %q", o.Mode)
	}
	switch o.Profile {
	case "", "native":
		opts.Profile = ProfileNative
	case "hosted":
		opts.Profile = ProfileHosted
	default:
		return opts, fmt.Errorf("unknown profile %q", o.Profile)
	}
	return opts, nil
}

func conformanceStorage(name string) (Storage, error) {
	switch name {
	case "", "lite":
		return StorageLite, nil
	case "directory":
		return StorageDirectory, nil
	default:
		return 0, fmt.Errorf("unknown storage %q", name)
	}
}

func conformanceOpenDB(path string, o conformanceOpen) (*DB, error) {
	opts, err := conformanceOpenOptions(o)
	if err != nil {
		return nil, err
	}
	if o.Create {
		return CreateWithOptions(path, opts)
	}
	return OpenWithOptions(path, opts)
}

func (r *conformanceRunner) openCurrent(o conformanceOpen) error {
	path := r.resolvePath(o.Path)
	db, err := conformanceOpenDB(path, o)
	if err != nil {
		return err
	}
	r.db, r.path = db, path
	return nil
}

func (r *conformanceRunner) closeCurrent() {
	if r.db != nil {
		r.db.Close()
		r.db = nil
	}
}

func conformanceWrites(writes []conformanceWrite) []WriteIntent {
	out := make([]WriteIntent, 0, len(writes))
	for _, w := range writes {
		intent := WriteIntent{Key: w.Key, Delete: w.Delete}
		if !w.Delete {
			intent.Value = []byte(w.Value)
		}
		out = append(out, intent)
	}
	return out
}

func conformanceTxnID(hexID string) (TxnID, error) {
	var id TxnID
	raw, err := hex.DecodeString(hexID)
	if err != nil || len(raw) != len(id) {
		return id, fmt.Errorf("txn_id %q must be 32 hex characters", hexID)
	}
	copy(id[:], raw)
	return id, nil
}

// runStep executes one step and checks its expectation.
func (r *conformanceRunner) runStep(step conformanceStep) error {
	result, err := r.execute(step)
	expect := step.Expect
	if expect == nil {
		return err
	}
	if expect.Error != "" {
		if err == nil {
			return fmt.Errorf("succeeded, want %s", expect.Error)
		}
		var code ErrorCode
		if !errors.As(err, &code) {
			return fmt.Errorf("error %v is not an ErrorCode, want %s", err, expect.Error)
		}
		if code.Name() != expect.Error {
			return fmt.Errorf("error %s, want %s", code.Name(), expect.Error)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return checkConformanceResult(result, expect)
}

func checkConformanceResult(result any, expect *conformanceExpect) error {
	text := ""
	switch v := result.(type) {
	case []byte:
		text = string(v)
	case nil:
	default:
		encoded, _ := json.Marshal(v)
		text = string(encoded)
	}
	if len(expect.JSONSubset) > 0 {
		var want, got any
		if err := json.Unmarshal(expect.JSONSubset, &want); err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(text), &got); err != nil {
			return fmt.Errorf("result is not JSON: %v: %s", err, text)
		}
		if !jsonSubset(want, got) {
			return fmt.Errorf("result %s does not contain %s", text, expect.JSONSubset)
		}
	}
	for _, s := range expect.Contains {
		if !strings.Contains(text, s) {
			return fmt.Errorf("result %s does not contain %q", text, s)
		}
	}
	for _, s := range expect.NotContains {
		if strings.Contains(text, s) {
			return fmt.Errorf("result %s unexpectedly contains %q", text, s)
		}
	}
	if len(expect.Equals) > 0 {
		var want any
		if err := json.Unmarshal(expect.Equals, &want); err != nil {
			return err
		}
		var got any
		if err := json.Unmarshal([]byte(text), &got); err != nil {
			return fmt.Errorf("scalar result %q is not JSON: %v", text, err)
		}
		if !reflect.DeepEqual(want, got) {
			return fmt.Errorf("result %v, want %v", got, want)
		}
	}
	return nil
}

func jsonSubset(want, got any) bool {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return false
		}
		for k, wv := range w {
			gv, ok := g[k]
			if !ok || !jsonSubset(wv, gv) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for i := range w {
			if !jsonSubset(w[i], g[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(want, got)
	}
}

func (r *conformanceRunner) execute(step conformanceStep) (any, error) {
	db := r.db
	switch step.Op {
	case "batch":
		return nil, db.Batch(conformanceWrites(step.Writes), step.Timestamp)
	case "batch_json":
		return db.BatchJSON(step.Request)
	case "lookup":
		return db.LookupJSON(step.Key)
	case "scan":
		return db.ScanJSON(step.Request)
	case "search":
		return db.SearchJSON(step.Request)
	case "stats":
		return db.StatsJSON()
	case "status":
		return db.StatusJSON()
	case "capabilities":
		return db.CapabilitiesJSON()
	case "check":
		return db.CheckJSON()
	case "pending_work_stats":
		return db.PendingWorkStatsJSON()
	case "run_until_idle":
		return nil, db.RunUntilIdle()
	case "get_schema":
		return db.SchemaJSON()
	case "set_schema":
		return nil, db.SetSchemaJSON(step.Schema)
	case "list_indexes":
		return db.IndexesJSON()
	case "add_index":
		return nil, db.AddIndexJSON(step.Config)
	case "delete_index":
		return db.DeleteIndex(step.Name)
	case "get_edges":
		directions := map[string]uint8{"": 0, "out": 0, "in": 1, "both": 2}
		direction, ok := directions[step.Direction]
		if !ok {
			return nil, fmt.Errorf("unknown direction %q", step.Direction)
		}
		return db.EdgesJSON(step.Index, step.Key, step.EdgeType, direction)
	case "list_enrichments":
		return db.EnrichmentsJSON()
	case "add_enrichment":
		return nil, db.AddEnrichmentJSON(step.Config)
	case "delete_enrichment":
		return db.DeleteEnrichment(step.Kind, step.Name)
	case "begin_transaction", "write_transaction", "resolve_transaction", "transaction_status", "commit_version":
		return r.executeTransaction(step)
	case "backup":
		backup, err := db.Backup()
		r.backup = backup
		return nil, err
	case "import_backup":
		return nil, db.ImportBackup(r.backup)
	case "restore_open":
		path := r.resolvePath(step.Path)
		storage, err := conformanceStorage(step.Storage)
		if err != nil {
			return nil, err
		}
		if err := Restore(path, r.backup, RestoreOptions{Storage: storage}); err != nil {
			return nil, err
		}
		r.closeCurrent()
		return nil, r.openCurrent(step.conformanceOpen)
	case "reopen":
		r.closeCurrent()
		o := step.conformanceOpen
		if o.Path == "" {
			o.Path = filepath.Base(r.path)
		}
		return nil, r.openCurrent(o)
	case "open_second":
		second, err := conformanceOpenDB(r.resolvePath(step.Path), step.conformanceOpen)
		if err == nil {
			second.Close()
		}
		return nil, err
	case "close":
		r.closeCurrent()
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown op %q", step.Op)
	}
}

func (r *conformanceRunner) executeTransaction(step conformanceStep) (any, error) {
	id, err := conformanceTxnID(step.TxnID)
	if err != nil {
		return nil, err
	}
	switch step.Op {
	case "begin_transaction":
		return nil, r.db.BeginTransaction(id, step.Timestamp, nil)
	case "write_transaction":
		return nil, r.db.WriteTransaction(id, conformanceWrites(step.Writes))
	case "resolve_transaction":
		status := TxnCommitted
		if step.Status == "aborted" {
			status = TxnAborted
		} else if step.Status != "committed" {
			return nil, fmt.Errorf("unknown status %q", step.Status)
		}
		return nil, r.db.ResolveTransaction(id, status, step.CommitVersion)
	case "transaction_status":
		status, err := r.db.TransactionStatus(id)
		if err != nil {
			return nil, err
		}
		names := map[TxnStatus]string{TxnPending: "pending", TxnCommitted: "committed", TxnAborted: "aborted"}
		return names[status], nil
	default: // commit_version
		return r.db.CommitVersion(id)
	}
}
