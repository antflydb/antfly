// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/antflydb/antfly/go/pkg/embedded"
)

// The smoke command exercises the embedded API without any model: it writes
// documents with caller-supplied vectors, runs full-text, dense, and hybrid
// search, reads a document back, and exports a portable backup. The
// lite-go-example build step runs it against a freshly built libantfly.

const smokeSchemaJSON = `{"version":1,"default_type":"note","document_schemas":{"note":{"schema":{"type":"object","required":["title","body"],"additionalProperties":true}}}}`

var smokeIndexes = [][]byte{
	[]byte(`{"name":"note_body_ft","kind":"full_text","config_json":"{}"}`),
	[]byte(`{"name":"note_embedding_v1","kind":"dense_vector","config_json":"{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}"}`),
}

var smokeDocuments = []embedded.WriteIntent{
	{
		Key: "note:local-first",
		Value: []byte(`{
			"title":"Local-first search",
			"body":"Antfly Lite keeps retrieval data in one local file.",
			"_embeddings":{"note_embedding_v1":[1.0,0.0,0.0]}
		}`),
	},
	{
		Key: "note:promotion",
		Value: []byte(`{
			"title":"Promotion path",
			"body":"Use portable backups to promote Lite data into normal Antfly.",
			"_embeddings":{"note_embedding_v1":[0.0,1.0,0.0]}
		}`),
	},
}

func runSmokeCmd(args []string) error {
	fs := flag.NewFlagSet("smoke", flag.ExitOnError)
	dbPath := fs.String("db", "smoke.aflite", "Antfly Lite database path")
	backupPath := fs.String("backup", "smoke.afb", "portable Antfly backup output path")
	reset := fs.Bool("reset", false, "remove the existing database and backup first")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *reset {
		for _, path := range []string{*dbPath, *backupPath} {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove %s: %w", path, err)
			}
		}
	}

	// The smoke check needs no inference runtime, so it uses plain Open and
	// Create rather than the model-backed open options.
	var db *embedded.DB
	created := false
	if _, err := os.Stat(*dbPath); os.IsNotExist(err) {
		created = true
		db, err = embedded.Create(*dbPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", *dbPath, err)
		}
	} else if err != nil {
		return err
	} else if db, err = embedded.Open(*dbPath); err != nil {
		return fmt.Errorf("open %s: %w", *dbPath, err)
	}
	defer db.Close()

	if created {
		if err := db.SetSchemaJSON([]byte(smokeSchemaJSON)); err != nil {
			return fmt.Errorf("set schema: %w", err)
		}
		for _, index := range smokeIndexes {
			if err := db.AddIndexJSON(index); err != nil {
				return fmt.Errorf("add index: %w", err)
			}
		}
	}
	if err := db.Batch(smokeDocuments, 1); err != nil {
		return fmt.Errorf("write documents: %w", err)
	}
	if _, err := db.RunUntilIdleStatus(); err != nil {
		return fmt.Errorf("drain indexes: %w", err)
	}

	lookup, err := db.LookupJSON("note:local-first")
	if err != nil {
		return fmt.Errorf("lookup: %w", err)
	}
	if err := requireContains("lookup", lookup, "Local-first search"); err != nil {
		return err
	}

	checks := []struct {
		label, want string
		request     string
	}{
		{"full-text", "Local-first search", `{"mode":"full_text","index_name":"note_body_ft","text_query_type":"match","field":"body","text":"local retrieval file","limit":3}`},
		{"dense", "note:promotion", `{"embeddings":{"note_embedding_v1":[0.0,1.0,0.0]},"indexes":["note_embedding_v1"],"limit":1}`},
		{"hybrid", "note:promotion", `{"full_text_search":{"match":{"field":"body","text":"portable backups promote"}},"embeddings":{"note_embedding_v1":[0.0,1.0,0.0]},"indexes":["note_embedding_v1"],"merge_config":{"strategy":"rrf"},"limit":3}`},
	}
	for _, check := range checks {
		result, err := db.SearchJSON([]byte(check.request))
		if err != nil {
			return fmt.Errorf("%s search: %w", check.label, err)
		}
		if err := requireContains(check.label, result, check.want); err != nil {
			return err
		}
		fmt.Printf("%s: %s\n", check.label, result)
	}

	if err := db.BackupToFile(*backupPath); err != nil {
		return fmt.Errorf("write portable backup: %w", err)
	}
	status, err := db.Status()
	if err != nil {
		return fmt.Errorf("read status: %w", err)
	}
	fmt.Printf("status: storage=%s/%s inference=%s configured=%t\n", status.Storage.Format, status.Storage.Engine, status.Inference.Mode, status.Inference.Configured)
	fmt.Printf("backup: %s\n", *backupPath)
	return nil
}

func requireContains(label string, body []byte, want string) error {
	if !bytes.Contains(body, []byte(want)) {
		return fmt.Errorf("%s result %s did not contain %q", label, body, want)
	}
	return nil
}
