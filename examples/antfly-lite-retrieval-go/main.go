// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/antflydb/antfly/go/pkg/antflylite"
)

const schemaJSON = `{"version":1,"default_type":"note","document_schemas":{"note":{"schema":{"type":"object","required":["title","body"],"additionalProperties":true}}}}`

var indexes = [][]byte{
	[]byte(`{"name":"note_body_ft","kind":"full_text","config_json":"{}"}`),
	[]byte(`{"name":"note_embedding_v1","kind":"dense_vector","config_json":"{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}"}`),
}

var documents = []antflylite.WriteIntent{
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

func main() {
	dbPath := flag.String("db", "retrieval.aflite", "Antfly Lite database path")
	backupPath := flag.String("backup", "retrieval.afb", "portable Antfly backup output path")
	reset := flag.Bool("reset", false, "remove the existing Lite database and backup before running")
	flag.Parse()

	if *reset {
		removeIfExists(*dbPath)
		removeIfExists(*backupPath)
	}

	db, created, err := openOrCreateLite(*dbPath)
	if err != nil {
		log.Fatalf("open or create Lite database: %v", err)
	}
	defer db.Close()

	if created {
		if err := db.SetSchemaJSON([]byte(schemaJSON)); err != nil {
			log.Fatalf("set schema: %v", err)
		}
		for _, index := range indexes {
			if err := db.AddIndexJSON(index); err != nil {
				log.Fatalf("add index: %v", err)
			}
		}
	}
	if err := db.Batch(documents, 1); err != nil {
		log.Fatalf("write documents: %v", err)
	}
	if _, err := db.RunUntilIdleStatus(); err != nil {
		log.Fatalf("drain indexes: %v", err)
	}

	fullText := mustSearch(db, []byte(`{"mode":"full_text","index_name":"note_body_ft","text_query_type":"match","field":"body","text":"local retrieval file","limit":3}`))
	requireContains("full-text", fullText, "Local-first search")

	dense := mustSearch(db, []byte(`{"embeddings":{"note_embedding_v1":[0.0,1.0,0.0]},"indexes":["note_embedding_v1"],"limit":1}`))
	requireContains("dense", dense, "note:promotion")

	hybrid := mustSearch(db, []byte(`{"full_text_search":{"match":{"field":"body","text":"portable backups promote"}},"embeddings":{"note_embedding_v1":[0.0,1.0,0.0]},"indexes":["note_embedding_v1"],"merge_config":{"strategy":"rrf"},"limit":3}`))
	requireContains("hybrid", hybrid, "note:promotion")

	if err := db.BackupToFile(*backupPath); err != nil {
		log.Fatalf("write portable backup: %v", err)
	}

	status, err := db.Status()
	if err != nil {
		log.Fatalf("read status: %v", err)
	}
	fmt.Printf("status: storage=%s/%s inference=%s configured=%t\n", status.Storage.Format, status.Storage.Engine, status.Inference.Mode, status.Inference.Configured)
	fmt.Printf("full-text: %s\n", fullText)
	fmt.Printf("dense: %s\n", dense)
	fmt.Printf("hybrid: %s\n", hybrid)
	fmt.Printf("backup: %s\n", *backupPath)
}

func removeIfExists(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Fatalf("remove %s: %v", path, err)
	}
}

func openOrCreateLite(path string) (*antflylite.DB, bool, error) {
	if _, err := os.Stat(path); err == nil {
		db, openErr := antflylite.Open(path)
		return db, false, openErr
	} else if !os.IsNotExist(err) {
		return nil, false, err
	}
	db, err := antflylite.Create(path)
	return db, true, err
}

func mustSearch(db *antflylite.DB, request []byte) []byte {
	result, err := db.SearchJSON(request)
	if err != nil {
		log.Fatalf("search %s: %v", request, err)
	}
	return result
}

func requireContains(label string, body []byte, want string) {
	if !bytes.Contains(body, []byte(want)) {
		log.Fatalf("%s result %s did not contain %q", label, body, want)
	}
}
