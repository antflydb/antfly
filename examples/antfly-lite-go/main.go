// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/antflydb/antfly/go/pkg/antflylite"
)

type document struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

func main() {
	dbPath := flag.String("db", "demo.aflite", "Antfly Lite database path")
	backupPath := flag.String("backup", "demo.afb", "portable Antfly backup output path")
	reset := flag.Bool("reset", false, "remove the existing Lite database and backup before running")
	flag.Parse()

	if *reset {
		if err := os.Remove(*dbPath); err != nil && !os.IsNotExist(err) {
			log.Fatalf("remove database: %v", err)
		}
		if err := os.Remove(*backupPath); err != nil && !os.IsNotExist(err) {
			log.Fatalf("remove backup: %v", err)
		}
	}

	db, err := openOrCreateLite(*dbPath)
	if err != nil {
		log.Fatalf("open or create Lite database: %v", err)
	}
	defer db.Close()

	body, err := json.Marshal(document{
		Title: "Embedded Antfly Lite",
		Body:  "This document lives in a local .aflite file.",
	})
	if err != nil {
		log.Fatalf("marshal document: %v", err)
	}

	if err := db.Batch([]antflylite.WriteIntent{{
		Key:   "doc:lite-go",
		Value: body,
	}}, 1); err != nil {
		log.Fatalf("write document: %v", err)
	}

	lookup, err := db.LookupJSON("doc:lite-go")
	if err != nil {
		log.Fatalf("lookup document: %v", err)
	}
	fmt.Printf("lookup: %s\n", lookup)

	status, err := db.Status()
	if err != nil {
		log.Fatalf("read status: %v", err)
	}
	fmt.Printf(
		"status: storage=%s/%s inference=%s configured=%t\n",
		status.Storage.Format,
		status.Storage.Engine,
		status.Inference.Mode,
		status.Inference.Configured,
	)

	backup, err := db.Backup()
	if err != nil {
		log.Fatalf("export portable backup: %v", err)
	}
	if err := os.WriteFile(*backupPath, backup, 0o600); err != nil {
		log.Fatalf("write backup: %v", err)
	}
	fmt.Printf("wrote portable backup: %s\n", *backupPath)

	check, err := antflylite.CheckFile(*dbPath)
	if err != nil {
		log.Fatalf("check Lite file: %v", err)
	}
	fmt.Printf("check: valid=%t size=%d compact_size=%d\n", check.Valid, check.FileSize, check.CompactSize)
}

func openOrCreateLite(path string) (*antflylite.DB, error) {
	if _, err := os.Stat(path); err == nil {
		return antflylite.Open(path)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return antflylite.Create(path)
}
