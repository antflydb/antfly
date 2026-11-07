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
	"flag"
	"fmt"
	"path/filepath"

	"github.com/antflydb/antfly/go/pkg/sources"
)

// Grep consumes the same native extraction units as the explorer. Semantic
// chunks remain explicitly selectable; overlap never duplicates default hits.
func grepCmd(args []string) error {
	f := flag.NewFlagSet("grep", flag.ContinueOnError)
	state := f.String("state", "./grepstein-corpus", "Prepared/published state")
	endpoint := f.String("url", DefaultAntflyURL, "Antfly API")
	table := f.String("table", "grepstein_corpus", "Native artifact table")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := validatePublication(*state); err != nil {
		return err
	}
	config := nativeGrepConfig(*endpoint, *table)
	output := filepath.Join(*state, "grep.json")
	if err := atomicCorpusJSON(output, config); err != nil {
		return err
	}
	fmt.Printf("Native artifact binding: %s\nSearch with antfly-grep --table-config %s PATTERN\n", output, output)
	return nil
}
func nativeGrepConfig(endpoint, table string) map[string]any {
	fields := sources.DocumentFields{URI: "original_url", Path: "filename", Revision: "version", IndexedAt: "indexed_at", Permissions: "permissions", Metadata: "metadata"}
	return map[string]any{
		"version": 1, "endpoint": endpoint, "principals": []string{"user:self"},
		"tables": []any{map[string]any{
			"name": "grepstein", "table": table, "fields": fields,
			"streams": []sources.TextStream{
				{Name: "extracted", Artifact: corpusUnits, Index: corpusUnitsIndex, Field: "text", Purpose: "extraction", Default: true, Boundary: "unit", LocationField: "provenance"},
				{Name: "chunks", Artifact: corpusChunks, Index: corpusTextIndex, Field: "text", Purpose: "semantic-chunks", Boundary: "unit", LocationField: "provenance"},
			},
		}},
	}
}
