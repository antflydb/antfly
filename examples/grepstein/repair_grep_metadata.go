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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type grepMetadataOptions struct {
	After     string
	Limit     int
	IndexedAt int64
	Reader    string
	Apply     bool
}

type grepMetadataResult struct {
	Scanned            int    `json:"scanned"`
	MissingIndexedAt   int    `json:"missing_indexed_at"`
	MissingPermissions int    `json:"missing_permissions"`
	Patched            int    `json:"patched"`
	NextAfter          string `json:"next_after,omitempty"`
}

type grepMetadataFields struct {
	ID          string          `json:"_id"`
	Filename    string          `json:"filename"`
	OriginalURL string          `json:"original_url"`
	URL         string          `json:"url"`
	Version     string          `json:"version"`
	MIMEType    string          `json:"mime_type"`
	IndexedAt   json.RawMessage `json:"indexed_at"`
	Permissions json.RawMessage `json:"permissions"`
}

func repairGrepMetadataCmd(args []string) error {
	f := flag.NewFlagSet("repair-grep-metadata", flag.ContinueOnError)
	endpoint := f.String("url", DefaultAntflyURL, "Antfly API")
	table := f.String("table", "", "Existing PDF corpus table")
	var opts grepMetadataOptions
	f.StringVar(&opts.After, "after", "", "Resume after this document key")
	f.IntVar(&opts.Limit, "limit", 100, "Maximum primary records to inspect (1–500)")
	f.Int64Var(&opts.IndexedAt, "indexed-at", 0, "Verified Unix ingestion timestamp for records missing indexed_at")
	f.StringVar(&opts.Reader, "reader", "", "Explicit private reader for records missing permissions")
	f.BoolVar(&opts.Apply, "apply", false, "Commit the missing fields using document-version predicates")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected argument: %s", f.Arg(0))
	}
	result, err := repairGrepMetadataPage(&http.Client{Timeout: 30 * time.Second}, *endpoint, *table, opts)
	if err != nil {
		return err
	}
	out, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func repairGrepMetadataPage(client *http.Client, endpoint, table string, opts grepMetadataOptions) (grepMetadataResult, error) {
	var result grepMetadataResult
	if table == "" || opts.Limit < 1 || opts.Limit > 500 {
		return result, fmt.Errorf("specify a table and a limit between 1 and 500")
	}
	if opts.Apply && (opts.IndexedAt <= 0 || strings.TrimSpace(opts.Reader) != opts.Reader || opts.Reader == "" || opts.Reader == "*") {
		return result, fmt.Errorf("--apply requires a verified --indexed-at and an explicit private --reader")
	}
	endpoint = strings.TrimRight(endpoint, "/")
	tableURL := endpoint + "/tables/" + url.PathEscape(table)
	raw, err := corpusRawRequest(client, http.MethodPost, tableURL+"/documents", map[string]any{
		"from": opts.After, "limit": opts.Limit, "fields": []string{"indexed_at", "permissions"},
	})
	if err != nil {
		return result, err
	}
	var predicates []map[string]string
	var transforms []map[string]any
	previous := opts.After
	dec := json.NewDecoder(bytes.NewReader(raw))
	for {
		var scanned grepMetadataFields
		if err := dec.Decode(&scanned); err == io.EOF {
			break
		} else if err != nil {
			return result, err
		}
		if scanned.ID <= previous || result.Scanned >= opts.Limit {
			return result, fmt.Errorf("unexpected document scan order or size")
		}
		previous = scanned.ID
		result.Scanned++
		if scanned.IndexedAt == nil {
			result.MissingIndexedAt++
		}
		if scanned.Permissions == nil {
			result.MissingPermissions++
		}
		if !opts.Apply || (scanned.IndexedAt != nil && scanned.Permissions != nil) {
			continue
		}

		// Scope the observed version and fields to the same lookup. A newer
		// ACL, deletion, or source change must win over this migration.
		var headers http.Header
		primary, err := corpusRawRequestWithHeaders(client, http.MethodGet, tableURL+"/documents/"+url.PathEscape(scanned.ID), nil, &headers)
		if err != nil {
			return result, err
		}
		var current grepMetadataFields
		if err := json.Unmarshal(primary, &current); err != nil {
			return result, err
		}
		revision := headers.Get("X-Antfly-Version")
		if version, err := strconv.ParseUint(revision, 10, 64); err != nil || version == 0 ||
			current.MIMEType != "application/pdf" || current.URL == "" || current.OriginalURL == "" || current.Filename == "" || current.Version == "" {
			return result, fmt.Errorf("%s: expected a versioned Grepstein PDF source", scanned.ID)
		}
		var operations []map[string]any
		if current.IndexedAt == nil {
			operations = append(operations, map[string]any{"op": "$set", "path": "$.indexed_at", "value": opts.IndexedAt})
		}
		if current.Permissions == nil {
			operations = append(operations, map[string]any{"op": "$set", "path": "$.permissions",
				"value": map[string]any{"public": false, "readers": []string{opts.Reader}}})
		}
		if len(operations) != 0 {
			transforms = append(transforms, map[string]any{"key": scanned.ID, "operations": operations})
			predicates = append(predicates, map[string]string{"table": table, "key": scanned.ID, "version": revision})
		}
	}
	if len(transforms) != 0 {
		response, err := corpusRequest(client, http.MethodPost, endpoint+"/transactions/commit", map[string]any{
			"read_set": predicates, "tables": map[string]any{table: map[string]any{"transforms": transforms}}, "sync_level": "write",
		})
		if err != nil {
			return result, err
		}
		var receipt struct{ Status string }
		if err := json.Unmarshal(response, &receipt); err != nil {
			return result, err
		}
		if receipt.Status != "committed" && receipt.Status != "committed_visibility_pending" {
			return result, fmt.Errorf("metadata transaction status %q; recheck this page before continuing", receipt.Status)
		}
		result.Patched = len(transforms)
	}
	if result.Scanned == opts.Limit {
		result.NextAfter = previous
	}
	return result, nil
}
