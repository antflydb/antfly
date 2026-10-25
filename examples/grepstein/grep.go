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
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources"
)

// A grep item is exactly one extracted page, with no chunk overlap or text
// rewriting. Multiline matching is exact within a page; cross-page matching is
// deliberately not promised. Snapshot publication uses the shared source
// Runner's scoped LinearMerge, readiness barriers, and portable catalog table.
type grepPage struct {
	Item    sources.Item
	Content string
}
type grepSnapshotConfig struct {
	Directory string `json:"directory"`
	Revision  string `json:"revision"`
}

func grepCmd(args []string) error {
	f := flag.NewFlagSet("grep", flag.ContinueOnError)
	state := f.String("state", "./grepstein-corpus", "Prepared/published state")
	endpoint := f.String("url", DefaultAntflyURL, "Antfly API")
	table := f.String("table", "grepstein_corpus", "Native artifact table")
	destination := f.String("grep-table", "grepstein_pages", "Canonical grep table")
	projectionBucket := f.String("gcs-projection-bucket", "", "Publish canonical pages and a Colony job plan to this private GCS bucket instead of a standalone catalog")
	providerAuthID := f.String("provider-auth-id", "", "Colony GCS connection ID for the projection bucket")
	restart := f.Bool("restart", false, "Restart a previously interrupted pinned source run")
	if err := f.Parse(args); err != nil {
		return err
	}
	unlock, err := corpusLock(*state)
	if err != nil {
		return err
	}
	defer unlock()
	if err = validatePublication(*state); err != nil {
		return err
	}
	ctx := context.Background()
	client := &http.Client{Timeout: time.Minute}
	root, err := os.MkdirTemp(*state, ".grep-snapshot-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	input, err := os.Open(filepath.Join(*state, "published.ndjson"))
	if err != nil {
		return err
	}
	defer input.Close()
	reader := bufio.NewReader(input)
	tableURL := strings.TrimRight(*endpoint, "/") + "/tables/" + url.PathEscape(*table)
	digest := sha256.New()
	partitions := map[string]string{}
	for {
		var record corpusRecord
		if _, err = readCorpusLine(reader, &record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		manifest, err := readCorpusGraphManifest(client, tableURL, record)
		if err != nil {
			return err
		}
		revision := corpusGraphRevision(record, manifest)
		var cursor []any
		count := 0
		for {
			query := map[string]any{"fields": []string{"unit_id", "text", "provenance"}, "hierarchy": map[string]any{"children": map[string]any{"level": "unit", "parent": map[string]any{"level": "source", "id": record.ID}}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 50}
			if cursor != nil {
				query["search_after"] = cursor
			}
			raw, err := corpusRequest(client, "POST", tableURL+"/query", query)
			if err != nil {
				return err
			}
			hits, err := corpusGraphHits(raw)
			if err != nil {
				return err
			}
			if len(hits) == 0 {
				break
			}
			for _, hit := range hits {
				count++
				text, ok := hit.Source["text"].(string)
				if !ok {
					return fmt.Errorf("unit has no canonical text")
				}
				provenance, _ := hit.Source["provenance"].(map[string]any)
				status, _ := provenance["extraction_status"].(string)
				if strings.HasPrefix(status, "failed_") || strings.HasPrefix(status, "pending_") {
					return fmt.Errorf("unit extraction incomplete: %s", status)
				}
				metadata, _ := record.Document["metadata"].(map[string]any)
				dataset, _ := metadata["dataset"].(string)
				if !datasetName.MatchString(dataset) {
					return fmt.Errorf("invalid dataset")
				}
				identity := sha256.Sum256([]byte(record.ID + "\x00" + hit.ID))
				key := hex.EncodeToString(identity[:])
				partition := dataset + "-" + key[:1]
				partitions[partition] = dataset
				citation, page := corpusGraphCitation(record, provenance)
				citation = strings.Replace(citation, "s3://", "gs://", 1)
				projected := grepPage{Item: sources.Item{Path: fmt.Sprintf("%s/%s/page-%07d.txt", dataset, record.ID, page), Filename: fmt.Sprintf("page-%07d.txt", page), Revision: revision, URL: citation, SourceKind: sources.ProviderGCS, Size: int64(len(text)), Metadata: map[string]any{"content_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(text))), "canonical_url": citation, "source_record_id": record.ID, "unit_id": hit.ID, "artifact_revision": revision, "provenance": provenance, "matching_scope": "page"}}, Content: text}
				data, err := json.Marshal(projected)
				if err != nil {
					return err
				}
				if len(text) > 32<<20 {
					return fmt.Errorf("canonical page exceeds source text budget")
				}
				dir := filepath.Join(root, partition)
				if err = os.MkdirAll(dir, 0700); err != nil {
					return err
				}
				if _, err = os.Stat(filepath.Join(dir, key+".json")); !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("duplicate canonical unit")
				}
				if err = os.WriteFile(filepath.Join(dir, key+".json"), data, 0600); err != nil {
					return err
				}
				digest.Write(data)
			}
			next := hits[len(hits)-1].Sort
			a, _ := json.Marshal(cursor)
			b, _ := json.Marshal(next)
			if len(next) != 2 || bytes.Equal(a, b) {
				return fmt.Errorf("unit pagination did not advance")
			}
			cursor = next
		}
		if count != manifest.Units {
			return fmt.Errorf("artifact unit inventory changed")
		}
		if err = checkCorpusGraphRevision(client, tableURL, record, revision); err != nil {
			return err
		}
	}
	if len(partitions) == 0 {
		return fmt.Errorf("no extracted pages to publish")
	}
	revision := hex.EncodeToString(digest.Sum(nil))
	snapshot := filepath.Join(*state, "grep-snapshots", revision)
	if err = os.MkdirAll(filepath.Dir(snapshot), 0700); err != nil {
		return err
	}
	if _, err = os.Stat(snapshot); errors.Is(err, os.ErrNotExist) {
		if err = os.Rename(root, snapshot); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if err = verifySnapshot(root, snapshot); err != nil {
		return err
	}
	snapshot, err = filepath.Abs(snapshot)
	if err != nil {
		return err
	}
	if *projectionBucket != "" {
		if *providerAuthID == "" {
			return fmt.Errorf("provider-auth-id is required for Cloud source jobs")
		}
		return publishGrepProjection(ctx, snapshot, revision, *projectionBucket, *providerAuthID, *destination, filepath.Join(*state, "cloud-jobs.json"))
	}
	profile := sources.Profile{Version: 1, Endpoint: *endpoint, DataPlaneTokenEnv: "ANTFLY_API_KEY", Connections: map[string]sources.Connection{"artifacts": {Provider: sources.ProviderGCS}}}
	names := make([]string, 0, len(partitions))
	for partition := range partitions {
		names = append(names, partition)
	}
	sort.Strings(names)
	for _, partition := range names {
		hash := sha256.Sum256([]byte("grepstein/page-source/v1/" + partition))
		id := fmt.Sprintf("%x-%x-%x-%x-%x", hash[:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16])
		config, _ := json.Marshal(grepSnapshotConfig{filepath.Join(snapshot, partition), revision})
		profile.Sources = append(profile.Sources, sources.Source{ID: id, Alias: "grepstein-" + partition, Connection: "artifacts", Table: *destination, Config: config, Permissions: &sources.Permissions{Public: true}, PermissionTTLSeconds: 86400})
	}
	runner := sources.Runner{Profile: profile, StateDir: filepath.Join(*state, "grep-sync"), Restart: *restart, Factory: func(context.Context, sources.Connection) (sources.Connector, error) { return snapshotConnector{}, nil }}
	if os.Getenv("ANTFLY_API_KEY") == "" {
		runner.Profile.DataPlaneTokenEnv = ""
	}
	if err = runner.Sync(ctx); err != nil {
		return err
	}
	fmt.Printf("Page-scoped grep sources published. Catalog: %s\n", filepath.Join(*state, "grep-sync", "catalog.json"))
	return nil
}

type snapshotConnector struct{}

func (snapshotConnector) Enumerate(ctx context.Context, r sources.Request) (sources.Inventory, error) {
	var cfg grepSnapshotConfig
	if err := json.Unmarshal(r.Config, &cfg); err != nil {
		return sources.Inventory{}, err
	}
	files, err := os.ReadDir(cfg.Directory)
	if err != nil {
		return sources.Inventory{}, err
	}
	if len(files) > 10000 {
		return sources.Inventory{}, fmt.Errorf("page partition exceeds 10,000 items")
	}
	inventory := sources.Inventory{Authoritative: true, SourceRevision: cfg.Revision}
	for _, file := range files {
		if err = ctx.Err(); err != nil {
			return inventory, err
		}
		page, err := readGrepPage(cfg.Directory, file.Name())
		if err != nil {
			return inventory, err
		}
		page.Item.ID = "gcs://" + r.SourceNamespace + "/" + file.Name()
		inventory.Objects = append(inventory.Objects, page.Item)
	}
	return inventory, nil
}
func readGrepPage(dir, name string) (grepPage, error) {
	var page grepPage
	if filepath.Base(name) != name {
		return page, fmt.Errorf("invalid snapshot item")
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return page, err
	}
	err = json.Unmarshal(data, &page)
	return page, err
}
func (snapshotConnector) Read(ctx context.Context, r sources.Request, item sources.Item) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var cfg grepSnapshotConfig
	if err := json.Unmarshal(r.Config, &cfg); err != nil {
		return nil, err
	}
	page, err := readGrepPage(cfg.Directory, filepath.Base(item.ID))
	if err != nil {
		return nil, err
	}
	if page.Item.Revision != item.Revision || page.Item.Metadata["content_sha256"] != fmt.Sprintf("%x", sha256.Sum256([]byte(page.Content))) {
		return nil, fmt.Errorf("snapshot revision changed")
	}
	return []byte(page.Content), nil
}
func (c snapshotConnector) CheckAccess(ctx context.Context, r sources.Request, item sources.Item) error {
	_, err := c.Read(ctx, r, item)
	return err
}
func (c snapshotConnector) ValidateInventory(ctx context.Context, r sources.Request, prior sources.Inventory) error {
	current, err := c.Enumerate(ctx, r)
	if err != nil {
		return err
	}
	a, _ := json.Marshal(current)
	b, _ := json.Marshal(prior)
	if !bytes.Equal(a, b) {
		return sources.ErrInventoryIncomplete
	}
	return nil
}
func (c snapshotConnector) ValidateAbsentSources(ctx context.Context, r sources.Request, ids []string) error {
	current, err := c.Enumerate(ctx, r)
	if err != nil {
		return err
	}
	present := map[string]bool{}
	for _, item := range current.Objects {
		present[item.ID] = true
	}
	for _, id := range ids {
		if present[id] {
			return sources.ErrInventoryIncomplete
		}
	}
	return nil
}

func verifySnapshot(current, existing string) error {
	count := 0
	err := filepath.WalkDir(current, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("invalid snapshot entry")
		}
		rel, err := filepath.Rel(current, path)
		if err != nil {
			return err
		}
		a, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(filepath.Join(existing, rel))
		if err != nil {
			return err
		}
		if !bytes.Equal(a, b) {
			return fmt.Errorf("stored grep snapshot is corrupt")
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	existingCount := 0
	err = filepath.WalkDir(existing, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			if !entry.Type().IsRegular() {
				return fmt.Errorf("invalid stored snapshot entry")
			}
			existingCount++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if count != existingCount {
		return fmt.Errorf("stored snapshot inventory is corrupt")
	}
	return nil
}
