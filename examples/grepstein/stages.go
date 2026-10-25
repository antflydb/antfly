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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func waitCmd(args []string) error {
	f := flag.NewFlagSet("wait", flag.ContinueOnError)
	endpoint := f.String("url", DefaultAntflyURL, "Antfly API")
	table := f.String("table", "grepstein_corpus", "Corpus table")
	index := f.String("index", corpusTextIndex, "Index whose complete/queryable milestones must both be true")
	timeout := f.Duration("timeout", 24*time.Hour, "Maximum time to wait")
	if err := f.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := &http.Client{Timeout: time.Minute}
	target := strings.TrimRight(*endpoint, "/") + "/tables/" + url.PathEscape(*table) + "/indexes/" + url.PathEscape(*index)
	for {
		raw, err := corpusRequestWithContext(ctx, client, "GET", target, nil)
		if err != nil {
			return err
		}
		complete, err := indexComplete(raw)
		if err != nil {
			return err
		}
		if complete {
			fmt.Printf("%s is queryable and complete\n", *index)
			return nil
		}
		fmt.Printf("Waiting for complete coverage of %s\n", *index)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
}

func indexComplete(raw []byte) (bool, error) {
	type observation struct {
		Readiness struct {
			Complete       bool     `json:"complete"`
			Queryable      bool     `json:"queryable"`
			State          string   `json:"state"`
			PendingReasons []string `json:"pending_reasons"`
		} `json:"readiness"`
	}
	var status struct {
		Status observation            `json:"status"`
		Shards map[string]observation `json:"shard_status"`
	}
	if err := json.Unmarshal(raw, &status); err != nil {
		return false, err
	}
	check := func(o observation) (bool, error) {
		if o.Readiness.State == "failed" {
			return false, fmt.Errorf("index failed: %v", o.Readiness.PendingReasons)
		}
		return o.Readiness.Complete && o.Readiness.Queryable, nil
	}
	if ready, err := check(status.Status); !ready || err != nil {
		return false, err
	}
	if len(status.Shards) == 0 {
		return false, nil
	}
	for _, shard := range status.Shards {
		if ready, err := check(shard); !ready || err != nil {
			return false, err
		}
	}
	return true, nil
}

// Index creation backfills the existing chunk artifacts. It does not rewrite
// source rows or alter the loader's manifest/table identity checkpoint.
func enrichCmd(args []string) error {
	f := flag.NewFlagSet("enrich", flag.ContinueOnError)
	endpoint := f.String("url", DefaultAntflyURL, "Antfly API")
	table := f.String("table", "grepstein_corpus", "Corpus table")
	stage := f.String("stage", "semantic", "semantic or graph (then run graph to materialize extracted units)")
	model := f.String("embedding-model", DefaultEmbeddingModel, "Registered embedding model (512 dimensions)")
	embedderPath := f.String("embedder-config", "", "Native embedder JSON including provider, model, project/location and dimension")
	extractor := f.String("extractor-model", DefaultAutographModel, "Registered GLiNER model")
	inference := f.String("inference-url", "", "Server-reachable inference root")
	inferenceSecret := f.String("inference-secret", "", "Server keystore key name for the selected inference endpoint")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *stage != "semantic" && *stage != "graph" {
		return fmt.Errorf("stage must be semantic or graph")
	}
	embedder, err := corpusEmbedderConfig(*model, *embedderPath)
	if err != nil {
		return err
	}
	indexes, err := corpusIndexes(*model, *extractor, *inference, "", *stage == "graph", *stage == "semantic", corpusIndexOptions{Embedder: embedder, InferenceSecret: *inferenceSecret})
	if err != nil {
		return err
	}
	name := DefaultEmbeddingIndex
	if *stage == "graph" {
		name = DefaultAutographIndex
	}
	raw, err := json.Marshal(indexes[name])
	if err != nil {
		return err
	}
	var request map[string]any
	if err = json.Unmarshal(raw, &request); err != nil {
		return err
	}
	delete(request, "name")
	target := strings.TrimRight(*endpoint, "/") + "/tables/" + url.PathEscape(*table) + "/indexes/" + url.PathEscape(name)
	result, err := corpusRequest(&http.Client{Timeout: time.Minute}, "POST", target, request)
	if err != nil {
		return err
	}
	fmt.Printf("Index submitted. Check GET %s until coverage is complete; submission is not readiness.\n%s\n", target, result)
	return nil
}

type dataset struct {
	ID      string `json:"id"`
	Release string `json:"release"`
	Stage   string `json:"stage"`
}

func smallDatasets() []dataset {
	result := []dataset{{"court-2024", "https://www.courtlistener.com/docket/4355835/giuffre-v-maxwell/", "v1"}}
	for n := 1; n <= 12; n++ {
		stage := "v1"
		if n >= 8 && n <= 11 {
			stage = "deferred"
		}
		result = append(result, dataset{fmt.Sprintf("doj-%02d", n), fmt.Sprintf("https://www.justice.gov/epstein/doj-disclosures/data-set-%d-files", n), stage})
	}
	return result
}
func datasetsCmd(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("datasets takes no arguments")
	}
	return printCorpusJSON(smallDatasets())
}

func corpusPageCitation(original string, metadata map[string]any, page float64) (string, int) {
	start := float64(1)
	if v, ok := metadata["page_start"].(float64); ok && v > 0 {
		start = v
	}
	number := int(start + page - 1)
	return strings.Split(original, "#")[0] + fmt.Sprintf("#page=%d", number), number
}
