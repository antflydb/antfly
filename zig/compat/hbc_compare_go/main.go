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

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"runtime/pprof"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/vector"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/vectorindex"
	"github.com/cockroachdb/pebble/v2"
)

type config struct {
	docs    int
	dims    int
	queries int
	k       int
	repeats int
	seed    uint64
	profile string
	metric  string
}

func main() {
	cfg := config{}
	flag.IntVar(&cfg.docs, "docs", 2048, "")
	flag.IntVar(&cfg.dims, "dims", 128, "")
	flag.IntVar(&cfg.queries, "queries", 25, "")
	flag.IntVar(&cfg.k, "k", 10, "")
	flag.IntVar(&cfg.repeats, "repeats", 10, "")
	flag.Uint64Var(&cfg.seed, "seed", 42, "")
	flag.StringVar(&cfg.profile, "insert-cpuprofile", "", "")
	flag.StringVar(&cfg.metric, "metric", "cosine", "")
	flag.Parse()

	if cfg.docs <= 0 || cfg.dims <= 0 || cfg.queries <= 0 || cfg.k <= 0 || cfg.repeats <= 0 {
		log.Fatal("all numeric args must be > 0")
	}
	metric := parseMetric(cfg.metric)

	dataset := makeDataset(cfg)
	queries := makeQueries(dataset, cfg)

	db, cleanup, err := setupPebble("bench_hbc_isolate")
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	idx, err := vectorindex.NewHBCIndex(vectorindex.HBCConfig{
		Dimension:           uint32(cfg.dims),
		DistanceMetric:      metric,
		Name:                "bench_hbc_isolate",
		SplitAlgo:           vector.ClustAlgorithm_Kmeans,
		Episilon2:           7,
		BranchingFactor:     7 * 24,
		LeafSize:            7 * 24,
		SearchWidth:         2 * 3 * 7 * 24,
		UseQuantization:     true,
		DisableReranking:    false,
		QuantizerSeed:       cfg.seed,
		UseRandomOrthoTrans: true,
		CacheSizeNodes:      100_000,
		VectorDB:            db,
		IndexDB:             db,
	}, rand.NewPCG(cfg.seed, cfg.seed^0x9e3779b97f4a7c15))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	batch := &vectorindex.Batch{
		Vectors:      dataset,
		MetadataList: makeMetadata(cfg.docs),
		IDs:          makeIDs(cfg.docs),
	}
	if cfg.profile != "" {
		f, err := os.Create(cfg.profile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			log.Fatal(err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
		}()
	}
	insertStart := time.Now()
	if err := persistVectors(db, "bench_hbc_isolate", dataset); err != nil {
		log.Fatal(err)
	}
	if err := idx.Batch(context.Background(), batch); err != nil {
		log.Fatal(err)
	}
	insertDur := time.Since(insertStart)

	for _, query := range queries {
		_, err := idx.Search(&vectorindex.SearchRequest{
			Embedding: query,
			K:         cfg.k,
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	var searchTotal time.Duration
	for range cfg.repeats {
		for _, query := range queries {
			start := time.Now()
			results, err := idx.Search(&vectorindex.SearchRequest{
				Embedding: query,
				K:         cfg.k,
			})
			if err != nil {
				log.Fatal(err)
			}
			searchTotal += time.Since(start)
			if len(results) == 0 {
				log.Fatal("unexpected empty result set")
			}
		}
	}

	totalSearches := cfg.queries * cfg.repeats
	avgSearch := searchTotal / time.Duration(totalSearches)
	fmt.Printf(
		"go_hbc docs=%d dims=%d queries=%d k=%d repeats=%d insert=%.3fms search=%.3fus\n",
		cfg.docs,
		cfg.dims,
		cfg.queries,
		cfg.k,
		cfg.repeats,
		float64(insertDur)/float64(time.Millisecond),
		float64(avgSearch)/float64(time.Microsecond),
	)
}

func makeDataset(cfg config) []vector.T {
	data := make([]vector.T, cfg.docs)
	for docIdx := range cfg.docs {
		base := float32(docIdx%8) * 0.25
		vecData := make(vector.T, cfg.dims)
		for dimIdx := range cfg.dims {
			vecData[dimIdx] = base + deterministicNoise(cfg.seed, docIdx, dimIdx)
		}
		if cfg.metric == "cosine" {
			normalize(vecData)
		}
		data[docIdx] = vecData
	}
	return data
}

func makeQueries(dataset []vector.T, cfg config) []vector.T {
	queries := make([]vector.T, cfg.queries)
	for i := range cfg.queries {
		srcIdx := (i * 997) % cfg.docs
		query := make(vector.T, cfg.dims)
		copy(query, dataset[srcIdx])
		queries[i] = query
	}
	return queries
}

func makeMetadata(docs int) [][]byte {
	out := make([][]byte, docs)
	for i := range docs {
		out[i] = []byte(fmt.Sprintf("doc:%08d", i))
	}
	return out
}

func makeIDs(docs int) []uint64 {
	out := make([]uint64, docs)
	for i := range docs {
		out[i] = uint64(i + 1)
	}
	return out
}

func deterministicNoise(seed uint64, docIdx, dimIdx int) float32 {
	x := seed ^
		(uint64(docIdx+1) * 0x9E3779B97F4A7C15) ^
		(uint64(dimIdx+1) * 0xC2B2AE3D27D4EB4F)
	x ^= x >> 33
	x *= 0xFF51AFD7ED558CCD
	x ^= x >> 33
	x *= 0xC4CEB9FE1A85EC53
	x ^= x >> 33
	return (float32(x&1023) / 1024.0) * 0.01
}

func parseMetric(raw string) vector.DistanceMetric {
	switch raw {
	case "cosine":
		return vector.DistanceMetric_Cosine
	case "l2_squared":
		return vector.DistanceMetric_L2Squared
	case "inner_product":
		return vector.DistanceMetric_InnerProduct
	default:
		log.Fatalf("unsupported metric: %s", raw)
		return vector.DistanceMetric_Cosine
	}
}

func normalize(v vector.T) {
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(norm))
	for i := range v {
		v[i] *= inv
	}
}

func setupPebble(indexName string) (*pebble.DB, func(), error) {
	_ = indexName
	dir, err := os.MkdirTemp("", "antfly-hbc-go-isolate-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}

	db, err := pebble.Open(dir, pebbleutils.NewPebbleOpts())
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return db, func() {
		_ = db.Close()
		cleanup()
	}, nil
}

func persistVectors(db *pebble.DB, indexName string, vectors []vector.T) error {
	suffix := fmt.Appendf(nil, ":i:%s:e", indexName)
	pbatch := db.NewBatch()
	defer pbatch.Close()

	for i, vecData := range vectors {
		docID := []byte(fmt.Sprintf("doc:%08d", i))
		vecKey := append(bytes.Clone(docID), suffix...)
		encoded := make([]byte, 0, 8+4*(len(vecData)+1))
		encoded, err := vectorindex.EncodeEmbeddingWithHashID(encoded, vecData, 0)
		if err != nil {
			return err
		}
		if err := pbatch.Set(vecKey, encoded, nil); err != nil {
			return err
		}
	}
	if err := pbatch.Commit(pebble.Sync); err != nil {
		return err
	}
	return nil
}
