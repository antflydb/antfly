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
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type config struct {
	docs       int
	dims       int
	queries    int
	candidates int
	batchSize  int
	cacheBytes int64
	storage    string
}

func main() {
	cfg := config{}
	flag.IntVar(&cfg.docs, "docs", 75000, "document count")
	flag.IntVar(&cfg.dims, "dims", 512, "vector dimensions")
	flag.IntVar(&cfg.queries, "queries", 1000, "query count")
	flag.IntVar(&cfg.candidates, "candidates", 800, "candidate keys per query")
	flag.IntVar(&cfg.batchSize, "batch-size", 1024, "write batch size")
	flag.Int64Var(&cfg.cacheBytes, "cache-bytes", 256*1024*1024, "Pebble block cache bytes")
	flag.StringVar(&cfg.storage, "storage", "memory", "storage mode: memory or disk")
	flag.Parse()
	if cfg.docs <= 0 || cfg.queries <= 0 || cfg.candidates <= 0 {
		log.Fatal("docs, queries, and candidates must be positive")
	}

	cache := pebble.NewCache(cfg.cacheBytes)
	defer cache.Unref()
	dir := ""
	opts := &pebble.Options{Cache: cache}
	switch cfg.storage {
	case "memory":
		opts.FS = vfs.NewMem()
	case "disk":
		var err error
		dir, err = os.MkdirTemp("", "hbc-pebble-read-bench-*")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(dir)
	default:
		log.Fatalf("unknown storage mode %q", cfg.storage)
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	metaKeys, artifactKeys, docKeys := makeKeys(cfg.docs)
	if err := populate(db, cfg, metaKeys, artifactKeys, docKeys); err != nil {
		log.Fatal(err)
	}

	metaQueries, artifactQueries := makeQueries(cfg, metaKeys, artifactKeys)
	warmMetadata(db, metaQueries)
	warmArtifactIterator(db, artifactQueries, cfg.dims)

	metadataPoint := timeMetadataGets(db, metaQueries)
	artifactIterator := timeArtifactIteratorLoads(db, artifactQueries, cfg.dims)

	fmt.Printf("hbc_storage_read_bench engine=go_pebble storage=%s docs=%d dims=%d queries=%d candidates=%d artifact_bytes=%d\n",
		cfg.storage, cfg.docs, cfg.dims, cfg.queries, cfg.candidates, artifactValueSize(cfg.dims))
	printTimed("metadata_point_get", metadataPoint, cfg)
	printTimed("artifact_iter_copy", artifactIterator, cfg)
}

type timedResult struct {
	total time.Duration
	hits  int
}

func printTimed(name string, result timedResult, cfg config) {
	reads := cfg.queries * cfg.candidates
	fmt.Printf("hbc_storage_read_result op=%s total_ms=%.3f per_query_us=%.3f per_key_ns=%.2f hits=%d\n",
		name,
		float64(result.total.Nanoseconds())/1_000_000.0,
		float64(result.total.Nanoseconds())/float64(cfg.queries)/1_000.0,
		float64(result.total.Nanoseconds())/float64(reads),
		result.hits,
	)
}

func populate(db *pebble.DB, cfg config, metaKeys, artifactKeys, docKeys [][]byte) error {
	artifactValue := bytes.Repeat([]byte{0}, artifactValueSize(cfg.dims))
	for offset := 0; offset < cfg.docs; offset += cfg.batchSize {
		end := min(offset+cfg.batchSize, cfg.docs)
		batch := db.NewBatch()
		for i := offset; i < end; i++ {
			putU64LE(artifactValue[:8], uint64(i))
			if err := batch.Set(metaKeys[i], docKeys[i], pebble.NoSync); err != nil {
				_ = batch.Close()
				return err
			}
			if err := batch.Set(artifactKeys[i], artifactValue, pebble.NoSync); err != nil {
				_ = batch.Close()
				return err
			}
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			_ = batch.Close()
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
	}
	return db.Flush()
}

func warmMetadata(db *pebble.DB, queries [][][]byte) {
	_ = timeMetadataGets(db, queries)
}

func warmArtifactIterator(db *pebble.DB, queries [][][]byte, dims int) {
	_ = timeArtifactIteratorLoads(db, queries, dims)
}

func timeMetadataGets(db *pebble.DB, queries [][][]byte) timedResult {
	started := time.Now()
	hits := 0
	for _, query := range queries {
		for _, key := range query {
			value, closer, err := db.Get(key)
			if err == pebble.ErrNotFound {
				continue
			}
			if err != nil {
				log.Fatal(err)
			}
			_ = bytes.Clone(value)
			hits++
			if err := closer.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
	return timedResult{total: time.Since(started), hits: hits}
}

func timeArtifactIteratorLoads(db *pebble.DB, queries [][][]byte, dims int) timedResult {
	started := time.Now()
	hits := 0
	dst := make([]byte, dims*4)
	for _, query := range queries {
		for _, key := range query {
			upper := prefixSuccessor(key)
			iter, err := db.NewIter(&pebble.IterOptions{
				LowerBound: key,
				UpperBound: upper,
			})
			if err != nil {
				log.Fatal(err)
			}
			if !iter.First() {
				_ = iter.Close()
				continue
			}
			if !bytes.Equal(iter.Key(), key) {
				_ = iter.Close()
				continue
			}
			value := iter.Value()
			if len(value) >= 16+len(dst) {
				copy(dst, value[16:16+len(dst)])
				hits++
			}
			if err := iter.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
	return timedResult{total: time.Since(started), hits: hits}
}

func prefixSuccessor(key []byte) []byte {
	out := bytes.Clone(key)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return append(out, 0)
}

func makeKeys(docs int) ([][]byte, [][]byte, [][]byte) {
	meta := make([][]byte, docs)
	artifacts := make([][]byte, docs)
	docKeys := make([][]byte, docs)
	for i := 0; i < docs; i++ {
		meta[i] = []byte(fmt.Sprintf("__hbc_meta__:dense_idx:%016d", i))
		artifacts[i] = []byte(fmt.Sprintf("__embedding__:doc:%016d:dense_idx", i))
		docKeys[i] = []byte(fmt.Sprintf("doc:%016d", i))
	}
	return meta, artifacts, docKeys
}

func makeQueries(cfg config, metaKeys, artifactKeys [][]byte) ([][][]byte, [][][]byte) {
	metaQueries := make([][][]byte, cfg.queries)
	artifactQueries := make([][][]byte, cfg.queries)
	ids := make([]int, cfg.candidates)
	for q := 0; q < cfg.queries; q++ {
		for j := range ids {
			ids[j] = pickDocID(q, j, cfg.docs)
		}
		sort.Ints(ids)
		metaQueries[q] = make([][]byte, cfg.candidates)
		artifactQueries[q] = make([][]byte, cfg.candidates)
		for j, id := range ids {
			metaQueries[q][j] = metaKeys[id]
			artifactQueries[q][j] = artifactKeys[id]
		}
	}
	return metaQueries, artifactQueries
}

func pickDocID(queryIndex, candidateIndex, docs int) int {
	x := uint64(queryIndex+1) * 0x9e3779b185ebca87
	x ^= uint64(candidateIndex+17) * 0xc2b2ae3d27d4eb4f
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return int(x % uint64(docs))
}

func artifactValueSize(dims int) int {
	return 16 + dims*4
}

func putU64LE(dst []byte, value uint64) {
	for i := range 8 {
		dst[i] = byte(value >> (8 * i))
	}
}
