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
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type config struct {
	cycles    int
	keys      int
	valueSize int
}

func main() {
	cfg := config{}
	flag.IntVar(&cfg.cycles, "cycles", 20, "")
	flag.IntVar(&cfg.keys, "keys", 512, "")
	flag.IntVar(&cfg.valueSize, "value-size", 64, "")
	flag.Parse()

	if cfg.cycles <= 0 || cfg.keys <= 0 || cfg.valueSize <= 0 {
		log.Fatal("all numeric args must be > 0")
	}

	dir, err := os.MkdirTemp("", "antfly-pebble-commit-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := pebble.Open(filepath.Clean(dir), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	value := make([]byte, cfg.valueSize)
	for i := range value {
		value[i] = 'v'
	}

	var putTotal time.Duration
	var commitTotal time.Duration

	for cycle := range cfg.cycles {
		batch := db.NewBatch()
		putStart := time.Now()
		for keyIdx := range cfg.keys {
			key := []byte(fmt.Sprintf("k-%04d-%08d", cycle, keyIdx))
			if err := batch.Set(key, value, nil); err != nil {
				_ = batch.Close()
				log.Fatal(err)
			}
		}
		putTotal += time.Since(putStart)

		commitStart := time.Now()
		if err := batch.Commit(pebble.Sync); err != nil {
			_ = batch.Close()
			log.Fatal(err)
		}
		commitTotal += time.Since(commitStart)
		if err := batch.Close(); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf(
		"pebble_commit cycles=%d keys=%d value_size=%d avg_put=%.3fms avg_commit=%.3fms\n",
		cfg.cycles,
		cfg.keys,
		cfg.valueSize,
		float64(putTotal.Nanoseconds()/int64(cfg.cycles))/1e6,
		float64(commitTotal.Nanoseconds()/int64(cfg.cycles))/1e6,
	)
}
