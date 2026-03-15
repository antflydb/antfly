// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package kv

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"
)

type DB interface {
	Get(ctx context.Context, key []byte) ([]byte, io.Closer, error)
	NewIter(ctx context.Context, opts *pebble.IterOptions) (*pebble.Iterator, error)
	Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error
}

type PebbleDB struct {
	DB *pebble.DB
}

func NewPebbleDB(db *pebble.DB) *PebbleDB {
	return &PebbleDB{DB: db}
}

func (p *PebbleDB) Get(_ context.Context, key []byte) ([]byte, io.Closer, error) {
	return p.DB.Get(key)
}

func (p *PebbleDB) NewIter(_ context.Context, opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return p.DB.NewIter(opts)
}

func (p *PebbleDB) Batch(_ context.Context, writes [][2][]byte, deletes [][]byte) error {
	batch := p.DB.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, write := range writes {
		if err := batch.Set(write[0], write[1], nil); err != nil {
			return fmt.Errorf("setting key %s: %w", write[0], err)
		}
	}
	for _, key := range deletes {
		if err := batch.Delete(key, nil); err != nil {
			return fmt.Errorf("deleting key %s: %w", key, err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}
	return nil
}
