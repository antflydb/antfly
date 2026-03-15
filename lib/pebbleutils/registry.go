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

package pebbleutils

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/pebble/v2"
)

// MergeFunc creates a ValueMerger for the given key and initial value.
// It follows the same signature as pebble.Merger.Merge.
type MergeFunc func(key, value []byte) (pebble.ValueMerger, error)

type route struct {
	prefix []byte
	merge  MergeFunc
}

// Registry holds prefix→MergeFunc mappings for dispatching merge operations.
type Registry struct {
	routes []route
}

// NewRegistry creates an empty merge dispatch registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a merge strategy for keys with the given prefix.
// When multiple prefixes match a key, the longest match wins.
func (r *Registry) Register(prefix []byte, fn MergeFunc) {
	r.routes = append(r.routes, route{
		prefix: bytes.Clone(prefix),
		merge:  fn,
	})
	// Sort by prefix length descending for longest-match-first dispatch.
	sort.Slice(r.routes, func(i, j int) bool {
		return len(r.routes[i].prefix) > len(r.routes[j].prefix)
	})
}

// NewMerger returns a pebble.Merger that dispatches by key prefix.
// The name is persisted in Pebble's MANIFEST and must be stable across
// DB reopens. Use a versioned name like "antfly.merge.v1".
func (r *Registry) NewMerger(name string) *pebble.Merger {
	return &pebble.Merger{
		Name: name,
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			for _, rt := range r.routes {
				if bytes.HasPrefix(key, rt.prefix) {
					return rt.merge(key, value)
				}
			}
			// No matching prefix — fall back to last-write-wins.
			return newLastWriteWins(value), nil
		},
	}
}
