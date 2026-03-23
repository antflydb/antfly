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

package indexes

import (
	"bytes"
	"fmt"

	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
)

// collectChunkReplacementDeletes returns exact deletes needed to replace the
// current chunk namespace for a single document/index with the provided chunk set.
// It deletes stale raw chunk keys and any stale derived chunk keys whose base
// chunk key is no longer present in desiredChunkKeys.
func collectChunkReplacementDeletes(
	db *pebble.DB,
	docKey []byte,
	indexName string,
	desiredChunkKeys [][]byte,
	derivedSuffixes ...[]byte,
) ([][2][]byte, error) {
	if db == nil {
		return nil, pebble.ErrClosed
	}

	desired := make(map[string]struct{}, len(desiredChunkKeys))
	for _, key := range desiredChunkKeys {
		desired[string(key)] = struct{}{}
	}

	prefix := storeutils.MakeChunkPrefix(docKey, indexName)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: utils.PrefixSuccessor(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("creating chunk replacement iterator: %w", err)
	}
	defer iter.Close()

	deletes := make([][2][]byte, 0)
	seenDeletes := make(map[string]struct{})

	appendDelete := func(key []byte) {
		sKey := string(key)
		if _, ok := seenDeletes[sKey]; ok {
			return
		}
		seenDeletes[sKey] = struct{}{}
		deletes = append(deletes, [2][]byte{bytes.Clone(key), nil})
	}

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		baseChunkKey, ok := chunkBaseKeyForReplacement(key, derivedSuffixes...)
		if !ok {
			continue
		}
		if _, keep := desired[string(baseChunkKey)]; keep {
			continue
		}

		appendDelete(key)
		appendDelete(baseChunkKey)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating chunk replacement keys: %w", err)
	}

	return deletes, nil
}

func chunkBaseKeyForReplacement(key []byte, derivedSuffixes ...[]byte) ([]byte, bool) {
	if storeutils.IsChunkKey(key) {
		return bytes.Clone(key), true
	}

	for _, suffix := range derivedSuffixes {
		if len(suffix) == 0 || !bytes.HasSuffix(key, suffix) {
			continue
		}
		base := key[:len(key)-len(suffix)]
		if storeutils.IsChunkKey(base) {
			return bytes.Clone(base), true
		}
	}

	return nil, false
}
