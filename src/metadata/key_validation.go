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

package metadata

import (
	"bytes"
	"fmt"

	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
)

func tableHasGraphIndexes(table *store.Table) bool {
	if table == nil {
		return false
	}
	for _, cfg := range table.Indexes {
		if indexes.NormalizeIndexType(cfg.Type) == indexes.IndexTypeGraph {
			return true
		}
	}
	return false
}

func validateNonemptyDocumentKey(key string) error {
	if key == "" {
		return fmt.Errorf("nonempty key required")
	}
	return nil
}

func validateReservedDocumentKeyShape(key string) error {
	keyBytes := []byte(key)

	switch {
	case bytes.Contains(keyBytes, storeutils.DBRangeStart):
		return fmt.Errorf("document id contains reserved internal separator DBRangeStart")
	case bytes.Contains(keyBytes, storeutils.DBRangeEnd):
		return fmt.Errorf("document id contains reserved internal separator DBRangeEnd")
	case bytes.HasPrefix(keyBytes, storeutils.MetadataPrefix):
		return fmt.Errorf("document id uses reserved metadata prefix")
	case bytes.HasSuffix(keyBytes, storeutils.EmbeddingSuffix):
		return fmt.Errorf("document id uses reserved enrichment suffix %q", storeutils.EmbeddingSuffix)
	case bytes.HasSuffix(keyBytes, storeutils.SummarySuffix):
		return fmt.Errorf("document id uses reserved enrichment suffix %q", storeutils.SummarySuffix)
	case storeutils.IsChunkKey(keyBytes):
		return fmt.Errorf("document id uses reserved chunk suffix")
	case bytes.Contains(keyBytes, []byte(":i:")) && bytes.HasSuffix(keyBytes, storeutils.SparseSuffix):
		return fmt.Errorf("document id uses reserved sparse index suffix %q", storeutils.SparseSuffix)
	case bytes.Contains(keyBytes, []byte(":i:")) && bytes.HasSuffix(keyBytes, []byte(":fh")):
		return fmt.Errorf("document id uses reserved graph field-hash suffix %q", []byte(":fh"))
	case storeutils.IsEdgeKey(keyBytes):
		return fmt.Errorf("document id uses reserved graph edge markers")
	default:
		return nil
	}
}

func validateDocumentMutationKey(key string) error {
	if err := validateNonemptyDocumentKey(key); err != nil {
		return err
	}
	return validateReservedDocumentKeyShape(key)
}

// validateDocumentInsertKey rejects document IDs that collide with the current
// storage key grammar. This is a temporary guardrail until internal key
// encodings are redesigned to support arbitrary IDs end-to-end.
func validateDocumentInsertKey(table *store.Table, key string) error {
	if err := validateDocumentMutationKey(key); err != nil {
		return err
	}

	if tableHasGraphIndexes(table) && bytes.Contains([]byte(key), []byte(":i:")) {
		return fmt.Errorf("document id containing %q is not supported on tables with graph indexes", []byte(":i:"))
	}
	return nil
}

func validateDocumentTransformKey(table *store.Table, key string, upsert bool) error {
	if err := validateDocumentMutationKey(key); err != nil {
		return err
	}
	if upsert {
		return validateDocumentInsertKey(table, key)
	}
	return nil
}
