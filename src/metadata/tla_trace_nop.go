//go:build !with_tla

package metadata

import (
	"github.com/antflydb/antfly/lib/types"
	"github.com/google/uuid"
)

// traceCheckPredicates is a no-op without the with_tla build tag.
func (ms *MetadataStore) traceCheckPredicates(_ uuid.UUID, _ map[types.ID]struct{}) {}
