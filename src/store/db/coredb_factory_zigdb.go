//go:build zigdb

package db

import (
	"os"
	"strings"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"go.uber.org/zap"
)

func newCoreDB(
	lg *zap.Logger,
	antflyConfig *common.Config,
	schema *schema.TableSchema,
	idxs map[string]indexes.IndexConfig,
	snapStore snapstore.SnapStore,
	shardNotifier ShardNotifier,
	cache *pebbleutils.Cache,
) DB {
	if strings.EqualFold(os.Getenv("ANTFLY_COREDB"), "zig") ||
		strings.EqualFold(os.Getenv("ANTFLY_COREDB"), "zigdb") {
		return NewZigCoreDB(
			lg,
			antflyConfig,
			schema,
			idxs,
			snapStore,
			shardNotifier,
			cache,
		)
	}

	return NewDBImpl(
		lg,
		antflyConfig,
		schema,
		idxs,
		snapStore,
		shardNotifier,
		cache,
	)
}
