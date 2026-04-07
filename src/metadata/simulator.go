package metadata

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/metadata/reconciler"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/antflydb/antfly/src/tracing"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type SimulationOptions struct {
	Clock                    clock.Clock
	Pool                     *workerpool.Pool
	TxnIDGenerator           func() uuid.UUID
	SplitFinalizeGracePeriod time.Duration
	EnableLeaderLoop         bool
}

// NewSimulationStore builds a MetadataStore around existing in-process metadata
// dependencies without creating listeners or transport clients.
func NewSimulationStore(
	logger *zap.Logger,
	config *common.Config,
	metadataKV *kv.MetadataStore,
	tm *tablemgr.TableManager,
	opts SimulationOptions,
) (*MetadataStore, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if metadataKV == nil {
		return nil, fmt.Errorf("metadataKV is required")
	}
	if tm == nil {
		return nil, fmt.Errorf("table manager is required")
	}

	clk := opts.Clock
	if clk == nil {
		clk = clock.RealClock{}
	}

	pool := opts.Pool
	if pool == nil {
		var err error
		pool, err = workerpool.NewPool()
		if err != nil {
			return nil, fmt.Errorf("creating worker pool: %w", err)
		}
	}

	ms := &MetadataStore{
		logger:           logger,
		metadataStore:    metadataKV,
		config:           config,
		tm:               tm,
		pool:             pool,
		runHealthCheckC:  make(chan struct{}, 1),
		reconcileShardsC: make(chan struct{}, 1),
		hlc:              NewHLCWithClock(clk),
		clock:            clk,
		txnIDGenerator:   opts.TxnIDGenerator,
		traceWriter:      tracing.NewAntflyTraceWriter(logger),
	}

	reconcilerConfig := reconciler.ReconciliationConfig{
		ReplicationFactor:        config.ReplicationFactor,
		MaxShardSizeBytes:        config.MaxShardSizeBytes,
		MinShardSizeBytes:        config.MinShardSizeBytes,
		MinShardsPerTable:        config.MinShardsPerTable,
		MaxShardsPerTable:        config.MaxShardsPerTable,
		DisableShardAlloc:        config.DisableShardAlloc,
		ShardCooldownPeriod:      config.ShardCooldownPeriod,
		SplitTimeout:             config.SplitTimeout,
		SplitFinalizeGracePeriod: opts.SplitFinalizeGracePeriod,
	}
	ms.reconciler = reconciler.NewReconcilerWithTimeProvider(
		NewMetadataShardOperations(ms),
		NewMetadataTableOperations(ms),
		NewMetadataStoreOperations(ms),
		reconcilerConfig,
		logger,
		clk,
	)
	if opts.EnableLeaderLoop {
		metadataKV.SetLeaderFactory(newSimulationLeaderFactory(logger, ms, metadataKV))
	}
	return ms, nil
}

// ReconcileOnce runs the same health check and reconciliation path used by the
// leader loop, but under direct caller control for deterministic simulation.
func (ms *MetadataStore) ReconcileOnce(ctx context.Context) {
	ms.runHealthCheck(ctx)
	ms.checkShardAssignments(ctx)
}

// InternalHandler exposes the internal resolve-intent endpoint required by the
// store shard notifier during in-process simulation.
func (ms *MetadataStore) InternalHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_internal/v1/shard/{shardID}/txn/resolve", ms.handleForwardResolveIntent)
	return mux
}

func newSimulationLeaderFactory(
	zl *zap.Logger,
	ms *MetadataStore,
	metadataStore *kv.MetadataStore,
) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := metadataStore.RegisterKeyPattern(
			"tm:t:{tableName}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				select {
				case ms.reconcileShardsC <- struct{}{}:
				default:
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register table pattern listener", zap.Error(err))
		}

		if err := metadataStore.RegisterKeyPattern(
			"tm:shs:{shardID}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				select {
				case ms.reconcileShardsC <- struct{}{}:
				default:
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register shard status pattern listener", zap.Error(err))
		}

		if err := metadataStore.RegisterKeyPattern(
			"tm:sts:{storeID}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				select {
				case ms.reconcileShardsC <- struct{}{}:
				default:
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register store status pattern listener", zap.Error(err))
		}

		if err := metadataStore.RegisterKeyPattern(
			"tm:stb:{storeID}",
			func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
				if !isDelete {
					select {
					case ms.reconcileShardsC <- struct{}{}:
					default:
					}
				}
				return nil
			},
		); err != nil {
			zl.Error("Failed to register store tombstone pattern listener", zap.Error(err))
		}

		metadataStore.RegisterKeyPrefixListener(
			[]byte("tm:rar:"),
			func(ctx context.Context, key, value []byte, isDelete bool) error {
				if !isDelete {
					select {
					case ms.reconcileShardsC <- struct{}{}:
					default:
					}
				}
				return nil
			},
		)

		go ms.reconcileShards(ctx)
		return nil
	}
}
