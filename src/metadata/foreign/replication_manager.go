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

package foreign

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

// MetadataTransformer applies transforms and deletes to Antfly documents.
// Implemented by the metadata store to avoid circular imports.
type MetadataTransformer interface {
	ForwardTransform(ctx context.Context, tableName, key string, ops []*db.TransformOp, upsert bool) error
	ForwardDelete(ctx context.Context, tableName, key string) error
}

// MetadataKV provides key-value access to the metadata Raft store.
// Used for persisting LSN checkpoints.
type MetadataKV interface {
	Get(ctx context.Context, key []byte) ([]byte, io.Closer, error)
	Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error
}

// TableLister lists tables in the metadata store.
type TableLister interface {
	TablesMap() (map[string]*store.Table, error)
}

// raftLSNStore persists LSN checkpoints in the metadata Raft KV store.
// Keys are stored at "cdc:lsn:<slotName>" and values are LSN strings (e.g. "0/16B3748").
// Written on every CommitMessage, read on worker startup.
// Replicated via Raft — survives leader failover.
type raftLSNStore struct {
	kv MetadataKV
}

func newRaftLSNStore(kv MetadataKV) *raftLSNStore {
	return &raftLSNStore{kv: kv}
}

func (s *raftLSNStore) lsnKey(slotName string) []byte {
	return []byte("cdc:lsn:" + slotName)
}

func (s *raftLSNStore) LoadLSN(ctx context.Context, slotName string) (pglogrepl.LSN, error) {
	val, closer, err := s.kv.Get(ctx, s.lsnKey(slotName))
	if err != nil {
		// Key not found means no checkpoint yet — start from 0
		return 0, nil
	}
	defer func() { _ = closer.Close() }()

	if len(val) == 0 {
		return 0, nil
	}

	lsn, err := pglogrepl.ParseLSN(string(val))
	if err != nil {
		return 0, fmt.Errorf("parsing stored LSN %q: %w", string(val), err)
	}
	return lsn, nil
}

func (s *raftLSNStore) SaveLSN(ctx context.Context, slotName string, lsn pglogrepl.LSN) error {
	key := s.lsnKey(slotName)
	val := []byte(lsn.String())
	return s.kv.Batch(ctx, [][2][]byte{{key, val}}, nil)
}

// ReplicationManager orchestrates CDC workers for all tables with replication sources.
// Runs on the metadata leader only. When the leader context is cancelled (leadership lost),
// all workers are stopped.
type ReplicationManager struct {
	logger      *zap.Logger
	transformer MetadataTransformer
	kv          MetadataKV
	tables      TableLister

	// tableChangedC is signalled by the table metadata key-pattern listener
	// when a table is created or modified. Buffered size 1 so sends never block.
	tableChangedC chan struct{}
}

// NewReplicationManager creates a new ReplicationManager.
func NewReplicationManager(
	logger *zap.Logger,
	transformer MetadataTransformer,
	kv MetadataKV,
	tables TableLister,
) *ReplicationManager {
	return &ReplicationManager{
		logger:        logger,
		transformer:   transformer,
		kv:            kv,
		tables:        tables,
		tableChangedC: make(chan struct{}, 1),
	}
}

// NotifyTableChanged signals that a table was created or modified,
// causing the manager to rescan for new replication sources.
func (rm *ReplicationManager) NotifyTableChanged() {
	select {
	case rm.tableChangedC <- struct{}{}:
	default:
		// Already queued
	}
}

// resolveSlotName returns the user-specified slot name if set, otherwise derives one.
func resolveSlotName(tableName, pgTable, override string) string {
	if override != "" {
		return override
	}
	return SlotName(tableName, pgTable)
}

// resolvePublicationName returns the user-specified publication name if set, otherwise derives one.
func resolvePublicationName(tableName, pgTable, override string) string {
	if override != "" {
		return override
	}
	return PublicationName(tableName, pgTable)
}

// workerKey uniquely identifies a replication source.
type workerKey struct {
	tableName string
	pgTable   string
}

// Run starts CDC workers for all tables with replication sources and periodically
// rescans for new tables. It blocks until ctx is cancelled (leadership lost).
// Individual worker errors are logged but don't stop other workers.
func (rm *ReplicationManager) Run(ctx context.Context) error {
	lsnStore := newRaftLSNStore(rm.kv)

	var (
		mu      sync.Mutex
		workers = make(map[workerKey]context.CancelFunc)
		wg      sync.WaitGroup
	)

	// scanAndStart checks for new replication sources and starts workers.
	scanAndStart := func() {
		tablesMap, err := rm.tables.TablesMap()
		if err != nil {
			rm.logger.Error("Failed to list tables for CDC scan", zap.Error(err))
			return
		}

		mu.Lock()
		defer mu.Unlock()

		for tableName, table := range tablesMap {
			if len(table.ReplicationSources) == 0 {
				continue
			}

			for i, src := range table.ReplicationSources {
				if src.Type != "postgres" {
					rm.logger.Warn("Skipping non-postgres replication source",
						zap.String("table", tableName),
						zap.String("type", src.Type),
						zap.Int("sourceIndex", i),
					)
					continue
				}

				key := workerKey{tableName: tableName, pgTable: src.PostgresTable}
				if _, running := workers[key]; running {
					continue // already have a worker for this source
				}

				cfg := ReplicationConfig{
					TableName:         tableName,
					DSN:               src.DSN,
					PostgresTable:     src.PostgresTable,
					KeyTemplate:       src.KeyTemplate,
					OnUpdate:          src.OnUpdate,
					OnDelete:          src.OnDelete,
					SlotName:          resolveSlotName(tableName, src.PostgresTable, src.SlotName),
					PublicationName:   resolvePublicationName(tableName, src.PostgresTable, src.PublicationName),
					PublicationFilter: src.PublicationFilter,
				}
				cfg.Routes = src.Routes

				transformerRef := rm.transformer
				transformFunc := func(ctx context.Context, tblName, key string, ops []*db.TransformOp, upsert bool) error {
					return transformerRef.ForwardTransform(ctx, tblName, key, ops, upsert)
				}
				deleteFunc := func(ctx context.Context, tblName, key string) error {
					return transformerRef.ForwardDelete(ctx, tblName, key)
				}

				workerLogger := rm.logger.With(
					zap.String("table", tableName),
					zap.String("pgTable", src.PostgresTable),
					zap.String("slot", cfg.SlotName),
				)

				worker := NewReplicationWorker(cfg, lsnStore, transformFunc, deleteFunc, workerLogger)
				workerCtx, workerCancel := context.WithCancel(ctx) //nolint:gosec // G118: cancel stored in workers map for lifecycle management
				workers[key] = workerCancel

				wg.Add(1)
				go func(k workerKey) {
					defer wg.Done()
					rm.logger.Info("Starting CDC replication worker",
						zap.String("table", k.tableName),
						zap.String("pgTable", k.pgTable),
						zap.String("slot", cfg.SlotName),
					)
					err := worker.Run(workerCtx)
					if err != nil && workerCtx.Err() == nil {
						rm.logger.Error("CDC replication worker stopped with error",
							zap.String("table", k.tableName),
							zap.String("pgTable", k.pgTable),
							zap.Error(err),
						)
					} else {
						rm.logger.Info("CDC replication worker stopped",
							zap.String("table", k.tableName),
							zap.String("pgTable", k.pgTable),
						)
					}
					mu.Lock()
					delete(workers, k)
					mu.Unlock()
				}(key)
			}
		}
	}

	// Initial scan
	scanAndStart()

	// Rescan when notified of table changes
	for {
		select {
		case <-ctx.Done():
			// Cancel all workers and wait
			mu.Lock()
			for _, cancel := range workers {
				cancel()
			}
			mu.Unlock()
			wg.Wait()
			return nil
		case <-rm.tableChangedC:
			scanAndStart()
		}
	}
}
