//go:build zigdb

package db

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store/db/indexes"
	zbridge "github.com/antflydb/antfly/src/store/db/zigdb"
	"github.com/antflydb/antfly/src/store/storeutils"
	bleve "github.com/blevesearch/bleve/v2"
	blevegeo "github.com/blevesearch/bleve/v2/geo"
	blevemapping "github.com/blevesearch/bleve/v2/mapping"
	bleveregistry "github.com/blevesearch/bleve/v2/registry"
	bleveSearch "github.com/blevesearch/bleve/v2/search"
	blevequery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

type ZigCoreDB struct {
	logger *zap.Logger

	mu              sync.RWMutex
	bridge          *zbridge.Bridge
	dir             string
	byteRange       types.Range
	splitState      *SplitState
	splitDeltaSeq   uint64
	splitDeltaFinal uint64
	shadowIndexDir  string
	indexes         map[string]indexes.IndexConfig
	schema          *schema.TableSchema
	snapStore       snapstore.SnapStore
}

var (
	txnParticipantsPrefix         = []byte("\x00\x00__txn_participants__:")
	txnResolvedParticipantsPrefix = []byte("\x00\x00__txn_resolved_participants__:")
	zigInternalPrefix             = []byte("\x00\x00__")
)

func NewZigCoreDB(
	lg *zap.Logger,
	_ *common.Config,
	tableSchema *schema.TableSchema,
	idxs map[string]indexes.IndexConfig,
	snapStore snapstore.SnapStore,
	_ ShardNotifier,
	_ *pebbleutils.Cache,
) DB {
	return &ZigCoreDB{
		logger:    lg.Named("zigCoreDB"),
		indexes:   maps.Clone(idxs),
		schema:    tableSchema,
		snapStore: snapStore,
		byteRange: types.Range{},
	}
}

func (db *ZigCoreDB) Open(dir string, _ bool, tableSchema *schema.TableSchema, byteRange types.Range) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.bridge != nil {
		db.bridge.Close()
		db.bridge = nil
	}

	if err := normalizeExtractedSnapshotLayout(dir); err != nil {
		return err
	}

	bridge, err := zbridge.Open(dir)
	if err != nil {
		return err
	}

	db.bridge = bridge
	db.dir = dir
	db.schema = tableSchema
	if err := db.reloadMetadataLocked(byteRange); err != nil {
		bridge.Close()
		db.bridge = nil
		return err
	}
	return nil
}

func (db *ZigCoreDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge != nil {
		db.bridge.Close()
		db.bridge = nil
	}
	return nil
}

func (db *ZigCoreDB) FindMedianKey() ([]byte, error) {
	db.mu.RLock()
	bridge := db.bridge
	byteRange := cloneRange(db.byteRange)
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("FindMedianKey without open bridge")
	}

	req := zbridge.ScanRequestPayload{}
	if len(byteRange[0]) > 0 {
		req.FromKeyB64 = encodeBase64(byteRange[0])
	}
	if len(byteRange[1]) > 0 {
		req.ToKeyB64 = encodeBase64(byteRange[1])
	}

	payload, err := bridge.Scan(req)
	if err != nil {
		return nil, err
	}

	docKeys := make([][]byte, 0, len(payload.Hashes))
	for _, item := range payload.Hashes {
		key, err := zbridge.DecodeBase64(item.IDB64)
		if err != nil {
			return nil, err
		}
		if bytes.HasPrefix(key, zigInternalPrefix) || bytes.HasPrefix(key, storeutils.MetadataPrefix) {
			continue
		}
		if _, err := bridge.LookupJSON(key); err != nil {
			continue
		}
		docKeys = append(docKeys, key)
	}
	if len(docKeys) == 0 {
		return nil, fmt.Errorf("no document keys found in range")
	}
	sort.Slice(docKeys, func(i, j int) bool {
		return bytes.Compare(docKeys[i], docKeys[j]) < 0
	})
	return bytes.Clone(docKeys[len(docKeys)/2]), nil
}

func (db *ZigCoreDB) SetRange(byteRange types.Range) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("SetRange without open bridge")
	}
	if err := db.bridge.UpdateRange(byteRange[0], byteRange[1]); err != nil {
		return err
	}
	db.byteRange = cloneRange(byteRange)
	return nil
}

func (db *ZigCoreDB) UpdateRange(byteRange types.Range) error {
	return db.SetRange(byteRange)
}

func (db *ZigCoreDB) GetRange() (types.Range, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return cloneRange(db.byteRange), nil
}

func (db *ZigCoreDB) GetSplitState() *SplitState {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return cloneSplitState(db.splitState)
}

func (db *ZigCoreDB) SetSplitState(state *SplitState) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("SetSplitState without open bridge")
	}
	if state == nil {
		if err := db.bridge.ClearSplitState(); err != nil {
			return err
		}
		db.splitState = nil
		return nil
	}
	payload, err := encodeSplitState(state)
	if err != nil {
		return err
	}
	if err := db.bridge.SetSplitState(payload); err != nil {
		return err
	}
	db.splitState = cloneSplitState(state)
	return nil
}

func (db *ZigCoreDB) ClearSplitState() error {
	return db.SetSplitState(nil)
}

func (db *ZigCoreDB) GetSplitDeltaSeq() (uint64, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return 0, zigUnsupported("GetSplitDeltaSeq without open bridge")
	}
	return bridge.GetSplitDeltaSeq()
}

func (db *ZigCoreDB) GetSplitDeltaFinalSeq() (uint64, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return 0, zigUnsupported("GetSplitDeltaFinalSeq without open bridge")
	}
	return bridge.GetSplitDeltaFinalSeq()
}

func (db *ZigCoreDB) SetSplitDeltaFinalSeq(seq uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("SetSplitDeltaFinalSeq without open bridge")
	}
	if err := db.bridge.SetSplitDeltaFinalSeq(seq); err != nil {
		return err
	}
	db.splitDeltaFinal = seq
	return nil
}

func (db *ZigCoreDB) ClearSplitDeltaFinalSeq() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("ClearSplitDeltaFinalSeq without open bridge")
	}
	if err := db.bridge.ClearSplitDeltaFinalSeq(); err != nil {
		return err
	}
	db.splitDeltaFinal = 0
	return nil
}

func (db *ZigCoreDB) ListSplitDeltaEntriesAfter(after uint64) ([]*SplitDeltaEntry, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("ListSplitDeltaEntriesAfter without open bridge")
	}
	payloads, err := bridge.ListSplitDeltaEntriesAfter(after)
	if err != nil {
		return nil, err
	}
	result := make([]*SplitDeltaEntry, 0, len(payloads))
	for _, payload := range payloads {
		entry, err := decodeSplitDeltaEntry(payload)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, nil
}

func (db *ZigCoreDB) ClearSplitDeltaEntries() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("ClearSplitDeltaEntries without open bridge")
	}
	if err := db.bridge.ClearSplitDeltaEntries(); err != nil {
		return err
	}
	db.splitDeltaSeq = 0
	db.splitDeltaFinal = 0
	return nil
}

func (db *ZigCoreDB) CreateShadowIndexManager(splitKey, originalRangeEnd []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("CreateShadowIndexManager without open bridge")
	}
	if err := db.bridge.CreateShadowIndexManager(splitKey, originalRangeEnd); err != nil {
		return err
	}
	dir, err := db.bridge.GetShadowIndexDir()
	if err != nil && !errors.Is(err, zbridge.ErrNotFound) {
		return err
	}
	db.shadowIndexDir = dir
	return nil
}

func (db *ZigCoreDB) CloseShadowIndexManager() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return nil
	}
	if err := db.bridge.CloseShadowIndexManager(); err != nil {
		return err
	}
	db.shadowIndexDir = ""
	return nil
}

func (db *ZigCoreDB) GetShadowIndexManager() *IndexManager {
	return nil
}

func (db *ZigCoreDB) GetShadowIndexDir() string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.shadowIndexDir
}

func (db *ZigCoreDB) UpdateSchema(tableSchema *schema.TableSchema) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("UpdateSchema without open bridge")
	}
	payload, err := encodeZigSchemaPayload(tableSchema)
	if err != nil {
		return err
	}
	if err := db.bridge.SetSchemaJSON(payload); err != nil {
		return err
	}
	db.schema = tableSchema
	return nil
}

func (db *ZigCoreDB) Get(_ context.Context, key []byte) (map[string]any, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("Get without open bridge")
	}

	buf, err := bridge.LookupJSON(key)
	if err != nil {
		if errors.Is(err, zbridge.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if len(buf) == 0 {
		return nil, ErrNotFound
	}

	var out map[string]any
	if err := json.Unmarshal(buf, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *ZigCoreDB) Scan(_ context.Context, fromKey, toKey []byte, opts ScanOptions) (*ScanResult, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("Scan without open bridge")
	}

	req := zbridge.ScanRequestPayload{
		FromKeyB64:       encodeBase64(fromKey),
		ToKeyB64:         encodeBase64(toKey),
		InclusiveFrom:    opts.InclusiveFrom,
		ExclusiveTo:      opts.ExclusiveTo,
		IncludeDocuments: opts.IncludeDocuments,
		Limit:            uint32(opts.Limit),
		IncludeAllFields: true,
	}

	var (
		filterQuery blevequery.Query
		err         error
	)
	if len(opts.FilterQuery) > 0 && !bytes.Equal(opts.FilterQuery, []byte("null")) {
		filterQuery, err = blevequery.ParseQuery(opts.FilterQuery)
		if err != nil {
			return nil, fmt.Errorf("parsing scan filter_query: %w", err)
		}
		// Evaluate scan filters honestly against stored documents even when the
		// caller only wants hashes back.
		req.IncludeDocuments = true
	}

	payload, err := bridge.Scan(req)
	if err != nil {
		return nil, err
	}

	result := &ScanResult{
		Hashes: make(map[string]uint64, len(payload.Hashes)),
	}
	if opts.IncludeDocuments {
		result.Documents = make(map[string]map[string]any, len(payload.Documents))
	}

	allowed := make(map[string]bool, len(payload.Documents))
	for _, item := range payload.Hashes {
		key, err := zbridge.DecodeBase64(item.IDB64)
		if err != nil {
			return nil, err
		}
		allowed[string(key)] = true
		result.Hashes[string(key)] = item.Hash
	}

	for _, item := range payload.Documents {
		key, err := zbridge.DecodeBase64(item.IDB64)
		if err != nil {
			return nil, err
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(item.JSON), &doc); err != nil {
			return nil, err
		}
		if filterQuery != nil {
			matched, err := matchesFilterQuery(filterQuery, doc)
			if err != nil {
				return nil, err
			}
			if !matched {
				delete(result.Hashes, string(key))
				delete(allowed, string(key))
				continue
			}
		}
		if result.Documents != nil {
			result.Documents[string(key)] = doc
		}
	}

	return result, nil
}

func (db *ZigCoreDB) Batch(_ context.Context, writes [][2][]byte, deletes [][]byte, syncLevel Op_SyncLevel) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("Batch without open bridge")
	}

	bridgeWrites := make([]zbridge.WriteIntent, 0, len(writes)+len(deletes))
	for _, write := range writes {
		bridgeWrites = append(bridgeWrites, zbridge.WriteIntent{
			Key:   write[0],
			Value: write[1],
		})
	}
	for _, key := range deletes {
		bridgeWrites = append(bridgeWrites, zbridge.WriteIntent{
			Key:      key,
			IsDelete: true,
		})
	}

	return db.bridge.Batch(bridgeWrites, nil, uint64(time.Now().UnixNano()), mapSyncLevel(syncLevel))
}

func (db *ZigCoreDB) Search(ctx context.Context, encodedRequest []byte) ([]byte, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("Search without open bridge")
	}

	var req indexes.RemoteIndexSearchRequest
	if err := json.Unmarshal(encodedRequest, &req); err != nil {
		return nil, err
	}
	if req.CountStar {
		if req.BleveSearchRequest == nil || len(req.VectorSearches) > 0 || len(req.SparseSearches) > 0 || len(req.GraphSearches) > 0 || req.MergeConfig != nil || req.RerankerConfig != nil {
			return nil, zigUnsupported("Search count outside narrowed full-text path")
		}
	}
	sortFields, err := parseSupportedSortFields(req.BlevePagingOpts.OrderBy)
	if err != nil {
		return nil, err
	}
	if len(req.BlevePagingOpts.SearchAfter) > 0 && len(req.BlevePagingOpts.SearchBefore) > 0 {
		return nil, zigUnsupported("Search with both search_after and search_before")
	}
	// Mixed-envelope sort/paging stays limited to cases where the adapter has one
	// authoritative final ranked doc set (for example fusion or expand_strategy
	// outputs). Requests outside that contract stay explicitly unsupported rather
	// than falling back to approximate Bleve compatibility.
	if len(req.BlevePagingOpts.OrderBy) > 0 &&
		(len(req.VectorSearches) > 0 || len(req.SparseSearches) > 0 || len(req.GraphSearches) > 0 || req.MergeConfig != nil) &&
		!supportsFusionSortPaging(&req) && !supportsGraphNodeSortPaging(&req) {
		return nil, zigUnsupported("Search custom bleve sort outside narrowed full-text path")
	}
	if (len(req.BlevePagingOpts.SearchAfter) > 0 || len(req.BlevePagingOpts.SearchBefore) > 0) &&
		(req.RerankerConfig != nil || len(req.VectorSearches) > 0 || len(req.SparseSearches) > 0 || len(req.GraphSearches) > 0 || req.MergeConfig != nil) &&
		!supportsFusionSortPaging(&req) && !supportsGraphNodeSortPaging(&req) {
		return nil, zigUnsupported("Search cursor paging outside narrowed full-text path")
	}
	var filterQuery blevequery.Query
	if len(req.FilterQuery) > 0 && !bytes.Equal(req.FilterQuery, []byte("null")) {
		q, err := blevequery.ParseQuery(req.FilterQuery)
		if err != nil {
			return nil, fmt.Errorf("parsing search filter_query: %w", err)
		}
		filterQuery = q
	}

	var nonTextBackendAggReqs []zbridge.SearchAggregationRequestPayload
	var graphOnlyBackendAggReqs []zbridge.SearchAggregationRequestPayload
	var fusionBackendAggReqs []zbridge.SearchAggregationRequestPayload
	if req.BleveSearchRequest == nil && len(req.AggregationRequests) > 0 {
		backendAggReqs, localAggReqs, err := splitBackendAggregationRequests(req.AggregationRequests)
		if err != nil {
			return nil, err
		}
		switch {
		case len(localAggReqs) > 0:
			// Unsupported request families stay rejected here until Zig can be the
			// source of truth for the final result set and aggregation domain.
			return nil, zigUnsupported("Search aggregations outside supported vector/sparse path")
		case req.ExpandStrategy != "" && len(req.GraphSearches) > 0 && len(req.VectorSearches)+len(req.SparseSearches) > 0 && req.MergeConfig == nil && req.RerankerConfig == nil:
			if backendAggRequiresFullTextIndex(backendAggReqs) {
				return nil, zigUnsupported("Search fusion aggregations requiring full-text corpus stats")
			}
			fusionBackendAggReqs = backendAggReqs
			req.AggregationRequests = nil
		case len(req.GraphSearches) == 1 && len(req.VectorSearches) == 0 && len(req.SparseSearches) == 0 && req.MergeConfig == nil && req.RerankerConfig == nil:
			if backendAggRequiresFullTextIndex(backendAggReqs) {
				return nil, zigUnsupported("Search graph aggregations requiring full-text corpus stats")
			}
			graphOnlyBackendAggReqs = backendAggReqs
			req.AggregationRequests = nil
		case req.MergeConfig != nil && len(req.GraphSearches) == 0 && len(req.VectorSearches)+len(req.SparseSearches) > 1:
			if backendAggRequiresFullTextIndex(backendAggReqs) {
				return nil, zigUnsupported("Search fusion aggregations requiring full-text corpus stats")
			}
			fusionBackendAggReqs = backendAggReqs
			req.AggregationRequests = nil
		case len(req.VectorSearches)+len(req.SparseSearches) > 0 && len(req.GraphSearches) == 0 && req.MergeConfig == nil && req.RerankerConfig == nil:
			if backendAggRequiresFullTextIndex(backendAggReqs) {
				return nil, zigUnsupported("Search vector/sparse aggregations requiring full-text corpus stats")
			}
			nonTextBackendAggReqs = backendAggReqs
			req.AggregationRequests = nil
		default:
			return nil, zigUnsupported("Search aggregations outside supported vector/sparse path")
		}
	}

	res := indexes.RemoteIndexSearchResult{
		VectorSearchResult: make(map[string]*vectorindex.SearchResult),
	}

	var backendAggResults bleveSearch.AggregationResults
	var backendGraphResults map[string]*indexes.GraphQueryResult
	var backendGraphStatus map[string]*indexes.SearchComponentStatus
	if req.BleveSearchRequest != nil {
		textReq := *req.BleveSearchRequest
		if req.CountStar {
			textReq.From = 0
			textReq.Size = 0
		}
		textFallbackLimit := req.Limit
		if req.CountStar {
			textFallbackLimit = 0
		}
		backendAggReqs, localAggReqs, err := splitBackendAggregationRequests(req.AggregationRequests)
		if err != nil {
			return nil, err
		}
		textBackendAggReqs := backendAggReqs
		if len(localAggReqs) > 0 {
			return nil, zigUnsupported("Search aggregations outside narrowed full-text path")
		}
		if (req.MergeConfig != nil || req.ExpandStrategy != "") && len(backendAggReqs) > 0 && len(localAggReqs) == 0 {
			if backendAggRequiresFullTextIndex(backendAggReqs) {
				return nil, zigUnsupported("Search fusion aggregations requiring full-text corpus stats")
			}
			fusionBackendAggReqs = backendAggReqs
			textBackendAggReqs = nil
		}
		backendGraphReqs, useBackendGraph, err := db.buildBackendGraphQueries(&req)
		if err != nil {
			return nil, err
		}
		bleveResult, zigAggResults, err := executeNarrowedTextSearch(db, bridge, &textReq, req.BlevePagingOpts, sortFields, textFallbackLimit, filterQuery, req.FilterPrefix, textBackendAggReqs)
		if err != nil {
			return nil, err
		}
		if req.CountStar {
			bleveResult.Hits = nil
			bleveResult.MaxScore = 0
		}
		res.BleveSearchResult = bleveResult
		res.Total = bleveResult.Total
		backendAggResults, err = decodeBackendAggregationResults(zigAggResults)
		if err != nil {
			return nil, err
		}
		if useBackendGraph {
			zigGraphResults, err := executeBackendGraphQueries(bridge, bleveResult, backendGraphReqs)
			if err != nil {
				return nil, err
			}
			backendGraphResults, backendGraphStatus, err = db.decodeBackendGraphResults(ctx, req.GraphSearches, zigGraphResults)
			if err != nil {
				return nil, err
			}
		}
		req.AggregationRequests = nil
	}

	if len(req.AggregationRequests) > 0 {
		return nil, zigUnsupported("Search aggregations outside narrowed full-text path")
	}
	if len(backendAggResults) > 0 {
		if res.AggregationResults == nil {
			res.AggregationResults = backendAggResults
		} else {
			for name, agg := range backendAggResults {
				res.AggregationResults[name] = agg
			}
		}
	}

	vectorLimit := req.Limit
	if req.VectorPagingOpts.Limit > 0 {
		vectorLimit = req.VectorPagingOpts.Limit
	}
	for indexName, vec := range req.VectorSearches {
		aggregations := []zbridge.SearchAggregationRequestPayload(nil)
		if len(nonTextBackendAggReqs) > 0 && len(req.VectorSearches)+len(req.SparseSearches) == 1 && filterQuery == nil && len(req.FilterPrefix) == 0 {
			aggregations = nonTextBackendAggReqs
		}
		payload, err := bridge.Search(zbridge.SearchRequestPayload{
			Mode:          "dense",
			IndexName:     indexName,
			Vector:        vec,
			K:             uint32(vectorLimit),
			Limit:         uint32(vectorLimit),
			IncludeStored: true,
			Aggregations:  aggregations,
		})
		if err != nil {
			return nil, err
		}
		vectorResult, err := decodeVectorSearchResult(indexName, payload)
		if err != nil {
			return nil, err
		}
		if filterQuery != nil {
			if err := applyVectorFilterQuery(filterQuery, vectorResult); err != nil {
				return nil, err
			}
		}
		applyVectorDistancePaging(req.VectorPagingOpts, vectorResult)
		if len(req.FilterPrefix) > 0 {
			applyVectorFilterPrefix(req.FilterPrefix, vectorResult)
		}
		res.VectorSearchResult[indexName] = vectorResult
		res.Total = max(res.Total, vectorResult.Total)
		if len(aggregations) > 0 {
			res.AggregationResults, err = decodeBackendAggregationResults(payload.Aggregations)
			if err != nil {
				return nil, err
			}
		} else if len(nonTextBackendAggReqs) > 0 {
			res.AggregationResults, err = executeBackendVectorAggregations(bridge, indexName, vectorResult, nonTextBackendAggReqs)
			if err != nil {
				return nil, err
			}
		}
	}

	for indexName, sparse := range req.SparseSearches {
		aggregations := []zbridge.SearchAggregationRequestPayload(nil)
		if len(nonTextBackendAggReqs) > 0 && len(req.VectorSearches)+len(req.SparseSearches) == 1 && filterQuery == nil && len(req.FilterPrefix) == 0 {
			aggregations = nonTextBackendAggReqs
		}
		payload, err := bridge.Search(zbridge.SearchRequestPayload{
			Mode:          "sparse",
			IndexName:     indexName,
			Indices:       sparse.Indices,
			Values:        sparse.Values,
			K:             uint32(vectorLimit),
			Limit:         uint32(vectorLimit),
			IncludeStored: true,
			Aggregations:  aggregations,
		})
		if err != nil {
			return nil, err
		}
		vectorResult, err := decodeVectorSearchResult(indexName, payload)
		if err != nil {
			return nil, err
		}
		if filterQuery != nil {
			if err := applyVectorFilterQuery(filterQuery, vectorResult); err != nil {
				return nil, err
			}
		}
		applyVectorDistancePaging(req.VectorPagingOpts, vectorResult)
		if len(req.FilterPrefix) > 0 {
			applyVectorFilterPrefix(req.FilterPrefix, vectorResult)
		}
		res.VectorSearchResult[indexName] = vectorResult
		res.Total = max(res.Total, vectorResult.Total)
		if len(aggregations) > 0 {
			res.AggregationResults, err = decodeBackendAggregationResults(payload.Aggregations)
			if err != nil {
				return nil, err
			}
		} else if len(nonTextBackendAggReqs) > 0 {
			res.AggregationResults, err = executeBackendVectorAggregations(bridge, indexName, vectorResult, nonTextBackendAggReqs)
			if err != nil {
				return nil, err
			}
		}
	}

	if req.MergeConfig != nil {
		mc := req.MergeConfig
		weights := map[string]float64(nil)
		if mc.Weights != nil {
			weights = *mc.Weights
		}
		switch {
		case mc.Strategy != nil && *mc.Strategy == indexes.MergeStrategy("rsf"):
			windowSize := mc.WindowSize
			if windowSize == 0 {
				windowSize = req.Limit
			}
			res.FusionResult = res.RSFResults(req.Limit, windowSize, weights)
		default:
			res.FusionResult = res.RRFResults(req.Limit, mc.RankConstant, weights)
		}
		if len(fusionBackendAggReqs) > 0 {
			res.AggregationResults, err = executeBackendFusionAggregations(bridge, res.FusionResult, fusionBackendAggReqs)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(req.GraphSearches) > 0 {
		if backendGraphResults != nil {
			res.GraphResults = backendGraphResults
			if res.Status == nil {
				res.Status = &indexes.RemoteIndexSearchStatus{}
			}
			res.Status.GraphStatus = backendGraphStatus
		} else {
			graphResults, graphStatus, err := db.executeGraphQueries(ctx, &req, &res)
			if err != nil {
				return nil, err
			}
			res.GraphResults = graphResults
			if res.Status == nil {
				res.Status = &indexes.RemoteIndexSearchStatus{}
			}
			res.Status.GraphStatus = graphStatus
		}
		if req.ExpandStrategy != "" {
			if err := applyGraphFusion(&res, req.ExpandStrategy, db.logger); err != nil {
				return nil, err
			}
		}
		if req.BleveSearchRequest == nil {
			if err := applyGraphFilterQuery(ctx, db, filterQuery, res.GraphResults); err != nil {
				return nil, err
			}
		}
		if supportsGraphNodeSortPaging(&req) {
			res.FusionResult = buildFusionResultFromGraphNodes(res.GraphResults)
		}
		if len(graphOnlyBackendAggReqs) > 0 {
			graphAggResults, err := executeBackendGraphAggregations(ctx, db, bridge, req.GraphSearches, res.GraphResults, graphOnlyBackendAggReqs, req.FilterPrefix)
			if err != nil {
				return nil, err
			}
			res.AggregationResults = graphAggResults
		}
	}
	if len(nonTextBackendAggReqs) > 0 && res.AggregationResults == nil && res.FusionResult == nil {
		unionAggResults, err := executeBackendUnionVectorAggregations(bridge, res.VectorSearchResult, nonTextBackendAggReqs)
		if err != nil {
			return nil, err
		}
		res.AggregationResults = unionAggResults
	}

	if len(req.FilterPrefix) > 0 {
		applyGraphFilterPrefix(req.FilterPrefix, res.GraphResults)
		applyFusionFilterPrefix(req.FilterPrefix, res.FusionResult)
	}
	if len(fusionBackendAggReqs) > 0 {
		if res.FusionResult == nil {
			return nil, zigUnsupported("Search fusion aggregations without fused result set")
		}
		fusionAggResults, err := executeBackendFusionAggregations(bridge, res.FusionResult, fusionBackendAggReqs)
		if err != nil {
			return nil, err
		}
		res.AggregationResults = fusionAggResults
	}
	if res.FusionResult != nil && (supportsFusionSortPaging(&req) || supportsGraphNodeSortPaging(&req)) {
		if len(sortFields) > 0 {
			if err := validateFusionSortFieldsOnHits(res.FusionResult.Hits, sortFields); err != nil {
				return nil, err
			}
			sortFusionHits(res.FusionResult.Hits, sortFields)
			if err := applySortedFusionPaging(res.FusionResult, req.BlevePagingOpts, sortFields, req.Limit); err != nil {
				return nil, err
			}
		} else if len(req.BlevePagingOpts.SearchAfter) > 0 || len(req.BlevePagingOpts.SearchBefore) > 0 || req.BlevePagingOpts.Limit > 0 || req.BlevePagingOpts.Offset > 0 {
			applyDefaultFusionCursorPaging(res.FusionResult, req.BlevePagingOpts, req.Limit)
		}
	}
	if err := applyRerankingToRemoteResults(ctx, req, &res, db.logger); err != nil {
		db.logger.Warn("Failed to rerank zigdb search results, using original scores", zap.Error(err))
	}

	return json.Marshal(res)
}

func applyRerankingToRemoteResults(
	ctx context.Context,
	request indexes.RemoteIndexSearchRequest,
	result *indexes.RemoteIndexSearchResult,
	logger *zap.Logger,
) error {
	if request.RerankerConfig == nil {
		return nil
	}
	if result == nil {
		return nil
	}

	if result.FusionResult != nil {
		documents := make([]map[string]any, 0, len(result.FusionResult.Hits))
		validHits := make([]*indexes.FusionHit, 0, len(result.FusionResult.Hits))
		for _, hit := range result.FusionResult.Hits {
			if hit == nil || hit.Fields == nil {
				continue
			}
			documents = append(documents, hit.Fields)
			validHits = append(validHits, hit)
		}
		result.FusionResult.Hits = validHits
		if len(documents) == 0 {
			return nil
		}
		scores, err := rerank(ctx, request, documents)
		if err != nil {
			return err
		}
		for i := range scores {
			score := float64(scores[i])
			result.FusionResult.Hits[i].RerankedScore = &score
		}
		sort.Slice(result.FusionResult.Hits, func(i, j int) bool {
			return scores[i] > scores[j]
		})
		return nil
	}

	if result.BleveSearchResult != nil {
		documents := make([]map[string]any, 0, len(result.BleveSearchResult.Hits))
		validHits := make([]*bleveSearch.DocumentMatch, 0, len(result.BleveSearchResult.Hits))
		for _, hit := range result.BleveSearchResult.Hits {
			if hit == nil || hit.Fields == nil {
				continue
			}
			documents = append(documents, hit.Fields)
			validHits = append(validHits, hit)
		}
		result.BleveSearchResult.Hits = validHits
		if len(documents) == 0 {
			return nil
		}
		scores, err := rerank(ctx, request, documents)
		if err != nil {
			return err
		}
		for i := range scores {
			result.BleveSearchResult.Hits[i].Score = float64(scores[i])
		}
		sort.Slice(result.BleveSearchResult.Hits, func(i, j int) bool {
			return scores[i] > scores[j]
		})
		return nil
	}

	if len(result.VectorSearchResult) > 0 {
		for indexName, vectorResult := range result.VectorSearchResult {
			documents := make([]map[string]any, 0, len(vectorResult.Hits))
			validHits := make([]*vectorindex.SearchHit, 0, len(vectorResult.Hits))
			for _, hit := range vectorResult.Hits {
				if hit == nil || hit.Fields == nil {
					continue
				}
				documents = append(documents, hit.Fields)
				validHits = append(validHits, hit)
			}
			vectorResult.Hits = validHits
			if len(documents) == 0 {
				continue
			}
			scores, err := rerank(ctx, request, documents)
			if err != nil {
				logger.Warn("Failed to rerank zigdb vector results, using original scores", zap.String("index", indexName), zap.Error(err))
				continue
			}
			for i := range scores {
				vectorResult.Hits[i].Score = scores[i]
			}
			sort.Slice(vectorResult.Hits, func(i, j int) bool {
				return scores[i] > scores[j]
			})
		}
	}

	return nil
}

func (db *ZigCoreDB) Split(currRange types.Range, splitKey []byte, destDir1, destDir2 string, prepareOnly bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("Split without open bridge")
	}
	if err := db.bridge.Split(currRange[0], currRange[1], splitKey, destDir1, destDir2, prepareOnly); err != nil {
		return err
	}
	if !prepareOnly {
		db.byteRange = types.Range{currRange[0], splitKey}
	}
	return nil
}

func (db *ZigCoreDB) FinalizeSplit(newRange types.Range) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("FinalizeSplit without open bridge")
	}
	if err := db.bridge.FinalizeSplit(newRange[0], newRange[1]); err != nil {
		return err
	}
	db.byteRange = cloneRange(newRange)
	db.shadowIndexDir = ""
	db.splitState = nil
	return nil
}

func (db *ZigCoreDB) Snapshot(id string) (int64, error) {
	db.mu.RLock()
	bridge := db.bridge
	dir := db.dir
	byteRange := cloneRange(db.byteRange)
	snapStore := db.snapStore
	db.mu.RUnlock()
	if bridge == nil {
		return 0, zigUnsupported("Snapshot without open bridge")
	}
	if snapStore == nil {
		return 0, zigUnsupported("Snapshot without snapstore")
	}
	if _, err := bridge.Snapshot(id); err != nil {
		return 0, err
	}

	snapshotRoot := filepath.Join(dir+".snapshots", id)
	defer func() { _ = os.RemoveAll(snapshotRoot) }()

	stagingDir, err := os.MkdirTemp("", "antfly-zig-snap-*")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(stagingDir) }()

	stagingPebbleDir := filepath.Join(stagingDir, "pebble")
	if err := common.CopyDir(snapshotRoot, stagingPebbleDir); err != nil {
		return 0, fmt.Errorf("copying zig snapshot into staging dir: %w", err)
	}

	indexDir := filepath.Join(dir, "indexes")
	if _, err := os.Stat(indexDir); err == nil {
		if err := common.CopyDir(indexDir, filepath.Join(stagingDir, "indexes")); err != nil {
			db.logger.Warn("failed to copy zig indexes into snapshot archive; restore will rebuild",
				zap.String("indexDir", indexDir),
				zap.Error(err))
			_ = os.RemoveAll(filepath.Join(stagingDir, "indexes"))
		}
	}

	var snapOpts *snapstore.SnapshotOptions
	nodeID, shardID, err := common.ParseStorageDBDir(dir)
	if err == nil {
		snapOpts = &snapstore.SnapshotOptions{
			ShardID: shardID,
			NodeID:  nodeID,
			Range:   byteRange,
		}
	}

	size, err := snapStore.CreateSnapshot(context.Background(), id, stagingDir, snapOpts)
	return size, err
}

func (db *ZigCoreDB) Stats() (uint64, bool, map[string]indexes.IndexStats, error) {
	db.mu.RLock()
	bridge := db.bridge
	dir := db.dir
	indexesByName := maps.Clone(db.indexes)
	db.mu.RUnlock()
	if bridge == nil {
		return 0, false, nil, zigUnsupported("Stats without open bridge")
	}
	stats, err := bridge.Stats()
	if err != nil {
		return 0, false, nil, err
	}
	diskSize, _ := common.GetDirectorySize(dir)
	statsByName := make(map[string]zbridge.DBIndexStatsPayload, len(stats.Indexes))
	for _, idx := range stats.Indexes {
		statsByName[idx.Name] = idx
	}
	indexStats := make(map[string]indexes.IndexStats, len(indexesByName))
	for name, cfg := range indexesByName {
		idxStats, hasIndexStats := statsByName[name]
		indexDiskUsage, _ := common.GetDirectorySize(filepath.Join(dir, "indexes", name))
		switch cfg.Type {
		case indexes.IndexTypeFullText:
			ft := indexes.FullTextIndexStats{DiskUsage: indexDiskUsage}
			if hasIndexStats {
				ft.TotalIndexed = idxStats.DocCount
			}
			indexStats[name] = ft.AsIndexStats()
		case indexes.IndexTypeGraph:
			gr := indexes.GraphIndexStats{}
			if hasIndexStats {
				gr.TotalEdges = idxStats.EdgeCount
			}
			indexStats[name] = gr.AsIndexStats()
		default:
			emb := indexes.EmbeddingsIndexStats{DiskUsage: indexDiskUsage}
			if hasIndexStats {
				emb.TotalIndexed = idxStats.DocCount
				emb.TotalNodes = idxStats.NodeCount
				emb.TotalTerms = idxStats.TermCount
			}
			indexStats[name] = emb.AsIndexStats()
		}
	}
	return diskSize, stats.DocCount == 0, indexStats, nil
}

func (db *ZigCoreDB) DetailedStats() (*zbridge.DBStatsPayload, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("DetailedStats without open bridge")
	}
	return bridge.Stats()
}

func normalizeExtractedSnapshotLayout(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, "data.mdb")); err == nil {
		return nil
	}
	pebbleDir := filepath.Join(dir, "pebble")
	if _, err := os.Stat(pebbleDir); err != nil {
		return nil
	}

	for _, name := range []string{"data.mdb", "lock.mdb", "derived_log"} {
		src := filepath.Join(pebbleDir, name)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		dst := filepath.Join(dir, name)
		if err := os.RemoveAll(dst); err != nil {
			return fmt.Errorf("clearing restored zig snapshot target %s: %w", dst, err)
		}
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("moving restored zig snapshot content %s: %w", name, err)
		}
	}
	_ = os.Remove(pebbleDir)
	return nil
}

func (db *ZigCoreDB) AddIndex(config indexes.IndexConfig) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("AddIndex without open bridge")
	}
	req, err := db.encodeIndexConfig(config)
	if err != nil {
		return err
	}
	if err := db.bridge.AddIndex(req); err != nil {
		return err
	}
	db.indexes[config.Name] = config
	return nil
}

func (db *ZigCoreDB) DeleteIndex(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.bridge == nil {
		return zigUnsupported("DeleteIndex without open bridge")
	}
	deleted, err := db.bridge.DeleteIndex([]byte(name))
	if err != nil {
		return err
	}
	if !deleted {
		return nil
	}
	delete(db.indexes, name)
	return nil
}

func (db *ZigCoreDB) HasIndex(name string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.indexes[name]
	return ok
}

func (db *ZigCoreDB) GetIndexes() map[string]indexes.IndexConfig {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return maps.Clone(db.indexes)
}

func (db *ZigCoreDB) LeaderFactory(context.Context, PersistFunc) error {
	return nil
}

func (db *ZigCoreDB) ExtractEnrichments(_ context.Context, writes [][2][]byte) ([][2][]byte, [][2][]byte, [][2][]byte, [][]byte, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, nil, nil, nil, zigUnsupported("ExtractEnrichments without open bridge")
	}

	payload, err := bridge.ExtractEnrichments(writes)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	embeddingWrites := make([][2][]byte, 0, len(payload.DenseEmbeddings)+len(payload.SparseEmbeddings))
	for _, item := range payload.DenseEmbeddings {
		docKey, err := zbridge.DecodeBase64(item.DocKeyB64)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		value, err := vectorindex.EncodeEmbeddingWithHashID(nil, item.Vector, 0)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		embeddingWrites = append(embeddingWrites, [2][]byte{
			storeutils.MakeEmbeddingKey(docKey, item.IndexName),
			value,
		})
	}
	for _, item := range payload.SparseEmbeddings {
		docKey, err := zbridge.DecodeBase64(item.DocKeyB64)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		sparseBytes := indexes.EncodeSparseVec(vector.NewSparseVector(item.Indices, item.Values))
		value := make([]byte, 8+len(sparseBytes))
		copy(value[8:], sparseBytes)
		embeddingWrites = append(embeddingWrites, [2][]byte{
			storeutils.MakeSparseKey(docKey, item.IndexName),
			value,
		})
	}

	summaryWrites := make([][2][]byte, 0, len(payload.Summaries))
	for _, item := range payload.Summaries {
		docKey, err := zbridge.DecodeBase64(item.DocKeyB64)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		hashID := xxhash.Sum64String(item.Text)
		value := encoding.EncodeUint64Ascending(nil, hashID)
		value = append(value, []byte(item.Text)...)
		summaryWrites = append(summaryWrites, [2][]byte{
			storeutils.MakeSummaryKey(docKey, item.IndexName),
			value,
		})
	}

	edgeWrites := make([][2][]byte, 0, len(payload.GraphWrites))
	for _, item := range payload.GraphWrites {
		source, err := zbridge.DecodeBase64(item.SourceB64)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		target, err := zbridge.DecodeBase64(item.TargetB64)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		edge, err := decodeExtractedEdge(item, source, target)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		value, err := indexes.EncodeEdgeValue(edge)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		edgeWrites = append(edgeWrites, [2][]byte{
			storeutils.MakeEdgeKey(source, target, item.IndexName, item.EdgeType),
			value,
		})
	}

	return embeddingWrites, summaryWrites, edgeWrites, nil, nil
}

func (db *ZigCoreDB) ComputeEnrichments(_ context.Context, writes [][2][]byte) ([][2][]byte, [][]byte, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, nil, zigUnsupported("ComputeEnrichments without open bridge")
	}

	payload, err := bridge.ComputeEnrichments(writes)
	if err != nil {
		return nil, nil, err
	}

	enrichmentWrites := make([][2][]byte, 0, len(payload.ArtifactWrites)+len(payload.Documents)+len(payload.DenseEmbeddings))
	for _, item := range payload.ArtifactWrites {
		key, err := zbridge.DecodeBase64(item.KeyB64)
		if err != nil {
			return nil, nil, err
		}
		value, err := zbridge.DecodeBase64(item.ValueB64)
		if err != nil {
			return nil, nil, err
		}
		enrichmentWrites = append(enrichmentWrites, [2][]byte{key, value})
	}
	for _, item := range payload.Documents {
		key, err := zbridge.DecodeBase64(item.KeyB64)
		if err != nil {
			return nil, nil, err
		}
		value, err := zbridge.DecodeBase64(item.ValueB64)
		if err != nil {
			return nil, nil, err
		}
		enrichmentWrites = append(enrichmentWrites, [2][]byte{key, value})
	}
	for _, item := range payload.DenseEmbeddings {
		docKey, err := zbridge.DecodeBase64(item.DocKeyB64)
		if err != nil {
			return nil, nil, err
		}
		value, err := vectorindex.EncodeEmbeddingWithHashID(nil, item.Vector, 0)
		if err != nil {
			return nil, nil, err
		}
		enrichmentWrites = append(enrichmentWrites, [2][]byte{
			storeutils.MakeEmbeddingKey(docKey, item.IndexName),
			value,
		})
	}

	failedKeys := make([][]byte, 0, len(payload.FailedKeysB64))
	for _, encoded := range payload.FailedKeysB64 {
		key, err := zbridge.DecodeBase64(encoded)
		if err != nil {
			return nil, nil, err
		}
		failedKeys = append(failedKeys, key)
	}

	return enrichmentWrites, failedKeys, nil
}

func (db *ZigCoreDB) InitTransaction(_ context.Context, op *InitTransactionOp) error {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return zigUnsupported("InitTransaction without open bridge")
	}

	var txnID [16]byte
	copy(txnID[:], op.GetTxnId())
	return bridge.BeginTransactionWithID(txnID, op.GetTimestamp(), op.GetParticipants())
}

func (db *ZigCoreDB) CommitTransaction(_ context.Context, op *CommitTransactionOp) (uint64, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return 0, zigUnsupported("CommitTransaction without open bridge")
	}

	var txnID [16]byte
	copy(txnID[:], op.GetTxnId())
	commitVersion := uint64(time.Now().UnixNano())
	if err := bridge.ResolveIntents(txnID, 1, commitVersion); err != nil {
		return 0, err
	}
	return commitVersion, nil
}

func (db *ZigCoreDB) AbortTransaction(_ context.Context, op *AbortTransactionOp) error {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return zigUnsupported("AbortTransaction without open bridge")
	}

	var txnID [16]byte
	copy(txnID[:], op.GetTxnId())
	return bridge.ResolveIntents(txnID, 2, 0)
}

func (db *ZigCoreDB) WriteIntent(_ context.Context, op *WriteIntentOp) error {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return zigUnsupported("WriteIntent without open bridge")
	}

	var txnID [16]byte
	copy(txnID[:], op.GetTxnId())

	batch := op.GetBatch()
	if batch == nil {
		return fmt.Errorf("zig coreDB: write intent batch is nil")
	}

	writes := make([]zbridge.WriteIntent, 0, len(batch.GetWrites())+len(batch.GetDeletes()))
	for _, write := range batch.GetWrites() {
		writes = append(writes, zbridge.WriteIntent{
			Key:   write.GetKey(),
			Value: write.GetValue(),
		})
	}
	for _, key := range batch.GetDeletes() {
		writes = append(writes, zbridge.WriteIntent{
			Key:      key,
			IsDelete: true,
		})
	}

	predicates := make([]zbridge.VersionPredicate, 0, len(op.GetPredicates()))
	for _, predicate := range op.GetPredicates() {
		predicates = append(predicates, zbridge.VersionPredicate{
			Key:             predicate.GetKey(),
			ExpectedVersion: predicate.GetExpectedVersion(),
		})
	}

	return bridge.WriteTransaction(txnID, writes, predicates)
}

func (db *ZigCoreDB) ResolveIntents(_ context.Context, op *ResolveIntentsOp) error {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return zigUnsupported("ResolveIntents without open bridge")
	}

	var txnID [16]byte
	copy(txnID[:], op.GetTxnId())
	return bridge.ResolveIntents(txnID, uint8(op.GetStatus()), op.GetCommitVersion())
}

func (db *ZigCoreDB) GetTransactionStatus(_ context.Context, txnID []byte) (int32, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return 0, zigUnsupported("GetTransactionStatus without open bridge")
	}

	var fixed [16]byte
	copy(fixed[:], txnID)
	status, err := bridge.GetTransactionStatus(fixed)
	return int32(status), err
}

func (db *ZigCoreDB) GetCommitVersion(_ context.Context, txnID []byte) (uint64, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return 0, zigUnsupported("GetCommitVersion without open bridge")
	}

	var fixed [16]byte
	copy(fixed[:], txnID)
	return bridge.GetCommitVersion(fixed)
}

func (db *ZigCoreDB) ListTxnRecords(_ context.Context) ([]TxnRecord, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("ListTxnRecords without open bridge")
	}

	payload, err := bridge.Scan(zbridge.ScanRequestPayload{
		FromKeyB64: encodeBase64(txnRecordsPrefix),
		ToKeyB64:   encodeBase64(utils.PrefixSuccessor(txnRecordsPrefix)),
	})
	if err != nil {
		return nil, err
	}

	records := make([]TxnRecord, 0, len(payload.Hashes))
	for _, item := range payload.Hashes {
		key, err := zbridge.DecodeBase64(item.IDB64)
		if err != nil {
			return nil, err
		}
		raw, err := bridge.GetRaw(key)
		if err != nil {
			return nil, err
		}
		record, err := decodeZigTxnRecord(key, raw, bridge)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (db *ZigCoreDB) ListTxnIntents(_ context.Context) ([]TxnIntent, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("ListTxnIntents without open bridge")
	}

	payload, err := bridge.Scan(zbridge.ScanRequestPayload{
		FromKeyB64: encodeBase64(txnIntentsPrefix),
		ToKeyB64:   encodeBase64(utils.PrefixSuccessor(txnIntentsPrefix)),
	})
	if err != nil {
		return nil, err
	}

	intents := make([]TxnIntent, 0, len(payload.Hashes))
	recordMeta := make(map[[16]byte]TxnRecord)
	for _, item := range payload.Hashes {
		key, err := zbridge.DecodeBase64(item.IDB64)
		if err != nil {
			return nil, err
		}
		raw, err := bridge.GetRaw(key)
		if err != nil {
			return nil, err
		}
		intent, err := decodeZigTxnIntent(key, raw, bridge, recordMeta)
		if err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, nil
}

func decodeZigTxnRecord(key, raw []byte, bridge *zbridge.Bridge) (TxnRecord, error) {
	if len(key) != len(txnRecordsPrefix)+16 {
		return TxnRecord{}, fmt.Errorf("parsing transaction record key %q", types.FormatKey(key))
	}
	if len(raw) != 17 && len(raw) != 33 {
		return TxnRecord{}, fmt.Errorf("unmarshaling transaction record: invalid zig txn record size %d", len(raw))
	}

	txnID := bytes.Clone(key[len(txnRecordsPrefix):])
	status := int32(raw[0])
	timestamp := binary.LittleEndian.Uint64(raw[1:9])
	commitVersion := uint64(0)
	createdAt := int64(0)
	finalizedAt := int64(0)
	if len(raw) == 33 {
		commitVersion = binary.LittleEndian.Uint64(raw[9:17])
		createdAt = int64(binary.LittleEndian.Uint64(raw[17:25]))
		finalizedAt = int64(binary.LittleEndian.Uint64(raw[25:33]))
	} else {
		if status == int32(TxnStatusCommitted) {
			commitVersion = timestamp
		}
		createdAt = int64(binary.LittleEndian.Uint64(raw[9:17]))
	}

	participants, err := loadZigTxnParticipantSet(bridge, txnParticipantsPrefix, txnID)
	if err != nil {
		return TxnRecord{}, err
	}
	resolved, err := loadZigTxnParticipantSet(bridge, txnResolvedParticipantsPrefix, txnID)
	if err != nil {
		return TxnRecord{}, err
	}
	resolvedParticipants := make([]string, 0, len(resolved))
	for _, participant := range resolved {
		resolvedParticipants = append(resolvedParticipants, string(participant))
	}

	return TxnRecord{
		TxnID:                txnID,
		Timestamp:            timestamp,
		CommitVersion:        commitVersion,
		Status:               status,
		Participants:         participants,
		ResolvedParticipants: resolvedParticipants,
		CreatedAt:            createdAt,
		FinalizedAt:          finalizedAt,
	}, nil
}

func decodeZigTxnIntent(key, raw []byte, bridge *zbridge.Bridge, recordMeta map[[16]byte]TxnRecord) (TxnIntent, error) {
	parsedTxnID, userKey := parseIntentKey(key)
	if parsedTxnID == nil || userKey == nil {
		return TxnIntent{}, fmt.Errorf("parsing transaction intent key %q", types.FormatKey(key))
	}

	intent := TxnIntent{
		TxnID:    bytes.Clone(parsedTxnID),
		UserKey:  bytes.Clone(userKey),
		IsDelete: len(raw) > 0 && raw[0] == 1,
	}
	if len(raw) > 1 {
		intent.Value = bytes.Clone(raw[1:])
	}

	var fixed [16]byte
	copy(fixed[:], parsedTxnID)
	record, ok := recordMeta[fixed]
	if !ok {
		recordKey := makeTxnKey(parsedTxnID)
		recordRaw, err := bridge.GetRaw(recordKey)
		if err != nil && !errors.Is(err, zbridge.ErrNotFound) {
			return TxnIntent{}, err
		}
		if len(recordRaw) > 0 {
			record, err = decodeZigTxnRecord(recordKey, recordRaw, bridge)
			if err != nil {
				return TxnIntent{}, err
			}
			recordMeta[fixed] = record
			ok = true
		}
	}
	if ok {
		intent.Timestamp = record.Timestamp
		intent.Status = record.Status
	}

	return intent, nil
}

func loadZigTxnParticipantSet(bridge *zbridge.Bridge, prefix, txnID []byte) ([][]byte, error) {
	raw, err := bridge.GetRaw(slices.Concat(prefix, txnID))
	if err != nil {
		if errors.Is(err, zbridge.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return decodeZigTxnParticipantList(raw)
}

func decodeZigTxnParticipantList(raw []byte) ([][]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) < 4 {
		return nil, fmt.Errorf("decoding zig txn participants: invalid payload")
	}
	count := int(binary.LittleEndian.Uint32(raw[:4]))
	result := make([][]byte, 0, count)
	offset := 4
	for i := 0; i < count; i++ {
		if offset+4 > len(raw) {
			return nil, fmt.Errorf("decoding zig txn participants: truncated length")
		}
		entryLen := int(binary.LittleEndian.Uint32(raw[offset : offset+4]))
		offset += 4
		if offset+entryLen > len(raw) {
			return nil, fmt.Errorf("decoding zig txn participants: truncated entry")
		}
		result = append(result, bytes.Clone(raw[offset:offset+entryLen]))
		offset += entryLen
	}
	return result, nil
}

func (db *ZigCoreDB) GetTimestamp(key []byte) (uint64, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return 0, zigUnsupported("GetTimestamp without open bridge")
	}
	return bridge.GetTimestamp(key)
}

func (db *ZigCoreDB) GetEdges(_ context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]indexes.Edge, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("GetEdges without open bridge")
	}
	payloads, err := bridge.GetEdges([]byte(indexName), key, []byte(edgeType), graphDirection(direction))
	if err != nil {
		return nil, err
	}
	result := make([]indexes.Edge, 0, len(payloads))
	for _, payload := range payloads {
		edge, err := decodeEdgePayload(payload)
		if err != nil {
			return nil, err
		}
		result = append(result, edge)
	}
	return result, nil
}

func (db *ZigCoreDB) TraverseEdges(_ context.Context, indexName string, startKey []byte, rules indexes.TraversalRules) ([]*indexes.TraversalResult, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("TraverseEdges without open bridge")
	}
	payloads, err := bridge.TraverseEdges(zbridge.TraversalRequestPayload{
		IndexName:        indexName,
		StartKeyB64:      encodeBase64(startKey),
		EdgeTypes:        rules.EdgeTypes,
		Direction:        graphDirection(rules.Direction),
		MaxDepth:         uint32(rules.MaxDepth),
		MinWeight:        rules.MinWeight,
		MaxWeight:        rules.MaxWeight,
		MaxResults:       uint32(rules.MaxResults),
		DeduplicateNodes: rules.DeduplicateNodes,
		IncludePaths:     rules.IncludePaths,
	})
	if err != nil {
		return nil, err
	}
	return decodeTraversalPayloads(payloads)
}

func (db *ZigCoreDB) GetNeighbors(_ context.Context, indexName string, key []byte, edgeType string, direction indexes.EdgeDirection) ([]*indexes.TraversalResult, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("GetNeighbors without open bridge")
	}
	payloads, err := bridge.GetNeighbors([]byte(indexName), key, []byte(edgeType), graphDirection(direction))
	if err != nil {
		return nil, err
	}
	return decodeTraversalPayloads(payloads)
}

func (db *ZigCoreDB) FindShortestPath(_ context.Context, indexName string, source, target []byte, edgeTypes []string, direction indexes.EdgeDirection, weightMode indexes.PathWeightMode, maxDepth int, minWeight, maxWeight float64) (*indexes.Path, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("FindShortestPath without open bridge")
	}
	payload, err := bridge.FindShortestPath(zbridge.ShortestPathRequestPayload{
		IndexName:  indexName,
		SourceB64:  encodeBase64(source),
		TargetB64:  encodeBase64(target),
		EdgeTypes:  edgeTypes,
		Direction:  graphDirection(direction),
		WeightMode: string(weightMode),
		MaxDepth:   uint32(maxDepth),
		MinWeight:  minWeight,
		MaxWeight:  maxWeight,
	})
	if err != nil || payload == nil {
		return nil, err
	}
	return decodePathPayload(payload)
}

func (db *ZigCoreDB) findKShortestPaths(indexName string, source, target []byte, k int, edgeTypes []string, direction indexes.EdgeDirection, weightMode indexes.PathWeightMode, maxDepth int, minWeight, maxWeight float64) ([]indexes.Path, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("FindKShortestPaths without open bridge")
	}
	payloads, err := bridge.FindKShortestPaths(zbridge.ShortestPathRequestPayload{
		IndexName:  indexName,
		SourceB64:  encodeBase64(source),
		TargetB64:  encodeBase64(target),
		EdgeTypes:  edgeTypes,
		Direction:  graphDirection(direction),
		WeightMode: string(weightMode),
		MaxDepth:   uint32(maxDepth),
		MinWeight:  minWeight,
		MaxWeight:  maxWeight,
		K:          uint32(k),
	})
	if err != nil {
		return nil, err
	}
	return decodePathPayloads(payloads)
}

func zigUnsupported(method string) error {
	return fmt.Errorf("zig coreDB adapter: %s not implemented", method)
}

func cloneRange(in types.Range) types.Range {
	var out types.Range
	if len(in[0]) > 0 {
		out[0] = append([]byte(nil), in[0]...)
	}
	if len(in[1]) > 0 {
		out[1] = append([]byte(nil), in[1]...)
	}
	return out
}

func cloneSplitState(state *SplitState) *SplitState {
	if state == nil {
		return nil
	}
	return SplitState_builder{
		Phase:              state.GetPhase(),
		SplitKey:           append([]byte(nil), state.GetSplitKey()...),
		NewShardId:         state.GetNewShardId(),
		StartedAtUnixNanos: state.GetStartedAtUnixNanos(),
		OriginalRangeEnd:   append([]byte(nil), state.GetOriginalRangeEnd()...),
	}.Build()
}

func mapSyncLevel(level Op_SyncLevel) uint8 {
	switch level {
	case Op_SyncLevelWrite:
		return 0
	case Op_SyncLevelFullText, Op_SyncLevelEmbeddings:
		return 1
	default:
		return 2
	}
}

func (db *ZigCoreDB) reloadMetadataLocked(fallbackRange types.Range) error {
	if db.bridge == nil {
		return zigUnsupported("reloadMetadataLocked without open bridge")
	}

	rangePayload, err := db.bridge.GetRange()
	if err != nil {
		return err
	}
	loadedRange, err := decodeRangePayload(rangePayload)
	if err != nil {
		return err
	}
	if len(loadedRange[0]) == 0 && len(loadedRange[1]) == 0 && (len(fallbackRange[0]) > 0 || len(fallbackRange[1]) > 0) {
		if err := db.bridge.UpdateRange(fallbackRange[0], fallbackRange[1]); err != nil {
			return err
		}
		db.byteRange = cloneRange(fallbackRange)
	} else {
		db.byteRange = loadedRange
	}

	splitState, err := db.bridge.GetSplitState()
	if err != nil {
		return err
	}
	if splitState != nil {
		decoded, err := decodeSplitState(splitState)
		if err != nil {
			return err
		}
		db.splitState = decoded
	} else {
		db.splitState = nil
	}

	db.splitDeltaSeq, err = db.bridge.GetSplitDeltaSeq()
	if err != nil {
		return err
	}
	db.splitDeltaFinal, err = db.bridge.GetSplitDeltaFinalSeq()
	if err != nil {
		return err
	}

	indexPayloads, err := db.bridge.ListIndexes()
	if err != nil {
		return err
	}
	if len(indexPayloads) > 0 {
		loaded := make(map[string]indexes.IndexConfig, len(indexPayloads))
		for _, payload := range indexPayloads {
			cfg, err := decodeIndexConfig(payload)
			if err != nil {
				return err
			}
			loaded[cfg.Name] = cfg
		}
		db.indexes = loaded
	}

	dir, err := db.bridge.GetShadowIndexDir()
	if err != nil && !errors.Is(err, zbridge.ErrNotFound) {
		return err
	}
	db.shadowIndexDir = dir
	return nil
}

func decodeRangePayload(payload zbridge.RangePayload) (types.Range, error) {
	start, err := zbridge.DecodeBase64(payload.StartB64)
	if err != nil {
		return types.Range{}, err
	}
	end, err := zbridge.DecodeBase64(payload.EndB64)
	if err != nil {
		return types.Range{}, err
	}
	return types.Range{start, end}, nil
}

func encodeSplitState(state *SplitState) (zbridge.SplitStatePayload, error) {
	return zbridge.SplitStatePayload{
		Phase:               uint8(state.GetPhase()),
		SplitKeyB64:         encodeBase64(state.GetSplitKey()),
		NewShardID:          state.GetNewShardId(),
		StartedAt:           uint64(state.GetStartedAtUnixNanos()),
		OriginalRangeEndB64: encodeBase64(state.GetOriginalRangeEnd()),
	}, nil
}

func decodeSplitState(payload *zbridge.SplitStatePayload) (*SplitState, error) {
	splitKey, err := zbridge.DecodeBase64(payload.SplitKeyB64)
	if err != nil {
		return nil, err
	}
	rangeEnd, err := zbridge.DecodeBase64(payload.OriginalRangeEndB64)
	if err != nil {
		return nil, err
	}
	return SplitState_builder{
		Phase:              SplitState_Phase(payload.Phase),
		SplitKey:           splitKey,
		NewShardId:         payload.NewShardID,
		StartedAtUnixNanos: int64(payload.StartedAt),
		OriginalRangeEnd:   rangeEnd,
	}.Build(), nil
}

func decodeSplitDeltaEntry(payload zbridge.SplitDeltaEntryPayload) (*SplitDeltaEntry, error) {
	writes := make([]*Write, 0, len(payload.Writes))
	for _, write := range payload.Writes {
		key, err := zbridge.DecodeBase64(write.KeyB64)
		if err != nil {
			return nil, err
		}
		value, err := zbridge.DecodeBase64(write.ValueB64)
		if err != nil {
			return nil, err
		}
		writes = append(writes, Write_builder{
			Key:   key,
			Value: value,
		}.Build())
	}
	deletes := make([][]byte, 0, len(payload.DeletesB64))
	for _, encoded := range payload.DeletesB64 {
		key, err := zbridge.DecodeBase64(encoded)
		if err != nil {
			return nil, err
		}
		deletes = append(deletes, key)
	}
	return SplitDeltaEntry_builder{
		Sequence:  payload.Sequence,
		Timestamp: payload.Timestamp,
		Writes:    writes,
		Deletes:   deletes,
	}.Build(), nil
}

func decodeIndexConfig(payload zbridge.IndexConfigPayload) (indexes.IndexConfig, error) {
	var cfg indexes.IndexConfig
	typeEnvelope := map[string]any{
		"name": payload.Name,
		"type": mapIndexType(payload.Kind),
	}
	if len(payload.ConfigJSON) > 0 {
		var specific map[string]any
		if err := json.Unmarshal([]byte(payload.ConfigJSON), &specific); err != nil {
			return indexes.IndexConfig{}, err
		}
		for k, v := range specific {
			typeEnvelope[k] = v
		}
	}
	raw, err := json.Marshal(typeEnvelope)
	if err != nil {
		return indexes.IndexConfig{}, err
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return indexes.IndexConfig{}, err
	}
	return cfg, nil
}

func mapIndexType(kind string) indexes.IndexType {
	switch kind {
	case "full_text":
		return indexes.IndexTypeFullText
	case "graph":
		return indexes.IndexTypeGraph
	default:
		return indexes.IndexTypeEmbeddings
	}
}

func encodeBase64(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return zbridge.EncodeBase64(b)
}

func encodeZigSchemaPayload(tableSchema *schema.TableSchema) ([]byte, error) {
	type zigSchemaFieldMapping struct {
		FieldType    string `json:"field_type"`
		DoIndex      bool   `json:"do_index"`
		Store        bool   `json:"store"`
		DocValues    bool   `json:"doc_values"`
		IncludeInAll bool   `json:"include_in_all"`
		Analyzer     string `json:"analyzer"`
	}
	type zigSchemaDynamicTemplate struct {
		Name         string                `json:"name"`
		MatchPattern string                `json:"match_pattern,omitempty"`
		PathMatch    string                `json:"path_match,omitempty"`
		Mapping      zigSchemaFieldMapping `json:"mapping"`
	}
	type zigSchemaPayload struct {
		Version          uint32                     `json:"version"`
		DefaultType      string                     `json:"default_type"`
		TtlDurationNs    uint64                     `json:"ttl_duration_ns"`
		TtlField         string                     `json:"ttl_field"`
		EnforceTypes     bool                       `json:"enforce_types"`
		DynamicTemplates []zigSchemaDynamicTemplate `json:"dynamic_templates,omitempty"`
	}

	if tableSchema == nil {
		return nil, fmt.Errorf("nil schema")
	}

	payload := zigSchemaPayload{
		Version:      tableSchema.Version,
		DefaultType:  tableSchema.DefaultType,
		TtlField:     tableSchema.TtlField,
		EnforceTypes: tableSchema.EnforceTypes,
	}
	if payload.DefaultType == "" {
		payload.DefaultType = "_default"
	}
	if payload.TtlField == "" {
		payload.TtlField = "_timestamp"
	}
	if tableSchema.TtlDuration != "" {
		duration, err := time.ParseDuration(tableSchema.TtlDuration)
		if err != nil {
			return nil, fmt.Errorf("parsing schema ttl_duration: %w", err)
		}
		payload.TtlDurationNs = uint64(duration)
	}
	for _, tmpl := range tableSchema.DynamicTemplates {
		if tmpl.MatchMappingType != "" || tmpl.Unmatch != "" || tmpl.PathUnmatch != "" {
			return nil, zigUnsupported("UpdateSchema dynamic template fields unsupported in zigdb")
		}
		fieldType := string(tmpl.Mapping.Type)
		if fieldType == "" {
			fieldType = string(schema.AntflyTypeText)
		}
		analyzer := tmpl.Mapping.Analyzer
		if analyzer == "" {
			analyzer = "standard"
		}
		payload.DynamicTemplates = append(payload.DynamicTemplates, zigSchemaDynamicTemplate{
			Name:         tmpl.Name,
			MatchPattern: tmpl.Match,
			PathMatch:    tmpl.PathMatch,
			Mapping: zigSchemaFieldMapping{
				FieldType:    fieldType,
				DoIndex:      tmpl.Mapping.Index,
				Store:        tmpl.Mapping.Store,
				DocValues:    tmpl.Mapping.DocValues,
				IncludeInAll: tmpl.Mapping.IncludeInAll,
				Analyzer:     analyzer,
			},
		})
	}
	return json.Marshal(payload)
}

func (db *ZigCoreDB) buildTextSearchRequest(req *bleve.SearchRequest) (zbridge.SearchRequestPayload, error) {
	out := zbridge.SearchRequestPayload{
		Mode:          "full_text",
		Limit:         uint32(req.Size),
		Offset:        uint32(req.From),
		IncludeStored: true,
	}
	if req.Query == nil {
		out.TextQueryType = "match_all"
		return out, nil
	}
	normalized, err := normalizeBackendTextQuery(req.Query, db.textAnalysisConfig(), db.textIndexMapping())
	if err != nil {
		return zbridge.SearchRequestPayload{}, err
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return zbridge.SearchRequestPayload{}, fmt.Errorf("marshalling text query: %w", err)
	}
	out.TextQueryJSON = string(raw)
	return out, nil
}

func normalizeBackendTextQuery(q blevequery.Query, analysisConfig *schema.AnalysisConfig, indexMapping *blevemapping.IndexMappingImpl) (map[string]any, error) {
	switch typed := q.(type) {
	case *blevequery.MatchAllQuery:
		return map[string]any{"match_all": map[string]any{}}, nil
	case *blevequery.MatchNoneQuery:
		return map[string]any{"match_none": map[string]any{}}, nil
	case *blevequery.PhraseQuery:
		maxEdits, autoFuzzy, err := extractNormalizedBleveFuzziness(typed)
		if err != nil {
			return nil, zigUnsupported("Search bleve query type")
		}
		terms := make([]any, 0, len(typed.Terms))
		for _, term := range typed.Terms {
			terms = append(terms, term)
		}
		payload := map[string]any{
			"field": typed.Field(),
			"terms": terms,
		}
		if maxEdits > 0 {
			payload["max_edits"] = maxEdits
		}
		if autoFuzzy {
			payload["auto_fuzzy"] = true
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"phrase": payload}, nil
	case *blevequery.MultiPhraseQuery:
		maxEdits, autoFuzzy, err := extractNormalizedBleveFuzziness(typed)
		if err != nil {
			return nil, zigUnsupported("Search bleve query type")
		}
		terms := make([]any, 0, len(typed.Terms))
		for _, position := range typed.Terms {
			alternatives := make([]any, 0, len(position))
			for _, term := range position {
				alternatives = append(alternatives, term)
			}
			terms = append(terms, alternatives)
		}
		payload := map[string]any{
			"field": typed.Field(),
			"terms": terms,
		}
		if maxEdits > 0 {
			payload["max_edits"] = maxEdits
		}
		if autoFuzzy {
			payload["auto_fuzzy"] = true
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"multi_phrase": payload}, nil
	case *blevequery.MatchQuery:
		payload := map[string]any{
			"field": typed.Field(),
			"text":  typed.Match,
		}
		if analyzer := normalizedTextQueryAnalyzer(typed.Field(), typed.Analyzer, analysisConfig); analyzer != "" {
			payload["analyzer"] = analyzer
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"match": payload}, nil
	case *blevequery.MatchPhraseQuery:
		maxEdits, autoFuzzy, err := extractNormalizedBleveFuzziness(typed)
		if err != nil {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field": typed.Field(),
			"text":  typed.MatchPhrase,
		}
		if analyzer := normalizedTextQueryAnalyzer(typed.Field(), typed.Analyzer, analysisConfig); analyzer != "" {
			payload["analyzer"] = analyzer
		}
		if maxEdits > 0 {
			payload["max_edits"] = maxEdits
		}
		if autoFuzzy {
			payload["auto_fuzzy"] = true
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"match_phrase": payload}, nil
	case *blevequery.PrefixQuery:
		payload := map[string]any{
			"field":  typed.Field(),
			"prefix": typed.Prefix,
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"prefix": payload}, nil
	case *blevequery.TermQuery:
		payload := map[string]any{
			"field": typed.Field(),
			"term":  typed.Term,
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"term": payload}, nil
	case *blevequery.FuzzyQuery:
		maxEdits, autoFuzzy, err := extractNormalizedBleveFuzziness(typed)
		if err != nil {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":         typed.Field(),
			"term":          typed.Term,
			"prefix_length": typed.Prefix,
		}
		if maxEdits > 0 {
			payload["max_edits"] = maxEdits
		}
		if autoFuzzy {
			payload["auto_fuzzy"] = true
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"fuzzy": payload}, nil
	case *blevequery.NumericRangeQuery:
		if typed.Min == nil && typed.Max == nil {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":         typed.Field(),
			"inclusive_min": true,
			"inclusive_max": false,
		}
		if typed.Min != nil {
			payload["min"] = *typed.Min
		}
		if typed.Max != nil {
			payload["max"] = *typed.Max
		}
		if typed.InclusiveMin != nil {
			payload["inclusive_min"] = *typed.InclusiveMin
		}
		if typed.InclusiveMax != nil {
			payload["inclusive_max"] = *typed.InclusiveMax
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"numeric_range": payload}, nil
	case *blevequery.DateRangeQuery:
		if typed.Start.Time.IsZero() && typed.End.Time.IsZero() {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":           typed.Field(),
			"inclusive_start": true,
			"inclusive_end":   false,
		}
		if !typed.Start.Time.IsZero() {
			payload["start_ns"] = typed.Start.Time.UnixNano()
		}
		if !typed.End.Time.IsZero() {
			payload["end_ns"] = typed.End.Time.UnixNano()
		}
		if typed.InclusiveStart != nil {
			payload["inclusive_start"] = *typed.InclusiveStart
		}
		if typed.InclusiveEnd != nil {
			payload["inclusive_end"] = *typed.InclusiveEnd
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"date_range": payload}, nil
	case *blevequery.DateRangeStringQuery:
		if typed.Start == "" && typed.End == "" {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":           typed.Field(),
			"inclusive_start": true,
			"inclusive_end":   false,
		}
		if typed.Start != "" {
			start, err := parseNormalizedDateString(typed.Start, typed.DateTimeParser, analysisConfig, indexMapping)
			if err != nil {
				return nil, zigUnsupported("Search bleve query type")
			}
			payload["start_ns"] = start.UnixNano()
		}
		if typed.End != "" {
			end, err := parseNormalizedDateString(typed.End, typed.DateTimeParser, analysisConfig, indexMapping)
			if err != nil {
				return nil, zigUnsupported("Search bleve query type")
			}
			payload["end_ns"] = end.UnixNano()
		}
		if typed.InclusiveStart != nil {
			payload["inclusive_start"] = *typed.InclusiveStart
		}
		if typed.InclusiveEnd != nil {
			payload["inclusive_end"] = *typed.InclusiveEnd
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"date_range": payload}, nil
	case *blevequery.DocIDQuery:
		if len(typed.IDs) == 0 {
			return nil, zigUnsupported("Search bleve query type")
		}
		items := make([]any, 0, len(typed.IDs))
		for _, id := range typed.IDs {
			items = append(items, id)
		}
		payload := map[string]any{"ids": items}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"doc_id": payload}, nil
	case *blevequery.BoolFieldQuery:
		payload := map[string]any{
			"field": typed.Field(),
			"value": typed.Bool,
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"bool_field": payload}, nil
	case *blevequery.GeoDistanceQuery:
		if len(typed.Location) != 2 {
			return nil, zigUnsupported("Search bleve query type")
		}
		radiusMeters, err := blevegeo.ParseDistance(typed.Distance)
		if err != nil {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":         typed.Field(),
			"lon":           typed.Location[0],
			"lat":           typed.Location[1],
			"radius_meters": radiusMeters,
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"geo_distance": payload}, nil
	case *blevequery.GeoBoundingBoxQuery:
		if len(typed.TopLeft) != 2 || len(typed.BottomRight) != 2 {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":   typed.Field(),
			"min_lat": typed.BottomRight[1],
			"min_lon": typed.TopLeft[0],
			"max_lat": typed.TopLeft[1],
			"max_lon": typed.BottomRight[0],
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"geo_bbox": payload}, nil
	case *blevequery.GeoBoundingPolygonQuery:
		if len(typed.Points) < 3 {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":    typed.Field(),
			"relation": "intersects",
			"polygon":  normalizeGeoPolygonPoints(typed.Points),
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"geo_shape": payload}, nil
	case *blevequery.GeoShapeQuery:
		return normalizeGeoShapeQuery(typed)
	case *blevequery.WildcardQuery:
		payload := map[string]any{
			"field":   typed.Field(),
			"pattern": typed.Wildcard,
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"wildcard": payload}, nil
	case *blevequery.RegexpQuery:
		payload := map[string]any{
			"field":   typed.Field(),
			"pattern": typed.Regexp,
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"regexp": payload}, nil
	case *blevequery.TermRangeQuery:
		if typed.Min == "" && typed.Max == "" {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field":         typed.Field(),
			"inclusive_min": true,
			"inclusive_max": false,
		}
		if typed.Min != "" {
			payload["min"] = typed.Min
		}
		if typed.Max != "" {
			payload["max"] = typed.Max
		}
		if typed.InclusiveMin != nil {
			payload["inclusive_min"] = *typed.InclusiveMin
		}
		if typed.InclusiveMax != nil {
			payload["inclusive_max"] = *typed.InclusiveMax
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"term_range": payload}, nil
	case *blevequery.IPRangeQuery:
		if typed.CIDR == "" {
			return nil, zigUnsupported("Search bleve query type")
		}
		payload := map[string]any{
			"field": typed.Field(),
			"cidr":  typed.CIDR,
		}
		appendNormalizedBoost(payload, typed.Boost())
		return map[string]any{"ip_range": payload}, nil
	case *blevequery.DisjunctionQuery:
		if len(typed.Disjuncts) == 0 {
			return nil, zigUnsupported("Search bleve query type")
		}
		items := make([]any, 0, len(typed.Disjuncts))
		for _, clause := range typed.Disjuncts {
			item, err := normalizeBackendTextQuery(clause, analysisConfig, indexMapping)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return map[string]any{"disjuncts": items}, nil
	case *blevequery.ConjunctionQuery:
		if len(typed.Conjuncts) == 0 {
			return nil, zigUnsupported("Search bleve query type")
		}
		items := make([]any, 0, len(typed.Conjuncts))
		for _, clause := range typed.Conjuncts {
			item, err := normalizeBackendTextQuery(clause, analysisConfig, indexMapping)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return map[string]any{"conjuncts": items}, nil
	case *blevequery.BooleanQuery:
		boolPayload := make(map[string]any)
		if typed.Must != nil {
			items, err := normalizeBackendTextQueryArray(typed.Must, analysisConfig, indexMapping, true)
			if err != nil {
				return nil, err
			}
			boolPayload["must"] = items
		}
		if typed.Should != nil {
			items, err := normalizeBackendTextQueryArray(typed.Should, analysisConfig, indexMapping, false)
			if err != nil {
				return nil, err
			}
			boolPayload["should"] = items
		}
		if typed.MustNot != nil {
			items, err := normalizeBackendTextQueryArray(typed.MustNot, analysisConfig, indexMapping, false)
			if err != nil {
				return nil, err
			}
			boolPayload["must_not"] = items
		}
		if typed.Filter != nil {
			items, err := normalizeBackendTextQueryArray(typed.Filter, analysisConfig, indexMapping, true)
			if err != nil {
				return nil, err
			}
			if existing, ok := boolPayload["must"].([]any); ok {
				boolPayload["must"] = append(existing, items...)
			} else {
				boolPayload["must"] = items
			}
		}
		if len(boolPayload) == 0 {
			return nil, zigUnsupported("Search bleve query type")
		}
		appendNormalizedBoost(boolPayload, typed.Boost())
		return map[string]any{"bool": boolPayload}, nil
	case *blevequery.QueryStringQuery:
		parsed, err := typed.Parse()
		if err != nil {
			return nil, zigUnsupported("Search bleve query type")
		}
		return normalizeBackendTextQuery(parsed, analysisConfig, indexMapping)
	default:
		return nil, zigUnsupported("Search bleve query type")
	}
}

func normalizedTextQueryAnalyzer(field, explicit string, analysisConfig *schema.AnalysisConfig) string {
	if explicit != "" {
		return explicit
	}
	if analysisConfig == nil || field == "" {
		return ""
	}
	return analysisConfig.FieldAnalyzers[field]
}

func extractNormalizedBleveFuzziness(q any) (int, bool, error) {
	raw, err := json.Marshal(q)
	if err != nil {
		return 0, false, err
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, false, err
	}
	value, ok := payload["fuzziness"]
	if !ok {
		return 0, false, nil
	}
	switch typed := value.(type) {
	case float64:
		return int(typed), false, nil
	case string:
		if typed == "auto" {
			return 0, true, nil
		}
		return 0, false, fmt.Errorf("unsupported fuzziness value %q", typed)
	default:
		return 0, false, fmt.Errorf("unsupported fuzziness type %T", value)
	}
}

func appendNormalizedBoost(payload map[string]any, boost float64) {
	if boost != 1.0 {
		payload["boost"] = boost
	}
}

func (db *ZigCoreDB) textIndexMapping() *blevemapping.IndexMappingImpl {
	if db == nil || db.schema == nil {
		return nil
	}
	indexMapping, ok := schema.NewIndexMapFromSchema(db.schema).(*blevemapping.IndexMappingImpl)
	if !ok {
		return nil
	}
	return indexMapping
}

func (db *ZigCoreDB) textAnalysisConfig() *schema.AnalysisConfig {
	if db == nil || db.schema == nil {
		return nil
	}
	db.schema.EnsureAnalysisConfig()
	return db.schema.AnalysisConfig
}

func analysisComponentConfigMap(component schema.AnalysisComponentConfig) map[string]any {
	config := make(map[string]any, len(component.Config)+1)
	config["type"] = component.Type
	for k, v := range component.Config {
		config[k] = v
	}
	return config
}

func parseDateTimeWithAnalysisConfig(value string, parser string, cfg *schema.AnalysisConfig) (time.Time, bool, error) {
	if cfg == nil {
		return time.Time{}, false, nil
	}
	name := parser
	if name == "" {
		name = cfg.DefaultDateTimeParser
	}
	if name == "" {
		return time.Time{}, false, nil
	}
	if component, ok := cfg.DateTimeParsers[name]; ok {
		cache := bleveregistry.NewCache()
		dateTimeParser, err := cache.DefineDateTimeParser(name, analysisComponentConfigMap(component))
		if err != nil {
			return time.Time{}, true, err
		}
		parsed, _, err := dateTimeParser.ParseDateTime(value)
		return parsed, true, err
	}
	return time.Time{}, false, nil
}

func parseNormalizedDateString(value string, parser string, analysisConfig *schema.AnalysisConfig, indexMapping *blevemapping.IndexMappingImpl) (time.Time, error) {
	if parsed, handled, err := parseDateTimeWithAnalysisConfig(value, parser, analysisConfig); handled {
		return parsed, err
	}
	if indexMapping != nil {
		dateTimeParser := indexMapping.DateTimeParserNamed(parser)
		if dateTimeParser != nil {
			parsed, _, err := dateTimeParser.ParseDateTime(value)
			if err != nil {
				return time.Time{}, err
			}
			return parsed, nil
		}
	}
	name := parser
	if name == "" {
		name = blevequery.QueryDateTimeParser
	}
	dateTimeParser, err := bleve.Config.Cache.DateTimeParserNamed(name)
	if err != nil {
		return time.Time{}, err
	}
	parsed, _, err := dateTimeParser.ParseDateTime(value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func normalizeBackendTextQueryArray(q blevequery.Query, analysisConfig *schema.AnalysisConfig, indexMapping *blevemapping.IndexMappingImpl, flattenConjunction bool) ([]any, error) {
	switch typed := q.(type) {
	case *blevequery.ConjunctionQuery:
		if flattenConjunction {
			if len(typed.Conjuncts) == 0 {
				return nil, zigUnsupported("Search bleve query type")
			}
			items := make([]any, 0, len(typed.Conjuncts))
			for _, clause := range typed.Conjuncts {
				item, err := normalizeBackendTextQuery(clause, analysisConfig, indexMapping)
				if err != nil {
					return nil, err
				}
				items = append(items, item)
			}
			return items, nil
		}
	case *blevequery.DisjunctionQuery:
		if !flattenConjunction {
			if len(typed.Disjuncts) == 0 {
				return nil, zigUnsupported("Search bleve query type")
			}
			items := make([]any, 0, len(typed.Disjuncts))
			for _, clause := range typed.Disjuncts {
				item, err := normalizeBackendTextQuery(clause, analysisConfig, indexMapping)
				if err != nil {
					return nil, err
				}
				items = append(items, item)
			}
			return items, nil
		}
	}
	item, err := normalizeBackendTextQuery(q, analysisConfig, indexMapping)
	if err != nil {
		return nil, err
	}
	return []any{item}, nil
}

func normalizeGeoPolygonPoints(points []blevegeo.Point) []any {
	items := make([]any, 0, len(points))
	for _, point := range points {
		items = append(items, map[string]any{
			"lon": point.Lon,
			"lat": point.Lat,
		})
	}
	if len(points) > 0 && (points[0].Lon != points[len(points)-1].Lon || points[0].Lat != points[len(points)-1].Lat) {
		items = append(items, map[string]any{
			"lon": points[0].Lon,
			"lat": points[0].Lat,
		})
	}
	return items
}

func normalizeGeoShapeQuery(q *blevequery.GeoShapeQuery) (map[string]any, error) {
	relation := q.Geometry.Relation
	if relation == "" {
		relation = "intersects"
	}
	switch relation {
	case "intersects", "within", "contains":
	default:
		return nil, zigUnsupported("Search bleve query type")
	}

	raw, err := q.Geometry.Shape.Value()
	if err != nil {
		return nil, zigUnsupported("Search bleve query type")
	}
	var parsed struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, zigUnsupported("Search bleve query type")
	}

	payload := map[string]any{
		"field":    q.Field(),
		"relation": relation,
	}
	switch strings.ToLower(parsed.Type) {
	case "polygon":
		var coordinates [][][]float64
		if err := json.Unmarshal(parsed.Coordinates, &coordinates); err != nil || len(coordinates) == 0 {
			return nil, zigUnsupported("Search bleve query type")
		}
		polygon, err := normalizeGeoJSONPolygon(coordinates[0])
		if err != nil {
			return nil, err
		}
		payload["polygon"] = polygon
	case "multipolygon":
		var coordinates [][][][]float64
		if err := json.Unmarshal(parsed.Coordinates, &coordinates); err != nil || len(coordinates) == 0 {
			return nil, zigUnsupported("Search bleve query type")
		}
		polygons := make([]any, 0, len(coordinates))
		for _, polygonCoords := range coordinates {
			if len(polygonCoords) == 0 {
				return nil, zigUnsupported("Search bleve query type")
			}
			polygon, err := normalizeGeoJSONPolygon(polygonCoords[0])
			if err != nil {
				return nil, err
			}
			polygons = append(polygons, polygon)
		}
		payload["polygons"] = polygons
	default:
		return nil, zigUnsupported("Search bleve query type")
	}
	appendNormalizedBoost(payload, q.Boost())

	return map[string]any{"geo_shape": payload}, nil
}

func normalizeGeoJSONPolygon(coords [][]float64) ([]any, error) {
	if len(coords) < 3 {
		return nil, zigUnsupported("Search bleve query type")
	}
	points := make([]any, 0, len(coords)+1)
	for _, coord := range coords {
		if len(coord) != 2 {
			return nil, zigUnsupported("Search bleve query type")
		}
		points = append(points, map[string]any{
			"lon": coord[0],
			"lat": coord[1],
		})
	}
	first := coords[0]
	last := coords[len(coords)-1]
	if first[0] != last[0] || first[1] != last[1] {
		points = append(points, map[string]any{
			"lon": first[0],
			"lat": first[1],
		})
	}
	return points, nil
}

func (db *ZigCoreDB) buildBackendGraphQueries(req *indexes.RemoteIndexSearchRequest) ([]zbridge.SearchGraphQueryPayload, bool, error) {
	if req == nil || len(req.GraphSearches) == 0 {
		return nil, false, nil
	}
	if req.BleveSearchRequest == nil || len(req.VectorSearches) > 0 || len(req.SparseSearches) > 0 || req.MergeConfig != nil {
		return nil, false, nil
	}
	payloads := make([]zbridge.SearchGraphQueryPayload, 0, len(req.GraphSearches))
	for name, query := range req.GraphSearches {
		payload, ok, err := toBackendGraphQuery(name, query)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		payloads = append(payloads, payload)
	}
	return payloads, true, nil
}

func toBackendGraphQuery(name string, query *indexes.GraphQuery) (zbridge.SearchGraphQueryPayload, bool, error) {
	if query == nil {
		return zbridge.SearchGraphQueryPayload{}, false, nil
	}
	switch query.Type {
	case indexes.GraphQueryTypeNeighbors, indexes.GraphQueryTypeTraverse, indexes.GraphQueryTypeShortestPath, indexes.GraphQueryTypeKShortestPaths:
	default:
		return zbridge.SearchGraphQueryPayload{}, false, nil
	}
	startNodes, ok, err := toBackendGraphNodeSelector(query.StartNodes)
	if err != nil || !ok {
		return zbridge.SearchGraphQueryPayload{}, false, err
	}
	var targetNodes *zbridge.SearchGraphNodeSelectorPayload
	if hasGraphSelector(query.TargetNodes) {
		payload, ok, err := toBackendGraphNodeSelector(query.TargetNodes)
		if err != nil || !ok {
			return zbridge.SearchGraphQueryPayload{}, false, err
		}
		targetNodes = &payload
	}
	return zbridge.SearchGraphQueryPayload{
		Name:         name,
		Type:         string(query.Type),
		IndexName:    query.IndexName,
		StartNodes:   startNodes,
		TargetNodes:  targetNodes,
		EdgeTypes:    append([]string(nil), query.Params.EdgeTypes...),
		Direction:    string(query.Params.Direction),
		MaxDepth:     uint32(query.Params.MaxDepth),
		MaxResults:   uint32(query.Params.MaxResults),
		MinWeight:    query.Params.MinWeight,
		MaxWeight:    query.Params.MaxWeight,
		Deduplicate:  query.Params.DeduplicateNodes,
		IncludePaths: query.Params.IncludePaths,
		WeightMode:   string(query.Params.WeightMode),
		K:            uint32(query.Params.K),
	}, true, nil
}

func toBackendGraphNodeSelector(selector indexes.GraphNodeSelector) (zbridge.SearchGraphNodeSelectorPayload, bool, error) {
	if len(selector.NodeFilter.FilterPrefix) > 0 || len(selector.NodeFilter.FilterQuery) > 0 {
		return zbridge.SearchGraphNodeSelectorPayload{}, false, nil
	}
	if len(selector.Keys) > 0 {
		return zbridge.SearchGraphNodeSelectorPayload{
			Keys: append([]string(nil), selector.Keys...),
		}, true, nil
	}
	if selector.ResultRef != "" {
		if selector.ResultRef != "$full_text_results" && !strings.HasPrefix(selector.ResultRef, "$graph_results.") {
			return zbridge.SearchGraphNodeSelectorPayload{}, false, nil
		}
		return zbridge.SearchGraphNodeSelectorPayload{
			ResultRef: selector.ResultRef,
			Limit:     uint32(selector.Limit),
		}, true, nil
	}
	return zbridge.SearchGraphNodeSelectorPayload{}, false, nil
}

func hasGraphSelector(selector indexes.GraphNodeSelector) bool {
	return len(selector.Keys) > 0 || selector.ResultRef != ""
}

func decodeBleveSearchResult(payload *zbridge.SearchResultPayload, req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	hits := make(bleveSearch.DocumentMatchCollection, 0, len(payload.Hits))
	maxScore := 0.0
	for _, hit := range payload.Hits {
		id := hit.IDRaw
		if len(id) == 0 {
			var err error
			id, err = zbridge.DecodeBase64(hit.IDB64)
			if err != nil {
				return nil, err
			}
		}
		score := 0.0
		if hit.Score != nil {
			score = float64(*hit.Score)
		}
		fields, err := decodeStoredFields(hit.StoredJSON)
		if err != nil {
			return nil, err
		}
		hits = append(hits, &bleveSearch.DocumentMatch{
			ID:     string(id),
			Score:  score,
			Fields: fields,
		})
		if score > maxScore {
			maxScore = score
		}
	}
	return &bleve.SearchResult{
		Status: &bleve.SearchStatus{
			Total:      1,
			Successful: 1,
			Failed:     0,
		},
		Request:  req,
		Hits:     hits,
		Total:    uint64(payload.TotalHits),
		MaxScore: maxScore,
		Took:     0,
	}, nil
}

func decodeVectorSearchResult(indexName string, payload *zbridge.SearchResultPayload) (*vectorindex.SearchResult, error) {
	hits := make([]*vectorindex.SearchHit, 0, len(payload.Hits))
	for _, hit := range payload.Hits {
		id := hit.IDRaw
		if len(id) == 0 {
			var err error
			id, err = zbridge.DecodeBase64(hit.IDB64)
			if err != nil {
				return nil, err
			}
		}
		score := float32(0)
		if hit.Score != nil {
			score = *hit.Score
		}
		fields, err := decodeStoredFields(hit.StoredJSON)
		if err != nil {
			return nil, err
		}
		hits = append(hits, &vectorindex.SearchHit{
			Index:  indexName,
			ID:     string(id),
			Score:  score,
			Fields: fields,
		})
	}
	return &vectorindex.SearchResult{
		Hits:  hits,
		Total: uint64(payload.TotalHits),
		Status: &vectorindex.SearchStatus{
			Total:      uint64(payload.TotalHits),
			Successful: len(hits),
			Failed:     0,
		},
	}, nil
}

func decodeStoredFields(raw string) (map[string]any, error) {
	if raw == "" {
		return nil, nil
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func executeNarrowedTextSearch(
	db *ZigCoreDB,
	bridge *zbridge.Bridge,
	req *bleve.SearchRequest,
	opts indexes.FullTextPagingOptions,
	sortFields []supportedSortField,
	fallbackLimit int,
	filterQuery blevequery.Query,
	filterPrefix []byte,
	aggregations []zbridge.SearchAggregationRequestPayload,
) (*bleve.SearchResult, []zbridge.SearchAggregationResultPayload, error) {
	if req == nil {
		return nil, nil, nil
	}
	requiresCompleteFetch := req.Size == 0 && (filterQuery != nil || len(filterPrefix) > 0)
	if len(sortFields) == 0 && len(opts.SearchAfter) == 0 && len(opts.SearchBefore) == 0 && !requiresCompleteFetch {
		textReq := *req
		applySimpleBlevePaging(&textReq, opts, fallbackLimit)
		searchReq, err := db.buildTextSearchRequest(&textReq)
		if err != nil {
			return nil, nil, err
		}
		searchReq.Aggregations = aggregations
		payload, err := bridge.Search(searchReq)
		if err != nil {
			return nil, nil, err
		}
		bleveResult, err := decodeBleveSearchResult(payload, req)
		if err != nil {
			return nil, nil, err
		}
		if filterQuery != nil {
			if err := applyBleveFilterQuery(filterQuery, bleveResult); err != nil {
				return nil, nil, err
			}
		}
		if len(filterPrefix) > 0 {
			applyBleveFilterPrefix(filterPrefix, bleveResult)
		}
		return bleveResult, payload.Aggregations, nil
	}

	bleveResult, aggResults, err := fetchCompleteTextSearchResult(db, bridge, req, filterQuery, filterPrefix, aggregations)
	if err != nil {
		return nil, nil, err
	}
	if len(sortFields) > 0 {
		sortPlan, err := buildBleveSortPlan(bleveResult.Hits, sortFields)
		if err != nil {
			return nil, nil, err
		}
		sortBleveHits(bleveResult.Hits, sortFields, sortPlan)
		if err := applySortedBlevePaging(bleveResult, opts, sortFields, sortPlan, fallbackLimit); err != nil {
			return nil, nil, err
		}
		return bleveResult, aggResults, nil
	}
	applyDefaultBleveCursorPaging(bleveResult, opts, fallbackLimit)
	return bleveResult, aggResults, nil
}

type supportedSortField struct {
	field string
	desc  bool
}

const bleveCompositeSortSentinel = "\U0010ffff\U0010ffff\U0010ffff"

type bleveSortFieldMode struct {
	synthetic bool
}

type bleveSortPlan struct {
	modes  []bleveSortFieldMode
	values map[*bleveSearch.DocumentMatch][]any
	tokens map[*bleveSearch.DocumentMatch][]string
}

func parseSupportedSortFields(fields []indexes.SortField) ([]supportedSortField, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	out := make([]supportedSortField, 0, len(fields))
	for _, field := range fields {
		if field.Field == "" {
			return nil, zigUnsupported("Search empty sort field")
		}
		desc := field.Desc != nil && *field.Desc
		out = append(out, supportedSortField{field: field.Field, desc: desc})
	}
	return out, nil
}

func fetchCompleteTextSearchResult(
	db *ZigCoreDB,
	bridge *zbridge.Bridge,
	req *bleve.SearchRequest,
	filterQuery blevequery.Query,
	filterPrefix []byte,
	aggregations []zbridge.SearchAggregationRequestPayload,
) (*bleve.SearchResult, []zbridge.SearchAggregationResultPayload, error) {
	fetchLimit := req.Size
	if fetchLimit <= 0 {
		fetchLimit = 32
	}
	if fetchLimit < 32 {
		fetchLimit = 32
	}

	for {
		textReq := *req
		textReq.From = 0
		textReq.Size = fetchLimit
		searchReq, err := db.buildTextSearchRequest(&textReq)
		if err != nil {
			return nil, nil, err
		}
		searchReq.Aggregations = aggregations
		payload, err := bridge.Search(searchReq)
		if err != nil {
			return nil, nil, err
		}
		bleveResult, err := decodeBleveSearchResult(payload, req)
		if err != nil {
			return nil, nil, err
		}
		if filterQuery != nil {
			if err := applyBleveFilterQuery(filterQuery, bleveResult); err != nil {
				return nil, nil, err
			}
		}
		if len(filterPrefix) > 0 {
			applyBleveFilterPrefix(filterPrefix, bleveResult)
		}
		if len(bleveResult.Hits) >= int(bleveResult.Total) || fetchLimit >= int(bleveResult.Total) {
			return bleveResult, payload.Aggregations, nil
		}
		fetchLimit *= 2
	}
}

func executeBackendGraphQueries(
	bridge *zbridge.Bridge,
	bleveResult *bleve.SearchResult,
	graphQueries []zbridge.SearchGraphQueryPayload,
) ([]zbridge.SearchGraphResultPayload, error) {
	if len(graphQueries) == 0 {
		return nil, nil
	}
	namedSet := zbridge.NamedGraphInputSetPayload{
		Name:      "$full_text_results",
		HitIDsB64: make([]string, 0, len(bleveResult.Hits)),
		TotalHits: uint32(len(bleveResult.Hits)),
	}
	for _, hit := range bleveResult.Hits {
		namedSet.HitIDsB64 = append(namedSet.HitIDsB64, zbridge.EncodeBase64([]byte(hit.ID)))
	}
	return bridge.ExecuteGraphQueries(zbridge.ExecuteGraphQueriesRequestPayload{
		GraphQueries:  graphQueries,
		NamedSets:     []zbridge.NamedGraphInputSetPayload{namedSet},
		Limit:         uint32(len(bleveResult.Hits)),
		Offset:        0,
		IncludeStored: true,
	})
}

func executeBackendGraphAggregations(
	ctx context.Context,
	db *ZigCoreDB,
	bridge *zbridge.Bridge,
	queries map[string]*indexes.GraphQuery,
	results map[string]*indexes.GraphQueryResult,
	aggregations []zbridge.SearchAggregationRequestPayload,
	filterPrefix []byte,
) (bleveSearch.AggregationResults, error) {
	if len(queries) != 1 || len(results) != 1 || len(aggregations) == 0 {
		return nil, zigUnsupported("Search graph aggregations outside supported graph-only path")
	}
	var graphQuery *indexes.GraphQuery
	var graphResult *indexes.GraphQueryResult
	for name := range queries {
		graphQuery = queries[name]
		graphResult = results[name]
		break
	}
	if graphQuery == nil || graphResult == nil {
		return nil, zigUnsupported("Search graph aggregations without graph results")
	}
	hitIDs, err := collectGraphAggregationHitIDs(ctx, db, graphQuery, graphResult, filterPrefix)
	if err != nil {
		return nil, err
	}
	payloads, err := bridge.AggregateHits(zbridge.AggregateHitsRequestPayload{
		HitIDsB64:    hitIDs,
		Aggregations: aggregations,
	})
	if err != nil {
		return nil, err
	}
	return decodeBackendAggregationResults(payloads)
}

func collectGraphAggregationHitIDs(
	ctx context.Context,
	db *ZigCoreDB,
	query *indexes.GraphQuery,
	result *indexes.GraphQueryResult,
	filterPrefix []byte,
) ([]string, error) {
	if result == nil {
		return nil, zigUnsupported("Search graph aggregations without graph results")
	}
	seen := make(map[string]struct{}, len(result.Nodes))
	out := make([]string, 0, len(result.Nodes))
	appendKey := func(key string) {
		if key == "" {
			return
		}
		encodedKey := key
		rawKey := []byte(key)
		if decoded, err := zbridge.DecodeBase64(key); err == nil {
			rawKey = decoded
		} else {
			encodedKey = zbridge.EncodeBase64(rawKey)
		}
		if len(filterPrefix) > 0 && !bytes.HasPrefix(rawKey, filterPrefix) {
			return
		}
		if _, ok := seen[encodedKey]; ok {
			return
		}
		seen[encodedKey] = struct{}{}
		out = append(out, encodedKey)
	}
	for _, node := range result.Nodes {
		if node.Key == "" {
			continue
		}
		if node.Document == nil && db != nil {
			doc, err := db.Get(ctx, []byte(node.Key))
			if err == nil {
				node.Document = doc
			}
		}
		appendKey(node.Key)
	}
	for _, path := range result.Paths {
		for _, key := range path.Nodes {
			appendKey(key)
		}
	}
	for _, match := range result.Matches {
		for alias, binding := range match.Bindings {
			if len(query.ReturnAliases) > 0 && !slices.Contains(query.ReturnAliases, alias) {
				continue
			}
			appendKey(binding.Key)
		}
	}
	return out, nil
}

func executeBackendVectorAggregations(
	bridge *zbridge.Bridge,
	indexName string,
	result *vectorindex.SearchResult,
	aggregations []zbridge.SearchAggregationRequestPayload,
) (bleveSearch.AggregationResults, error) {
	if result == nil || len(aggregations) == 0 {
		return nil, nil
	}
	hitIDs := make([]string, 0, len(result.Hits))
	for _, hit := range result.Hits {
		if hit == nil || hit.ID == "" {
			continue
		}
		hitIDs = append(hitIDs, zbridge.EncodeBase64([]byte(hit.ID)))
	}
	payloads, err := bridge.AggregateHits(zbridge.AggregateHitsRequestPayload{
		IndexName:    indexName,
		HitIDsB64:    hitIDs,
		Aggregations: aggregations,
	})
	if err != nil {
		return nil, err
	}
	return decodeBackendAggregationResults(payloads)
}

func executeBackendFusionAggregations(
	bridge *zbridge.Bridge,
	result *indexes.FusionResult,
	aggregations []zbridge.SearchAggregationRequestPayload,
) (bleveSearch.AggregationResults, error) {
	if result == nil || len(aggregations) == 0 {
		return nil, nil
	}
	hitIDs := make([]string, 0, len(result.Hits))
	for _, hit := range result.Hits {
		if hit == nil || hit.ID == "" {
			continue
		}
		hitIDs = append(hitIDs, zbridge.EncodeBase64([]byte(hit.ID)))
	}
	payloads, err := bridge.AggregateHits(zbridge.AggregateHitsRequestPayload{
		HitIDsB64:    hitIDs,
		Aggregations: aggregations,
	})
	if err != nil {
		return nil, err
	}
	return decodeBackendAggregationResults(payloads)
}

func executeBackendUnionVectorAggregations(
	bridge *zbridge.Bridge,
	results map[string]*vectorindex.SearchResult,
	aggregations []zbridge.SearchAggregationRequestPayload,
) (bleveSearch.AggregationResults, error) {
	if len(results) == 0 || len(aggregations) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	hitIDs := make([]string, 0)
	for _, result := range results {
		if result == nil {
			continue
		}
		for _, hit := range result.Hits {
			if hit == nil || hit.ID == "" {
				continue
			}
			if _, ok := seen[hit.ID]; ok {
				continue
			}
			seen[hit.ID] = struct{}{}
			hitIDs = append(hitIDs, zbridge.EncodeBase64([]byte(hit.ID)))
		}
	}
	payloads, err := bridge.AggregateHits(zbridge.AggregateHitsRequestPayload{
		HitIDsB64:    hitIDs,
		Aggregations: aggregations,
	})
	if err != nil {
		return nil, err
	}
	return decodeBackendAggregationResults(payloads)
}

func backendAggRequiresFullTextIndex(requests []zbridge.SearchAggregationRequestPayload) bool {
	for _, req := range requests {
		if req.Type == "significant_terms" {
			return true
		}
		if backendAggRequiresFullTextIndex(req.Aggregations) {
			return true
		}
	}
	return false
}

func supportsFusionSortPaging(req *indexes.RemoteIndexSearchRequest) bool {
	if req == nil || req.RerankerConfig != nil {
		return false
	}
	return req.MergeConfig != nil || req.ExpandStrategy != ""
}

func supportsGraphNodeSortPaging(req *indexes.RemoteIndexSearchRequest) bool {
	if req == nil || req.RerankerConfig != nil || req.MergeConfig != nil || req.ExpandStrategy != "" {
		return false
	}
	if req.BleveSearchRequest != nil || len(req.VectorSearches) > 0 || len(req.SparseSearches) > 0 || len(req.GraphSearches) != 1 {
		return false
	}
	for _, graphQuery := range req.GraphSearches {
		if graphQuery == nil {
			return false
		}
		switch graphQuery.Type {
		case indexes.GraphQueryTypeNeighbors, indexes.GraphQueryTypeTraverse:
			return true
		default:
			return false
		}
	}
	return false
}

func buildFusionResultFromGraphNodes(results map[string]*indexes.GraphQueryResult) *indexes.FusionResult {
	if len(results) == 0 {
		return &indexes.FusionResult{}
	}
	seen := make(map[string]*indexes.FusionHit)
	for _, result := range results {
		if result == nil {
			continue
		}
		for _, node := range result.Nodes {
			nodeID := decodeGraphNodeKey(node.Key)
			if nodeID == "" {
				continue
			}
			score := 1.0 / (1.0 + node.Distance)
			if existing, ok := seen[nodeID]; ok {
				if existing.Fields == nil && node.Document != nil {
					existing.Fields = node.Document
				}
				if score > existing.Score {
					existing.Score = score
				}
				continue
			}
			seen[nodeID] = &indexes.FusionHit{
				ID:     nodeID,
				Score:  score,
				Fields: node.Document,
			}
		}
	}
	result := &indexes.FusionResult{
		Hits:  make([]*indexes.FusionHit, 0, len(seen)),
		Total: uint64(len(seen)),
	}
	for _, hit := range seen {
		result.Hits = append(result.Hits, hit)
	}
	result.FinalizeSort()
	return result
}

func sortBleveHits(hits bleveSearch.DocumentMatchCollection, fields []supportedSortField, plan *bleveSortPlan) {
	sort.SliceStable(hits, func(i, j int) bool {
		return compareBleveHits(hits[i], hits[j], fields, plan) < 0
	})
}

func sortFusionHits(hits []*indexes.FusionHit, fields []supportedSortField) {
	sort.SliceStable(hits, func(i, j int) bool {
		return compareFusionHits(hits[i], hits[j], fields) < 0
	})
}

func compareBleveHits(a, b *bleveSearch.DocumentMatch, fields []supportedSortField, plan *bleveSortPlan) int {
	sawSyntheticComposite := false
	leftValues := plan.values[a]
	rightValues := plan.values[b]
	for i, field := range fields {
		left := leftValues[i]
		right := rightValues[i]
		cmp := compareBleveSortValues(left, right)
		if field.desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
		sawSyntheticComposite = sawSyntheticComposite || plan.modes[i].synthetic
	}
	if sawSyntheticComposite {
		return 0
	}
	return strings.Compare(a.ID, b.ID)
}

func compareFusionHits(a, b *indexes.FusionHit, fields []supportedSortField) int {
	sawSyntheticComposite := false
	for _, field := range fields {
		left := fusionSortValue(a, field.field)
		right := fusionSortValue(b, field.field)
		cmp := compareBleveSortValues(left, right)
		if field.desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
		sawSyntheticComposite = sawSyntheticComposite || usesSyntheticCompositeSortValue(left) || usesSyntheticCompositeSortValue(right)
	}
	if sawSyntheticComposite {
		return 0
	}
	return strings.Compare(a.ID, b.ID)
}

func compareBleveHitToTokens(hit *bleveSearch.DocumentMatch, tokens []string, fields []supportedSortField, plan *bleveSortPlan) (int, error) {
	if len(tokens) != len(fields) {
		return 0, zigUnsupported("Search cursor token count mismatch")
	}
	hitValues := plan.values[hit]
	for i, field := range fields {
		hitValue := hitValues[i]
		tokenValue, err := bleveTokenValue(field.field, tokens[i], hitValue)
		if err != nil {
			return 0, err
		}
		cmp := compareBleveSortValues(hitValue, tokenValue)
		if field.desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func compareFusionHitToTokens(hit *indexes.FusionHit, tokens []string, fields []supportedSortField) (int, error) {
	if len(tokens) != len(fields) {
		return 0, zigUnsupported("Search cursor token count mismatch")
	}
	for i, field := range fields {
		hitValue := fusionSortValue(hit, field.field)
		tokenValue, err := bleveTokenValue(field.field, tokens[i], hitValue)
		if err != nil {
			return 0, err
		}
		cmp := compareBleveSortValues(hitValue, tokenValue)
		if field.desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func bleveSortValue(hit *bleveSearch.DocumentMatch, field string) any {
	switch field {
	case "_id":
		return hit.ID
	case "_score":
		return hit.Score
	default:
		if hit.Fields == nil {
			return nil
		}
		return hit.Fields[field]
	}
}

func bleveSortTokenValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func fusionSortValue(hit *indexes.FusionHit, field string) any {
	switch field {
	case "_id":
		return hit.ID
	case "_score":
		if hit.RerankedScore != nil {
			return *hit.RerankedScore
		}
		return hit.Score
	default:
		if hit.Fields == nil {
			return nil
		}
		return hit.Fields[field]
	}
}

func bleveTokenValue(field string, token string, exemplar any) (any, error) {
	switch field {
	case "_id":
		return token, nil
	case "_score":
		score, err := strconv.ParseFloat(token, 64)
		if err != nil {
			return nil, zigUnsupported("Search cursor score token")
		}
		return score, nil
	default:
		switch exemplar.(type) {
		case float64, float32, int, int64, int32, uint64, uint32, json.Number:
			num, err := strconv.ParseFloat(token, 64)
			if err != nil {
				return nil, zigUnsupported("Search cursor numeric token")
			}
			return num, nil
		case bool:
			boolean, err := strconv.ParseBool(token)
			if err != nil {
				return nil, zigUnsupported("Search cursor bool token")
			}
			return boolean, nil
		default:
			return token, nil
		}
	}
}

func compareBleveSortValues(left, right any) int {
	left, _ = normalizeBleveSortValue(left)
	right, _ = normalizeBleveSortValue(right)
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return 1
	}
	if right == nil {
		return -1
	}
	if leftNum, ok := coerceNumeric(left); ok {
		if rightNum, ok := coerceNumeric(right); ok {
			switch {
			case leftNum < rightNum:
				return -1
			case leftNum > rightNum:
				return 1
			default:
				return 0
			}
		}
	}
	switch l := left.(type) {
	case bool:
		if r, ok := right.(bool); ok {
			switch {
			case !l && r:
				return -1
			case l && !r:
				return 1
			default:
				return 0
			}
		}
	}
	return strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
}

func validateBleveSortFieldsOnHits(hits bleveSearch.DocumentMatchCollection, fields []supportedSortField) error {
	fieldKinds := make(map[string]string, len(fields))
	for _, hit := range hits {
		for _, field := range fields {
			value, ok := normalizeBleveSortValue(bleveSortValue(hit, field.field))
			if !ok {
				return zigUnsupported("Search sort field " + field.field + " requires scalar stored values")
			}
			if err := validateSortValueKind(fieldKinds, field.field, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateFusionSortFieldsOnHits(hits []*indexes.FusionHit, fields []supportedSortField) error {
	fieldKinds := make(map[string]string, len(fields))
	for _, hit := range hits {
		for _, field := range fields {
			value, ok := normalizeBleveSortValue(fusionSortValue(hit, field.field))
			if !ok {
				return zigUnsupported("Search sort field " + field.field + " requires scalar stored values")
			}
			if err := validateSortValueKind(fieldKinds, field.field, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateSortValueKind(fieldKinds map[string]string, field string, value any) error {
	kind := supportedSortValueKind(value)
	if kind == "" {
		return nil
	}
	if existing, ok := fieldKinds[field]; ok && existing != kind {
		return zigUnsupported("Search sort field " + field + " requires consistent scalar value types")
	}
	fieldKinds[field] = kind
	return nil
}

func supportedSortValueKind(value any) string {
	if value == nil {
		return ""
	}
	if _, ok := coerceNumeric(value); ok {
		return "numeric"
	}
	switch value.(type) {
	case string:
		return "string"
	case bool:
		return "bool"
	default:
		return ""
	}
}

func normalizeBleveSortValue(value any) (any, bool) {
	if value == nil {
		return nil, true
	}
	if _, ok := coerceNumeric(value); ok {
		return value, true
	}
	switch v := value.(type) {
	case string, bool, json.Number:
		return v, true
	case []string:
		if len(v) == 0 {
			return bleveCompositeSortSentinel, true
		}
		minValue := v[0]
		for _, item := range v[1:] {
			if item < minValue {
				minValue = item
			}
		}
		return minValue, true
	case []any:
		if len(v) == 0 {
			return bleveCompositeSortSentinel, true
		}
		var (
			minValue string
			hasValue bool
		)
		for _, item := range v {
			if item == nil {
				continue
			}
			str, ok := item.(string)
			if !ok {
				return bleveCompositeSortSentinel, true
			}
			if !hasValue || str < minValue {
				minValue = str
				hasValue = true
			}
		}
		if !hasValue {
			return bleveCompositeSortSentinel, true
		}
		return minValue, true
	case map[string]any:
		return bleveCompositeSortSentinel, true
	default:
		return nil, false
	}
}

func usesSyntheticCompositeSortValue(value any) bool {
	normalized, ok := normalizeBleveSortValue(value)
	if !ok {
		return false
	}
	str, ok := normalized.(string)
	return ok && str == bleveCompositeSortSentinel
}

func buildBleveSortPlan(hits bleveSearch.DocumentMatchCollection, fields []supportedSortField) (*bleveSortPlan, error) {
	plan := &bleveSortPlan{
		modes:  make([]bleveSortFieldMode, len(fields)),
		values: make(map[*bleveSearch.DocumentMatch][]any, len(hits)),
		tokens: make(map[*bleveSearch.DocumentMatch][]string, len(hits)),
	}
	if len(fields) == 0 {
		return plan, nil
	}

	for i, field := range fields {
		kinds := map[string]struct{}{}
		for _, hit := range hits {
			value, ok := normalizeBleveSortValue(bleveSortValue(hit, field.field))
			if !ok {
				plan.modes[i].synthetic = true
				break
			}
			if usesSyntheticCompositeSortValue(value) {
				plan.modes[i].synthetic = true
				break
			}
			if kind := supportedSortValueKind(value); kind != "" {
				kinds[kind] = struct{}{}
				if len(kinds) > 1 {
					plan.modes[i].synthetic = true
					break
				}
			}
		}
	}

	for _, hit := range hits {
		values := make([]any, 0, len(fields))
		tokens := make([]string, 0, len(fields))
		for i, field := range fields {
			value, ok := normalizeBleveSortValue(bleveSortValue(hit, field.field))
			if !ok || plan.modes[i].synthetic {
				value = bleveCompositeSortSentinel
			}
			values = append(values, value)
			tokens = append(tokens, bleveSortTokenValue(value))
		}
		plan.values[hit] = values
		plan.tokens[hit] = tokens
	}
	return plan, nil
}

func populateBleveSortTokens(result *bleve.SearchResult, plan *bleveSortPlan) {
	if result == nil || plan == nil {
		return
	}
	for _, hit := range result.Hits {
		hit.Sort = append(hit.Sort[:0], plan.tokens[hit]...)
	}
}

func sliceBleveHits(result *bleve.SearchResult, start, limit int) {
	if result == nil {
		return
	}
	if start < 0 {
		start = 0
	}
	if limit <= 0 {
		limit = len(result.Hits)
	}
	end := start + limit
	if start > len(result.Hits) {
		start = len(result.Hits)
	}
	if end > len(result.Hits) {
		end = len(result.Hits)
	}
	if start >= end {
		result.Hits = bleveSearch.DocumentMatchCollection{}
		result.MaxScore = 0
		return
	}
	result.Hits = result.Hits[start:end]
	maxScore := 0.0
	for _, hit := range result.Hits {
		if hit.Score > maxScore {
			maxScore = hit.Score
		}
	}
	result.MaxScore = maxScore
}

func applyDefaultBleveCursorPaging(result *bleve.SearchResult, opts indexes.FullTextPagingOptions, fallbackLimit int) {
	limit := opts.Limit
	if limit <= 0 {
		if fallbackLimit > 0 {
			limit = fallbackLimit
		} else {
			limit = len(result.Hits)
		}
	}

	cursorID := opts.SearchAfter
	before := false
	if len(opts.SearchBefore) > 0 {
		cursorID = opts.SearchBefore
		before = true
	}
	if len(cursorID) == 0 {
		sliceBleveHits(result, 0, limit)
		return
	}
	cursorDocID := cursorID[len(cursorID)-1]
	index := -1
	for i, hit := range result.Hits {
		if hit.ID == cursorDocID {
			index = i
			break
		}
	}
	if index < 0 {
		result.Hits = bleveSearch.DocumentMatchCollection{}
		result.MaxScore = 0
		return
	}
	if before {
		start := index - limit
		if start < 0 {
			start = 0
		}
		sliceBleveHits(result, start, min(limit, index))
		return
	}
	sliceBleveHits(result, index+1, limit)
}

func applyDefaultFusionCursorPaging(result *indexes.FusionResult, opts indexes.FullTextPagingOptions, fallbackLimit int) {
	limit := opts.Limit
	if limit <= 0 {
		if fallbackLimit > 0 {
			limit = fallbackLimit
		} else {
			limit = len(result.Hits)
		}
	}

	cursorID := opts.SearchAfter
	before := false
	if len(opts.SearchBefore) > 0 {
		cursorID = opts.SearchBefore
		before = true
	}
	if len(cursorID) == 0 {
		sliceFusionHits(result, opts.Offset, limit)
		return
	}
	cursorDocID := cursorID[len(cursorID)-1]
	index := -1
	for i, hit := range result.Hits {
		if hit.ID == cursorDocID {
			index = i
			break
		}
	}
	if index < 0 {
		result.Hits = []*indexes.FusionHit{}
		result.MaxScore = 0
		return
	}
	if before {
		start := index - limit
		if start < 0 {
			start = 0
		}
		sliceFusionHits(result, start, min(limit, index))
		return
	}
	sliceFusionHits(result, index+1, limit)
}

func applySortedBlevePaging(result *bleve.SearchResult, opts indexes.FullTextPagingOptions, fields []supportedSortField, plan *bleveSortPlan, fallbackLimit int) error {
	limit := opts.Limit
	if limit <= 0 {
		if fallbackLimit > 0 {
			limit = fallbackLimit
		} else {
			limit = len(result.Hits)
		}
	}
	if len(opts.SearchAfter) > 0 || len(opts.SearchBefore) > 0 {
		tokens := opts.SearchAfter
		before := false
		if len(opts.SearchBefore) > 0 {
			tokens = opts.SearchBefore
			before = true
		}
		filtered := make(bleveSearch.DocumentMatchCollection, 0, len(result.Hits))
		for _, hit := range result.Hits {
			cmp, err := compareBleveHitToTokens(hit, tokens, fields, plan)
			if err != nil {
				return err
			}
			if (!before && cmp > 0) || (before && cmp < 0) {
				filtered = append(filtered, hit)
			}
		}
		result.Hits = filtered
		if before {
			start := len(filtered) - limit
			if start < 0 {
				start = 0
			}
			sliceBleveHits(result, start, limit)
			populateBleveSortTokens(result, plan)
			return nil
		}
		sliceBleveHits(result, 0, limit)
		populateBleveSortTokens(result, plan)
		return nil
	}
	sliceBleveHits(result, opts.Offset, limit)
	populateBleveSortTokens(result, plan)
	return nil
}

func applySortedFusionPaging(result *indexes.FusionResult, opts indexes.FullTextPagingOptions, fields []supportedSortField, fallbackLimit int) error {
	limit := opts.Limit
	if limit <= 0 {
		if fallbackLimit > 0 {
			limit = fallbackLimit
		} else {
			limit = len(result.Hits)
		}
	}
	if len(opts.SearchAfter) > 0 || len(opts.SearchBefore) > 0 {
		tokens := opts.SearchAfter
		before := false
		if len(opts.SearchBefore) > 0 {
			tokens = opts.SearchBefore
			before = true
		}
		filtered := make([]*indexes.FusionHit, 0, len(result.Hits))
		for _, hit := range result.Hits {
			cmp, err := compareFusionHitToTokens(hit, tokens, fields)
			if err != nil {
				return err
			}
			if (!before && cmp > 0) || (before && cmp < 0) {
				filtered = append(filtered, hit)
			}
		}
		result.Hits = filtered
		if before {
			start := len(filtered) - limit
			if start < 0 {
				start = 0
			}
			sliceFusionHits(result, start, limit)
			return nil
		}
		sliceFusionHits(result, 0, limit)
		return nil
	}
	sliceFusionHits(result, opts.Offset, limit)
	return nil
}

func sliceFusionHits(result *indexes.FusionResult, start, limit int) {
	if result == nil {
		return
	}
	if start < 0 {
		start = 0
	}
	if limit <= 0 {
		limit = len(result.Hits)
	}
	end := start + limit
	if start > len(result.Hits) {
		start = len(result.Hits)
	}
	if end > len(result.Hits) {
		end = len(result.Hits)
	}
	if start >= end {
		result.Hits = []*indexes.FusionHit{}
		result.MaxScore = 0
		result.Total = 0
		return
	}
	result.Hits = result.Hits[start:end]
	maxScore := 0.0
	for _, hit := range result.Hits {
		score := hit.Score
		if hit.RerankedScore != nil {
			score = *hit.RerankedScore
		}
		if score > maxScore {
			maxScore = score
		}
	}
	result.MaxScore = maxScore
	result.Total = uint64(len(result.Hits))
}

func applySimpleBlevePaging(req *bleve.SearchRequest, opts indexes.FullTextPagingOptions, fallbackLimit int) {
	if req == nil {
		return
	}
	if opts.Limit > 0 {
		req.Size = opts.Limit
	} else if req.Size == 0 && fallbackLimit > 0 {
		req.Size = fallbackLimit
	}
	if opts.Offset > 0 {
		req.From = opts.Offset
	}
}

func applyBleveFilterQuery(filterQuery blevequery.Query, result *bleve.SearchResult) error {
	if filterQuery == nil || result == nil {
		return nil
	}
	filtered := make(bleveSearch.DocumentMatchCollection, 0, len(result.Hits))
	maxScore := 0.0
	for _, hit := range result.Hits {
		matched, err := matchesFilterQuery(filterQuery, hit.Fields)
		if err != nil {
			return err
		}
		if !matched {
			continue
		}
		filtered = append(filtered, hit)
		if hit.Score > maxScore {
			maxScore = hit.Score
		}
	}
	result.Hits = filtered
	result.Total = uint64(len(filtered))
	result.MaxScore = maxScore
	if result.Status != nil {
		result.Status.Successful = 1
	}
	return nil
}

func applyBleveFilterPrefix(prefix []byte, result *bleve.SearchResult) {
	if len(prefix) == 0 || result == nil {
		return
	}
	filtered := make(bleveSearch.DocumentMatchCollection, 0, len(result.Hits))
	maxScore := 0.0
	for _, hit := range result.Hits {
		if !bytes.HasPrefix([]byte(hit.ID), prefix) {
			continue
		}
		filtered = append(filtered, hit)
		if hit.Score > maxScore {
			maxScore = hit.Score
		}
	}
	result.Hits = filtered
	result.Total = uint64(len(filtered))
	result.MaxScore = maxScore
}

func applyVectorFilterQuery(filterQuery blevequery.Query, result *vectorindex.SearchResult) error {
	if filterQuery == nil || result == nil {
		return nil
	}
	filtered := make([]*vectorindex.SearchHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		matched, err := matchesFilterQuery(filterQuery, hit.Fields)
		if err != nil {
			return err
		}
		if matched {
			filtered = append(filtered, hit)
		}
	}
	result.Hits = filtered
	result.Total = uint64(len(filtered))
	if result.Status != nil {
		result.Status.Total = uint64(len(filtered))
		result.Status.Successful = len(filtered)
		result.Status.Failed = 0
	}
	return nil
}

func applyVectorDistancePaging(opts indexes.VectorPagingOptions, result *vectorindex.SearchResult) {
	if result == nil || (opts.DistanceOver == nil && opts.DistanceUnder == nil) {
		return
	}
	filtered := make([]*vectorindex.SearchHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		if opts.DistanceOver != nil && hit.Distance <= *opts.DistanceOver {
			continue
		}
		if opts.DistanceUnder != nil && hit.Distance >= *opts.DistanceUnder {
			continue
		}
		filtered = append(filtered, hit)
	}
	result.Hits = filtered
	result.Total = uint64(len(filtered))
	if result.Status != nil {
		result.Status.Total = uint64(len(filtered))
		result.Status.Successful = len(filtered)
		result.Status.Failed = 0
	}
}

func applyVectorFilterPrefix(prefix []byte, result *vectorindex.SearchResult) {
	if len(prefix) == 0 || result == nil {
		return
	}
	filtered := make([]*vectorindex.SearchHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		if bytes.HasPrefix([]byte(hit.ID), prefix) {
			filtered = append(filtered, hit)
		}
	}
	result.Hits = filtered
	result.Total = uint64(len(filtered))
	if result.Status != nil {
		result.Status.Total = uint64(len(filtered))
		result.Status.Successful = len(filtered)
		result.Status.Failed = 0
	}
}

func applyGraphFilterPrefix(prefix []byte, results map[string]*indexes.GraphQueryResult) {
	if len(prefix) == 0 {
		return
	}
	for _, result := range results {
		if result == nil {
			continue
		}
		filtered := make([]indexes.GraphResultNode, 0, len(result.Nodes))
		for _, node := range result.Nodes {
			if bytes.HasPrefix([]byte(node.Key), prefix) {
				filtered = append(filtered, node)
			}
		}
		result.Nodes = filtered
		result.Total = len(filtered)
	}
}

func applyFusionFilterPrefix(prefix []byte, result *indexes.FusionResult) {
	if len(prefix) == 0 || result == nil {
		return
	}
	filtered := make([]*indexes.FusionHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		if bytes.HasPrefix([]byte(hit.ID), prefix) {
			filtered = append(filtered, hit)
		}
	}
	result.Hits = filtered
	result.Total = uint64(len(filtered))
	if len(filtered) > 0 {
		result.FinalizeSort()
	} else {
		result.MaxScore = 0
	}
}

func splitBackendAggregationRequests(requests indexes.AggregationRequests) ([]zbridge.SearchAggregationRequestPayload, indexes.AggregationRequests, error) {
	if len(requests) == 0 {
		return nil, nil, nil
	}
	backend := make([]zbridge.SearchAggregationRequestPayload, 0, len(requests))
	local := make(indexes.AggregationRequests)
	for name, req := range requests {
		if payload, ok, err := toBackendAggregationRequest(name, req); err != nil {
			return nil, nil, err
		} else if ok {
			backend = append(backend, payload)
			continue
		}
		local[name] = req
	}
	if len(backend) == 0 {
		backend = nil
	}
	if len(local) == 0 {
		local = nil
	}
	return backend, local, nil
}

func toBackendAggregationRequest(name string, req *indexes.AggregationRequest) (zbridge.SearchAggregationRequestPayload, bool, error) {
	if req == nil {
		return zbridge.SearchAggregationRequestPayload{}, false, nil
	}
	children := make([]zbridge.SearchAggregationRequestPayload, 0, len(req.Aggregations))
	for childName, childReq := range req.Aggregations {
		payload, ok, err := toBackendAggregationRequest(childName, childReq)
		if err != nil {
			return zbridge.SearchAggregationRequestPayload{}, false, err
		}
		if !ok {
			return zbridge.SearchAggregationRequestPayload{}, false, nil
		}
		children = append(children, payload)
	}

	base := zbridge.SearchAggregationRequestPayload{
		Name:                  name,
		Type:                  req.Type,
		Field:                 req.Field,
		Size:                  req.Size,
		Interval:              req.Interval,
		CalendarInterval:      req.CalendarInterval,
		FixedInterval:         req.FixedInterval,
		MinDocCount:           req.MinDocCount,
		SignificanceAlgorithm: req.SignificanceAlgorithm,
		BucketPath:            req.BucketPath,
		SortOrder:             req.BucketSortOrder,
		From:                  req.BucketFrom,
		Window:                req.PipelineWindow,
		GapPolicy:             req.PipelineGapPolicy,
		TermPrefix:            req.TermPrefix,
		TermPattern:           req.TermPattern,
		GeohashPrecision:      req.GeohashPrecision,
		Aggregations:          children,
	}
	if len(req.BackgroundFilter) > 0 {
		backgroundQueryType, backgroundField, backgroundText, err := parseBackendBackgroundFilter(req.BackgroundFilter)
		if err != nil {
			return zbridge.SearchAggregationRequestPayload{}, false, err
		}
		base.BackgroundQueryType = backgroundQueryType
		base.BackgroundField = backgroundField
		base.BackgroundText = backgroundText
	}

	switch req.Type {
	case "count":
		return base, true, nil
	case "sum", "min", "max", "avg", "stats", "cardinality":
		return base, req.Field != "", nil
	case "terms":
		return base, req.Field != "", nil
	case "significant_terms":
		return base, req.Field != "", nil
	case "bucket_sort":
		return base, req.BucketPath != "", nil
	case "sum_bucket", "avg_bucket", "min_bucket", "max_bucket", "stats_bucket", "extended_stats_bucket", "percentiles_bucket", "cumulative_sum", "derivative":
		return base, req.BucketPath != "", nil
	case "moving_avg":
		return base, req.BucketPath != "" && req.PipelineWindow > 0, nil
	case "histogram":
		return base, req.Field != "" && req.Interval > 0, nil
	case "date_histogram":
		return base, req.Field != "" && (req.CalendarInterval != "" || req.FixedInterval != ""), nil
	case "geohash_grid":
		return base, req.Field != "" && req.GeohashPrecision > 0, nil
	case "date_range":
		ranges := make([]zbridge.SearchDateTimeRangePayload, 0, len(req.DateTimeRanges))
		for _, r := range req.DateTimeRanges {
			if r == nil {
				continue
			}
			ranges = append(ranges, zbridge.SearchDateTimeRangePayload{Name: r.Name, Start: r.Start, End: r.End})
		}
		if len(ranges) == 0 || req.Field == "" {
			return zbridge.SearchAggregationRequestPayload{}, false, nil
		}
		base.DateTimeRanges = ranges
		return base, true, nil
	case "range":
		switch {
		case len(req.DistanceRanges) > 0 && len(req.NumericRanges) == 0 && len(req.DateTimeRanges) == 0:
			ranges := make([]zbridge.SearchDistanceRangePayload, 0, len(req.DistanceRanges))
			for _, r := range req.DistanceRanges {
				if r == nil {
					continue
				}
				ranges = append(ranges, zbridge.SearchDistanceRangePayload{Name: r.Name, From: r.From, To: r.To})
			}
			if len(ranges) == 0 || req.Field == "" {
				return zbridge.SearchAggregationRequestPayload{}, false, nil
			}
			base.DistanceRanges = ranges
			base.CenterLat = req.CenterLat
			base.CenterLon = req.CenterLon
			base.DistanceUnit = req.DistanceUnit
			return base, true, nil
		case len(req.NumericRanges) > 0 && len(req.DateTimeRanges) == 0 && len(req.DistanceRanges) == 0:
			ranges := make([]zbridge.SearchNumericRangePayload, 0, len(req.NumericRanges))
			for _, r := range req.NumericRanges {
				if r == nil {
					continue
				}
				ranges = append(ranges, zbridge.SearchNumericRangePayload{Name: r.Name, Start: r.Start, End: r.End})
			}
			if len(ranges) == 0 || req.Field == "" {
				return zbridge.SearchAggregationRequestPayload{}, false, nil
			}
			base.NumericRanges = ranges
			return base, true, nil
		case len(req.DateTimeRanges) > 0 && len(req.NumericRanges) == 0 && len(req.DistanceRanges) == 0:
			ranges := make([]zbridge.SearchDateTimeRangePayload, 0, len(req.DateTimeRanges))
			for _, r := range req.DateTimeRanges {
				if r == nil {
					continue
				}
				ranges = append(ranges, zbridge.SearchDateTimeRangePayload{Name: r.Name, Start: r.Start, End: r.End})
			}
			if len(ranges) == 0 || req.Field == "" {
				return zbridge.SearchAggregationRequestPayload{}, false, nil
			}
			base.Type = "date_range"
			base.DateTimeRanges = ranges
			return base, true, nil
		default:
			return zbridge.SearchAggregationRequestPayload{}, false, nil
		}
	default:
		return zbridge.SearchAggregationRequestPayload{}, false, nil
	}
}

func parseBackendBackgroundFilter(raw json.RawMessage) (queryType string, field string, text string, err error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", "", "", nil
	}
	q, err := blevequery.ParseQuery(raw)
	if err != nil {
		return "", "", "", fmt.Errorf("parsing significant_terms background_filter: %w", err)
	}
	switch typed := q.(type) {
	case *blevequery.MatchAllQuery:
		return "match_all", "", "", nil
	case *blevequery.MatchQuery:
		return "match", typed.Field(), typed.Match, nil
	case *blevequery.TermQuery:
		return "term", typed.Field(), typed.Term, nil
	default:
		return "", "", "", zigUnsupported("significant_terms background_filter query type")
	}
}

func decodeBackendAggregationResults(payload []zbridge.SearchAggregationResultPayload) (bleveSearch.AggregationResults, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	out := make(bleveSearch.AggregationResults, len(payload))
	for _, agg := range payload {
		result := &bleveSearch.AggregationResult{
			Field: agg.Field,
			Type:  agg.Type,
		}
		if agg.MetadataJSON != "" {
			var metadata map[string]any
			if err := json.Unmarshal([]byte(agg.MetadataJSON), &metadata); err != nil {
				return nil, fmt.Errorf("decoding backend aggregation metadata for %s: %w", agg.Name, err)
			}
			result.Metadata = metadata
		}
		if agg.ValueJSON != "" {
			var value any
			if err := json.Unmarshal([]byte(agg.ValueJSON), &value); err != nil {
				return nil, fmt.Errorf("decoding backend aggregation value for %s: %w", agg.Name, err)
			}
			result.Value = value
		}
		if len(agg.Buckets) > 0 {
			result.Buckets = make([]*bleveSearch.Bucket, 0, len(agg.Buckets))
			for _, bucket := range agg.Buckets {
				var key any
				if err := json.Unmarshal([]byte(bucket.KeyJSON), &key); err != nil {
					return nil, fmt.Errorf("decoding backend aggregation bucket key %q: %w", bucket.KeyJSON, err)
				}
				var nested map[string]*bleveSearch.AggregationResult
				if len(bucket.Aggregations) > 0 {
					decoded, err := decodeBackendAggregationResults(bucket.Aggregations)
					if err != nil {
						return nil, err
					}
					nested = decoded
				}
				var metadata map[string]any
				if bucket.Score != nil || bucket.BgCount != nil {
					metadata = make(map[string]any, 2)
					if bucket.Score != nil {
						metadata["score"] = *bucket.Score
					}
					if bucket.BgCount != nil {
						metadata["bg_count"] = *bucket.BgCount
					}
				}
				result.Buckets = append(result.Buckets, &bleveSearch.Bucket{
					Key:          key,
					Count:        bucket.Count,
					Metadata:     metadata,
					Aggregations: nested,
				})
			}
		}
		out[agg.Name] = result
	}
	return out, nil
}

func (db *ZigCoreDB) decodeBackendGraphResults(
	ctx context.Context,
	queries map[string]*indexes.GraphQuery,
	payloads []zbridge.SearchGraphResultPayload,
) (map[string]*indexes.GraphQueryResult, map[string]*indexes.SearchComponentStatus, error) {
	results := make(map[string]*indexes.GraphQueryResult, len(payloads))
	status := make(map[string]*indexes.SearchComponentStatus, len(payloads))
	for _, payload := range payloads {
		query := queries[payload.Name]
		if query == nil {
			continue
		}
		if query.Type == indexes.GraphQueryTypeShortestPath || query.Type == indexes.GraphQueryTypeKShortestPaths {
			paths, err := decodePathPayloads(payload.Paths)
			if err != nil {
				return nil, nil, err
			}
			results[payload.Name] = &indexes.GraphQueryResult{
				Type:  query.Type,
				Paths: paths,
				Total: len(paths),
			}
			status[payload.Name] = &indexes.SearchComponentStatus{Success: true}
			continue
		}
		storedByKey := make(map[string]map[string]any, len(payload.Hits))
		for _, hit := range payload.Hits {
			if hit.StoredJSON == "" {
				continue
			}
			fields, err := decodeStoredFields(hit.StoredJSON)
			if err != nil {
				return nil, nil, err
			}
			storedByKey[hit.IDB64] = fields
		}
		nodes := make([]indexes.GraphResultNode, 0, len(payload.Nodes))
		for _, nodePayload := range payload.Nodes {
			node := indexes.GraphResultNode{
				Key:      nodePayload.KeyB64,
				Depth:    int(nodePayload.Depth),
				Distance: nodePayload.Distance,
				Path:     append([]string(nil), nodePayload.PathB64...),
			}
			if len(nodePayload.PathEdges) > 0 {
				node.PathEdges = make([]indexes.PathEdge, 0, len(nodePayload.PathEdges))
				for _, edge := range nodePayload.PathEdges {
					node.PathEdges = append(node.PathEdges, indexes.PathEdge{
						Source: edge.SourceB64,
						Target: edge.TargetB64,
						Type:   edge.EdgeType,
						Weight: edge.Weight,
					})
				}
			}
			key, err := zbridge.DecodeBase64(nodePayload.KeyB64)
			if err != nil {
				return nil, nil, err
			}
			if query.IncludeDocuments {
				if doc, ok := storedByKey[nodePayload.KeyB64]; ok {
					node.Document = doc
				} else {
					doc, err := db.Get(ctx, key)
					if err == nil {
						node.Document = doc
					}
				}
			}
			if query.IncludeEdges {
				edges, err := db.GetEdges(ctx, query.IndexName, key, "", parseDirection(query.Params.Direction))
				if err == nil {
					node.Edges = edges
				}
			}
			if len(query.Fields) > 0 && node.Document != nil {
				node.Document = ProjectFields(node.Document, query.Fields)
			}
			nodes = append(nodes, node)
		}
		results[payload.Name] = &indexes.GraphQueryResult{
			Type:  query.Type,
			Nodes: nodes,
			Total: int(payload.TotalHits),
		}
		status[payload.Name] = &indexes.SearchComponentStatus{Success: true}
	}
	return results, status, nil
}

func applyGraphFilterQuery(ctx context.Context, db *ZigCoreDB, filterQuery blevequery.Query, results map[string]*indexes.GraphQueryResult) error {
	if filterQuery == nil {
		return nil
	}
	for _, result := range results {
		if result == nil || len(result.Nodes) == 0 {
			continue
		}
		filtered := make([]indexes.GraphResultNode, 0, len(result.Nodes))
		for _, node := range result.Nodes {
			doc := node.Document
			if doc == nil && db != nil && node.Key != "" {
				loaded, err := db.Get(ctx, []byte(node.Key))
				if err == nil {
					doc = loaded
				}
			}
			matched, err := matchesFilterQuery(filterQuery, doc)
			if err != nil {
				return err
			}
			if matched {
				node.Document = doc
				filtered = append(filtered, node)
			}
		}
		result.Nodes = filtered
		result.Total = len(filtered)
	}
	return nil
}

func collectFieldValues(fields map[string]any, field string) []any {
	if fields == nil {
		return nil
	}
	value, ok := fields[field]
	if !ok {
		return nil
	}
	switch typed := value.(type) {
	case []any:
		return typed
	default:
		return []any{typed}
	}
}

func collectNumericAggregationValues(hits bleveSearch.DocumentMatchCollection, field string) (float64, []float64, error) {
	if field == "" {
		return 0, nil, fmt.Errorf("zig coreDB: numeric aggregation requires field")
	}
	values := make([]float64, 0)
	sum := 0.0
	for _, hit := range hits {
		for _, raw := range collectFieldValues(hit.Fields, field) {
			num, ok := coerceNumeric(raw)
			if !ok {
				continue
			}
			values = append(values, num)
			sum += num
		}
	}
	return sum, values, nil
}

func collectCardinality(hits bleveSearch.DocumentMatchCollection, field string) (int64, error) {
	if field == "" {
		return 0, fmt.Errorf("zig coreDB: cardinality aggregation requires field")
	}
	seen := map[string]struct{}{}
	for _, hit := range hits {
		for _, raw := range collectFieldValues(hit.Fields, field) {
			seen[fmt.Sprint(raw)] = struct{}{}
		}
	}
	return int64(len(seen)), nil
}

func buildStatsAggregationValue(sum float64, nums []float64) map[string]any {
	if len(nums) == 0 {
		return map[string]any{
			"count":       int64(0),
			"sum":         float64(0),
			"avg":         float64(0),
			"min":         nil,
			"max":         nil,
			"sum_squares": float64(0),
			"variance":    float64(0),
			"std_dev":     float64(0),
		}
	}
	count := int64(len(nums))
	minVal := nums[0]
	maxVal := nums[0]
	sumSquares := 0.0
	for _, num := range nums {
		if num < minVal {
			minVal = num
		}
		if num > maxVal {
			maxVal = num
		}
		sumSquares += num * num
	}
	avg := sum / float64(count)
	var variance float64
	for _, num := range nums {
		diff := num - avg
		variance += diff * diff
	}
	variance /= float64(count)
	return map[string]any{
		"count":       count,
		"sum":         sum,
		"avg":         avg,
		"min":         minVal,
		"max":         maxVal,
		"sum_squares": sumSquares,
		"variance":    variance,
		"std_dev":     math.Sqrt(variance),
	}
}

func coerceNumeric(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case json.Number:
		f, err := typed.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func matchesNumericRange(values []any, r *indexes.NumericRange) bool {
	for _, raw := range values {
		num, ok := coerceNumeric(raw)
		if !ok {
			continue
		}
		if r.Start != nil && num < *r.Start {
			continue
		}
		if r.End != nil && num >= *r.End {
			continue
		}
		return true
	}
	return false
}

func matchesDateTimeRange(values []any, r *indexes.DateTimeRange) (bool, error) {
	var start, end *time.Time
	if r.Start != nil && *r.Start != "" {
		parsed, err := time.Parse(time.RFC3339, *r.Start)
		if err != nil {
			return false, fmt.Errorf("parsing range start: %w", err)
		}
		start = &parsed
	}
	if r.End != nil && *r.End != "" {
		parsed, err := time.Parse(time.RFC3339, *r.End)
		if err != nil {
			return false, fmt.Errorf("parsing range end: %w", err)
		}
		end = &parsed
	}
	for _, raw := range values {
		text, ok := raw.(string)
		if !ok {
			continue
		}
		ts, err := time.Parse(time.RFC3339, text)
		if err != nil {
			continue
		}
		if start != nil && ts.Before(*start) {
			continue
		}
		if end != nil && !ts.Before(*end) {
			continue
		}
		return true, nil
	}
	return false, nil
}

func matchesFilterQuery(filterQuery blevequery.Query, doc map[string]any) (bool, error) {
	if filterQuery == nil {
		return true, nil
	}
	if doc == nil {
		return false, nil
	}

	mapping := bleve.NewIndexMapping()
	mapping.StoreDynamic = false
	mapping.DocValuesDynamic = false
	searIndex, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return false, fmt.Errorf("creating in-memory filter index: %w", err)
	}
	defer func() { _ = searIndex.Close() }()

	if err := searIndex.Index("doc", doc); err != nil {
		return false, fmt.Errorf("indexing document for filter_query: %w", err)
	}

	searchReq := bleve.NewSearchRequest(filterQuery)
	searchReq.Size = 1
	searchResult, err := searIndex.Search(searchReq)
	if err != nil {
		return false, fmt.Errorf("evaluating filter_query: %w", err)
	}
	return searchResult.Total > 0, nil
}

func (db *ZigCoreDB) executeGraphQueries(
	ctx context.Context,
	req *indexes.RemoteIndexSearchRequest,
	res *indexes.RemoteIndexSearchResult,
) (map[string]*indexes.GraphQueryResult, map[string]*indexes.SearchComponentStatus, error) {
	sortedQueryNames, err := SortGraphQueriesByDependencies(req.GraphSearches)
	if err != nil {
		return nil, nil, err
	}

	graphResults := make(map[string]*indexes.GraphQueryResult, len(req.GraphSearches))
	graphStatus := make(map[string]*indexes.SearchComponentStatus, len(req.GraphSearches))
	res.GraphResults = graphResults

	for _, queryName := range sortedQueryNames {
		graphQuery := req.GraphSearches[queryName]
		graphResult, status, err := db.executeGraphQuery(ctx, graphQuery, res)
		if err != nil {
			if status == nil {
				status = &indexes.SearchComponentStatus{Success: false, Error: err.Error()}
			}
			graphStatus[queryName] = status
			return nil, nil, err
		}
		graphResults[queryName] = graphResult
		graphStatus[queryName] = status
	}

	return graphResults, graphStatus, nil
}

func (db *ZigCoreDB) executeGraphQuery(
	ctx context.Context,
	query *indexes.GraphQuery,
	searchResult *indexes.RemoteIndexSearchResult,
) (*indexes.GraphQueryResult, *indexes.SearchComponentStatus, error) {
	startNodes, err := resolveGraphNodeSelector(query.StartNodes, searchResult)
	if err != nil {
		return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
	}
	if len(startNodes) == 0 {
		err := fmt.Errorf("no start nodes provided")
		return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
	}

	switch query.Type {
	case indexes.GraphQueryTypeNeighbors:
		results := make([]*indexes.TraversalResult, 0)
		edgeTypes := query.Params.EdgeTypes
		if len(edgeTypes) == 0 {
			edgeTypes = []string{""}
		}
		for _, start := range startNodes {
			for _, edgeType := range edgeTypes {
				items, err := db.GetNeighbors(ctx, query.IndexName, start, edgeType, parseDirection(query.Params.Direction))
				if err != nil {
					return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
				}
				results = append(results, items...)
			}
		}
		graphResult, err := db.convertTraversalToGraphResult(ctx, query, results)
		if err != nil {
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		return graphResult, &indexes.SearchComponentStatus{Success: true}, nil

	case indexes.GraphQueryTypeTraverse:
		rules := indexes.TraversalRules{
			EdgeTypes:        query.Params.EdgeTypes,
			Direction:        parseDirection(query.Params.Direction),
			MaxDepth:         query.Params.MaxDepth,
			MinWeight:        query.Params.MinWeight,
			MaxWeight:        query.Params.MaxWeight,
			MaxResults:       query.Params.MaxResults,
			DeduplicateNodes: query.Params.DeduplicateNodes,
			IncludePaths:     query.Params.IncludePaths,
		}
		results := make([]*indexes.TraversalResult, 0)
		for _, start := range startNodes {
			items, err := db.TraverseEdges(ctx, query.IndexName, start, rules)
			if err != nil {
				return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
			}
			results = append(results, items...)
		}
		graphResult, err := db.convertTraversalToGraphResult(ctx, query, results)
		if err != nil {
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		return graphResult, &indexes.SearchComponentStatus{Success: true}, nil

	case indexes.GraphQueryTypeShortestPath:
		targetNodes, err := resolveGraphNodeSelector(query.TargetNodes, searchResult)
		if err != nil {
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		if len(targetNodes) == 0 {
			err := fmt.Errorf("no target nodes provided")
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		path, err := db.FindShortestPath(
			ctx,
			query.IndexName,
			startNodes[0],
			targetNodes[0],
			query.Params.EdgeTypes,
			parseDirection(query.Params.Direction),
			query.Params.WeightMode,
			query.Params.MaxDepth,
			query.Params.MinWeight,
			query.Params.MaxWeight,
		)
		if err != nil {
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		result := &indexes.GraphQueryResult{
			Type: query.Type,
		}
		if path != nil {
			result.Paths = []indexes.Path{*path}
			result.Total = 1
		}
		return result, &indexes.SearchComponentStatus{Success: true}, nil

	case indexes.GraphQueryTypeKShortestPaths:
		targetNodes, err := resolveGraphNodeSelector(query.TargetNodes, searchResult)
		if err != nil {
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		if len(targetNodes) == 0 {
			err := fmt.Errorf("no target nodes provided")
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		k := query.Params.K
		if k <= 0 {
			k = 1
		}
		paths, err := db.findKShortestPaths(
			query.IndexName,
			startNodes[0],
			targetNodes[0],
			k,
			query.Params.EdgeTypes,
			parseDirection(query.Params.Direction),
			query.Params.WeightMode,
			query.Params.MaxDepth,
			query.Params.MinWeight,
			query.Params.MaxWeight,
		)
		if err != nil {
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		return &indexes.GraphQueryResult{
			Type:  query.Type,
			Paths: paths,
			Total: len(paths),
		}, &indexes.SearchComponentStatus{Success: true}, nil

	case indexes.GraphQueryTypePattern:
		matches, err := db.executePatternQuery(ctx, query, startNodes)
		if err != nil {
			return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
		}
		return &indexes.GraphQueryResult{
			Type:    query.Type,
			Matches: matches,
			Total:   len(matches),
		}, &indexes.SearchComponentStatus{Success: true}, nil

	default:
		err := zigUnsupported("Search graph query type")
		return nil, &indexes.SearchComponentStatus{Success: false, Error: err.Error()}, err
	}
}

func (db *ZigCoreDB) executePatternQuery(
	ctx context.Context,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) ([]indexes.PatternMatch, error) {
	db.mu.RLock()
	bridge := db.bridge
	db.mu.RUnlock()
	if bridge == nil {
		return nil, zigUnsupported("Pattern query without open bridge")
	}
	if len(query.Pattern) == 0 {
		return nil, fmt.Errorf("pattern query requires at least one pattern step")
	}

	maxResults := query.Params.MaxResults
	if maxResults <= 0 {
		maxResults = 100
	}

	req := zbridge.PatternRequestPayload{
		IndexName:     query.IndexName,
		StartNodesB64: make([]string, 0, len(startNodes)),
		Pattern:       make([]zbridge.PatternStepPayload, 0, len(query.Pattern)),
		MaxResults:    uint32(maxResults),
		ReturnAliases: append([]string(nil), query.ReturnAliases...),
	}
	for _, start := range startNodes {
		req.StartNodesB64 = append(req.StartNodesB64, encodeBase64(start))
	}
	for _, step := range query.Pattern {
		nodeFilter, err := toBackendPatternNodeFilter(step.NodeFilter)
		if err != nil {
			return nil, err
		}
		req.Pattern = append(req.Pattern, zbridge.PatternStepPayload{
			Alias: step.Alias,
			Edge: zbridge.PatternEdgeStepPayload{
				Direction: graphDirection(parseDirection(step.Edge.Direction)),
				MinHops:   uint32(step.Edge.MinHops),
				MaxHops:   uint32(step.Edge.MaxHops),
				MinWeight: step.Edge.MinWeight,
				MaxWeight: step.Edge.MaxWeight,
				Types:     append([]string(nil), step.Edge.Types...),
			},
			NodeFilter: nodeFilter,
		})
	}

	payloads, err := bridge.MatchPattern(req)
	if err != nil {
		return nil, err
	}
	return db.decodePatternPayloads(ctx, query, payloads)
}

func toBackendPatternNodeFilter(filter indexes.NodeFilter) (zbridge.PatternNodeFilterPayload, error) {
	payload := zbridge.PatternNodeFilterPayload{FilterPrefix: filter.FilterPrefix}
	if len(filter.FilterQuery) == 0 {
		return payload, nil
	}
	normalized, err := normalizePatternFilterQuery(filter.FilterQuery)
	if err != nil {
		return zbridge.PatternNodeFilterPayload{}, err
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return zbridge.PatternNodeFilterPayload{}, fmt.Errorf("marshalling pattern node_filter.filter_query: %w", err)
	}
	payload.QueryJSON = string(raw)
	return payload, nil
}

func normalizePatternFilterQuery(raw map[string]any) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, zigUnsupported("Pattern node_filter.filter_query")
	}
	if _, ok := raw["match_all"]; ok {
		return map[string]any{"match_all": map[string]any{}}, nil
	}
	if _, ok := raw["match_none"]; ok {
		return map[string]any{"match_none": map[string]any{}}, nil
	}
	if term, ok := raw["term"].(map[string]any); ok {
		field, text, ok := singleFieldFilterValue(term)
		if !ok {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		return map[string]any{"term": map[string]any{field: text}}, nil
	}
	if match, ok := raw["match"].(map[string]any); ok {
		field, text, ok := singleFieldFilterValue(match)
		if !ok {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		return map[string]any{"match": map[string]any{field: text}}, nil
	}
	if prefix, ok := raw["prefix"].(map[string]any); ok {
		field, text, ok := singleFieldFilterValue(prefix)
		if !ok {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		return map[string]any{"prefix": map[string]any{field: text}}, nil
	}
	if wildcard, ok := raw["wildcard"].(map[string]any); ok {
		field, text, ok := singleFieldFilterValue(wildcard)
		if !ok {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		return map[string]any{"wildcard": map[string]any{field: text}}, nil
	}
	if regexp, ok := raw["regexp"].(map[string]any); ok {
		field, text, ok := singleFieldFilterValue(regexp)
		if !ok {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		return map[string]any{"regexp": map[string]any{field: text}}, nil
	}
	if fuzzy, ok := raw["fuzzy"].(map[string]any); ok {
		field, value, ok := singleFieldFilterLeafValue(fuzzy)
		if !ok {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		return map[string]any{"fuzzy": map[string]any{field: value}}, nil
	}
	if numericRange, ok := raw["numeric_range"].(map[string]any); ok {
		payload, err := normalizePatternNumericRangeFilter(numericRange)
		if err != nil {
			return nil, err
		}
		return map[string]any{"numeric_range": payload}, nil
	}
	if dateRange, ok := raw["date_range"].(map[string]any); ok {
		payload, err := normalizePatternDateRangeFilter(dateRange)
		if err != nil {
			return nil, err
		}
		return map[string]any{"date_range": payload}, nil
	}
	if docIDs, ok := raw["doc_id"].(map[string]any); ok {
		payload, err := normalizePatternDocIDFilter(docIDs)
		if err != nil {
			return nil, err
		}
		return map[string]any{"doc_id": payload}, nil
	}
	if ids, ok := raw["ids"].([]any); ok {
		payload, err := normalizePatternDocIDFilter(map[string]any{"ids": ids})
		if err != nil {
			return nil, err
		}
		return map[string]any{"doc_id": payload}, nil
	}
	if conjuncts, ok := raw["conjuncts"].([]any); ok {
		if len(conjuncts) == 0 {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		normalized := make([]map[string]any, 0, len(conjuncts))
		for _, clause := range conjuncts {
			clauseMap, ok := clause.(map[string]any)
			if !ok {
				return nil, zigUnsupported("Pattern node_filter.filter_query")
			}
			item, err := normalizePatternFilterQuery(clauseMap)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, item)
		}
		out := make([]any, 0, len(normalized))
		for _, item := range normalized {
			out = append(out, item)
		}
		return map[string]any{"conjuncts": out}, nil
	}
	if disjuncts, ok := raw["disjuncts"].([]any); ok {
		if len(disjuncts) == 0 {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		normalized := make([]map[string]any, 0, len(disjuncts))
		for _, clause := range disjuncts {
			clauseMap, ok := clause.(map[string]any)
			if !ok {
				return nil, zigUnsupported("Pattern node_filter.filter_query")
			}
			item, err := normalizePatternFilterQuery(clauseMap)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, item)
		}
		out := make([]any, 0, len(normalized))
		for _, item := range normalized {
			out = append(out, item)
		}
		return map[string]any{"disjuncts": out}, nil
	}
	if boolQuery, ok := raw["bool"].(map[string]any); ok {
		normalized := make(map[string]any)
		for _, key := range []string{"must", "should", "must_not"} {
			items, exists, err := normalizePatternFilterQueryArray(boolQuery, key)
			if err != nil {
				return nil, err
			}
			if exists {
				normalized[key] = items
			}
		}
		if len(normalized) == 0 {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		return map[string]any{"bool": normalized}, nil
	}
	return nil, zigUnsupported("Pattern node_filter.filter_query")
}

func normalizePatternFilterQueryArray(raw map[string]any, key string) ([]any, bool, error) {
	value, ok := raw[key]
	if !ok {
		return nil, false, nil
	}
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil, false, zigUnsupported("Pattern node_filter.filter_query")
	}
	normalized := make([]any, 0, len(items))
	for _, clause := range items {
		clauseMap, ok := clause.(map[string]any)
		if !ok {
			return nil, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		item, err := normalizePatternFilterQuery(clauseMap)
		if err != nil {
			return nil, false, err
		}
		normalized = append(normalized, item)
	}
	return normalized, true, nil
}

func normalizePatternNumericRangeFilter(raw map[string]any) (map[string]any, error) {
	return normalizePatternRangeFilter(raw, false)
}

func normalizePatternDateRangeFilter(raw map[string]any) (map[string]any, error) {
	return normalizePatternRangeFilter(raw, true)
}

func normalizePatternRangeFilter(raw map[string]any, isDate bool) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, zigUnsupported("Pattern node_filter.filter_query")
	}
	if field, ok := raw["field"].(string); ok {
		payload := map[string]any{
			"field": field,
		}
		if isDate {
			if start, ok, err := normalizePatternDateBound(raw, "start_ns", "start"); err != nil {
				return nil, err
			} else if ok {
				payload["start_ns"] = start
			}
			if end, ok, err := normalizePatternDateBound(raw, "end_ns", "end"); err != nil {
				return nil, err
			} else if ok {
				payload["end_ns"] = end
			}
			payload["inclusive_start"] = true
			payload["inclusive_end"] = false
			if value, ok := raw["inclusive_start"].(bool); ok {
				payload["inclusive_start"] = value
			}
			if value, ok := raw["inclusive_end"].(bool); ok {
				payload["inclusive_end"] = value
			}
		} else {
			if min, ok, err := normalizePatternNumericBound(raw, "min"); err != nil {
				return nil, err
			} else if ok {
				payload["min"] = min
			}
			if max, ok, err := normalizePatternNumericBound(raw, "max"); err != nil {
				return nil, err
			} else if ok {
				payload["max"] = max
			}
			payload["inclusive_min"] = true
			payload["inclusive_max"] = false
			if value, ok := raw["inclusive_min"].(bool); ok {
				payload["inclusive_min"] = value
			}
			if value, ok := raw["inclusive_max"].(bool); ok {
				payload["inclusive_max"] = value
			}
		}
		if _, ok := payload["min"]; !ok && !isDate {
			if _, ok := payload["max"]; !ok {
				return nil, zigUnsupported("Pattern node_filter.filter_query")
			}
		}
		if _, ok := payload["start_ns"]; !ok && isDate {
			if _, ok := payload["end_ns"]; !ok {
				return nil, zigUnsupported("Pattern node_filter.filter_query")
			}
		}
		return payload, nil
	}

	if len(raw) != 1 {
		return nil, zigUnsupported("Pattern node_filter.filter_query")
	}
	for field, clauseAny := range raw {
		clause, ok := clauseAny.(map[string]any)
		if !ok || len(clause) == 0 {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		payload := map[string]any{"field": field}
		if isDate {
			if start, ok, err := normalizePatternDateClauseBound(clause, "gte", "gt"); err != nil {
				return nil, err
			} else if ok {
				payload["start_ns"] = start.value
				payload["inclusive_start"] = start.inclusive
			}
			if end, ok, err := normalizePatternDateClauseBound(clause, "lte", "lt"); err != nil {
				return nil, err
			} else if ok {
				payload["end_ns"] = end.value
				payload["inclusive_end"] = end.inclusive
			}
			if _, ok := payload["start_ns"]; !ok {
				if _, ok := payload["end_ns"]; !ok {
					return nil, zigUnsupported("Pattern node_filter.filter_query")
				}
			}
			return payload, nil
		}

		if min, ok, err := normalizePatternNumericClauseBound(clause, "gte", "gt"); err != nil {
			return nil, err
		} else if ok {
			payload["min"] = min.value
			payload["inclusive_min"] = min.inclusive
		}
		if max, ok, err := normalizePatternNumericClauseBound(clause, "lte", "lt"); err != nil {
			return nil, err
		} else if ok {
			payload["max"] = max.value
			payload["inclusive_max"] = max.inclusive
		}
		if _, ok := payload["min"]; !ok {
			if _, ok := payload["max"]; !ok {
				return nil, zigUnsupported("Pattern node_filter.filter_query")
			}
		}
		return payload, nil
	}
	return nil, zigUnsupported("Pattern node_filter.filter_query")
}

func normalizePatternDocIDFilter(raw map[string]any) (map[string]any, error) {
	items, ok := raw["ids"].([]any)
	if !ok || len(items) == 0 {
		return nil, zigUnsupported("Pattern node_filter.filter_query")
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok || value == "" {
			return nil, zigUnsupported("Pattern node_filter.filter_query")
		}
		out = append(out, value)
	}
	return map[string]any{"ids": out}, nil
}

type normalizedPatternRangeBound[T any] struct {
	value     T
	inclusive bool
}

const (
	maxPatternInt64 = int64(^uint64(0) >> 1)
	minPatternInt64 = -maxPatternInt64 - 1
)

func normalizePatternNumericBound(raw map[string]any, key string) (float64, bool, error) {
	value, ok := raw[key]
	if !ok {
		return 0, false, nil
	}
	number, ok := toFloat64(value)
	if !ok {
		return 0, false, zigUnsupported("Pattern node_filter.filter_query")
	}
	return number, true, nil
}

func normalizePatternDateBound(raw map[string]any, normalizedKey, stringKey string) (int64, bool, error) {
	if value, ok := raw[normalizedKey]; ok {
		ns, ok := toInt64(value)
		if !ok {
			return 0, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		return ns, true, nil
	}
	value, ok := raw[stringKey]
	if !ok {
		return 0, false, nil
	}
	text, ok := value.(string)
	if !ok {
		return 0, false, zigUnsupported("Pattern node_filter.filter_query")
	}
	ts, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return 0, false, zigUnsupported("Pattern node_filter.filter_query")
	}
	return ts.UnixNano(), true, nil
}

func normalizePatternNumericClauseBound(raw map[string]any, inclusiveKey, exclusiveKey string) (normalizedPatternRangeBound[float64], bool, error) {
	if value, ok := raw[inclusiveKey]; ok {
		number, ok := toFloat64(value)
		if !ok {
			return normalizedPatternRangeBound[float64]{}, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		return normalizedPatternRangeBound[float64]{value: number, inclusive: true}, true, nil
	}
	if value, ok := raw[exclusiveKey]; ok {
		number, ok := toFloat64(value)
		if !ok {
			return normalizedPatternRangeBound[float64]{}, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		return normalizedPatternRangeBound[float64]{value: number, inclusive: false}, true, nil
	}
	return normalizedPatternRangeBound[float64]{}, false, nil
}

func normalizePatternDateClauseBound(raw map[string]any, inclusiveKey, exclusiveKey string) (normalizedPatternRangeBound[int64], bool, error) {
	if value, ok := raw[inclusiveKey]; ok {
		text, ok := value.(string)
		if !ok {
			return normalizedPatternRangeBound[int64]{}, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		ts, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return normalizedPatternRangeBound[int64]{}, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		return normalizedPatternRangeBound[int64]{value: ts.UnixNano(), inclusive: true}, true, nil
	}
	if value, ok := raw[exclusiveKey]; ok {
		text, ok := value.(string)
		if !ok {
			return normalizedPatternRangeBound[int64]{}, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		ts, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return normalizedPatternRangeBound[int64]{}, false, zigUnsupported("Pattern node_filter.filter_query")
		}
		return normalizedPatternRangeBound[int64]{value: ts.UnixNano(), inclusive: false}, true, nil
	}
	return normalizedPatternRangeBound[int64]{}, false, nil
}

func toFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	default:
		return 0, false
	}
}

func toInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case uint64:
		if typed > uint64(maxPatternInt64) {
			return 0, false
		}
		return int64(typed), true
	case uint:
		if uint64(typed) > uint64(maxPatternInt64) {
			return 0, false
		}
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint16:
		return int64(typed), true
	case uint8:
		return int64(typed), true
	case float64:
		if typed != math.Trunc(typed) || typed < float64(minPatternInt64) || typed > float64(maxPatternInt64) {
			return 0, false
		}
		return int64(typed), true
	case float32:
		f := float64(typed)
		if f != math.Trunc(f) || f < float64(minPatternInt64) || f > float64(maxPatternInt64) {
			return 0, false
		}
		return int64(f), true
	default:
		return 0, false
	}
}

func (db *ZigCoreDB) decodePatternPayloads(
	ctx context.Context,
	query *indexes.GraphQuery,
	payloads []zbridge.PatternMatchPayload,
) ([]indexes.PatternMatch, error) {
	matches := make([]indexes.PatternMatch, 0, len(payloads))
	for _, payload := range payloads {
		bindings := make(map[string]indexes.GraphResultNode, len(payload.Bindings))
		for _, binding := range payload.Bindings {
			key, err := zbridge.DecodeBase64(binding.KeyB64)
			if err != nil {
				return nil, err
			}
			node := indexes.GraphResultNode{
				Key:   binding.KeyB64,
				Depth: int(binding.Depth),
			}
			if query.IncludeDocuments {
				doc, err := db.Get(ctx, key)
				if err == nil {
					node.Document = doc
				}
			}
			if query.IncludeEdges {
				edges, err := db.GetEdges(ctx, query.IndexName, key, "", parseDirection(query.Params.Direction))
				if err == nil {
					node.Edges = edges
				}
			}
			if len(query.Fields) > 0 && node.Document != nil {
				node.Document = ProjectFields(node.Document, query.Fields)
			}
			bindings[binding.Alias] = node
		}

		path := make([]indexes.PathEdge, 0, len(payload.Path))
		for _, edge := range payload.Path {
			source, err := zbridge.DecodeBase64(edge.SourceB64)
			if err != nil {
				return nil, err
			}
			target, err := zbridge.DecodeBase64(edge.TargetB64)
			if err != nil {
				return nil, err
			}
			path = append(path, indexes.PathEdge{
				Source: base64.StdEncoding.EncodeToString(source),
				Target: base64.StdEncoding.EncodeToString(target),
				Type:   edge.EdgeType,
				Weight: edge.Weight,
			})
		}

		matches = append(matches, indexes.PatternMatch{
			Bindings: bindings,
			Path:     path,
		})
	}
	return matches, nil
}

func singleFieldFilterValue(input map[string]any) (string, string, bool) {
	if len(input) != 1 {
		return "", "", false
	}
	for field, raw := range input {
		switch value := raw.(type) {
		case string:
			return field, value, true
		case map[string]any:
			if query, ok := value["query"].(string); ok {
				return field, query, true
			}
		}
	}
	return "", "", false
}

func singleFieldFilterLeafValue(input map[string]any) (string, any, bool) {
	if len(input) != 1 {
		return "", nil, false
	}
	for field, raw := range input {
		switch value := raw.(type) {
		case string:
			return field, value, true
		case map[string]any:
			if query, ok := value["query"].(string); ok {
				if len(value) == 1 {
					return field, query, true
				}
				return field, value, true
			}
		}
	}
	return "", nil, false
}

func (db *ZigCoreDB) convertTraversalToGraphResult(
	ctx context.Context,
	query *indexes.GraphQuery,
	results []*indexes.TraversalResult,
) (*indexes.GraphQueryResult, error) {
	nodes := make([]indexes.GraphResultNode, 0, len(results))
	for _, result := range results {
		node := indexes.GraphResultNode{
			Key:      base64.StdEncoding.EncodeToString(result.Key),
			Depth:    result.Depth,
			Distance: result.TotalWeight,
			Document: result.Document,
		}
		if len(result.Path) > 0 {
			node.Path = make([]string, len(result.Path))
			for i, pathKey := range result.Path {
				node.Path[i] = base64.StdEncoding.EncodeToString(pathKey)
			}
		}
		if len(result.PathEdges) > 0 {
			node.PathEdges = make([]indexes.PathEdge, len(result.PathEdges))
			for i, edge := range result.PathEdges {
				node.PathEdges[i] = indexes.PathEdge{
					Source:   base64.StdEncoding.EncodeToString(edge.Source),
					Target:   base64.StdEncoding.EncodeToString(edge.Target),
					Type:     edge.Type,
					Weight:   edge.Weight,
					Metadata: edge.Metadata,
				}
			}
		}
		if query.IncludeDocuments && node.Document == nil {
			doc, err := db.Get(ctx, result.Key)
			if err == nil {
				node.Document = doc
			}
		}
		if query.IncludeEdges {
			edges, err := db.GetEdges(ctx, query.IndexName, result.Key, "", parseDirection(query.Params.Direction))
			if err == nil {
				node.Edges = edges
			}
		}
		if len(query.Fields) > 0 && node.Document != nil {
			node.Document = ProjectFields(node.Document, query.Fields)
		}
		nodes = append(nodes, node)
	}
	return &indexes.GraphQueryResult{
		Type:  query.Type,
		Nodes: nodes,
		Total: len(nodes),
	}, nil
}

func resolveGraphNodeSelector(
	selector indexes.GraphNodeSelector,
	searchResult *indexes.RemoteIndexSearchResult,
) ([][]byte, error) {
	if len(selector.Keys) > 0 {
		keys := make([][]byte, 0, len(selector.Keys))
		for _, keyStr := range selector.Keys {
			key, err := base64.StdEncoding.DecodeString(keyStr)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 key: %w", err)
			}
			keys = append(keys, key)
		}
		return keys, nil
	}

	if selector.ResultRef != "" {
		return resolveGraphResultRef(selector.ResultRef, selector.Limit, searchResult)
	}

	return nil, fmt.Errorf("node selector must specify keys or result_ref")
}

func resolveGraphResultRef(
	ref string,
	limit int,
	searchResult *indexes.RemoteIndexSearchResult,
) ([][]byte, error) {
	if searchResult == nil {
		return nil, fmt.Errorf("cannot resolve result reference without search results")
	}
	switch {
	case ref == "$full_text_results":
		if searchResult.BleveSearchResult == nil {
			return nil, fmt.Errorf("no full-text results available")
		}
		return extractKeysFromBleve(searchResult.BleveSearchResult, limit), nil
	case ref == "$fusion_results":
		if searchResult.FusionResult == nil {
			return nil, fmt.Errorf("no fusion results available")
		}
		ids := make([]string, 0, len(searchResult.FusionResult.Hits))
		for _, hit := range searchResult.FusionResult.Hits {
			ids = append(ids, hit.ID)
		}
		return extractKeysFromHitIDs(ids, limit), nil
	case strings.HasPrefix(ref, "$aknn_results."), strings.HasPrefix(ref, "$embeddings_results."):
		indexName := strings.TrimPrefix(strings.TrimPrefix(ref, "$aknn_results."), "$embeddings_results.")
		if searchResult.VectorSearchResult == nil {
			return nil, fmt.Errorf("no vector results available")
		}
		vectorResult := searchResult.VectorSearchResult[indexName]
		if vectorResult == nil {
			return nil, fmt.Errorf("vector index not found: %s", indexName)
		}
		return extractKeysFromVector(vectorResult, limit), nil
	case strings.HasPrefix(ref, "$graph_results."):
		queryName := strings.TrimPrefix(ref, "$graph_results.")
		if searchResult.GraphResults == nil {
			return nil, fmt.Errorf("no graph results available")
		}
		graphResult := searchResult.GraphResults[queryName]
		if graphResult == nil {
			return nil, fmt.Errorf("graph query not found: %s", queryName)
		}
		return extractKeysFromGraph(graphResult, limit), nil
	default:
		return nil, fmt.Errorf("unknown result reference: %s", ref)
	}
}

func (db *ZigCoreDB) encodeIndexConfig(config indexes.IndexConfig) (zbridge.AddIndexPayload, error) {
	var (
		kind       string
		configJSON []byte
	)
	switch config.Type {
	case indexes.IndexTypeFullText:
		kind = "full_text"
		specific, serr := config.AsFullTextIndexConfig()
		if serr != nil {
			return zbridge.AddIndexPayload{}, serr
		}
		payload := map[string]any{
			"mem_only": specific.MemOnly,
		}
		if analysisConfig := db.textAnalysisConfig(); analysisConfig != nil {
			payload["analysis_config"] = analysisConfig
		}
		var err error
		configJSON, err = json.Marshal(payload)
		if err != nil {
			return zbridge.AddIndexPayload{}, err
		}
	case indexes.IndexTypeGraph:
		kind = "graph"
		specific, serr := config.AsGraphIndexConfig()
		if serr != nil {
			return zbridge.AddIndexPayload{}, serr
		}
		var err error
		configJSON, err = json.Marshal(specific)
		if err != nil {
			return zbridge.AddIndexPayload{}, err
		}
	default:
		specific, serr := config.AsEmbeddingsIndexConfig()
		if serr != nil {
			return zbridge.AddIndexPayload{}, serr
		}
		if specific.Field == "" {
			return zbridge.AddIndexPayload{}, zigUnsupported("AddIndex embeddings without field")
		}
		if specific.Sparse {
			kind = "sparse_vector"
			configJSON, serr = json.Marshal(map[string]any{
				"field": specific.Field,
			})
		} else {
			kind = "dense_vector"
			metric := string(specific.DistanceMetric)
			if metric == "" {
				metric = string(indexes.DistanceMetricL2Squared)
			}
			configJSON, serr = json.Marshal(map[string]any{
				"field":  specific.Field,
				"dims":   specific.Dimension,
				"metric": metric,
			})
		}
		if serr != nil {
			return zbridge.AddIndexPayload{}, serr
		}
	}
	return zbridge.AddIndexPayload{
		Name:       config.Name,
		Kind:       kind,
		ConfigJSON: string(configJSON),
	}, nil
}

func graphDirection(direction indexes.EdgeDirection) uint8 {
	switch direction {
	case indexes.EdgeDirection("in"):
		return 1
	case indexes.EdgeDirection("both"):
		return 2
	default:
		return 0
	}
}

func decodeEdgePayload(payload zbridge.EdgePayload) (indexes.Edge, error) {
	source, err := zbridge.DecodeBase64(payload.SourceB64)
	if err != nil {
		return indexes.Edge{}, err
	}
	target, err := zbridge.DecodeBase64(payload.TargetB64)
	if err != nil {
		return indexes.Edge{}, err
	}
	var metadata map[string]interface{}
	if payload.MetadataJSON != "" {
		if err := json.Unmarshal([]byte(payload.MetadataJSON), &metadata); err != nil {
			return indexes.Edge{}, err
		}
	}
	return indexes.Edge{
		Source:    source,
		Target:    target,
		Type:      payload.EdgeType,
		Weight:    payload.Weight,
		CreatedAt: time.Unix(int64(payload.CreatedAt), 0),
		UpdatedAt: time.Unix(int64(payload.UpdatedAt), 0),
		Metadata:  metadata,
	}, nil
}

func decodeExtractedEdge(payload zbridge.ExtractGraphWritePayload, source, target []byte) (*indexes.Edge, error) {
	var metadata map[string]interface{}
	if payload.MetadataJSON != "" {
		if err := json.Unmarshal([]byte(payload.MetadataJSON), &metadata); err != nil {
			return nil, err
		}
	}
	return &indexes.Edge{
		Source:    source,
		Target:    target,
		Type:      payload.EdgeType,
		Weight:    payload.Weight,
		CreatedAt: time.Unix(int64(payload.CreatedAt), 0),
		UpdatedAt: time.Unix(int64(payload.UpdatedAt), 0),
		Metadata:  metadata,
	}, nil
}

func decodeTraversalPayloads(payloads []zbridge.TraversalResultPayload) ([]*indexes.TraversalResult, error) {
	result := make([]*indexes.TraversalResult, 0, len(payloads))
	for _, payload := range payloads {
		key, err := zbridge.DecodeBase64(payload.KeyB64)
		if err != nil {
			return nil, err
		}
		var path [][]byte
		if len(payload.PathB64) > 0 {
			path = make([][]byte, 0, len(payload.PathB64))
			for _, entry := range payload.PathB64 {
				decoded, err := zbridge.DecodeBase64(entry)
				if err != nil {
					return nil, err
				}
				path = append(path, decoded)
			}
		}
		result = append(result, &indexes.TraversalResult{
			Key:         key,
			Depth:       int(payload.Depth),
			TotalWeight: payload.TotalWeight,
			Path:        path,
		})
	}
	return result, nil
}

func decodePathPayload(payload *zbridge.PathPayload) (*indexes.Path, error) {
	nodes := make([]string, 0, len(payload.NodesB64))
	for _, entry := range payload.NodesB64 {
		decoded, err := zbridge.DecodeBase64(entry)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, string(decoded))
	}
	edges := make([]indexes.PathEdge, 0, len(payload.Edges))
	for _, entry := range payload.Edges {
		source, err := zbridge.DecodeBase64(entry.SourceB64)
		if err != nil {
			return nil, err
		}
		target, err := zbridge.DecodeBase64(entry.TargetB64)
		if err != nil {
			return nil, err
		}
		edges = append(edges, indexes.PathEdge{
			Source: string(source),
			Target: string(target),
			Type:   entry.EdgeType,
			Weight: entry.Weight,
		})
	}
	return &indexes.Path{
		Nodes:       nodes,
		Edges:       edges,
		Length:      int(payload.Length),
		TotalWeight: payload.TotalWeight,
	}, nil
}

func decodePathPayloads(payloads []zbridge.PathPayload) ([]indexes.Path, error) {
	result := make([]indexes.Path, 0, len(payloads))
	for i := range payloads {
		path, err := decodePathPayload(&payloads[i])
		if err != nil {
			return nil, err
		}
		result = append(result, *path)
	}
	return result, nil
}
