// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	storeclient "github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
)

// ExecutionProvider selects the query-plane shard index implementation and the
// control-plane StoreRPC implementation for a metadata runtime.
type ExecutionProvider interface {
	MaterializeIndexes(
		httpClient *http.Client,
		plan *indexes.BaseShardIndexPlan,
		peers map[types.ID][]string,
	) (indexes.ShardIndexes, error)
	StoreClientFactory() tablemgr.StoreClientFactory
}

var ErrLocalStoreNotBound = errors.New("local execution provider store is not bound")

type remoteExecutionProvider struct{}

func DefaultExecutionProvider() ExecutionProvider {
	return remoteExecutionProvider{}
}

func (remoteExecutionProvider) MaterializeIndexes(
	httpClient *http.Client,
	plan *indexes.BaseShardIndexPlan,
	peers map[types.ID][]string,
) (indexes.ShardIndexes, error) {
	return indexes.MakeRemoteIndexesFromPlan(httpClient, plan, peers)
}

func (remoteExecutionProvider) StoreClientFactory() tablemgr.StoreClientFactory {
	return func(client *http.Client, id types.ID, url string) storeclient.StoreRPC {
		return storeclient.NewStoreClient(client, id, url)
	}
}

type localExecutionProvider struct {
	mu    sync.RWMutex
	store store.StoreIface
}

func NewLocalExecutionProvider(s store.StoreIface) ExecutionProvider {
	p := NewDeferredLocalExecutionProvider()
	p.BindStore(s)
	return p
}

func NewDeferredLocalExecutionProvider() *localExecutionProvider {
	return &localExecutionProvider{}
}

func (p *localExecutionProvider) BindStore(s store.StoreIface) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = s
}

func (p *localExecutionProvider) storeOrErr() (store.StoreIface, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.store == nil {
		return nil, ErrLocalStoreNotBound
	}
	return p.store, nil
}

func (p *localExecutionProvider) MaterializeIndexes(
	_ *http.Client,
	plan *indexes.BaseShardIndexPlan,
	_ map[types.ID][]string,
) (indexes.ShardIndexes, error) {
	if _, err := p.storeOrErr(); err != nil {
		return nil, err
	}
	searcher := indexes.ShardSearcher(&storeShardSearcher{store: p.storeOrErr})
	return indexes.MakeLocalIndexesFromPlan(searcher, plan), nil
}

func (p *localExecutionProvider) StoreClientFactory() tablemgr.StoreClientFactory {
	return func(_ *http.Client, id types.ID, _ string) storeclient.StoreRPC {
		return storeclient.NewDeferredLocalStoreClient(id, p.storeOrErr)
	}
}

// storeShardSearcher adapts store.StoreIface to indexes.ShardSearcher.
type storeShardSearcher struct {
	store func() (store.StoreIface, error)
}

func (s *storeShardSearcher) SearchShardTyped(
	ctx context.Context,
	shardID types.ID,
	req *indexes.RemoteIndexSearchRequest,
) (*indexes.RemoteIndexSearchResult, error) {
	st, err := s.store()
	if err != nil {
		return nil, err
	}
	shard, ok := st.Shard(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}
	return shard.SearchTyped(ctx, req)
}
