/*
Copyright 2025 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package antfly

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/query"
	"github.com/cespare/xxhash/v2"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
)

// The provider used in the registry.
const provider = "antfly"

// The metadata key used to hold document text.
const textKey = "text"

// The metadata key to hold document metadata.
const metadataKey string = "metadata"

// Antfly passes configuration options to the plugin.
type Antfly struct {
	BaseURL string // Base URL of the Antfly instance.

	client  *antfly.AntflyClient // Client for the Antfly database.
	mu      sync.Mutex           // Mutex to control access.
	initted bool                 // Whether the plugin has been initialized.
}

// Name returns the name of the plugin.
func (w *Antfly) Name() string {
	return provider
}

// Init initializes the Antfly plugin.
func (w *Antfly) Init(ctx context.Context) []api.Action {
	if w == nil {
		w = &Antfly{}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.initted {
		panic("plugin already initialized")
	}

	var url string
	if url == "" {
		url = os.Getenv("ANTFLY_URL")
	}
	if url == "" {
		url = "http://localhost:8080/api/v1"
	}

	client, err := antfly.NewAntflyClient(url, http.DefaultClient)
	if err != nil {
		panic(fmt.Errorf("antfly.Init: initialization failed: %v", err))
	}

	w.BaseURL = url
	w.client = client
	w.initted = true

	return []api.Action{}
}

// IndexConfig holds configuration options for a retriever.
// Antfly stores data in tables and indexes.
// Use a separate genkit Retriever for each different index.
type IndexConfig struct {
	// The antfly table name. May not be the empty string.
	TableName string
	IndexName string
}

// DefineRetriever defines [ai.Retriever]
// that use the same class.
// The name uniquely identifies the Retriever in the registry.
func DefineRetriever(ctx context.Context, g *genkit.Genkit, cfg IndexConfig, opts *ai.RetrieverOptions) (*Docstore, ai.Retriever, error) {
	if cfg.TableName == "" {
		return nil, nil, errors.New("antfly: table name required")
	}
	if cfg.IndexName == "" {
		return nil, nil, errors.New("antfly: index name required")
	}

	w, _ := genkit.LookupPlugin(g, provider).(*Antfly)
	if w == nil {
		return nil, nil, errors.New("antfly plugin not found; did you call genkit.Init with the antfly plugin?")
	}

	ds, err := w.newDocstore(ctx, &cfg)
	if err != nil {
		return nil, nil, err
	}
	log.Println("Defined antfly retriever for table/index:", cfg.TableName, ":", cfg.IndexName)
	retriever := genkit.DefineRetriever(g, api.NewName(provider, cfg.TableName+":"+cfg.IndexName), opts, ds.Retrieve)
	return ds, retriever, nil
}

// Docstore defines a Retriever.
type Docstore struct {
	Client    *antfly.AntflyClient
	TableName string
	IndexName string
}

// newDocstore creates a Docstore.
func (w *Antfly) newDocstore(ctx context.Context, cfg *IndexConfig) (*Docstore, error) {
	if w.client == nil {
		return nil, errors.New("antfly.Init not called")
	}

	tableStatus, err := w.client.GetTable(ctx, cfg.TableName)
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return nil, fmt.Errorf("antfly get table %q failed: %v", cfg.TableName, err)
		} else {
			modelConfig, err := antfly.NewEmbedderConfig(antfly.OllamaEmbedderConfig{Model: "all-minilm"})
			if err != nil {
				return nil, fmt.Errorf("antfly model config failed: %v", err)
			}
			indexConfig, err := antfly.NewIndexConfig(cfg.IndexName, antfly.EmbeddingsIndexConfig{
				Field:    textKey,
				Embedder: *modelConfig,
			})
			if err != nil {
				return nil, fmt.Errorf("antfly index config failed: %v", err)
			}
			err = w.client.CreateTable(ctx, cfg.TableName, antfly.CreateTableRequest{
				Indexes: map[string]antfly.IndexConfig{
					cfg.IndexName: *indexConfig,
				},
			})
			if err != nil {
				return nil, fmt.Errorf("antfly create table %q failed: %v", cfg.TableName, err)
			}
		}
	} else if tableStatus.Indexes == nil || tableStatus.Indexes[cfg.IndexName].Type == "" {
		modelConfig, err := antfly.NewEmbedderConfig(antfly.OllamaEmbedderConfig{Model: "all-minilm"})
		if err != nil {
			return nil, fmt.Errorf("antfly model config failed: %v", err)
		}
		indexConfig, err := antfly.NewIndexConfig(cfg.IndexName, antfly.EmbeddingsIndexConfig{
			Field:    textKey,
			Embedder: *modelConfig,
		})
		if err != nil {
			return nil, fmt.Errorf("antfly index config failed: %v", err)
		}
		err = w.client.CreateIndex(ctx, cfg.TableName, cfg.IndexName, *indexConfig)
		if err != nil {
			return nil, fmt.Errorf("antfly create index %q failed: %v", cfg.TableName, err)
		}
	}
	ds := &Docstore{
		Client:    w.client,
		TableName: cfg.TableName,
		IndexName: cfg.IndexName,
	}
	return ds, nil
}

// Retriever returns the retriever for the given class.
func Retriever(g *genkit.Genkit, class string) ai.Retriever {
	return genkit.LookupRetriever(g, api.NewName(provider, class))
}

// RetrieverOptions may be passed in the Options field
// [ai.RetrieverRequest] to pass options to Antfly.
// The options field should be either nil or
// a value of type *RetrieverOptions.
type RetrieverOptions struct {
	// Maximum number of values to retrieve.
	Count int `json:"count,omitempty"`
	// Keys to retrieve from document metadata.
	MetadataKeys []string `json:"metadata_keys,omitempty"`
	// Bleve query to filter results.
	FilterQuery *query.Query `json:"-"`
	// OrderBy specifies fields to order by with direction
	OrderBy []antfly.SortField `json:"order_by,omitempty"`
}

// Retrieve implements the genkit Retriever.Retrieve method.
func (ds *Docstore) Retrieve(ctx context.Context, req *ai.RetrieverRequest) (*ai.RetrieverResponse, error) {
	count := 3 // by default we fetch 3 documents
	var metadataKeys []string
	var filterQuery *query.Query
	var orderBy []antfly.SortField
	if req.Options != nil {
		ropt, ok := req.Options.(*RetrieverOptions)
		if !ok {
			return nil, fmt.Errorf("antfly.Retrieve options have type %T, want %T", req.Options, &RetrieverOptions{})
		}
		count = ropt.Count
		metadataKeys = ropt.MetadataKeys
		filterQuery = ropt.FilterQuery
		orderBy = ropt.OrderBy
	}
	var sb strings.Builder
	for _, p := range req.Query.Content {
		sb.WriteString(p.Text)
	}
	var indexes []string
	semanticSearch := sb.String()
	if semanticSearch != "" {
		indexes = []string{ds.IndexName}
	}

	q := antfly.QueryRequest{
		Table:   ds.TableName,
		Indexes: indexes,

		SemanticSearch: semanticSearch,
		// TODO (ajr) Add ability to pass sub keys
		Fields:  []string{textKey, metadataKey},
		Limit:   count,
		OrderBy: orderBy,
	}
	if semanticSearch == "" {
		q.FullTextSearch = filterQuery
	} else {
		q.FilterQuery = filterQuery
	}
	res, err := ds.Client.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("antfly retrieve failed: %v", err)
	}
	if res == nil {
		return nil, errors.New("antfly retrieve returned nil result")
	}
	if len(res.Responses) == 0 {
		return nil, errors.New("antfly retrieve returned empty responses")
	}
	if len(res.Responses) > 1 {
		return nil, fmt.Errorf("antfly retrieve returned %d responses, want 1", len(res.Responses))
	}
	if res.Responses[0].Error != "" {
		return nil, fmt.Errorf("antfly retrieve failed: %v", res.Responses[0].Error)
	}

	var docs []*ai.Document
	for _, dv := range res.Responses[0].Hits.Hits {
		t, ok := dv.Source[textKey]
		if !ok {
			return nil, fmt.Errorf("antfly doc missing key %q", textKey)
		}
		s, ok := t.(string)
		if !ok {
			return nil, fmt.Errorf("antfly text is type %T, want %T", t, "")
		}
		props := map[string]any{}
		origProps, _ := dv.Source[metadataKey].(map[string]any)
		// TODO (ajr) Support this filtering in the query
		for k, v := range origProps {
			if slices.Contains(metadataKeys, k) {
				props[k] = v
			}
		}
		d := ai.DocumentFromText(s, props)
		docs = append(docs, d)
	}

	ret := &ai.RetrieverResponse{
		Documents: docs,
	}
	return ret, nil
}

// Helper function to get started with indexing
func Index(ctx context.Context, docs []*ai.Document, ds *Docstore) error {
	if len(docs) == 0 {
		return nil
	}

	inserts := make(map[string]any, len(docs))
	for _, doc := range docs {
		var sb strings.Builder
		for _, p := range doc.Content {
			sb.WriteString(p.Text)
		}
		m := map[string]any{
			textKey: sb.String(),
		}
		if len(doc.Metadata) > 0 {
			m[metadataKey] = doc.Metadata
		}
		id := xxhash.Sum64String(sb.String())
		inserts[strconv.FormatUint(id, 16)] = m

		metadata := make(map[string]any)
		metadata[textKey] = sb.String()

		if doc.Metadata != nil {
			metadata[metadataKey] = doc.Metadata
		}
	}
	// TODO (ajr) Pass context through
	_, err := ds.Client.Batch(ctx, ds.TableName, antfly.BatchRequest{
		Inserts: inserts,
	})
	if err != nil {
		return fmt.Errorf("antfly insert failed: %v", err)
	}

	return nil
}
