package antfly

import (
	"flag"
	"strings"
	"testing"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/query"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core"
	"github.com/firebase/genkit/go/genkit"
	"github.com/stretchr/testify/require"
)

var (
	testURL   = flag.String("test-antfly-url", "", "Antfly url to use for tests")
	testTable = flag.String("test-antfly-table", "", "Antfly table to use for tests")
	testIndex = flag.String("test-antfly-index", "", "Antfly index to use for tests")
)

func TestGenkit(t *testing.T) {
	if *testURL == "" {
		t.Skip("skipping test because -test-antfly-url flag not used")
	}
	if *testTable == "" {
		t.Skip("skipping test because -test-antfly-table flag not used")
	}
	if *testIndex == "" {
		t.Skip("skipping test because -test-antfly-index flag not used")
	}

	ctx := t.Context()
	af := &Antfly{
		BaseURL: *testURL,
	}

	g := genkit.Init(ctx, genkit.WithPlugins(af))

	err := af.client.DropTable(ctx, *testTable)
	require.NoError(t, err)

	// TODO (ajr) Maybe we want a mock embedder for tests in antfly?
	modelConfig, err := antfly.NewEmbedderConfig(antfly.OllamaEmbedderConfig{Model: "all-minilm"})
	require.NoError(t, err)
	idxConfig, err := antfly.NewIndexConfig(*testIndex, antfly.EmbeddingsIndexConfig{
		Field:    textKey,
		Embedder: *modelConfig,
	})
	require.NoError(t, err)
	err = af.client.CreateTable(ctx, *testTable, antfly.CreateTableRequest{
		Indexes: map[string]antfly.IndexConfig{
			*testIndex: *idxConfig,
		},
	})
	require.NoError(t, err)

	d1 := ai.DocumentFromText("hello1", map[string]any{"name": "hello1"})
	d2 := ai.DocumentFromText("hello2", map[string]any{"name": "hello2"})
	d3 := ai.DocumentFromText("goodbye", map[string]any{"name": "goodbye"})

	classCfg := IndexConfig{
		TableName: *testTable,
		IndexName: *testIndex,
	}
	retOpts := &ai.RetrieverOptions{
		ConfigSchema: core.InferSchemaMap(RetrieverOptions{}),
		Label:        provider,
		Supports: &ai.RetrieverSupports{
			Media: false,
		},
	}
	ds, retriever, err := DefineRetriever(ctx, g, classCfg, retOpts)
	require.NoError(t, err)

	err = Index(ctx, []*ai.Document{d1, d2, d3}, ds)
	require.NoError(t, err, "Index operation failed")

	q := query.BooleanQuery{
		Must: query.ConjunctionQuery{
			Conjuncts: []query.Query{
				query.FuzzyQuery{Field: "name", Term: "hello1", PrefixLength: 5, Fuzziness: query.FuzzinessInt(1)}.ToQuery(),
			},
		},
	}.ToQuery()
	retrieverOptions := &RetrieverOptions{
		Count:        2,
		MetadataKeys: []string{"name"},
		FilterQuery:  &q,
	}
	retrieverResp, err := genkit.Retrieve(ctx, g,
		ai.WithRetriever(retriever),
		ai.WithDocs(d1),
		ai.WithConfig(retrieverOptions))
	require.NoError(t, err, "Retrieve operation failed")

	docs := retrieverResp.Documents
	require.Len(t, docs, 2, "expected 2 documents returned")
	for _, d := range docs {
		text := d.Content[0].Text
		if !strings.HasPrefix(text, "hello") {
			t.Errorf("returned doc text %q does not start with %q", text, "hello")
		}
		name, ok := d.Metadata["name"]
		if !ok {
			t.Errorf("missing metadata entry for name: %v", d.Metadata)
		} else if !strings.HasPrefix(name.(string), "hello") {
			t.Errorf("metadata name entry %q does not start with %q", name, "hello")
		}
	}
}
