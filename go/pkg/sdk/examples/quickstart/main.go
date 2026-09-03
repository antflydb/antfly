// Command quickstart is the compile-checked Go counterpart to the documentation
// quickstart. It expects Antfly at http://localhost:8080 and the Wikipedia JSONL
// fixture to have been loaded by the shared CLI ingestion step.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	antfly "github.com/antflydb/antfly/go/pkg/sdk"
	"github.com/antflydb/antfly/go/pkg/sdk/query"
)

func main() {
	ctx := context.Background()
	client, err := antfly.NewAntflyClient("http://localhost:8080", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	embedder, err := antfly.NewEmbedderConfig(antfly.AntflyEmbedderConfig{
		Model: "antflydb/clipclap",
	})
	if err != nil {
		log.Fatal(err)
	}
	titleBody, err := antfly.NewCreateIndexRequest(antfly.EmbeddingsIndexConfig{
		Template: "{{title}} {{body}}",
		Embedder: *embedder,
		Chunker: antfly.ChunkerConfig{
			Provider: antfly.ChunkerProviderAntfly,
			Text: antfly.TextChunkOptions{
				TargetTokens:  200,
				OverlapTokens: 25,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := client.CreateTable(ctx, "wikipedia", antfly.CreateTableRequest{
		Indexes: map[string]antfly.CreateIndexRequest{"title_body": *titleBody},
	}); err != nil {
		log.Fatal(err)
	}

	fullText := query.NewQueryString(`body:"Korea"`)
	results, err := client.Query(ctx, antfly.QueryRequest{
		Table:          "wikipedia",
		FullTextSearch: &fullText,
		Fields:         []string{"title", "url"},
		Limit:          5,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("full text: %+v\n", results)

	results, err = client.Query(ctx, antfly.QueryRequest{
		Table:          "wikipedia",
		SemanticSearch: "anatomy and physiology",
		Indexes:        []string{"title_body"},
		Fields:         []string{"title", "url"},
		Limit:          5,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("semantic: %+v\n", results)

	reranker, err := antfly.NewRerankerConfig(antfly.AntflyRerankerConfig{
		Model: "cross-encoder/ms-marco-MiniLM-L6-v2",
	})
	if err != nil {
		log.Fatal(err)
	}
	reranker.Field = "body"
	fullText = query.NewQueryString("body:Einstein")
	results, err = client.Query(ctx, antfly.QueryRequest{
		Table:          "wikipedia",
		FullTextSearch: &fullText,
		SemanticSearch: "theory of relativity and physics",
		Indexes:        []string{"title_body"},
		Fields:         []string{"title", "url"},
		Limit:          10,
		Reranker:       reranker,
		Pruner:         antfly.Pruner{MinScoreRatio: 0.01},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("hybrid: %+v\n", results)

	thumbnailRequest, err := antfly.NewCreateIndexRequest(antfly.EmbeddingsIndexConfig{
		CoveragePolicy: antfly.DerivedCoveragePolicyPartial,
		Template:       "{{#if thumbnail_url}}{{remoteMedia url=thumbnail_url}}{{/if}}",
		Embedder:       *embedder,
		Dimension:      512,
		DistanceMetric: antfly.DistanceMetricCosine,
	})
	if err != nil {
		log.Fatal(err)
	}
	createdIndex, err := client.CreateIndex(ctx, "wikipedia", "thumbnail", *thumbnailRequest)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("thumbnail index: %+v\n", createdIndex)

	results, err = client.Query(ctx, antfly.QueryRequest{
		Table:          "wikipedia",
		SemanticSearch: "map of a country",
		Indexes:        []string{"thumbnail"},
		Fields:         []string{"title", "url", "thumbnail_url"},
		Limit:          5,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("image: %+v\n", results)

	generator, err := antfly.NewGeneratorConfig(antfly.AntflyGeneratorConfig{
		Model:     "ggml-org/gemma-4-E4B-it-GGUF",
		MaxTokens: 128,
	})
	if err != nil {
		log.Fatal(err)
	}
	fields := []string{"title", "body"}
	agentResult, err := client.RetrievalAgent(ctx, antfly.RetrievalAgentRequest{
		Queries: []antfly.RetrievalQueryRequest{{
			Table:          "wikipedia",
			SemanticSearch: "What are the major events in Korean history?",
			Indexes:        []string{"title_body"},
			Fields:         &fields,
			Limit:          5,
			Reranker:       reranker,
			Pruner: antfly.Pruner{
				MinScoreRatio:      0.6,
				MaxScoreGapPercent: 40,
			},
		}},
		Generator:        *generator,
		MaxContextTokens: 512,
		Steps: antfly.RetrievalAgentSteps{
			Classification: antfly.ClassificationStepConfig{Enabled: true, WithReasoning: true},
			Generation:     antfly.GenerationStepConfig{Enabled: true},
			Followup:       antfly.FollowupStepConfig{Enabled: true},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("RAG: %+v\n", agentResult)
}
