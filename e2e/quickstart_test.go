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

package e2e

import (
	"context"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/termite/pkg/termite/lib/modelregistry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_Quickstart mirrors the quickstart guide: creates a table with a
// Termite BGE text embedder and a CLIP image embedder, inserts documents with
// thumbnail URLs, and runs text search, image search, hybrid search, and RAG.
func TestE2E_Quickstart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	// Ensure required models are downloaded
	ensureRegistryModel(t, "BAAI/bge-small-en-v1.5", modelregistry.ModelTypeEmbedder, []string{modelregistry.VariantF32})
	ensureRegistryModel(t, "openai/clip-vit-base-patch32", modelregistry.ModelTypeEmbedder, []string{modelregistry.VariantF32})
	ensureRegistryModel(t, "mixedbread-ai/mxbai-rerank-base-v1", modelregistry.ModelTypeReranker, []string{modelregistry.VariantF32})
	ensureHuggingFaceModel(t, "onnx-community/gemma-3-270m-it-ONNX", modelregistry.ModelTypeGenerator)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Start swarm with Termite for BGE + CLIP + Gemma-3-ONNX.
	t.Log("Starting Antfly swarm with Termite...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{})
	defer swarm.Cleanup()

	termiteURL := GetTermiteURL()
	if termiteURL == "" {
		t.Fatal("Termite URL not set — swarm should have started Termite")
	}

	tableName := "wikipedia"

	// --- Create table with two indexes matching quickstart guide ---

	// Text embedder: BGE via Termite
	bgeEmbedder, err := antfly.NewEmbedderConfig(antfly.TermiteEmbedderConfig{
		Model:  "BAAI/bge-small-en-v1.5",
		ApiUrl: termiteURL,
	})
	require.NoError(t, err)

	chunker := antfly.ChunkerConfig{}
	err = chunker.FromAntflyChunkerConfig(antfly.AntflyChunkerConfig{
		Text: antfly.TextChunkOptions{
			TargetTokens:  200,
			OverlapTokens: 25,
		},
	})
	require.NoError(t, err)

	titleBodyIndex := antfly.IndexConfig{
		Name: "title_body",
		Type: "aknn_v0",
	}
	err = titleBodyIndex.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Template: "{{title}} {{body}}",
		Embedder: *bgeEmbedder,
		Chunker:  chunker,
	})
	require.NoError(t, err)

	// Image embedder: CLIP via Termite
	clipEmbedder, err := antfly.NewEmbedderConfig(antfly.TermiteEmbedderConfig{
		Model:  "openai/clip-vit-base-patch32",
		ApiUrl: termiteURL,
	})
	require.NoError(t, err)

	thumbnailIndex := antfly.IndexConfig{
		Name: "thumbnail",
		Type: "aknn_v0",
	}
	err = thumbnailIndex.FromEmbeddingsIndexConfig(antfly.EmbeddingsIndexConfig{
		Dimension:      512,
		Template:       "{{media url=thumbnail_url}}",
		Embedder:       *clipEmbedder,
		DistanceMetric: antfly.DistanceMetricCosine,
	})
	require.NoError(t, err)

	err = swarm.Client.CreateTable(ctx, tableName, antfly.CreateTableRequest{
		NumShards: 1,
		Indexes: map[string]antfly.IndexConfig{
			"title_body": titleBodyIndex,
			"thumbnail":  thumbnailIndex,
		},
	})
	require.NoError(t, err, "Failed to create table")
	waitForShardsReady(t, ctx, swarm.Client, tableName, 30*time.Second)
	t.Log("Table created with BGE text index + CLIP thumbnail index")

	// --- Insert documents (some with thumbnails, some without) ---

	cdnImages := "https://cdn.antfly.io/datasets/wiki-articles-10k-v001-images"

	docs := map[string]any{
		"Korean History": map[string]any{
			"title":         "Korean History",
			"url":           "https://en.wikipedia.org/wiki/History_of_Korea",
			"body":          "Korea has a long and rich history spanning thousands of years. The Korean Peninsula has been home to various kingdoms including Goguryeo, Baekje, and Silla during the Three Kingdoms period. The Joseon Dynasty ruled from 1392 to 1897 and established many cultural traditions still practiced today.",
			"thumbnail_url": cdnImages + "/wikipedia/commons/thumb/5/53/History_of_Korea-476.PNG/330px-History_of_Korea-476.PNG",
		},
		"Theory of Relativity": map[string]any{
			"title":         "Theory of Relativity",
			"url":           "https://en.wikipedia.org/wiki/Theory_of_relativity",
			"body":          "Albert Einstein published the theory of special relativity in 1905 and general relativity in 1915. Special relativity deals with objects moving at constant speeds near the speed of light. General relativity describes gravity as the curvature of spacetime caused by mass and energy.",
			"thumbnail_url": cdnImages + "/wikipedia/commons/thumb/d/d3/Albert_Einstein_Head.jpg/330px-Albert_Einstein_Head.jpg",
		},
		"Quantum Physics": map[string]any{
			"title": "Quantum Physics",
			"url":   "https://en.wikipedia.org/wiki/Quantum_mechanics",
			"body":  "Quantum mechanics is a fundamental theory in physics that describes nature at the smallest scales of energy levels of atoms and subatomic particles. It was developed in the early 20th century by Max Planck, Niels Bohr, Werner Heisenberg, and Erwin Schrödinger among others.",
			// No thumbnail — tests that CLIP index handles missing images gracefully
		},
		"Ancient Rome": map[string]any{
			"title":         "Ancient Rome",
			"url":           "https://en.wikipedia.org/wiki/Ancient_Rome",
			"body":          "Ancient Rome was a civilization that grew from a small town on the Tiber River into an empire that encompassed most of continental Europe, Britain, western Asia, and northern Africa. The Roman Republic was established in 509 BC and transitioned to the Roman Empire in 27 BC under Augustus.",
			"thumbnail_url": cdnImages + "/wikipedia/commons/thumb/d/de/Colosseo_2020.jpg/330px-Colosseo_2020.jpg",
		},
		"Machine Learning": map[string]any{
			"title":         "Machine Learning",
			"url":           "https://en.wikipedia.org/wiki/Machine_learning",
			"body":          "Machine learning is a branch of artificial intelligence that focuses on building systems that learn from data. It uses algorithms and statistical models to analyze and draw inferences from patterns in data. Deep learning, a subset of machine learning, uses neural networks with many layers.",
			"thumbnail_url": cdnImages + "/wikipedia/commons/thumb/f/fe/Kernel_Machine.svg/330px-Kernel_Machine.svg.png",
		},
	}

	_, err = swarm.Client.Batch(ctx, tableName, antfly.BatchRequest{
		Inserts:   docs,
		SyncLevel: antfly.SyncLevelAknn,
	})
	require.NoError(t, err, "Failed to insert documents")
	t.Logf("Inserted %d documents (4 with thumbnails, 1 without)", len(docs))

	// --- Wait for text embeddings ---
	waitForEmbeddings(t, ctx, swarm.Client, tableName, "title_body", len(docs), 5*time.Minute)

	// --- Semantic text search ---
	t.Log("Running semantic text search...")
	results, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "theory of relativity and physics",
		Indexes:        []string{"title_body"},
		Fields:         []string{"title", "url"},
		Limit:          5,
	})
	require.NoError(t, err, "Semantic search failed")
	require.NotEmpty(t, results.Responses)
	hits := results.Responses[0].Hits.Hits
	require.NotEmpty(t, hits, "Expected search results")
	t.Logf("Semantic search returned %d hits", len(hits))
	for i, hit := range hits {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// Verify physics-related documents rank well
	topIDs := make([]string, 0, len(hits))
	for _, hit := range hits {
		topIDs = append(topIDs, hit.ID)
	}
	physicsDocIDs := map[string]bool{
		"Theory of Relativity": true,
		"Quantum Physics":      true,
	}
	foundPhysics := 0
	for _, id := range topIDs[:min(3, len(topIDs))] {
		if physicsDocIDs[id] {
			foundPhysics++
		}
	}
	require.GreaterOrEqual(t, foundPhysics, 1,
		"Expected at least 1 physics document in top-3 results, got %d (top IDs: %v)", foundPhysics, topIDs)

	// --- Image search via CLIP ---
	// Wait for CLIP embeddings (only docs with thumbnail_url get embedded)
	waitForEmbeddings(t, ctx, swarm.Client, tableName, "thumbnail", 4, 5*time.Minute)

	t.Log("Running image search via CLIP...")
	results, err = swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "ancient building architecture",
		Indexes:        []string{"thumbnail"},
		Fields:         []string{"title", "thumbnail_url"},
		Limit:          5,
	})
	require.NoError(t, err, "Image search failed")
	require.NotEmpty(t, results.Responses)
	imageHits := results.Responses[0].Hits.Hits
	require.NotEmpty(t, imageHits, "Expected image search results")
	t.Logf("Image search returned %d hits", len(imageHits))
	for i, hit := range imageHits {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// The Colosseum thumbnail should rank well for "ancient building architecture"
	assert.Equal(t, "Ancient Rome", imageHits[0].ID,
		"Expected Ancient Rome (Colosseum) as top image result")

	// --- Hybrid search with reranker ---
	t.Log("Running hybrid search with reranker...")
	rerankerConfig, err := antfly.NewRerankerConfig(antfly.TermiteRerankerConfig{
		Model: "mixedbread-ai/mxbai-rerank-base-v1",
		Url:   termiteURL,
	})
	require.NoError(t, err)
	rerankerConfig.Field = "body"

	results, err = swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "theory of relativity and physics",
		Indexes:        []string{"title_body"},
		Fields:         []string{"title", "url"},
		Limit:          5,
		Reranker:       rerankerConfig,
		Pruner:         antfly.Pruner{MinScoreRatio: 0.01},
	})
	require.NoError(t, err, "Hybrid search with reranker failed")
	require.NotEmpty(t, results.Responses)
	rerankedHits := results.Responses[0].Hits.Hits
	require.NotEmpty(t, rerankedHits, "Expected reranked search results")
	t.Logf("Hybrid search with reranker returned %d hits", len(rerankedHits))
	for i, hit := range rerankedHits {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// --- Multi-index hybrid search (text + image) ---
	t.Log("Running multi-index hybrid search (title_body + thumbnail)...")
	results, err = swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:          tableName,
		SemanticSearch: "ancient civilizations and archaeology",
		Indexes:        []string{"title_body", "thumbnail"},
		Fields:         []string{"title", "url", "thumbnail_url"},
		Limit:          5,
	})
	require.NoError(t, err, "Multi-index hybrid search failed")
	require.NotEmpty(t, results.Responses)
	multiHits := results.Responses[0].Hits.Hits
	require.NotEmpty(t, multiHits, "Expected multi-index search results")
	t.Logf("Multi-index hybrid search returned %d hits", len(multiHits))
	for i, hit := range multiHits {
		t.Logf("  [%d] id=%s score=%.4f", i, hit.ID, hit.Score)
	}

	// Ancient Rome should appear in top results (strong signal from both text and image)
	foundRome := false
	for _, hit := range multiHits[:min(3, len(multiHits))] {
		if hit.ID == "Ancient Rome" {
			foundRome = true
			break
		}
	}
	assert.True(t, foundRome, "Expected Ancient Rome in top-3 multi-index results")

	// --- RAG with Termite Gemma-3-ONNX ---
	t.Log("Running RAG query...")
	generatorConfig, err := antfly.NewGeneratorConfig(antfly.TermiteGeneratorConfig{
		Model:       "onnx-community/gemma-3-270m-it-ONNX",
		ApiUrl:      termiteURL,
		Temperature: 0.1,
		MaxTokens:   256,
	})
	require.NoError(t, err)

	ragResult, err := swarm.Client.RetrievalAgent(ctx, antfly.RetrievalAgentRequest{
		Query: "What is the theory of relativity?",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          tableName,
				SemanticSearch: "What is the theory of relativity?",
				Indexes:        []string{"title_body"},
				Fields:         []string{"title", "body"},
				Limit:          3,
			},
		},
		Generator:        *generatorConfig,
		MaxContextTokens: 512,
		MaxIterations:    0, // Pipeline mode: execute queries directly
		Stream:           false,
		Steps: antfly.RetrievalAgentSteps{
			Generation: antfly.GenerationStepConfig{
				Enabled: true,
			},
		},
	})
	require.NoError(t, err, "RAG query failed")
	require.NotNil(t, ragResult)
	require.NotEmpty(t, ragResult.Generation, "Expected a generated answer")
	t.Logf("RAG answer (first 200 chars): %.200s", ragResult.Generation)

	// --- RAG with classification, reasoning, followup (non-streaming) ---
	t.Log("Running RAG with classify + reasoning + followup (non-streaming)...")
	ragFullResult, err := swarm.Client.RetrievalAgent(ctx, antfly.RetrievalAgentRequest{
		Query: "What are the major events in Korean history?",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          tableName,
				SemanticSearch: "What are the major events in Korean history?",
				Indexes:        []string{"title_body"},
				Fields:         []string{"title", "body"},
				Limit:          5,
			},
		},
		Generator:        *generatorConfig,
		MaxContextTokens: 2048,
		MaxIterations:    0,
		Stream:           false,
		Steps: antfly.RetrievalAgentSteps{
			Classification: antfly.ClassificationStepConfig{
				Enabled:       true,
				WithReasoning: true,
			},
			Generation: antfly.GenerationStepConfig{
				Enabled: true,
			},
			Followup: antfly.FollowupStepConfig{
				Enabled: true,
			},
		},
	})
	require.NoError(t, err, "RAG with full steps failed")
	require.NotNil(t, ragFullResult)

	t.Logf("Classification: strategy=%s route=%s confidence=%.2f",
		ragFullResult.Classification.Strategy,
		ragFullResult.Classification.RouteType,
		ragFullResult.Classification.Confidence)
	t.Logf("Classification reasoning: %s", ragFullResult.Classification.Reasoning)
	t.Logf("Generation (first 200 chars): %.200s", ragFullResult.Generation)
	t.Logf("Followup questions: %v", ragFullResult.FollowupQuestions)

	assert.NotEmpty(t, ragFullResult.Classification.Strategy, "Expected classification strategy")
	assert.NotEmpty(t, ragFullResult.Classification.RouteType, "Expected classification route type")
	assert.NotEmpty(t, ragFullResult.Classification.Reasoning, "Expected classification reasoning (--reasoning flag)")
	assert.NotEmpty(t, ragFullResult.Generation, "Expected a generated answer")
	assert.NotEmpty(t, ragFullResult.FollowupQuestions, "Expected followup questions (--followup flag)")

	// --- RAG with classification, reasoning, followup (streaming) ---
	t.Log("Running RAG with classify + reasoning + followup (streaming)...")
	var streamedReasoning []string
	var streamedFollowups []string
	var streamedGeneration []string

	ragStreamResult, err := swarm.Client.RetrievalAgent(ctx, antfly.RetrievalAgentRequest{
		Query: "What are the major events in Korean history?",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          tableName,
				SemanticSearch: "What are the major events in Korean history?",
				Indexes:        []string{"title_body"},
				Fields:         []string{"title", "body"},
				Limit:          5,
			},
		},
		Generator:        *generatorConfig,
		MaxContextTokens: 2048,
		MaxIterations:    0,
		Stream:           true,
		Steps: antfly.RetrievalAgentSteps{
			Classification: antfly.ClassificationStepConfig{
				Enabled:       true,
				WithReasoning: true,
			},
			Generation: antfly.GenerationStepConfig{
				Enabled: true,
			},
			Followup: antfly.FollowupStepConfig{
				Enabled: true,
			},
		},
	}, antfly.RetrievalAgentOptions{
		OnReasoning: func(chunk string) error {
			streamedReasoning = append(streamedReasoning, chunk)
			return nil
		},
		OnGeneration: func(chunk string) error {
			streamedGeneration = append(streamedGeneration, chunk)
			return nil
		},
		OnFollowup: func(question string) error {
			streamedFollowups = append(streamedFollowups, question)
			return nil
		},
	})
	require.NoError(t, err, "RAG streaming with full steps failed")
	require.NotNil(t, ragStreamResult)

	t.Logf("Streamed reasoning chunks: %d", len(streamedReasoning))
	t.Logf("Streamed generation chunks: %d", len(streamedGeneration))
	t.Logf("Streamed followup questions: %v", streamedFollowups)

	assert.NotEmpty(t, streamedReasoning, "Expected streaming reasoning chunks via OnReasoning callback")
	assert.NotEmpty(t, streamedGeneration, "Expected streaming generation chunks via OnGeneration callback")
	assert.NotEmpty(t, streamedFollowups, "Expected streaming followup questions via OnFollowup callback")

	t.Log("Quickstart e2e test passed")
}
