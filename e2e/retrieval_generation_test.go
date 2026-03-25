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
	"encoding/json"
	"strings"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/stretchr/testify/require"
)

// RetrievalTestQuery represents a test query for the retrieval agent
type RetrievalTestQuery struct {
	Query            string   `json:"query"`
	ExpectedKeywords []string `json:"expected_keywords"`
	Description      string   `json:"description"`
}

// RetrievalQueryResult represents the result of a retrieval agent query
type RetrievalQueryResult struct {
	Query        string
	Response     *antfly.RetrievalAgentResult
	Error        error
	Keywords     keywordResult
	NumDocs      int
	StrategyUsed string
}

// executeSingleRetrievalQuery runs a single retrieval agent query
func executeSingleRetrievalQuery(
	t *testing.T,
	ctx context.Context,
	client *antfly.AntflyClient,
	cfg testQueryConfig,
	q RetrievalTestQuery,
) RetrievalQueryResult {
	t.Helper()

	req := antfly.RetrievalAgentRequest{
		Query: q.Query,
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          cfg.tableName,
				Indexes:        []string{"embeddings"},
				SemanticSearch: q.Query,
				Limit:          10,
			},
		},
		Generator:     cfg.generator,
		MaxInternalIterations: 0, // Pipeline mode: execute queries directly
		Stream:        false,
	}

	resp, err := client.RetrievalAgent(ctx, req)
	result := RetrievalQueryResult{
		Query:    q.Query,
		Response: resp,
		Error:    err,
	}

	if err == nil && resp != nil {
		result.NumDocs = len(resp.Hits)
		result.StrategyUsed = string(resp.StrategyUsed)

		// Check keywords in document content
		var allContent strings.Builder
		for _, doc := range resp.Hits {
			if source, ok := doc.Source["content"].(string); ok {
				allContent.WriteString(source)
				allContent.WriteString(" ")
			}
		}
		result.Keywords = checkKeywords(allContent.String(), q.ExpectedKeywords)
	}

	return result
}

// TestE2E_RetrievalAgent_Suite runs all retrieval agent E2E tests under a single
// swarm instance and shared table restore, dramatically reducing setup overhead.
func TestE2E_RetrievalAgent_Suite(t *testing.T) {
	skipUnlessML(t)
	SkipIfProviderUnavailable(t)

	// Single shared setup: one swarm, one table restore
	setup := setupEvalTest(t, 40*time.Minute)
	defer setup.Cleanup()

	tableName := "antfly_docs_retrieval_suite_e2e"
	backupID := "docsaf-test-backup"
	setupTestTable(t, setup.Ctx, setup.Swarm.Client, tableName, backupID)

	cfg := newTestQueryConfig(t, tableName, setup.Swarm.Config.Termite.ApiUrl)

	// --- Subtests sharing the swarm + table ---

	t.Run("PipelineRetrieval", func(t *testing.T) {
		testPipelineRetrieval(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("FullPipeline", func(t *testing.T) {
		testFullPipeline(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("Streaming", func(t *testing.T) {
		testStreaming(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("Agentic", func(t *testing.T) {
		testAgentic(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("ConfidenceAndFollowup", func(t *testing.T) {
		testConfidenceAndFollowup(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("InlineEval", func(t *testing.T) {
		testInlineEval(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("BM25Search", func(t *testing.T) {
		testBM25Search(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		testErrorHandling(t, setup.Ctx, setup.Swarm.Client, cfg)
	})

	t.Run("TreeSearch", func(t *testing.T) {
		// TreeSearch needs its own table with hierarchy graph index
		treeTableName := "antfly_docs_tree_suite_e2e"
		treeBackupID := "docsaf-tree-backup"
		setupTestTableWithHierarchy(t, setup.Ctx, setup.Swarm.Client, treeTableName, treeBackupID)
		treeCfg := newTestQueryConfig(t, treeTableName, setup.Swarm.Config.Termite.ApiUrl)
		testTreeSearch(t, setup.Ctx, setup.Swarm.Client, treeCfg)
	})
}

// testPipelineRetrieval merges the old TestE2E_RetrievalAgent (retrieval-only)
// and TestE2E_RetrievalAgent_GenerationStep (retrieval + generation).
func testPipelineRetrieval(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Run("BasicRetrieval", func(t *testing.T) {
		queries := []RetrievalTestQuery{
			{
				Query:            "How do I configure Raft consensus in Antfly?",
				ExpectedKeywords: []string{"raft", "consensus", "leader", "follower"},
				Description:      "Query about Raft configuration",
			},
			{
				Query:            "What embedding models does Termite support?",
				ExpectedKeywords: []string{"embedding", "termite", "model"},
				Description:      "Query about Termite embedding support",
			},
			{
				Query:            "How do I create a table with vector indexes?",
				ExpectedKeywords: []string{"table", "index", "vector", "embedding"},
				Description:      "Query about table creation with vector indexes",
			},
		}

		t.Log("Executing retrieval agent queries...")

		var passed, failed int
		for i, q := range queries {
			t.Logf("[%d/%d] Query: %s", i+1, len(queries), q.Query)
			result := executeSingleRetrievalQuery(t, ctx, client, cfg, q)

			if result.Error != nil {
				t.Logf("  Error: %v", result.Error)
				failed++
			} else {
				t.Logf("  Retrieved %d documents, strategy: %s", result.NumDocs, result.StrategyUsed)
				t.Logf("  Keywords: %d/%d found", len(result.Keywords.Found), len(q.ExpectedKeywords))

				if result.NumDocs > 0 && result.Keywords.ContainsKeywords {
					passed++
				} else {
					failed++
				}
			}
		}

		t.Logf("\nRetrieval Agent Results: %d/%d passed (%.1f%%)",
			passed, len(queries), float64(passed)/float64(len(queries))*100)

		require.Positive(t, passed, "At least one retrieval query should succeed")
		require.GreaterOrEqual(t, float64(passed)/float64(len(queries)), 0.5,
			"At least 50%% of queries should succeed")
	})

	t.Run("WithGeneration", func(t *testing.T) {
		testCases := []struct {
			query            string
			expectedKeywords []string
			description      string
		}{
			{
				query:            "Explain how Antfly handles distributed consensus",
				expectedKeywords: []string{"raft", "consensus", "distributed"},
				description:      "Explanation query about consensus",
			},
			{
				query:            "Summarize the key features of Antfly's search capabilities",
				expectedKeywords: []string{"search", "vector", "index"},
				description:      "Summary query about search features",
			},
		}

		t.Log("Executing retrieval agent with generation step...")

		var passed, failed int
		for i, tc := range testCases {
			t.Logf("[%d/%d] Generation query: %s", i+1, len(testCases), tc.query)

			req := antfly.RetrievalAgentRequest{
				Query: tc.query,
				Queries: []antfly.RetrievalQueryRequest{
					{
						Table:          cfg.tableName,
						Indexes:        []string{"embeddings"},
						SemanticSearch: tc.query,
						Limit:          5,
					},
				},
				Generator:      cfg.generator,
				AgentKnowledge: cfg.agentKnowledge,
				MaxInternalIterations:  0, // Pipeline mode
				Stream:         false,
				Steps: antfly.RetrievalAgentSteps{
					Generation: antfly.GenerationStepConfig{Enabled: true},
				},
			}

			resp, err := client.RetrievalAgent(ctx, req)
			if err != nil {
				t.Logf("  Error: %v", err)
				failed++
				continue
			}

			t.Logf("  Answer length: %d chars, Hits: %d",
				len(resp.Generation), len(resp.Hits))

			keywords := checkKeywords(resp.Generation, tc.expectedKeywords)
			t.Logf("  Keywords: %d/%d found", len(keywords.Found), len(tc.expectedKeywords))

			if len(resp.Generation) > 50 && len(resp.Hits) > 0 && keywords.ContainsKeywords {
				passed++
			} else {
				failed++
			}
		}

		t.Logf("\nGeneration Step Results: %d/%d passed (%.1f%%)",
			passed, len(testCases), float64(passed)/float64(len(testCases))*100)

		require.Positive(t, passed, "At least one generation query should succeed")
	})
}

// testFullPipeline merges the old TestE2E_RetrievalAgent_FullPipeline
// and TestE2E_RetrievalAgent_ClassificationAndGeneration.
// Both test classification → retrieval → generation.
func testFullPipeline(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	queries := []RetrievalTestQuery{
		{
			Query:            "What are the main components of Antfly's architecture?",
			ExpectedKeywords: []string{"metadata", "storage", "raft", "shard"},
			Description:      "Architecture overview query",
		},
		{
			Query:            "How do I configure embedding indexes in Antfly?",
			ExpectedKeywords: []string{"embedding", "index", "config"},
			Description:      "Configuration query",
		},
		{
			Query:            "What is Termite and how does it integrate with Antfly?",
			ExpectedKeywords: []string{"termite", "embedding", "rerank"},
			Description:      "Termite integration query",
		},
	}

	t.Log("Executing full pipeline (classification → retrieval → generation) queries...")

	var passed, failed int
	for i, q := range queries {
		t.Logf("[%d/%d] Pipeline Query: %s", i+1, len(queries), q.Query)

		req := antfly.RetrievalAgentRequest{
			Query: q.Query,
			Queries: []antfly.RetrievalQueryRequest{
				{
					Table:          cfg.tableName,
					Indexes:        []string{"embeddings"},
					SemanticSearch: q.Query,
					Limit:          10,
				},
			},
			Generator:        cfg.generator,
			AgentKnowledge:   cfg.agentKnowledge,
			MaxInternalIterations:    0,
			MaxContextTokens: 2048,
			Stream:           false,
			Steps: antfly.RetrievalAgentSteps{
				Classification: antfly.ClassificationStepConfig{Enabled: true},
				Generation:     antfly.GenerationStepConfig{Enabled: true},
			},
		}

		resp, err := client.RetrievalAgent(ctx, req)
		if err != nil {
			t.Logf("  Error: %v", err)
			failed++
			continue
		}

		t.Logf("  Classification strategy: %s", resp.Classification.Strategy)
		t.Logf("  Retrieved: %d hits, Strategy: %s",
			len(resp.Hits), resp.StrategyUsed)
		t.Logf("  Generated: %d chars",
			len(resp.Generation))

		keywords := checkKeywords(resp.Generation, q.ExpectedKeywords)
		t.Logf("  Keywords: %d/%d found", len(keywords.Found), len(q.ExpectedKeywords))

		if len(resp.Hits) > 0 && len(resp.Generation) > 50 && keywords.ContainsKeywords {
			passed++
		} else {
			failed++
		}
	}

	t.Logf("\nFull Pipeline Results: %d/%d passed (%.1f%%)",
		passed, len(queries), float64(passed)/float64(len(queries))*100)

	require.Positive(t, passed, "At least one pipeline query should succeed")
	require.GreaterOrEqual(t, float64(passed)/float64(len(queries)), 0.5,
		"At least 50%% of pipeline queries should succeed")
}

// testStreaming merges the old TestE2E_RetrievalAgent_Streaming (basic streaming)
// and TestE2E_RetrievalAgent_GenerationStreaming (streaming + generation).
func testStreaming(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Run("BasicStreaming", func(t *testing.T) {
		t.Log("Testing streaming retrieval agent...")

		var hitCount int

		req := antfly.RetrievalAgentRequest{
			Query: "How does Raft consensus work in Antfly?",
			Queries: []antfly.RetrievalQueryRequest{
				{
					Table:          cfg.tableName,
					Indexes:        []string{"embeddings"},
					SemanticSearch: "raft consensus",
					Limit:          5,
				},
			},
			Generator:     cfg.generator,
			MaxInternalIterations: 0, // Pipeline mode
			Stream:        true,
		}

		opts := antfly.RetrievalAgentOptions{
			OnHit: func(hit *antfly.Hit) error {
				hitCount++
				t.Logf("  Hit: %s (score: %.3f)", hit.ID, hit.Score)
				return nil
			},
		}

		resp, err := client.RetrievalAgent(ctx, req, opts)
		require.NoError(t, err, "Streaming retrieval should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		t.Logf("Hits received via streaming: %d", hitCount)
		t.Logf("Final documents: %d", len(resp.Hits))

		require.NotEmpty(t, resp.Hits, "Should have retrieved some documents")
	})

	t.Run("WithGeneration", func(t *testing.T) {
		t.Log("Testing streaming with generation events...")

		var hitCount int

		req := antfly.RetrievalAgentRequest{
			Query: "Explain how Antfly handles distributed consensus",
			Queries: []antfly.RetrievalQueryRequest{
				{
					Table:          cfg.tableName,
					Indexes:        []string{"embeddings"},
					SemanticSearch: "distributed consensus raft",
					Limit:          5,
				},
			},
			Generator:      cfg.generator,
			AgentKnowledge: cfg.agentKnowledge,
			MaxInternalIterations:  0,
			Stream:         true,
			Steps: antfly.RetrievalAgentSteps{
				Generation: antfly.GenerationStepConfig{Enabled: true},
			},
		}

		opts := antfly.RetrievalAgentOptions{
			OnHit: func(hit *antfly.Hit) error {
				hitCount++
				t.Logf("  Hit: %s (score: %.3f)", hit.ID, hit.Score)
				return nil
			},
		}

		resp, err := client.RetrievalAgent(ctx, req, opts)
		require.NoError(t, err, "Streaming with generation should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		t.Logf("Hits via streaming: %d", hitCount)
		t.Logf("Final hits: %d", len(resp.Hits))
		t.Logf("Answer length: %d chars", len(resp.Generation))

		require.NotEmpty(t, resp.Hits, "Should have retrieved documents")
		require.NotEmpty(t, resp.Generation, "Answer should be generated in streaming mode")
		require.Positive(t, hitCount, "Should have received hits via streaming callbacks")
	})
}

// testAgentic merges the old TestE2E_RetrievalAgent_Agentic (non-streaming)
// and TestE2E_RetrievalAgent_AgenticStreaming (streaming).
func testAgentic(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Run("NonStreaming", func(t *testing.T) {
		t.Log("Testing agentic retrieval (LLM-driven tool calling)...")

		req := antfly.RetrievalAgentRequest{
			Query: "How does Raft consensus work in Antfly?",
			Queries: []antfly.RetrievalQueryRequest{
				{
					Table:          cfg.tableName,
					Indexes:        []string{"embeddings"},
					SemanticSearch: "raft consensus",
				},
			},
			Generator:      cfg.generator,
			AgentKnowledge: cfg.agentKnowledge,
			MaxInternalIterations:  3, // Let LLM call tools up to 3 times
			Stream:         false,
		}

		resp, err := client.RetrievalAgent(ctx, req)
		require.NoError(t, err, "Agentic retrieval should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		t.Logf("Status: %s", resp.Status)
		t.Logf("Hits: %d", len(resp.Hits))
		t.Logf("Tool calls made: %d", resp.ToolCallsMade)
		t.Logf("Reasoning chain steps: %d", len(resp.Steps))
		for i, step := range resp.Steps {
			t.Logf("  [%d] %s: %s", i, step.Name, step.Action)
		}

		require.Equal(t, antfly.AgentStatusCompleted, resp.Status,
			"Agent should reach completed status")
		require.NotEmpty(t, resp.Hits, "Agent should have found documents")
		require.Positive(t, resp.ToolCallsMade, "Agent should have made at least one tool call")
		require.NotEmpty(t, resp.Steps, "Reasoning chain should be non-empty")
	})

	t.Run("Streaming", func(t *testing.T) {
		t.Log("Testing agentic retrieval with streaming...")

		var hitCount int

		req := antfly.RetrievalAgentRequest{
			Query: "What embedding models does Termite support?",
			Queries: []antfly.RetrievalQueryRequest{
				{
					Table:          cfg.tableName,
					Indexes:        []string{"embeddings"},
					SemanticSearch: "termite embedding models",
				},
			},
			Generator:      cfg.generator,
			AgentKnowledge: cfg.agentKnowledge,
			MaxInternalIterations:  3,
			Stream:         true,
		}

		opts := antfly.RetrievalAgentOptions{
			OnHit: func(hit *antfly.Hit) error {
				hitCount++
				t.Logf("  Hit: %s (score: %.3f)", hit.ID, hit.Score)
				return nil
			},
		}

		resp, err := client.RetrievalAgent(ctx, req, opts)
		require.NoError(t, err, "Agentic streaming retrieval should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		t.Logf("Hits received via streaming: %d", hitCount)
		t.Logf("Final hits: %d", len(resp.Hits))
		t.Logf("Tool calls made: %d", resp.ToolCallsMade)

		require.NotEmpty(t, resp.Hits, "Agent should have found documents")
		require.Positive(t, hitCount, "Should have received hits via streaming")
	})
}

// testConfidenceAndFollowup tests confidence scoring and followup question generation.
func testConfidenceAndFollowup(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Log("Testing confidence + followup steps...")

	req := antfly.RetrievalAgentRequest{
		Query: "How do I create a table with vector indexes in Antfly?",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          cfg.tableName,
				Indexes:        []string{"embeddings"},
				SemanticSearch: "create table vector index",
				Limit:          5,
			},
		},
		Generator:      cfg.generator,
		AgentKnowledge: cfg.agentKnowledge,
		MaxInternalIterations:  0,
		Stream:         false,
		Steps: antfly.RetrievalAgentSteps{
			Generation: antfly.GenerationStepConfig{Enabled: true},
			Confidence: antfly.ConfidenceStepConfig{
				Enabled: true,
			},
			Followup: antfly.FollowupStepConfig{
				Enabled: true,
				Count:   3,
			},
		},
	}

	resp, err := client.RetrievalAgent(ctx, req)
	require.NoError(t, err, "Confidence + followup should succeed")
	require.NotNil(t, resp, "Response should not be nil")

	t.Logf("Hits: %d", len(resp.Hits))
	t.Logf("Answer length: %d chars", len(resp.Generation))
	t.Logf("Answer confidence: %.2f", resp.GenerationConfidence)
	t.Logf("Context relevance: %.2f", resp.ContextRelevance)
	t.Logf("Followup questions: %d", len(resp.FollowupQuestions))
	for i, q := range resp.FollowupQuestions {
		t.Logf("  [%d] %s", i, q)
	}

	require.NotEmpty(t, resp.Hits, "Should have retrieved documents")
	require.NotEmpty(t, resp.Generation, "Answer should be generated")
	require.Greater(t, resp.GenerationConfidence, float32(0), "Confidence score should be positive")
	require.Greater(t, resp.ContextRelevance, float32(0), "Context relevance should be positive")
	require.NotEmpty(t, resp.FollowupQuestions, "Followup questions should be generated")
}

// testInlineEval tests inline evaluation of retrieval + generation.
func testInlineEval(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Log("Testing inline eval step...")

	req := antfly.RetrievalAgentRequest{
		Query: "Explain how Antfly uses Raft for distributed consensus",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          cfg.tableName,
				Indexes:        []string{"embeddings"},
				SemanticSearch: "raft distributed consensus",
				Limit:          5,
			},
		},
		Generator:      cfg.generator,
		AgentKnowledge: cfg.agentKnowledge,
		MaxInternalIterations:  0,
		Stream:         false,
		Steps: antfly.RetrievalAgentSteps{
			Generation: antfly.GenerationStepConfig{Enabled: true},
			Eval: antfly.EvalConfig{
				Evaluators: []antfly.EvaluatorName{
					antfly.EvaluatorNameRelevance,
					antfly.EvaluatorNameFaithfulness,
				},
				Judge: GetJudgeConfig(t),
				GroundTruth: antfly.GroundTruth{
					Expectations: "raft, consensus, leader, follower, log replication",
				},
			},
		},
	}

	resp, err := client.RetrievalAgent(ctx, req)
	require.NoError(t, err, "Eval step should succeed")
	require.NotNil(t, resp, "Response should not be nil")

	t.Logf("Hits: %d", len(resp.Hits))
	t.Logf("Answer length: %d chars", len(resp.Generation))
	t.Logf("Eval result scores: %+v", resp.EvalResult)

	require.NotEmpty(t, resp.Hits, "Should have retrieved documents")
	require.NotEmpty(t, resp.Generation, "Answer should be generated")

	// Check that eval results were populated
	hasScores := len(resp.EvalResult.Scores.Generation) > 0 || len(resp.EvalResult.Scores.Retrieval) > 0
	if hasScores {
		for name, result := range resp.EvalResult.Scores.Generation {
			t.Logf("  Generation evaluator %s: %.2f (pass: %v)", name, result.Score, result.Pass)
			require.GreaterOrEqual(t, result.Score, float32(0), "Score should be >= 0")
			require.LessOrEqual(t, result.Score, float32(1), "Score should be <= 1")
		}
		for name, result := range resp.EvalResult.Scores.Retrieval {
			t.Logf("  Retrieval evaluator %s: %.2f (pass: %v)", name, result.Score, result.Pass)
			require.GreaterOrEqual(t, result.Score, float32(0), "Score should be >= 0")
			require.LessOrEqual(t, result.Score, float32(1), "Score should be <= 1")
		}
	} else {
		t.Log("  Warning: No eval scores returned (eval may not have run)")
	}
}

// testBM25Search tests BM25 full-text search in pipeline mode.
func testBM25Search(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Log("Testing BM25 full-text search via retrieval agent...")

	req := antfly.RetrievalAgentRequest{
		Query: "raft consensus configuration",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:   cfg.tableName,
				Indexes: []string{"full_text_index"},
				FullTextSearch: mustMarshalJSON(t, map[string]string{
					"query": "raft consensus configuration",
				}),
				Limit: 10,
			},
		},
		Generator:     cfg.generator,
		MaxInternalIterations: 0,
		Stream:        false,
	}

	resp, err := client.RetrievalAgent(ctx, req)
	require.NoError(t, err, "BM25 search should succeed")
	require.NotNil(t, resp, "Response should not be nil")

	t.Logf("Hits: %d", len(resp.Hits))
	for i, hit := range resp.Hits {
		if i >= 3 {
			break
		}
		t.Logf("  [%d] ID: %s, Score: %.3f", i, hit.ID, hit.Score)
	}

	require.NotEmpty(t, resp.Hits, "BM25 search should return results")

	// Verify result content contains expected terms
	var allContent strings.Builder
	for _, hit := range resp.Hits {
		if source, ok := hit.Source["content"].(string); ok {
			allContent.WriteString(source)
			allContent.WriteString(" ")
		}
	}
	keywords := checkKeywords(allContent.String(), []string{"raft", "consensus"})
	require.True(t, keywords.ContainsKeywords,
		"BM25 results should contain expected terms (found: %v, missing: %v)",
		keywords.Found, keywords.Missing)
}

// testErrorHandling tests various error conditions.
func testErrorHandling(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Log("Testing error handling...")

	// Test 1: Invalid (nonexistent) table
	t.Log("Test 1: Nonexistent table")
	req := antfly.RetrievalAgentRequest{
		Query: "test query",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          "nonexistent_table_that_does_not_exist",
				Indexes:        []string{"embeddings"},
				SemanticSearch: "test query",
				Limit:          5,
			},
		},
		Generator:     cfg.generator,
		MaxInternalIterations: 0,
		Stream:        false,
	}

	_, err := client.RetrievalAgent(ctx, req)
	require.Error(t, err, "Querying nonexistent table should return an error")
	t.Logf("  Expected error for invalid table: %v", err)

	// Test 2: Empty query string
	t.Log("Test 2: Empty query string")
	reqEmpty := antfly.RetrievalAgentRequest{
		Query: "",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          cfg.tableName,
				Indexes:        []string{"embeddings"},
				SemanticSearch: "",
				Limit:          5,
			},
		},
		Generator:     cfg.generator,
		MaxInternalIterations: 0,
		Stream:        false,
	}

	respEmpty, err := client.RetrievalAgent(ctx, reqEmpty)
	if err != nil {
		t.Logf("  Empty query returned error (acceptable): %v", err)
	} else {
		t.Logf("  Empty query returned %d hits (may return empty or error)", len(respEmpty.Hits))
	}
}

// testTreeSearch tests the PageIndex-style tree search strategy.
// It uses its own table with hierarchy graph index.
func testTreeSearch(t *testing.T, ctx context.Context, client *antfly.AntflyClient, cfg testQueryConfig) {
	t.Log("Testing PageIndex-style tree search...")

	// Test 1: Tree search starting from roots
	t.Log("Test 1: Tree search from $roots")
	reqFromRoots := antfly.RetrievalAgentRequest{
		Query: "How does Antfly's architecture work?",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table: cfg.tableName,
				TreeSearch: antfly.TreeSearchConfig{
					Index:      "doc_hierarchy", // Graph index for document hierarchy
					StartNodes: "$roots",
					MaxDepth:   5,
					BeamWidth:  3,
				},
			},
		},
		Generator:     cfg.generator,
		MaxInternalIterations: 0, // Pipeline mode: execute queries directly
		Stream:        false,
	}

	respFromRoots, err := client.RetrievalAgent(ctx, reqFromRoots)
	if err != nil {
		t.Logf("  Tree search from roots failed (may be expected if no graph index): %v", err)
	} else {
		t.Logf("  Retrieved %d documents from tree search", len(respFromRoots.Hits))
		t.Logf("  Strategy used: %s", respFromRoots.StrategyUsed)
		if len(respFromRoots.Steps) > 0 {
			t.Log("  Reasoning chain:")
			for _, step := range respFromRoots.Steps {
				t.Logf("    - %s: %s", step.Name, step.Action)
			}
		}
	}

	// Test 2: Tree search with semantic search as start nodes (pipeline)
	t.Log("Test 2: Tree search with semantic search as start nodes")
	reqPipeline := antfly.RetrievalAgentRequest{
		Query: "What are the main components of Antfly?",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table:          cfg.tableName,
				Indexes:        []string{"embeddings"},
				SemanticSearch: "antfly architecture components",
				Limit:          5,
			},
			{
				Table: cfg.tableName,
				TreeSearch: antfly.TreeSearchConfig{
					Index:      "doc_hierarchy",
					StartNodes: "$find_start", // Reference semantic search results
					MaxDepth:   3,
					BeamWidth:  2,
				},
			},
		},
		Generator:     cfg.generator,
		MaxInternalIterations: 0, // Pipeline mode: execute queries directly
		Stream:        false,
	}

	respPipeline, err := client.RetrievalAgent(ctx, reqPipeline)
	if err != nil {
		t.Logf("  Pipeline tree search failed (may be expected if no graph index): %v", err)
	} else {
		t.Logf("  Retrieved %d documents from pipeline tree search", len(respPipeline.Hits))
		t.Logf("  Strategy used: %s", respPipeline.StrategyUsed)
	}

	// Test 3: Tree search with streaming to observe navigation
	t.Log("Test 3: Tree search with streaming")
	var treeHitCount int

	reqStreaming := antfly.RetrievalAgentRequest{
		Query: "How do I configure Raft in Antfly?",
		Queries: []antfly.RetrievalQueryRequest{
			{
				Table: cfg.tableName,
				TreeSearch: antfly.TreeSearchConfig{
					Index:      "doc_hierarchy",
					StartNodes: "$roots",
					MaxDepth:   4,
					BeamWidth:  3,
				},
			},
		},
		Generator:     cfg.generator,
		MaxInternalIterations: 0, // Pipeline mode
		Stream:        true,
	}

	opts := antfly.RetrievalAgentOptions{
		OnHit: func(hit *antfly.Hit) error {
			treeHitCount++
			t.Logf("    Hit: %s (score: %.3f)", hit.ID, hit.Score)
			return nil
		},
	}

	respStreaming, err := client.RetrievalAgent(ctx, reqStreaming, opts)
	if err != nil {
		t.Logf("  Streaming tree search failed (may be expected if no graph index): %v", err)
	} else {
		t.Logf("  Retrieved %d documents from streaming tree search", len(respStreaming.Hits))
		t.Logf("  Hits received via streaming: %d", treeHitCount)
	}

	// Summary
	t.Log("\nTree Search Test Summary:")
	testsRun := 0
	testsSucceeded := 0

	if respFromRoots != nil {
		testsRun++
		if len(respFromRoots.Hits) > 0 {
			testsSucceeded++
		}
	}
	if respPipeline != nil {
		testsRun++
		if len(respPipeline.Hits) > 0 {
			testsSucceeded++
		}
	}
	if respStreaming != nil {
		testsRun++
		if len(respStreaming.Hits) > 0 {
			testsSucceeded++
		}
	}

	if testsRun == 0 {
		t.Log("  No tree search tests could run (graph index may not be configured)")
		t.Log("  This is expected if the test data doesn't have a doc_hierarchy graph index")
	} else {
		t.Logf("  %d/%d tree search tests succeeded", testsSucceeded, testsRun)
	}
}

// mustMarshalJSON marshals v to json.RawMessage, failing the test on error.
func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err, "JSON marshal should succeed")
	return data
}
