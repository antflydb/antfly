// Copyright 2026 Antfly, Inc.
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

package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/ai"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/websearch"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/go/pkg/generating"
	"github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func webSearchConnection(
	t *testing.T,
	provider string,
	config common.WebSearchConnectionConfig,
) common.ConnectionConfig {
	t.Helper()
	var connection common.ConnectionConfig
	require.NoError(t, connection.FromWebSearchConnectionVariant(common.WebSearchConnectionVariant{
		Capabilities: []string{"web.search", "agents.use"},
		Provider:     provider,
		WebSearch:    config,
	}))
	return connection
}

func TestWebSearchConfigFromConnection(t *testing.T) {
	safeSearch := false
	config, err := webSearchConfigFromConnection(
		"agent-web",
		webSearchConnection(t, "exa", common.WebSearchConnectionConfig{
			ApiKey:            "${secret:exa.api_key}",
			CredentialsPath:   "${secret:vertex.service_account_path}",
			DataStore:         "docs-store",
			Endpoint:          "https://search.example.test",
			MaxResults:        7,
			TimeoutMs:         2500,
			Language:          "en",
			Location:          "global",
			ProjectId:         "test-project",
			Region:            "us",
			ServingConfig:     "default_config",
			SafeSearch:        &safeSearch,
			IncludeContent:    true,
			IncludeHighlights: true,
		}),
	)

	assert.NoError(t, err)
	assert.Equal(t, "exa", string(config.Provider))
	assert.Equal(t, "${secret:exa.api_key}", config.ApiKey)
	assert.Equal(t, "${secret:vertex.service_account_path}", config.CredentialsPath)
	assert.Equal(t, "docs-store", config.DataStore)
	assert.Equal(t, "https://search.example.test", config.Endpoint)
	assert.Equal(t, 7, config.MaxResults)
	assert.Equal(t, 2500, config.TimeoutMs)
	assert.Equal(t, "en", config.Language)
	assert.Equal(t, "global", config.Location)
	assert.Equal(t, "test-project", config.ProjectId)
	assert.Equal(t, "us", config.Region)
	assert.Equal(t, "default_config", config.ServingConfig)
	if assert.NotNil(t, config.SafeSearch) {
		assert.False(t, *config.SafeSearch)
	}
	assert.True(t, config.IncludeContent)
	assert.True(t, config.IncludeHighlights)
}

func TestWebSearchConfigFromConnectionRejectsWrongKind(t *testing.T) {
	var connection common.ConnectionConfig
	require.NoError(t, connection.FromInferenceConnectionVariant(common.InferenceConnectionVariant{
		Provider: "exa",
		Inference: common.InferenceConnectionConfig{
			Provider: "exa",
		},
	}))
	_, err := webSearchConfigFromConnection("model", connection)
	assert.ErrorContains(t, err, "expected web_search")
}

func TestWebSearchConfigFromConnectionAllowsVertex(t *testing.T) {
	config, err := webSearchConfigFromConnection(
		"agent-search",
		webSearchConnection(t, "vertex", common.WebSearchConnectionConfig{
			CredentialsPath: "${secret:vertex.service_account_path}",
			DataStore:       "docs-store",
			Location:        "global",
			ProjectId:       "test-project",
			ServingConfig:   "default_config",
		}),
	)

	require.NoError(t, err)
	assert.Equal(t, websearch.WebSearchProviderVertex, config.Provider)
	assert.Equal(t, "${secret:vertex.service_account_path}", config.CredentialsPath)
	assert.Equal(t, "docs-store", config.DataStore)
	assert.Equal(t, "global", config.Location)
	assert.Equal(t, "test-project", config.ProjectId)
	assert.Equal(t, "default_config", config.ServingConfig)
}

func TestResolveEffectiveGeneratorChain(t *testing.T) {
	originalDefault := generating.GetDefaultChain()
	t.Cleanup(func() {
		generating.SetDefaultChain(originalDefault)
	})

	defaultChain := []ai.ChainLink{
		{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderGemini}},
	}
	generating.SetDefaultChain(defaultChain)

	t.Run("prefers explicit chain over single generator", func(t *testing.T) {
		req := &RetrievalAgentRequest{
			Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOpenai},
			Chain: []ai.ChainLink{
				{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderAnthropic}},
			},
		}

		chain := resolveEffectiveGeneratorChain(req)
		if assert.Len(t, chain, 1) {
			assert.Equal(t, ai.GeneratorProviderAnthropic, chain[0].Generator.Provider)
		}
	})

	t.Run("wraps explicit generator when chain is absent", func(t *testing.T) {
		req := &RetrievalAgentRequest{
			Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOpenai},
		}

		chain := resolveEffectiveGeneratorChain(req)
		if assert.Len(t, chain, 1) {
			assert.Equal(t, ai.GeneratorProviderOpenai, chain[0].Generator.Provider)
		}
	})

	t.Run("falls back to default chain", func(t *testing.T) {
		req := &RetrievalAgentRequest{}

		chain := resolveEffectiveGeneratorChain(req)
		if assert.Len(t, chain, 1) {
			assert.Equal(t, ai.GeneratorProviderGemini, chain[0].Generator.Provider)
		}
	})

	t.Run("returns nil when no explicit or default chain exists", func(t *testing.T) {
		generating.SetDefaultChain(nil)

		req := &RetrievalAgentRequest{}
		assert.Nil(t, resolveEffectiveGeneratorChain(req))
	})
}

func TestResolveProviderName(t *testing.T) {
	originalDefault := generating.GetDefaultChain()
	t.Cleanup(func() {
		generating.SetDefaultChain(originalDefault)
	})

	t.Run("uses first provider from explicit chain", func(t *testing.T) {
		generating.SetDefaultChain([]ai.ChainLink{
			{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderGemini}},
		})
		req := &RetrievalAgentRequest{
			Chain: []ai.ChainLink{
				{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderAnthropic}},
				{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOpenai}},
			},
		}

		assert.Equal(t, "anthropic", resolveProviderName(req))
		assert.Equal(t, ai.GeneratorProviderAnthropic, resolveProvider(req))
	})

	t.Run("uses default chain provider when request does not specify one", func(t *testing.T) {
		generating.SetDefaultChain([]ai.ChainLink{
			{Generator: ai.GeneratorConfig{Provider: ai.GeneratorProviderOllama}},
		})

		req := &RetrievalAgentRequest{}
		assert.Equal(t, "ollama", resolveProviderName(req))
		assert.Equal(t, ai.GeneratorProviderOllama, resolveProvider(req))
	})

	t.Run("returns unknown when no provider can be resolved", func(t *testing.T) {
		generating.SetDefaultChain(nil)

		req := &RetrievalAgentRequest{}
		assert.Equal(t, "unknown", resolveProviderName(req))
		assert.Equal(t, ai.GeneratorProvider(""), resolveProvider(req))
	})
}

func TestAsGenerationErrorResponse(t *testing.T) {
	t.Run("generic query failures are not classified as generation errors", func(t *testing.T) {
		message, statusCode, ok := asGenerationErrorResponse(errors.New("query failed (table=test): bad filter"))
		assert.False(t, ok)
		assert.Empty(t, message)
		assert.Zero(t, statusCode)
	})

	t.Run("preserves typed generation errors", func(t *testing.T) {
		message, statusCode, ok := asGenerationErrorResponse(&ai.GenerationError{
			Kind:        ai.GenerationErrorRateLimit,
			UserMessage: "Rate limit reached for provider 'openrouter'. Please wait and try again.",
		})
		assert.True(t, ok)
		assert.Equal(t, "Rate limit reached for provider 'openrouter'. Please wait and try again.", message)
		assert.Equal(t, 429, statusCode)
	})
}

func TestNormalizeRetrievalAgentSessionRequest(t *testing.T) {
	req := &RetrievalAgentRequest{
		MaxInternalIterations: 3,
		MaxUserClarifications: 2,
		Decisions:             []AgentDecision{{QuestionId: "q1", Answer: "OAuth 2.0"}},
		Messages:              []ai.ChatMessage{ai.NewTextChatMessage(ai.ChatMessageRoleUser, "prior")},
	}

	normalizeRetrievalAgentSessionRequest(req)

	assert.Equal(t, 3, req.MaxInternalIterations)
	assert.NotEmpty(t, req.SessionId)
	if assert.Len(t, req.Messages, 1) {
		assert.Equal(t, "prior", ai.ChatMessageContentAsText(req.Messages[0].Content))
	}
}

func TestBuildEffectiveRetrievalQueryIncludesDecisions(t *testing.T) {
	req := &RetrievalAgentRequest{
		Query: "find recent OAuth docs",
		Decisions: []AgentDecision{
			{QuestionId: "oauth_version", Answer: "OAuth 2.0"},
			{QuestionId: "confirm_scope", Approved: true},
		},
	}

	query := buildEffectiveRetrievalQuery(req)

	assert.Contains(t, query, "find recent OAuth docs")
	assert.Contains(t, query, "Resolved user decisions:")
	assert.Contains(t, query, "oauth_version: OAuth 2.0")
	assert.Contains(t, query, "confirm_scope: approved")
}

func TestEffectiveRetrievalToolsConfigIntersectsGlobalAndRetrievalTools(t *testing.T) {
	global := []ai.ChatToolName{ai.ToolNameSemanticSearch, ai.ToolNameFullTextSearch}
	retrieval := []ai.ChatToolName{ai.ToolNameSemanticSearch}
	req := &RetrievalAgentRequest{
		Tools: ai.ChatToolsConfig{EnabledTools: &global},
	}
	req.Steps.Retrieval.Tools.EnabledTools = &retrieval

	config, err := effectiveRetrievalToolsConfig(req)

	require.NoError(t, err)
	assert.True(t, config.IsToolEnabled(ai.ToolNameSemanticSearch))
	assert.False(t, config.IsToolEnabled(ai.ToolNameFullTextSearch))
}

func TestEffectiveRetrievalToolsConfigKeepsEmptyIntersectionDisabled(t *testing.T) {
	global := []ai.ChatToolName{ai.ToolNameFullTextSearch}
	retrieval := []ai.ChatToolName{ai.ToolNameSemanticSearch}
	req := &RetrievalAgentRequest{
		Tools: ai.ChatToolsConfig{EnabledTools: &global},
	}
	req.Steps.Retrieval.Tools.EnabledTools = &retrieval

	config, err := effectiveRetrievalToolsConfig(req)

	require.NoError(t, err)
	assert.False(t, config.IsToolEnabled(ai.ToolNameSemanticSearch))
	assert.False(t, config.IsToolEnabled(ai.ToolNameFullTextSearch))
	assert.False(t, config.IsToolEnabled(ai.ToolNameFilter))
}

func TestEffectiveRetrievalToolsConfigRejectsOutOfRangeMaxToolIterations(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value int
		path  string
	}{
		{name: "zero global", value: 0, path: "tools.max_tool_iterations"},
		{name: "too large global", value: 21, path: "tools.max_tool_iterations"},
		{name: "zero retrieval", value: 0, path: "steps.retrieval.tools.max_tool_iterations"},
		{name: "too large retrieval", value: 21, path: "steps.retrieval.tools.max_tool_iterations"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := &RetrievalAgentRequest{}
			if tt.path == "tools.max_tool_iterations" {
				req.Tools.MaxToolIterations = &tt.value
			} else {
				req.Steps.Retrieval.Tools.MaxToolIterations = &tt.value
			}

			_, err := effectiveRetrievalToolsConfig(req)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.path+" must be between 1 and 20")
		})
	}
}

func TestRetrievalQueryAllowedByTools(t *testing.T) {
	fullTextOnly := []ai.ChatToolName{ai.ToolNameFullTextSearch}
	config := ai.ChatToolsConfig{EnabledTools: &fullTextOnly}

	assert.True(t, retrievalQueryAllowedByTools(RetrievalQueryRequest{
		FullTextSearch: json.RawMessage(`{"query":"body:raft"}`),
	}, config))
	assert.False(t, retrievalQueryAllowedByTools(RetrievalQueryRequest{
		SemanticSearch: "raft consensus",
	}, config))
}

func TestRetrievalQueryAllowedByToolsRequiresEveryQueryCapability(t *testing.T) {
	semanticOnly := []ai.ChatToolName{ai.ToolNameSemanticSearch}
	config := ai.ChatToolsConfig{EnabledTools: &semanticOnly}

	assert.False(t, retrievalQueryAllowedByTools(RetrievalQueryRequest{
		SemanticSearch: "raft consensus",
		FilterQuery:    json.RawMessage(`{"query":"status:published"}`),
	}, config))
	assert.False(t, retrievalQueryAllowedByTools(RetrievalQueryRequest{
		SemanticSearch: "raft consensus",
		GraphSearches: map[string]indexes.GraphQuery{
			"related": {},
		},
	}, config))

	withFilterAndGraph := []ai.ChatToolName{
		ai.ToolNameSemanticSearch,
		ai.ToolNameFilter,
		ai.ToolNameGraphSearch,
	}
	config.EnabledTools = &withFilterAndGraph
	assert.True(t, retrievalQueryAllowedByTools(RetrievalQueryRequest{
		SemanticSearch: "raft consensus",
		FilterQuery:    json.RawMessage(`{"query":"status:published"}`),
		GraphSearches: map[string]indexes.GraphQuery{
			"related": {},
		},
	}, config))
}

func TestRetrievalQueryAllowedByToolsTreatsAggregateAsFirstClassTool(t *testing.T) {
	aggregateOnly := []ai.ChatToolName{ai.ToolNameAggregate}
	filterOnly := []ai.ChatToolName{ai.ToolNameFilter}
	filterAndAggregate := []ai.ChatToolName{ai.ToolNameFilter, ai.ToolNameAggregate}
	aggregationQuery := RetrievalQueryRequest{
		Table: "docs",
		Aggregations: map[string]AggregationRequest{
			"by_author": {
				Type:  AggregationTypeTerms,
				Field: "author",
			},
		},
	}

	assert.True(t, retrievalQueryAllowedByTools(aggregationQuery, ai.ChatToolsConfig{EnabledTools: &aggregateOnly}))
	assert.False(t, retrievalQueryAllowedByTools(aggregationQuery, ai.ChatToolsConfig{EnabledTools: &filterOnly}))

	aggregationWithFilter := aggregationQuery
	aggregationWithFilter.FilterQuery = json.RawMessage(`{"query":"status:published"}`)
	assert.False(t, retrievalQueryAllowedByTools(aggregationWithFilter, ai.ChatToolsConfig{EnabledTools: &aggregateOnly}))
	assert.False(t, retrievalQueryAllowedByTools(aggregationWithFilter, ai.ChatToolsConfig{EnabledTools: &filterOnly}))
	assert.True(t, retrievalQueryAllowedByTools(aggregationWithFilter, ai.ChatToolsConfig{EnabledTools: &filterAndAggregate}))
}

func TestRetrievalQueryShouldExecuteAggregateOnlyQuery(t *testing.T) {
	assert.True(t, retrievalQueryShouldExecute(RetrievalQueryRequest{
		Table: "docs",
		Aggregations: map[string]AggregationRequest{
			"by_author": {
				Type:  AggregationTypeTerms,
				Field: "author",
			},
		},
	}))
	assert.False(t, retrievalQueryShouldExecute(RetrievalQueryRequest{Table: "docs"}))
}

func TestExecutePipelineRejectsDisabledRetrievalTool(t *testing.T) {
	fullTextOnly := []ai.ChatToolName{ai.ToolNameFullTextSearch}
	req := &RetrievalAgentRequest{
		Query: "find raft",
		Tools: ai.ChatToolsConfig{EnabledTools: &fullTextOnly},
		Queries: []RetrievalQueryRequest{{
			Table:          "docs",
			SemanticSearch: "raft consensus",
			Indexes:        []string{"semantic_idx"},
		}},
	}

	result, err := (&TableApi{}).ExecutePipeline(context.Background(), req, nil, nil)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), `retrieval query strategy "semantic" uses disabled tool(s): semantic_search`)
}

func TestApplyPendingClarificationPopulatesSharedQuestions(t *testing.T) {
	executor := &retrievalToolExecutor{
		pendingQuestion: &ai.ClarificationRequest{
			Question: "Which OAuth version?",
			Options:  &[]string{"1.0", "2.0"},
		},
	}
	result := &RetrievalAgentResult{Status: AgentStatusCompleted}

	applyPendingQuestion(executor, result)

	assert.Equal(t, AgentStatusClarificationRequired, result.Status)
	if assert.Len(t, result.Questions, 1) {
		assert.Equal(t, AgentQuestionKindSingleChoice, result.Questions[0].Kind)
		assert.Equal(t, "Which OAuth version?", result.Questions[0].Question)
		assert.Equal(t, []string{"1.0", "2.0"}, result.Questions[0].Options)
	}
	if assert.Len(t, result.Steps, 1) {
		assert.Equal(t, AgentStepKindClarification, result.Steps[0].Kind)
		assert.Equal(t, "clarification", result.Steps[0].Name)
	}
}

func TestFinalizeRetrievalAgentSession(t *testing.T) {
	req := &RetrievalAgentRequest{
		SessionId:             "rags_test",
		MaxInternalIterations: 4,
		MaxUserClarifications: 2,
		Decisions:             []AgentDecision{{QuestionId: "q1"}, {QuestionId: "q2"}},
	}
	result := &RetrievalAgentResult{
		ToolCallsMade: 3,
		Steps: []AgentStep{{
			Name:   "semantic_search",
			Kind:   AgentStepKindToolCall,
			Action: "Search docs",
			Status: AgentStepStatusSuccess,
		}},
	}

	finalizeRetrievalAgentSession(req, result)

	assert.Equal(t, "rags_test", result.SessionId)
	assert.Equal(t, 3, result.Iteration)
	assert.Equal(t, 1, result.RemainingInternalIterations)
	assert.Equal(t, 2, result.ClarificationCount)
	assert.Equal(t, 0, result.RemainingUserClarifications)
	if assert.Len(t, result.Steps, 1) {
		assert.Equal(t, AgentStepKindToolCall, result.Steps[0].Kind)
		assert.Equal(t, "semantic_search", result.Steps[0].Name)
		assert.Equal(t, AgentStepStatusSuccess, result.Steps[0].Status)
	}
}
