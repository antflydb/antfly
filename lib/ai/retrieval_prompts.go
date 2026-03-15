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

package ai

import (
	"fmt"
	"strings"
)

// RetrievalAgentSystemPrompt builds the dynamic system prompt for the retrieval agent.
// It describes available tools based on the table's indexes and the user's configuration.
func RetrievalAgentSystemPrompt(availableIndexes []IndexInfo, toolDefs []string, tableSchema string, agentKnowledge string) string {
	var sb strings.Builder

	sb.WriteString(`You are an intelligent retrieval agent with access to a document database. Your role is to find the most relevant documents to answer the user's query.

## How You Work

You have access to search tools that let you query the database. Analyze the user's query, choose the right tool(s), and iteratively refine your search until you have enough relevant documents.

## Available Tools

`)

	for _, td := range toolDefs {
		sb.WriteString(td)
		sb.WriteString("\n")
	}

	// Describe available indexes
	if len(availableIndexes) > 0 {
		sb.WriteString("\n## Available Indexes\n\n")
		for _, idx := range availableIndexes {
			desc := idx.Description
			if desc == "" {
				switch idx.Type {
				case "embeddings", "aknn_v0":
					desc = "Vector/semantic similarity search"
				case "full_text", "full_text_v0":
					desc = "Full-text BM25 keyword search"
				case "graph", "graph_v0":
					desc = "Graph/tree document relationships"
				}
			}
			fmt.Fprintf(&sb, "- **%s** (%s): %s\n", idx.Name, idx.Type, desc)
		}
	}

	if tableSchema != "" {
		sb.WriteString("\n## Document Schema\n\n")
		sb.WriteString(tableSchema)
		sb.WriteString("\n")
	}

	if agentKnowledge != "" {
		sb.WriteString("\n## Domain Knowledge\n\n")
		sb.WriteString(agentKnowledge)
		sb.WriteString("\n")
	}

	sb.WriteString(`
## Guidelines

- Start with the most likely search tool based on the query type
- Use semantic_search for natural language questions and conceptual queries
- Use full_text_search for keyword/phrase matching and exact terms
- Use tree_search to navigate hierarchical document structures
- Use graph_search to explore relationships between documents
- Use add_filter to narrow results by field values before or after searching
- Use ask_clarification ONLY if the query is truly ambiguous and you cannot proceed
- You may call multiple tools or the same tool multiple times to refine results
- When you have found sufficient relevant documents, stop calling tools and provide your final response
- Always explain your search strategy briefly in your response
- Cite documents using their IDs: [resource_id <ID>]
`)

	return sb.String()
}

// RetrievalAgentSystemPromptWithoutTools builds the system prompt for Ollama-style
// structured output fallback (no native tool calling).
func RetrievalAgentSystemPromptWithoutTools(availableIndexes []IndexInfo, tableSchema string, agentKnowledge string) string {
	var sb strings.Builder

	sb.WriteString(`You are an intelligent retrieval agent with access to a document database. Your role is to find the most relevant documents to answer the user's query.

## How You Work

When you need to search or perform actions, use XML-like tags in your response.

## Available Actions

`)

	// Describe available actions based on indexes
	var hasAknn, hasFullText, hasGraph bool
	for _, idx := range availableIndexes {
		switch idx.Type {
		case "embeddings", "aknn_v0":
			hasAknn = true
		case "full_text", "full_text_v0":
			hasFullText = true
		case "graph", "graph_v0":
			hasGraph = true
		}
	}

	sb.WriteString(`### Add a filter:
<filter>
field: field_name
operator: eq|ne|gt|gte|lt|lte|contains|prefix|range|in
value: filter_value
</filter>

### Ask for clarification:
<clarification>
question: Your clarifying question here
options:
- Option 1
- Option 2
</clarification>

`)

	if hasAknn {
		sb.WriteString(`### Semantic search (vector similarity):
<semantic_search>
query: Your natural language search query
index: index_name
limit: 10
</semantic_search>

`)
	}

	if hasFullText {
		sb.WriteString(`### Full-text search (BM25 keyword matching):
<full_text_search>
query: Your keyword search query
index: index_name
limit: 10
</full_text_search>

`)
	}

	if hasGraph {
		sb.WriteString(`### Tree search (hierarchical navigation):
<tree_search>
query: Your query for tree exploration
index: index_name
start_nodes: optional starting query
max_depth: 3
beam_width: 3
</tree_search>

### Graph search (relationship traversal):
<graph_search>
start_node: node_id
index: index_name
edge_type: relationship_type
direction: outgoing|incoming|both
depth: 1
</graph_search>

`)
	}

	// Describe available indexes
	if len(availableIndexes) > 0 {
		sb.WriteString("## Available Indexes\n\n")
		for _, idx := range availableIndexes {
			desc := idx.Description
			if desc == "" {
				switch idx.Type {
				case "embeddings", "aknn_v0":
					desc = "Vector/semantic similarity search"
				case "full_text", "full_text_v0":
					desc = "Full-text BM25 keyword search"
				case "graph", "graph_v0":
					desc = "Graph/tree document relationships"
				}
			}
			fmt.Fprintf(&sb, "- **%s** (%s): %s\n", idx.Name, idx.Type, desc)
		}
		sb.WriteString("\n")
	}

	if tableSchema != "" {
		sb.WriteString("## Document Schema\n\n")
		sb.WriteString(tableSchema)
		sb.WriteString("\n")
	}

	if agentKnowledge != "" {
		sb.WriteString("## Domain Knowledge\n\n")
		sb.WriteString(agentKnowledge)
		sb.WriteString("\n")
	}

	sb.WriteString(`## Guidelines

- Start with the most likely search action based on the query type
- Use semantic_search for natural language questions and conceptual queries
- Use full_text_search for keyword/phrase matching and exact terms
- Use tree_search to navigate hierarchical document structures
- Use graph_search to explore relationships between documents
- Use ask_clarification ONLY if the query is truly ambiguous
- You can combine multiple actions in one response
- When you have found sufficient relevant documents, stop using actions and provide your final response
- Cite documents using their IDs: [resource_id <ID>]
`)

	return sb.String()
}

// TreeSearchNavigationPrompt is the system prompt for tree search branch selection.
// It evaluates summaries at each tree level to select which branches to explore.
const TreeSearchNavigationPrompt = `You are a navigation assistant for hierarchical document search.

Your task is to select which branches of a document tree are most relevant to the user's query.

## CONTEXT

You will be shown summaries of document sections at the current level.
Select the branches most likely to contain the answer.

## SELECTION CRITERIA

- Relevance to the query
- Likelihood of containing detailed information
- Coverage of different aspects of the query

## OUTPUT FORMAT

Respond in JSON:

{
  "selected": ["node_id_1", "node_id_2"],
  "skipped": ["node_id_3"],
  "reasoning": "Why these branches were selected",
  "confidence": 0.0-1.0,
  "should_continue": true/false // Whether to continue deeper
}`

// TreeSearchNavigationUserPrompt is the user prompt for tree navigation
const TreeSearchNavigationUserPrompt = `User Query: "{{query}}"

## CURRENT LEVEL (depth={{depth}})
{{#each nodes}}
### Node: {{this.id}}
Summary: {{this.summary}}
Has children: {{this.has_children}}
---
{{/each}}

## ALREADY COLLECTED
{{#if collected}}
{{#each collected}}
- {{this.id}}: {{this.summary}}
{{/each}}
{{else}}
No documents collected yet.
{{/if}}

Select which branches to explore. Return JSON.`

// TreeSearchSufficiencyPrompt is the system prompt for checking if collected documents are sufficient.
const TreeSearchSufficiencyPrompt = `You are an evaluator for document retrieval sufficiency.

Your task is to determine if the collected documents are sufficient to answer the user's query.

## SUFFICIENCY CRITERIA

Consider:
1. Do collected documents directly address the query?
2. Are there likely to be better/more relevant documents in unexplored branches?
3. Is the current coverage comprehensive enough?

## OUTPUT FORMAT

Respond in JSON:

{
  "sufficient": true/false,
  "confidence": 0.0-1.0,
  "reason": "Why the collection is/isn't sufficient",
  "missing_aspects": ["aspect1", "aspect2"] // What's still missing (if not sufficient)
}`

// TreeSearchSufficiencyUserPrompt is the user prompt for sufficiency check
const TreeSearchSufficiencyUserPrompt = `User Query: "{{query}}"

## COLLECTED DOCUMENTS
{{#each collected}}
### Document: {{this.id}}
Content: {{this.content}}
---
{{/each}}

## UNEXPLORED BRANCHES
{{#if unexplored}}
{{#each unexplored}}
- {{this.id}}: {{this.summary}}
{{/each}}
{{else}}
No unexplored branches remaining.
{{/if}}

Evaluate if the collected documents are sufficient. Return JSON.`
