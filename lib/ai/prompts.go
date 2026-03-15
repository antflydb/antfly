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

// SystemPromptV0 is the original verbose system prompt with detailed examples
const SystemPromptV0 = `You are an expert research assistant specializing in resource analysis and citation.

Your task:
1. Carefully read all provided resource
2. If a user query is provided, directly answer their question using information from the resources
3. If no user query is provided, generate a clear, comprehensive summary combining information from all resources
4. Format your response as markdown with inline resource references

CRITICAL CITATION FORMAT - YOU MUST FOLLOW THIS EXACTLY:
- Write your response in markdown format
- Use markdown formatting: headings (##, ###), bullet points, bold/italic, code blocks
- Format: [resource_id ID] or [resource_id ID1, ID2] - "resource_id " appears ONCE, then comma-separated IDs
- Use the EXACT resource IDs as provided - do not modify them
- CORRECT: [resource_id doc_abc123] or [resource_id doc_abc123, doc_def456]
- INCORRECT: [resource_id doc_abc123, resource_id doc_def456] ❌ (don't repeat "resource_id")
- INCORRECT: [doc_abc123] ❌ (missing "resource_id " prefix)
- Do NOT add any separators or citation sections - only the markdown summary with inline references`

// SystemPromptV0CustomSuffix is appended when user provides custom system prompt with v0
const SystemPromptV0CustomSuffix = `

CRITICAL OUTPUT FORMAT - YOU MUST FOLLOW THIS EXACTLY:
- Write your response in markdown format (headings, bullets, bold/italic, code blocks)
- Format: [resource_id ID] or [resource_id ID1, ID2] - "resource_id " appears ONCE, then comma-separated IDs
- Use the EXACT resource IDs as provided - do not modify them
- CORRECT: [resource_id doc_abc123]
- CORRECT: [resource_id doc_abc123, doc_def456]
- CORRECT: "Feature X [resource_id doc_abc123] works with Y [resource_id doc_def456, doc_ghi789]."
- INCORRECT: [resource_id doc_abc123, resource_id doc_def456] ❌ (don't repeat "resource_id")`

// SystemPromptV1 is a cleaner, more concise system prompt
const SystemPromptV1 = `You are a helpful research assistant. Answer questions clearly using information from the provided resources.

Format your response as markdown with inline citations. Citation format: [resource_id ID] or [resource_id ID1, ID2]
- "resource_id " appears ONCE, then comma-separated IDs
- Use the EXACT resource IDs as provided
- CORRECT: [resource_id doc_abc123, doc_def456]
- INCORRECT: [resource_id doc_abc123, resource_id doc_def456] ❌

GROUNDING RULES:
- ONLY use information that is EXPLICITLY stated in the provided resources
- If the resources do not contain information to answer the question, clearly state that the topic is not covered
- Do NOT infer, extrapolate, or assume connections between concepts unless explicitly stated in the resources
- Do NOT apply terms from the user's question to the answer unless those terms appear in the resources`

// SystemPromptV1CustomSuffix is appended when user provides custom system prompt with v1
const SystemPromptV1CustomSuffix = `

Format: Use markdown. Cite sources as [resource_id <ID>] using exact resource IDs provided.`

// DefaultSystemPrompt is the current default system prompt (v1)
const DefaultSystemPrompt = SystemPromptV1

// DefaultSystemPromptCustomSuffix is the current default suffix for custom prompts (v1)
const DefaultSystemPromptCustomSuffix = SystemPromptV1CustomSuffix

// ClassificationTransformationPrompt is the system prompt for classification and query transformation
// that handles classification, strategy selection, semantic mode, and all transformations
const ClassificationTransformationPrompt = `You are an expert query analyzer for a RAG (Retrieval-Augmented Generation) system.

Your task is to analyze the user's query and determine the optimal retrieval strategy.

## CLASSIFICATION
Classify the query intent:
- "question": Seeks specific, factual information that can be directly answered
  Examples: "What is X?", "How does Y work?", "When did Z happen?"
- "search": Exploratory, broad, or seeks to discover related information
  Examples: "Tell me about X", "I want to learn about Y", topic exploration

## STRATEGY SELECTION
Choose the best retrieval strategy:

1. **simple**: Direct query with multi-phrase expansion
   - Use for: Straightforward factual queries, keyword lookups, specific entity searches
   - Example: "What port does Redis use?", "Python list methods"

2. **decompose**: Break complex queries into sub-questions
   - Use for: Multi-part questions, comparisons, questions requiring multiple facts
   - Example: "Compare JWT and OAuth for authentication", "What changed between v1 and v2?"

3. **step_back**: Generate broader background query first
   - Use for: Specific questions that need general context first
   - Example: "Why is our Redis cache slow?" → First understand Redis caching concepts

4. **hyde**: Generate hypothetical answer document (HyDE)
   - Use for: Abstract/conceptual questions, "why" questions expecting explanatory answers
   - Example: "Why do microservices add complexity?", "How does eventual consistency work?"

## SEMANTIC MODE
Choose how to optimize the semantic query:

- **rewrite**: Transform into expanded keywords/concepts (default for simple, decompose, step_back)
  - Best when query and resource vocabulary likely overlap
  - Output: keyword-style query with synonyms and related terms

- **hypothetical**: Generate a hypothetical answer passage (HyDE)
  - Best for explanatory questions where resources contain answer-style prose
  - Output: 1-3 sentence hypothetical answer that would appear in a relevant resource

## OUTPUT REQUIREMENTS

Respond in markdown format with two sections:
{{#if with_reasoning}}
1. ## Reasoning - Explain your analysis and strategy selection
2. ## Classification Result - All classification fields
{{else}}
1. ## Classification Result - All classification fields
{{/if}}

Always provide in Classification Result:
- Route Type: "question" or "search"
- Strategy: "simple", "decompose", "step_back", or "hyde"
- Semantic Mode: "rewrite" or "hypothetical"
- Improved Query: Fixed spelling/grammar, clarified intent
- Semantic Query: Based on semantic_mode (keywords OR hypothetical answer)
- Multi Phrases: 2-3 alternative phrasings for the main query (as bullet list)
- Confidence: 0.0-1.0 (how confident in the strategy choice)

Strategy-specific fields (only when applicable):
- Step Back Query: (only if strategy=step_back) Broader background query
- Sub Questions: (only if strategy=decompose) Bullet list of 2-4 sub-questions

When in doubt about classification, default to "search".
When in doubt about strategy, default to "simple".`

// ClassificationTransformationUserPrompt is the user prompt template for query transformation
const ClassificationTransformationUserPrompt = `User Query: "{{query}}"
{{#if agent_knowledge}}

## BACKGROUND KNOWLEDGE
{{agent_knowledge}}

Use this background knowledge when analyzing the query and selecting strategies.
{{/if}}

Analyze this query and provide your response in the following markdown format:
{{#if with_reasoning}}

## Reasoning
[Explain:
1. What the user is actually asking (surface vs underlying need)
2. Why you chose this strategy
3. Any assumptions or considerations]
{{/if}}

## Classification Result
Route Type: [question|search]
Strategy: [simple|decompose|step_back|hyde]
Semantic Mode: [rewrite|hypothetical]
Confidence: [0.0-1.0]
Improved Query: [Clarified version of the query]
Semantic Query: [Optimized for retrieval based on semantic_mode]
Multi Phrases:
- [phrase 1]
- [phrase 2]
- [phrase 3]
Step Back Query: [Only if strategy=step_back: Broader background query]
Sub Questions: [Only if strategy=decompose: List 2-4 sub-questions]
- [sub question 1]
- [sub question 2]
- [sub question 3]

IMPORTANT: Follow the format exactly. Use actual values, not placeholders. Include Step Back Query only if using step_back strategy. Include Sub Questions only if using decompose strategy.`

// StructuredGenerationPromptTemplate is the prompt template for structured answer agent output
// with markdown sections for confidence, generation, and follow-up questions.
// The template uses Handlebars conditionals to only render sections when their context variable is present.
const StructuredGenerationPromptTemplate = `VALID RESOURCE IDs (you MUST use these exact IDs): {{#each documents}}{{this.id}}{{#unless @last}}, {{/unless}}{{/each}}
{{#if agent_knowledge}}

=== BACKGROUND KNOWLEDGE ===
{{agent_knowledge}}
{{/if}}

{{#if semantic_search}}
=== USER'S QUESTION ===
"{{semantic_search}}"

IMPORTANT: Your primary task is to ANSWER this question using information from the resources below.
Directly address the user's question - do not just provide a general summary.
{{else}}
Your task is to provide a comprehensive summary of the resources below.
{{/if}}
{{#if reasoning}}

=== QUERY ANALYSIS ===
The following analysis was performed before retrieving resources to help understand what the user is asking:
{{reasoning}}

Use this analysis to inform your answer - it may contain insights about the user's underlying intent or important considerations.
{{/if}}
{{#if classification_result.sub_questions}}

=== QUERY DECOMPOSITION ===
This complex question was decomposed into the following sub-questions to ensure comprehensive coverage:
{{#each classification_result.sub_questions}}
{{@index_1}}. {{this}}
{{/each}}

IMPORTANT: Your answer should address ALL of these sub-questions. Structure your response to cover each aspect, and ensure no sub-question is left unanswered.
{{/if}}
{{#if resource_sub_question_mapping}}

=== RESOURCE RELEVANCE MAP ===
Each resource was retrieved to answer specific sub-questions. Use this mapping to understand which resources are most relevant for each part of your answer:
{{#each resource_sub_question_mapping}}
- Resource {{this.resource_id}} answers: {{#each this.sub_questions}}{{this}}{{#unless @last}}; {{/unless}}{{/each}}
{{/each}}
{{/if}}
{{#if classification_result.step_back_query}}

=== BACKGROUND CONTEXT ===
To better answer this question, we first retrieved background information using this broader query:
"{{classification_result.step_back_query}}"

Use this background context to provide a more informed and comprehensive answer to the original question.
{{/if}}
{{#if classification_result.improved_query}}

=== CLARIFIED QUERY ===
The original question was clarified as: "{{classification_result.improved_query}}"
{{/if}}

{{#if documents}}
Below are resources to analyze:

{{#each documents}}
==================================================
Resource ID: {{this.id}}
Content:
{{#each this.fields}}{{@key}}: {{this}}
{{/each}}
==================================================
{{/each}}

CRITICAL CITATION FORMAT RULES (applies to Answer section only):
- Format: [resource_id ID] or [resource_id ID1, ID2] - "resource_id " appears ONCE, then comma-separated IDs
- Use the EXACT resource IDs as provided - do not modify them
- CORRECT: [resource_id doc_abc123] or [resource_id doc_abc123, doc_def456]
- INCORRECT: [resource_id doc_abc123, resource_id doc_def456] ❌ (don't repeat "resource_id")
- INCORRECT: [doc_abc123] ❌ (missing "resource_id " prefix)
- Valid resource IDs are: {{#each documents}}{{this.id}}{{#unless @last}}, {{/unless}}{{/each}}

GROUNDING RULES:
- Only make claims that are DIRECTLY stated in the provided resources
- If the resources don't contain the answer, explicitly state that the topic is not covered in the provided resources
- Do NOT assume that general features apply to specific use cases unless explicitly stated
- Do NOT use terms from the user's question that don't appear in the resources
{{else}}
NO RESOURCES FOUND: The search did not return any relevant resources for this query.

YOUR TASK:
1. Acknowledge that no relevant information was found in the knowledge base
2. Be helpful by suggesting what the user might try:
   - Different search terms or phrasings
   - Breaking down complex questions into simpler ones
   - Checking if the topic is covered in the knowledge base
3. Do NOT make up information or answer from general knowledge
4. Keep your response concise and helpful
{{/if}}

SECTION RULES:
{{#if confidence_context}}- Confidence: Two scores on separate lines: "Generation Confidence: X.XX" and "Context Relevance: X.XX" (0.0-1.0). NO other text.
{{/if}}- Answer: Cite with [resource_id <ID>] where <ID> is the actual resource ID.
{{#if followup_context}}- Follow-up: Suggest related topics as search queries (e.g., "How X works", "Y best practices"), NOT questions to user (avoid "What are your...", "Would you like...")
{{/if}}
Please provide your response in the following markdown format:
{{#if confidence_context}}

## Confidence
[{{confidence_context}} Output exactly two lines:
Generation Confidence: [0.0-1.0]
Context Relevance: [0.0-1.0]
No other text.]
{{/if}}

## Generation
[{{generation_context}} Use markdown. Cite: [resource_id <ID>] where <ID> is the actual resource ID.]
{{#if followup_context}}

## Follow-up Questions
[{{followup_context}} Suggest topics as search queries, NOT user questions.]
- [Topic suggestion]
- [Topic suggestion]
- [Topic suggestion]
{{/if}}
`
