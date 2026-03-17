/*
Copyright 2025 The Antfly Authors

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
package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	antfly "github.com/antflydb/antfly/pkg/client"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/spf13/cobra"
)

func addAgentCommands(parent *cobra.Command) {
	agentsCmd := &cobra.Command{
		Use:   "agents",
		Short: "Run AI agents",
		Long:  `The agents command provides subcommands for running AI agents such as the retrieval agent.`,
	}

	retrievalCmd := &cobra.Command{
		Use:   "retrieval",
		Short: "Performs a retrieval agent query",
		Long: `Performs a retrieval agent query that retrieves relevant documents and generates a summary using an LLM.

Pipeline steps (enable with flags):
  --classify     Analyze the query and select optimal retrieval strategy
  --reasoning    Show pre-retrieval reasoning during classification (implies --classify)
  --generate     Synthesize an answer from retrieved documents
  --followup     Generate suggested follow-up questions
  --confidence   Score confidence in the generated answer

Tool calling & chat mode:
  --max-iterations N   Enable agentic tool calling with N rounds per turn.
                        Starts an interactive REPL where conversation history,
                        filters, and clarification requests persist across turns.
  --tools              Comma-separated list of enabled tools. When omitted, defaults to
                        add_filter, ask_clarification, search plus any index-based tools
                        (semantic_search, full_text_search, tree_search, graph_search)
                        auto-enabled from the table's indexes. When set explicitly, only
                        the listed tools are enabled.
                        Available: add_filter, ask_clarification, search, websearch, fetch,
                        semantic_search, full_text_search, tree_search, graph_search.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			params, err := parseSearchParams(cmd)
			if err != nil {
				return err
			}

			generatorStr, _ := cmd.Flags().GetString("generator")
			systemPrompt, _ := cmd.Flags().GetString("system-prompt")
			streaming, _ := cmd.Flags().GetBool("streaming")
			docRenderer, _ := cmd.Flags().GetString("template")
			prompt, _ := cmd.Flags().GetString("prompt")
			evalStr, _ := cmd.Flags().GetString("eval")
			classify, _ := cmd.Flags().GetBool("classify")
			reasoning, _ := cmd.Flags().GetBool("reasoning")
			generate, _ := cmd.Flags().GetBool("generate")
			followup, _ := cmd.Flags().GetBool("followup")
			confidence, _ := cmd.Flags().GetBool("confidence")
			maxIterations, _ := cmd.Flags().GetInt("max-iterations")
			maxContextTokens, _ := cmd.Flags().GetInt("max-context-tokens")

			generator, err := parseJSONFlag[antfly.GeneratorConfig](generatorStr, "generator")
			if err != nil {
				return err
			}

			var evalConfig *antfly.EvalConfig
			if evalStr != "" {
				parsed, parseErr := parseJSONFlag[antfly.EvalConfig](evalStr, "eval")
				if parseErr != nil {
					return parseErr
				}
				evalConfig = &parsed
			}

			toolsConfig, err := parseToolsConfig(cmd)
			if err != nil {
				return err
			}

			// Chat mode: --max-iterations > 0 starts an interactive REPL
			if maxIterations > 0 {
				return runChat(cmd, params, generator, systemPrompt, docRenderer, maxIterations, toolsConfig)
			}

			// Default query to prompt, then semantic search, then full-text-search flag
			queryText := prompt
			if queryText == "" {
				queryText = params.SemanticSearch
			}
			if queryText == "" {
				queryText, _ = cmd.Flags().GetString("full-text-search")
			}

			ragReq := antfly.RetrievalAgentRequest{
				Generator:        generator,
				Query:            queryText,
				Stream:           streaming,
				MaxContextTokens: maxContextTokens,
			}

			ragReq.Steps.Classification.Enabled = classify || reasoning
			ragReq.Steps.Classification.WithReasoning = reasoning
			ragReq.Steps.Generation.Enabled = generate
			if systemPrompt != "" {
				ragReq.Steps.Generation.SystemPrompt = systemPrompt
			}
			ragReq.Steps.Followup.Enabled = followup
			ragReq.Steps.Confidence.Enabled = confidence
			ragReq.Steps.Tools = toolsConfig

			if evalConfig != nil {
				ragReq.Steps.Eval = *evalConfig
			}

			ragOpts := antfly.RetrievalAgentOptions{}
			if streaming {
				sp := newSpinner(os.Stderr)
				if classify || reasoning {
					sp.start("Classifying query...")
				} else {
					sp.start("Searching...")
				}
				configureStreamingCallbacks(sp, &ragOpts)
			} else {
				fmt.Fprintln(os.Stderr, "Executing retrieval agent query...")
			}

			resp, err := antflyClient.RunRetrievalAgent(cmd.Context(), params, docRenderer, ragReq, ragOpts)
			if err != nil {
				return fmt.Errorf("retrieval agent failed: %w", err)
			}

			if streaming {
				printStreamingFooter(resp)
			} else {
				resultJSON, jsonErr := json.EncodeIndented(resp, "", "  ", json.SortMapKeys)
				if jsonErr != nil {
					return fmt.Errorf("failed to marshal result to JSON: %w", jsonErr)
				}
				fmt.Print(string(resultJSON))
				fmt.Println()
			}
			return nil
		},
	}

	agentsCmd.AddCommand(retrievalCmd)

	addSearchFlags(retrievalCmd)
	retrievalCmd.Flags().String("generator", "", "JSON string defining generator model configuration (e.g. '{\"provider\":\"ollama\",\"model\":\"gemma3:4b-it-qat\"}')")
	retrievalCmd.Flags().String("prompt", "", "Handlebars template for customizing the user prompt sent to the generator")
	retrievalCmd.Flags().String("system-prompt", "", "Optional system prompt to guide the summarization")
	retrievalCmd.Flags().Bool("streaming", true, "Stream the response as it's generated (disable for structured JSON with citations)")
	retrievalCmd.Flags().String("template", "", "Optional Handlebars template string for rendering document content to the prompt")
	retrievalCmd.Flags().String("eval", "", `JSON string defining evaluation configuration (e.g. '{"evaluators":["recall","faithfulness"],"ground_truth":{"relevant_ids":["doc1","doc2"]},"judge":{"provider":"ollama","model":"gemma3:4b"}}')`)

	// Pipeline step flags
	retrievalCmd.Flags().Bool("classify", false, "Enable query classification and strategy selection")
	retrievalCmd.Flags().Bool("reasoning", false, "Show pre-retrieval reasoning during classification (implies --classify)")
	retrievalCmd.Flags().Bool("generate", false, "Enable answer generation from retrieved documents")
	retrievalCmd.Flags().Bool("followup", false, "Enable follow-up question generation")
	retrievalCmd.Flags().Bool("confidence", false, "Enable confidence scoring")

	// Tool calling & chat mode
	retrievalCmd.Flags().Int("max-context-tokens", 0, "Maximum tokens for document context (0 = no limit). Documents exceeding this budget are pruned.")
	retrievalCmd.Flags().Int("max-iterations", 0, "Enable multi-turn chat with N tool-calling rounds per turn (0 = single-shot)")
	retrievalCmd.Flags().String("tools", "", "Comma-separated list of enabled tools (e.g. semantic_search,websearch,fetch)")
	retrievalCmd.Flags().String("websearch-config", "", "JSON config for websearch tool (e.g. '{\"provider\":\"tavily\"}')")
	retrievalCmd.Flags().String("fetch-config", "", "JSON config for fetch tool (e.g. '{\"max_content_length\":10000}')")

	_ = retrievalCmd.MarkFlagRequired("generator")

	// Query builder subcommand
	queryBuilderCmd := &cobra.Command{
		Use:   "query-builder",
		Short: "Generate a structured search query from natural language",
		Long: `Uses an LLM to translate a natural language search intent into a structured Bleve query.

The generated query can be used with the --full-text-search-json flag of the query command.

Example:
  antfly agents query-builder \
    --intent "find published articles about machine learning from 2024" \
    --table articles \
    --generator '{"provider":"ollama","model":"gemma3:4b-it-qat"}'`,
		RunE: func(cmd *cobra.Command, args []string) error {
			intent, _ := cmd.Flags().GetString("intent")
			table, _ := cmd.Flags().GetString("table")
			generatorStr, _ := cmd.Flags().GetString("generator")
			schemaFieldsStr, _ := cmd.Flags().GetString("schema-fields")

			generator, err := parseJSONFlag[antfly.GeneratorConfig](generatorStr, "generator")
			if err != nil {
				return err
			}

			req := antfly.QueryBuilderRequest{
				Intent:    intent,
				Generator: generator,
				Table:     table,
			}

			req.SchemaFields = splitCSV(schemaFieldsStr)

			result, err := antflyClient.RunQueryBuilder(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("query builder failed: %w", err)
			}

			resultJSON, jsonErr := json.EncodeIndented(result, "", "  ", json.SortMapKeys)
			if jsonErr != nil {
				return fmt.Errorf("failed to marshal result to JSON: %w", jsonErr)
			}
			fmt.Print(string(resultJSON))
			fmt.Println()
			return nil
		},
	}

	agentsCmd.AddCommand(queryBuilderCmd)

	queryBuilderCmd.Flags().String("intent", "", "Natural language description of the search intent (required)")
	queryBuilderCmd.Flags().String("table", "", "Table name for schema context")
	queryBuilderCmd.Flags().String("generator", "", "JSON string defining generator model configuration (required)")
	queryBuilderCmd.Flags().String("schema-fields", "", "Comma-separated list of searchable field names (overrides table schema)")

	_ = queryBuilderCmd.MarkFlagRequired("intent")
	_ = queryBuilderCmd.MarkFlagRequired("generator")

	parent.AddCommand(agentsCmd)
}

// parseToolsConfig parses the --tools, --websearch-config, and --fetch-config flags into a ChatToolsConfig.
func parseToolsConfig(cmd *cobra.Command) (antfly.ChatToolsConfig, error) {
	var config antfly.ChatToolsConfig

	toolsStr, _ := cmd.Flags().GetString("tools")
	for _, t := range splitCSV(toolsStr) {
		name := antfly.ChatToolName(t)
		if err := antfly.ValidateToolName(name); err != nil {
			return config, fmt.Errorf("invalid --tools value: %w", err)
		}
		config.EnabledTools = append(config.EnabledTools, name)
	}

	websearchStr, _ := cmd.Flags().GetString("websearch-config")
	if websearchStr != "" {
		wsc, err := parseJSONFlag[antfly.WebSearchConfig](websearchStr, "websearch-config")
		if err != nil {
			return config, err
		}
		config.WebsearchConfig = wsc
	}

	fetchStr, _ := cmd.Flags().GetString("fetch-config")
	if fetchStr != "" {
		fc, err := parseJSONFlag[antfly.FetchConfig](fetchStr, "fetch-config")
		if err != nil {
			return config, err
		}
		config.FetchConfig = fc
	}

	return config, nil
}

// configureStreamingCallbacks wires up the SSE streaming callbacks on a spinner.
func configureStreamingCallbacks(sp *spinner, ragOpts *antfly.RetrievalAgentOptions) {
	ragOpts.OnStepStarted = func(id, step, action string) error {
		sp.start(action)
		return nil
	}
	ragOpts.OnStepProgress = func(data map[string]any) error {
		step, _ := data["step"].(string)
		if depth, ok := data["depth"]; ok {
			sp.setDetail(fmt.Sprintf("[%s] depth=%v nodes=%v", step, depth, data["num_nodes"]))
		}
		if sufficient, ok := data["sufficient"]; ok {
			sp.setDetail(fmt.Sprintf("[%s] sufficient=%v reason=%v", step, sufficient, data["reason"]))
		}
		return nil
	}
	ragOpts.OnStepCompleted = func(step *antfly.RetrievalReasoningStep) error {
		msg := fmt.Sprintf("%s (%dms)", step.Action, step.DurationMs)
		if step.Status == "error" {
			sp.stopError(msg)
		} else {
			sp.stop(msg)
		}
		return nil
	}
	ragOpts.OnHit = func(hit *antfly.Hit) error {
		sp.log(fmt.Sprintf("  \033[2m→ %s (score=%.4f)\033[0m", hit.ID, hit.Score))
		return nil
	}
	ragOpts.OnToolMode = func(mode string, toolsCount int) error {
		sp.setDetail(fmt.Sprintf("tool mode: %s (%d tools)", mode, toolsCount))
		return nil
	}
	var reasoningBuf string
	ragOpts.OnClassification = func(c *antfly.ClassificationTransformationResult) error {
		if reasoningBuf != "" {
			for line := range strings.SplitSeq(strings.TrimSpace(reasoningBuf), "\n") {
				sp.log(fmt.Sprintf("  \033[2m%s\033[0m", line))
			}
		}
		sp.log(fmt.Sprintf("\033[32m✓\033[0m Classified: strategy=%s route=%s confidence=%.2f", c.Strategy, c.RouteType, c.Confidence))
		sp.start("Searching...")
		return nil
	}
	ragOpts.OnReasoning = func(chunk string) error {
		reasoningBuf += chunk
		lines := strings.Split(strings.TrimSpace(reasoningBuf), "\n")
		sp.setDetail(lines[len(lines)-1])
		return nil
	}
	generationStarted := false
	ragOpts.OnGeneration = func(chunk string) error {
		if !generationStarted {
			generationStarted = true
			sp.stop("Generating response")
		}
		_, _ = fmt.Fprint(os.Stdout, chunk)
		return nil
	}
	followupStarted := false
	ragOpts.OnFollowup = func(question string) error {
		if !followupStarted {
			followupStarted = true
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, "\033[2mSuggested follow-ups:\033[0m")
		}
		fmt.Fprintf(os.Stderr, "\033[2m  → %s\033[0m\n", question)
		return nil
	}
	ragOpts.OnError = func(err *antfly.RetrievalAgentError) error {
		sp.stopError(err.Error)
		return nil
	}
}

// printStreamingFooter prints post-stream information (trailing newline, confidence).
func printStreamingFooter(resp *antfly.RetrievalAgentResult) {
	if resp.Generation != "" {
		fmt.Println()
	}
	if resp.GenerationConfidence > 0 {
		fmt.Fprintf(os.Stderr, "\033[2mConfidence: %.0f%%\033[0m\n", resp.GenerationConfidence*100)
	}
}

// runChat implements the interactive multi-turn REPL for --max-iterations mode.
func runChat(cmd *cobra.Command, params SearchParams, generator antfly.GeneratorConfig, systemPrompt, docRenderer string, maxIterations int, toolsConfig antfly.ChatToolsConfig) error {
	var messages []antfly.ChatMessage
	var accumulatedFilters []antfly.FilterSpec

	reader := bufio.NewReader(os.Stdin)

	fmt.Fprintln(os.Stderr, "\033[1mAntfly Chat\033[0m — type your question, or \033[2m/quit\033[0m to exit.")
	fmt.Fprintln(os.Stderr)

	for {
		fmt.Fprint(os.Stderr, "\033[1m> \033[0m")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr)
			return nil
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		if input == "/quit" || input == "/exit" || input == "/q" {
			return nil
		}

		ragReq := antfly.RetrievalAgentRequest{
			Generator:          generator,
			Query:              input,
			Stream:             true,
			Messages:           messages,
			AccumulatedFilters: accumulatedFilters,
			MaxIterations:      maxIterations,
		}

		ragReq.Steps.Generation.Enabled = true
		if systemPrompt != "" {
			ragReq.Steps.Generation.SystemPrompt = systemPrompt
		}
		ragReq.Steps.Followup.Enabled = true
		ragReq.Steps.Tools = toolsConfig

		sp := newSpinner(os.Stderr)
		sp.start("Thinking...")

		ragOpts := antfly.RetrievalAgentOptions{}
		configureStreamingCallbacks(sp, &ragOpts)

		resp, err := antflyClient.RunRetrievalAgent(cmd.Context(), params, docRenderer, ragReq, ragOpts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\033[31mError: %v\033[0m\n\n", err)
			continue
		}

		printStreamingFooter(resp)

		// Update conversation state for next turn
		messages = resp.Messages
		if len(resp.AppliedFilters) > 0 {
			accumulatedFilters = resp.AppliedFilters
		}

		// Handle clarification requests
		if resp.Status == antfly.RetrievalAgentStatusIncomplete &&
			resp.IncompleteDetails.Reason == "clarification_needed" {
			cr := resp.ClarificationRequest
			fmt.Fprintln(os.Stderr)
			fmt.Fprintf(os.Stderr, "\033[33m? %s\033[0m\n", cr.Question)
			if cr.Reason != "" {
				fmt.Fprintf(os.Stderr, "\033[2m  (%s)\033[0m\n", cr.Reason)
			}
			for i, opt := range cr.Options {
				fmt.Fprintf(os.Stderr, "\033[2m  %d. %s\033[0m\n", i+1, opt)
			}
		}

		fmt.Fprintln(os.Stderr)
	}
}
