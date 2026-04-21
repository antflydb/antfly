//go:build onnx && ORT

package pipelines

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"
)

func TestGemmaGreedyGeneration(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "model.onnx")); err != nil {
		t.Skip("Gemma model not downloaded, skipping")
	}

	// Load model
	sessionManager := backends.NewSessionManager()
	pipeline, _, err := LoadTextGenerationPipeline(
		modelPath,
		sessionManager,
		[]string{"onnx"},
	)
	if err != nil {
		t.Fatalf("loading pipeline: %v", err)
	}
	defer pipeline.Close()

	// Verify generation_config.json sampling params were loaded
	t.Logf("Pipeline GenerationConfig: DoSample=%v Temperature=%.2f TopK=%d TopP=%.2f",
		pipeline.GenerationConfig.DoSample,
		pipeline.GenerationConfig.Temperature,
		pipeline.GenerationConfig.TopK,
		pipeline.GenerationConfig.TopP,
	)

	// Force greedy decoding for deterministic test (override model's do_sample=true)
	pipeline.GenerationConfig.DoSample = false

	// Use the chat template prompt that Python produces for this model
	prompt := "<bos><start_of_turn>user\nWhat is the capital of France?<end_of_turn>\n<start_of_turn>model\n"

	// Log tokenization
	promptTokens := pipeline.Tokenizer.Encode(prompt)
	t.Logf("Prompt tokens (%d): %v", len(promptTokens), promptTokens)

	// Limit to a small number of tokens
	pipeline.GenerationConfig.MaxNewTokens = 20

	result, err := pipeline.Generate(context.Background(), prompt)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}

	t.Logf("Generated %d tokens: %q", result.TokenCount, result.Text)
	t.Logf("StoppedAtEOS: %v", result.StoppedAtEOS)
	t.Logf("TokenIDs: %v", result.TokenIDs)

	// The model should mention "Paris" — greedy decoding is deterministic
	if !strings.Contains(strings.ToLower(result.Text), "paris") {
		t.Errorf("expected output to contain 'Paris', got: %q", result.Text)
	}
}

func TestGemmaGenerationConfigFromJSON(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "generation_config.json")); err != nil {
		t.Skip("Gemma model not downloaded, skipping")
	}

	// Verify generation_config.json is correctly parsed
	genCfg := loadGenerationConfig(modelPath)
	if genCfg == nil {
		t.Fatal("loadGenerationConfig returned nil")
	}

	// The model's generation_config.json has: do_sample=true, top_k=64, top_p=0.95
	if !genCfg.DoSample {
		t.Error("expected DoSample=true from generation_config.json")
	}
	if genCfg.TopK != 64 {
		t.Errorf("expected TopK=64, got %d", genCfg.TopK)
	}
	if genCfg.TopP != 0.95 {
		t.Errorf("expected TopP=0.95, got %f", genCfg.TopP)
	}

	// Verify buildGenerationConfig produces correct backends.GenerationConfig
	cfg := buildGenerationConfig(genCfg)
	if !cfg.DoSample {
		t.Error("expected DoSample=true in GenerationConfig")
	}
	if cfg.TopK != 64 {
		t.Errorf("expected TopK=64 in GenerationConfig, got %d", cfg.TopK)
	}
	if cfg.TopP != 0.95 {
		t.Errorf("expected TopP=0.95 in GenerationConfig, got %f", cfg.TopP)
	}
}

func TestGemmaRAGPromptGeneration(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "model.onnx")); err != nil {
		t.Skip("Gemma model not downloaded, skipping")
	}

	sessionManager := backends.NewSessionManager()
	pipeline, _, err := LoadTextGenerationPipeline(
		modelPath,
		sessionManager,
		[]string{"onnx"},
	)
	if err != nil {
		t.Fatalf("loading pipeline: %v", err)
	}
	defer pipeline.Close()

	// Force greedy decoding for deterministic test
	pipeline.GenerationConfig.DoSample = false
	pipeline.GenerationConfig.MaxNewTokens = 50

	// Simulate a RAG classification prompt similar to what the retrieval agent sends
	prompt := `<bos><start_of_turn>user
You are a search query classifier. Classify the following query into one of these categories:
- "semantic" - Questions about meaning, concepts, or requiring understanding
- "keyword" - Simple keyword lookups or exact matches
- "hybrid" - Queries that benefit from both semantic and keyword search

Context documents:
1. "Korean history spans thousands of years, from the ancient kingdom of Gojoseon founded in 2333 BC to the modern divided nations of North and South Korea. Major periods include the Three Kingdoms era (57 BC–668 AD), the Goryeo Dynasty (918–1392), and the Joseon Dynasty (1392–1897)."
2. "The Korean War (1950-1953) was a major conflict between North Korea, supported by China and the Soviet Union, and South Korea, supported by the United Nations, principally the United States."
3. "The Gwangju Uprising of 1980 was a popular uprising in the city of Gwangju, South Korea, against the military dictatorship. It is considered a pivotal event in the country's democratization movement."

Query: "What are the major events in Korean history?"

Respond with only the category name.<end_of_turn>
<start_of_turn>model
`

	promptTokens := pipeline.Tokenizer.Encode(prompt)
	t.Logf("RAG prompt tokens: %d", len(promptTokens))

	result, err := pipeline.Generate(context.Background(), prompt)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}

	t.Logf("Generated %d tokens: %q", result.TokenCount, result.Text)
	t.Logf("StoppedAtEOS: %v", result.StoppedAtEOS)

	// The key assertion: with multi-EOS fix, the model should stop at EOS
	// (token 106 = <end_of_turn>) rather than generating all 50 tokens.
	if !result.StoppedAtEOS {
		t.Errorf("expected generation to stop at EOS token, but it generated all %d tokens", result.TokenCount)
	}
}

func TestGemmaRAGClassificationPromptFromServer(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "model.onnx")); err != nil {
		t.Skip("Gemma model not downloaded, skipping")
	}

	sessionManager := backends.NewSessionManager()
	pipeline, _, err := LoadTextGenerationPipeline(
		modelPath,
		sessionManager,
		[]string{"onnx"},
	)
	if err != nil {
		t.Fatalf("loading pipeline: %v", err)
	}
	defer pipeline.Close()

	// Don't override DoSample here — let the model's default (do_sample=true) stand.
	// The PooledPipelineGenerator would override this to greedy for API requests,
	// but at the pipeline level we test that the EOS fix works even with sampling.
	// Use greedy for deterministic test results.
	pipeline.GenerationConfig.DoSample = false
	pipeline.GenerationConfig.MaxNewTokens = 512

	// This is the exact 3799-char prompt from the server logs that caused a 102s timeout.
	prompt := "<bos><start_of_turn>user\nYou are an expert query analyzer for a RAG (Retrieval-Augmented Generation) system.\n\nYour task is to analyze the user's query and determine the optimal retrieval strategy.\n\n## CLASSIFICATION\nClassify the query intent:\n- \"question\": Seeks specific, factual information that can be directly answered\n  Examples: \"What is X?\", \"How does Y work?\", \"When did Z happen?\"\n- \"search\": Exploratory, broad, or seeks to discover related information\n  Examples: \"Tell me about X\", \"I want to learn about Y\", topic exploration\n\n## STRATEGY SELECTION\nChoose the best retrieval strategy:\n\n1. **simple**: Direct query with multi-phrase expansion\n   - Use for: Straightforward factual queries, keyword lookups, specific entity searches\n   - Example: \"What port does Redis use?\", \"Python list methods\"\n\n2. **decompose**: Break complex queries into sub-questions\n   - Use for: Multi-part questions, comparisons, questions requiring multiple facts\n   - Example: \"Compare JWT and OAuth for authentication\", \"What changed between v1 and v2?\"\n\n3. **step_back**: Generate broader background query first\n   - Use for: Specific questions that need general context first\n   - Example: \"Why is our Redis cache slow?\" → First understand Redis caching concepts\n\n4. **hyde**: Generate hypothetical answer document (HyDE)\n   - Use for: Abstract/conceptual questions, \"why\" questions expecting explanatory answers\n   - Example: \"Why do microservices add complexity?\", \"How does eventual consistency work?\"\n\n## SEMANTIC MODE\nChoose how to optimize the semantic query:\n\n- **rewrite**: Transform into expanded keywords/concepts (default for simple, decompose, step_back)\n  - Best when query and resource vocabulary likely overlap\n  - Output: keyword-style query with synonyms and related terms\n\n- **hypothetical**: Generate a hypothetical answer passage (HyDE)\n  - Best for explanatory questions where resources contain answer-style prose\n  - Output: 1-3 sentence hypothetical answer that would appear in a relevant resource\n\n## OUTPUT REQUIREMENTS\n\nRespond in markdown format with two sections:\n1. ## Classification Result - All classification fields\n\nAlways provide in Classification Result:\n- Route Type: \"question\" or \"search\"\n- Strategy: \"simple\", \"decompose\", \"step_back\", or \"hyde\"\n- Semantic Mode: \"rewrite\" or \"hypothetical\"\n- Improved Query: Fixed spelling/grammar, clarified intent\n- Semantic Query: Based on semantic_mode (keywords OR hypothetical answer)\n- Multi Phrases: 2-3 alternative phrasings for the main query (as bullet list)\n- Confidence: 0.0-1.0 (how confident in the strategy choice)\n\nStrategy-specific fields (only when applicable):\n- Step Back Query: (only if strategy=step_back) Broader background query\n- Sub Questions: (only if strategy=decompose) Bullet list of 2-4 sub-questions\n\nWhen in doubt about classification, default to \"search\".\nWhen in doubt about strategy, default to \"simple\".\n\nUser Query: \"What are the major events in Korean history?\"\n\nAnalyze this query and provide your response in the following markdown format:\n\n## Classification Result\nRoute Type: [question|search]\nStrategy: [simple|decompose|step_back|hyde]\nSemantic Mode: [rewrite|hypothetical]\nConfidence: [0.0-1.0]\nImproved Query: [Clarified version of the query]\nSemantic Query: [Optimized for retrieval based on semantic_mode]\nMulti Phrases:\n- [phrase 1]\n- [phrase 2]\n- [phrase 3]\nStep Back Query: [Only if strategy=step_back: Broader background query]\nSub Questions: [Only if strategy=decompose: List 2-4 sub-questions]\n- [sub question 1]\n- [sub question 2]\n- [sub question 3]\n\nIMPORTANT: Follow the format exactly. Use actual values, not placeholders. Include Step Back Query only if using step_back strategy. Include Sub Questions only if using decompose strategy.<end_of_turn>\n<start_of_turn>model\n"

	promptTokens := pipeline.Tokenizer.Encode(prompt)
	t.Logf("Exact server prompt: %d chars, %d tokens", len(prompt), len(promptTokens))

	result, err := pipeline.Generate(context.Background(), prompt)
	if err != nil {
		t.Fatalf("generation failed: %v", err)
	}

	t.Logf("Generated %d tokens in response", result.TokenCount)
	t.Logf("StoppedAtEOS: %v", result.StoppedAtEOS)
	t.Logf("Output: %q", result.Text)

	// With the multi-EOS fix, the model MUST stop at EOS (token 106 = <end_of_turn>).
	// Without the fix, it would generate all 2048 tokens, taking ~102s on CPU.
	if !result.StoppedAtEOS {
		t.Fatalf("CRITICAL: model did not stop at EOS — this is the bug that causes server timeouts. Generated %d tokens", result.TokenCount)
	}

	// The response should be reasonable in length (not 2048 tokens)
	if result.TokenCount > 500 {
		t.Errorf("response suspiciously long (%d tokens) — EOS may not be working correctly", result.TokenCount)
	}
}

func TestGemmaStreamingGeneration(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "model.onnx")); err != nil {
		t.Skip("Gemma model not downloaded, skipping")
	}

	sessionManager := backends.NewSessionManager()
	pipeline, _, err := LoadTextGenerationPipeline(
		modelPath,
		sessionManager,
		[]string{"onnx"},
	)
	if err != nil {
		t.Fatalf("loading pipeline: %v", err)
	}
	defer pipeline.Close()

	// Force greedy decoding for deterministic test
	pipeline.GenerationConfig.DoSample = false
	pipeline.GenerationConfig.MaxNewTokens = 20

	prompt := "<bos><start_of_turn>user\nWhat is the capital of France?<end_of_turn>\n<start_of_turn>model\n"

	var streamedTokens []string
	callback := func(token int32, text string) bool {
		streamedTokens = append(streamedTokens, text)
		return true
	}

	result, err := pipeline.GenerateWithStreaming(context.Background(), prompt, callback)
	if err != nil {
		t.Fatalf("streaming generation failed: %v", err)
	}

	t.Logf("Streamed %d tokens: %v", len(streamedTokens), streamedTokens)
	t.Logf("Final text: %q", result.Text)
	t.Logf("StoppedAtEOS: %v", result.StoppedAtEOS)

	fullText := strings.Join(streamedTokens, "")
	if !strings.Contains(strings.ToLower(fullText), "paris") {
		t.Errorf("expected streamed output to contain 'Paris', got: %q", fullText)
	}

	if !result.StoppedAtEOS {
		t.Errorf("expected generation to stop at EOS, but it generated all tokens")
	}
}

func TestGemmaTokenizerSpecialTokens(t *testing.T) {
	modelPath := filepath.Join(os.Getenv("HOME"), ".termite/models/generators/onnx-community/gemma-3-270m-it-ONNX")
	if _, err := os.Stat(filepath.Join(modelPath, "tokenizer.json")); err != nil {
		t.Skip("Gemma model not downloaded, skipping")
	}

	tok, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		t.Fatalf("loading tokenizer: %v", err)
	}

	prompt := "<bos><start_of_turn>user\nWhat is the capital of France?<end_of_turn>\n<start_of_turn>model\n"
	ids := tok.Encode(prompt)

	t.Logf("Encoded %d tokens: %v", len(ids), ids)

	// The Rust tokenizer should recognize special tokens.
	// With special tokens recognized: ~16-17 tokens
	// Without (literal chars): ~31 tokens
	if len(ids) > 20 {
		t.Errorf("tokenizer appears to not recognize special tokens: got %d tokens (expected ~16-17)", len(ids))
	}

	// BOS token should be ID 2
	if len(ids) > 0 && ids[0] != 2 {
		t.Errorf("first token should be BOS (2), got %d", ids[0])
	}
}
