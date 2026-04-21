// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipelines

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/antflydb/antfly/pkg/termite/lib/tokenizers"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// ============================================================================
// GLiNER Config Types and Loading
// ============================================================================

// GLiNERModelType represents the type of GLiNER model architecture.
type GLiNERModelType string

const (
	// GLiNERModelUniEncoder is the standard GLiNER model, best for <30 entity types.
	GLiNERModelUniEncoder GLiNERModelType = "uniencoder"
	// GLiNERModelBiEncoder is optimized for 50-200+ entity types with pre-computed embeddings.
	GLiNERModelBiEncoder GLiNERModelType = "biencoder"
	// GLiNERModelTokenLevel is optimized for extracting long entity spans (multi-sentence).
	GLiNERModelTokenLevel GLiNERModelType = "token_level"
	// GLiNERModelMultiTask supports multiple tasks: NER, classification, QA, relation extraction.
	GLiNERModelMultiTask GLiNERModelType = "multitask"
	// GLiNERModelGLiNER2 is the unified GLiNER2 multi-task model from Fastino.
	// Supports NER, classification, structured extraction, and relation extraction.
	GLiNERModelGLiNER2 GLiNERModelType = "gliner2"
)

// GLiNERModelConfig holds parsed configuration for a GLiNER model.
type GLiNERModelConfig struct {
	// Path to the model directory
	ModelPath string

	// ModelFile is the ONNX file for the GLiNER model
	ModelFile string

	// MaxWidth is the maximum entity span width in tokens
	MaxWidth int

	// MaxLength is the maximum sequence length
	MaxLength int

	// DefaultLabels are the entity labels to use if none specified
	DefaultLabels []string

	// Threshold is the score threshold for entity detection (0.0-1.0)
	Threshold float32

	// FlatNER if true, don't allow nested/overlapping entities
	FlatNER bool

	// MultiLabel if true, allow entities to have multiple labels
	MultiLabel bool

	// ModelType indicates the GLiNER architecture variant
	ModelType GLiNERModelType

	// RelationLabels are default relation types for relationship extraction
	RelationLabels []string

	// RelationThreshold is the score threshold for relationship detection
	RelationThreshold float32

	// WordsJoiner is the character used to join words (typically space)
	WordsJoiner string

	// Capabilities lists the model's capabilities from gliner_config.json
	// (e.g., "ner", "zeroshot", "classification", "relations", "extraction").
	Capabilities []string
}

// LoadGLiNERModelConfig loads and parses configuration for a GLiNER model.
func LoadGLiNERModelConfig(modelPath string) (*GLiNERModelConfig, error) {
	config := &GLiNERModelConfig{
		ModelPath:         modelPath,
		MaxWidth:          12,
		MaxLength:         512,
		DefaultLabels:     []string{"person", "organization", "location", "date", "product"},
		Threshold:         0.5,
		FlatNER:           true,
		MultiLabel:        false,
		ModelType:         GLiNERModelUniEncoder,
		RelationThreshold: 0.5,
		WordsJoiner:       " ",
	}

	// Detect model file
	config.ModelFile = FindONNXFile(modelPath, []string{
		"model.onnx",
		"model_quantized.onnx",
		"gliner.onnx",
	})

	if config.ModelFile == "" {
		return nil, fmt.Errorf("no ONNX model file found in %s", modelPath)
	}

	// Load gliner_config.json if present
	glinerConfigPath := filepath.Join(modelPath, "gliner_config.json")
	if data, err := os.ReadFile(glinerConfigPath); err == nil {
		var rawConfig rawGLiNERConfig
		if err := json.Unmarshal(data, &rawConfig); err == nil {
			if rawConfig.MaxWidth > 0 {
				config.MaxWidth = rawConfig.MaxWidth
			}
			if rawConfig.MaxLength > 0 {
				config.MaxLength = rawConfig.MaxLength
			}
			if len(rawConfig.Labels) > 0 {
				config.DefaultLabels = rawConfig.Labels
			}
			if rawConfig.Threshold > 0 {
				config.Threshold = rawConfig.Threshold
			}
			if rawConfig.FlatNER != nil {
				config.FlatNER = *rawConfig.FlatNER
			}
			if rawConfig.MultiLabel != nil {
				config.MultiLabel = *rawConfig.MultiLabel
			}
			if rawConfig.ModelType != "" {
				config.ModelType = GLiNERModelType(rawConfig.ModelType)
			}
			if len(rawConfig.RelationLabels) > 0 {
				config.RelationLabels = rawConfig.RelationLabels
			}
			if rawConfig.RelationThreshold > 0 {
				config.RelationThreshold = rawConfig.RelationThreshold
			}
			if rawConfig.WordsJoiner != "" {
				config.WordsJoiner = rawConfig.WordsJoiner
			}
			if len(rawConfig.Capabilities) > 0 {
				config.Capabilities = rawConfig.Capabilities
			}
		}
	}

	// Detect model type from model name if not explicitly set in config
	if config.ModelType == GLiNERModelUniEncoder {
		config.ModelType = detectGLiNERModelType(modelPath)
	}

	return config, nil
}

// rawGLiNERConfig represents gliner_config.json structure.
// Pointer fields distinguish "not set" from "set to zero/false".
type rawGLiNERConfig struct {
	MaxWidth          int      `json:"max_width"`
	MaxLength         int      `json:"max_len"`
	Labels            []string `json:"labels"`
	Threshold         float32  `json:"threshold"`
	FlatNER           *bool    `json:"flat_ner"`
	MultiLabel        *bool    `json:"multi_label"`
	ModelType         string   `json:"model_type"`
	RelationLabels    []string `json:"relation_labels"`
	RelationThreshold float32  `json:"relation_threshold"`
	WordsJoiner       string   `json:"words_joiner"`
	Capabilities      []string `json:"capabilities"`
}

// detectGLiNERModelType attempts to detect the model type from the model name.
func detectGLiNERModelType(modelPath string) GLiNERModelType {
	modelName := strings.ToLower(filepath.Base(modelPath))
	parentDir := strings.ToLower(filepath.Base(filepath.Dir(modelPath)))

	// Check for GLiNER2 models (from Fastino)
	// GLiNER2 models have "gliner2" in name or are from "fastino" organization
	if strings.Contains(modelName, "gliner2") ||
		(strings.Contains(parentDir, "fastino") && strings.Contains(modelName, "gliner")) {
		return GLiNERModelGLiNER2
	}

	switch {
	case strings.Contains(modelName, "multitask"):
		return GLiNERModelMultiTask
	case strings.Contains(modelName, "biencoder") || strings.Contains(modelName, "bi-"):
		return GLiNERModelBiEncoder
	case strings.Contains(modelName, "token") || strings.Contains(modelName, "large"):
		return GLiNERModelTokenLevel
	default:
		return GLiNERModelUniEncoder
	}
}

// IsGLiNERModel checks if a model path contains a GLiNER model.
func IsGLiNERModel(modelPath string) bool {
	// Check for gliner_config.json
	configPath := filepath.Join(modelPath, "gliner_config.json")
	if _, err := os.Stat(configPath); err == nil {
		return true
	}

	// Check if model name contains "gliner"
	modelName := strings.ToLower(filepath.Base(modelPath))
	return strings.Contains(modelName, "gliner")
}

// ============================================================================
// NER Entity Types (shared by NERPipeline and GLiNERPipeline)
// ============================================================================

// NEREntity represents a named entity extracted from text.
// Used by both NERPipeline (token classification) and GLiNERPipeline (zero-shot).
type NEREntity struct {
	// Text is the entity text
	Text string `json:"text"`
	// Label is the entity type
	Label string `json:"label"`
	// Start is the character offset where the entity begins
	Start int `json:"start"`
	// End is the character offset where the entity ends (exclusive)
	End int `json:"end"`
	// Score is the confidence score (0.0 to 1.0)
	Score float32 `json:"score"`
}

// NERRelation represents a relationship between two entities.
type NERRelation struct {
	// HeadEntity is the source entity
	HeadEntity NEREntity `json:"head"`
	// TailEntity is the target entity
	TailEntity NEREntity `json:"tail"`
	// Label is the relationship type
	Label string `json:"label"`
	// Score is the confidence score
	Score float32 `json:"score"`
}

// NEROutput holds the output from GLiNER inference.
type NEROutput struct {
	// Entities holds entities for each input text
	Entities [][]NEREntity
	// Relations holds relations for each input text (if supported)
	Relations [][]NERRelation
}

// glinerTaskType represents different task types for GLiNER2 prompt construction.
type glinerTaskType int

const (
	glinerTaskNER glinerTaskType = iota
	glinerTaskRelations
	glinerTaskClassification
)

// NERClassification represents a text classification result.
type NERClassification struct {
	// Label is the classification label
	Label string `json:"label"`
	// Score is the confidence score (0.0 to 1.0)
	Score float32 `json:"score"`
}

// NERClassificationConfig holds configuration for classification.
type NERClassificationConfig struct {
	// Threshold is the score threshold for positive classification
	Threshold float32
	// MultiLabel if true, allow multiple labels per text
	MultiLabel bool
	// TopK returns top K predictions (0 = all above threshold)
	TopK int
}

// DefaultNERClassificationConfig returns sensible defaults.
func DefaultNERClassificationConfig() *NERClassificationConfig {
	return &NERClassificationConfig{
		Threshold:  0.5,
		MultiLabel: false,
		TopK:       1,
	}
}


// ============================================================================
// GLiNER Pipeline
// ============================================================================

// labelEmbeddingCache stores precomputed label embeddings for BiEncoder models.
// This allows labels to be encoded once and reused across many inference calls.
type labelEmbeddingCache struct {
	mu         sync.RWMutex
	embeddings map[string][]float32 // label -> embedding
	labels     []string             // cached labels in order
}

// GLiNERPipeline wraps a GLiNER model for zero-shot Named Entity Recognition.
// Unlike traditional NER models, GLiNER can extract entities of any type
// specified at inference time without requiring retraining.
type GLiNERPipeline struct {
	// Session is the low-level ONNX session for running inference
	Session backends.Session

	// LabelEncoderSession is an optional separate session for encoding labels (BiEncoder only)
	LabelEncoderSession backends.Session

	// Tokenizer handles text-to-token conversion
	Tokenizer tokenizers.Tokenizer

	// Config holds model configuration
	Config *GLiNERModelConfig

	// PipelineConfig holds pipeline-specific configuration
	PipelineConfig *GLiNERPipelineConfig

	// backend type used
	backendType backends.BackendType

	// labelCache stores precomputed label embeddings (BiEncoder models only)
	labelCache *labelEmbeddingCache
}

// GLiNERPipelineConfig holds configuration for GLiNER inference.
type GLiNERPipelineConfig struct {
	// Threshold is the score threshold for entity detection
	Threshold float32

	// MaxWidth is the maximum entity span width in tokens
	MaxWidth int

	// FlatNER if true, don't allow nested/overlapping entities
	FlatNER bool

	// MultiLabel if true, allow entities to have multiple labels
	MultiLabel bool

	// DefaultLabels are the entity labels to use if none specified
	DefaultLabels []string
}

// DefaultGLiNERPipelineConfig returns sensible defaults for GLiNER.
func DefaultGLiNERPipelineConfig() *GLiNERPipelineConfig {
	return &GLiNERPipelineConfig{
		Threshold:     0.5,
		MaxWidth:      12,
		FlatNER:       true,
		MultiLabel:    false,
		DefaultLabels: []string{"person", "organization", "location"},
	}
}

// NewGLiNERPipeline creates a new GLiNER pipeline from a session.
func NewGLiNERPipeline(
	session backends.Session,
	tokenizer tokenizers.Tokenizer,
	modelConfig *GLiNERModelConfig,
	pipelineConfig *GLiNERPipelineConfig,
	backendType backends.BackendType,
) *GLiNERPipeline {
	if pipelineConfig == nil {
		pipelineConfig = DefaultGLiNERPipelineConfig()
	}

	// Override pipeline config with model config values if not explicitly set
	if modelConfig != nil {
		if pipelineConfig.Threshold == 0 {
			pipelineConfig.Threshold = modelConfig.Threshold
		}
		if pipelineConfig.MaxWidth == 0 {
			pipelineConfig.MaxWidth = modelConfig.MaxWidth
		}
		if len(pipelineConfig.DefaultLabels) == 0 {
			pipelineConfig.DefaultLabels = modelConfig.DefaultLabels
		}
	}

	p := &GLiNERPipeline{
		Session:        session,
		Tokenizer:      tokenizer,
		Config:         modelConfig,
		PipelineConfig: pipelineConfig,
		backendType:    backendType,
	}

	// Initialize label cache for BiEncoder models
	if modelConfig != nil && modelConfig.ModelType == GLiNERModelBiEncoder {
		p.labelCache = &labelEmbeddingCache{
			embeddings: make(map[string][]float32),
		}
	}

	return p
}

// Recognize extracts entities from texts using the default labels.
func (p *GLiNERPipeline) Recognize(ctx context.Context, texts []string) (*NEROutput, error) {
	return p.RecognizeWithLabels(ctx, texts, p.PipelineConfig.DefaultLabels)
}

// RecognizeWithLabels extracts entities of the specified types (zero-shot NER).
// This is the key feature of GLiNER - it can extract any entity type without retraining.
func (p *GLiNERPipeline) RecognizeWithLabels(ctx context.Context, texts []string, labels []string) (*NEROutput, error) {
	if len(texts) == 0 {
		return &NEROutput{Entities: [][]NEREntity{}}, nil
	}

	if len(labels) == 0 {
		labels = p.PipelineConfig.DefaultLabels
	}

	// Process each text
	allEntities := make([][]NEREntity, len(texts))
	for i, text := range texts {
		entities, err := p.processText(ctx, text, labels)
		if err != nil {
			return nil, fmt.Errorf("processing text %d: %w", i, err)
		}
		allEntities[i] = entities
	}

	return &NEROutput{Entities: allEntities}, nil
}

// processText processes a single text with the given labels.
func (p *GLiNERPipeline) processText(ctx context.Context, text string, labels []string) ([]NEREntity, error) {
	return p.processTextWithConfig(ctx, text, labels, p.PipelineConfig.Threshold, p.PipelineConfig.FlatNER)
}

// processTextWithConfig runs NER extraction with explicit threshold and flatNER parameters,
// avoiding mutation of shared PipelineConfig state for thread safety.
func (p *GLiNERPipeline) processTextWithConfig(ctx context.Context, text string, labels []string, threshold float32, flatNER bool) ([]NEREntity, error) {
	if text == "" {
		return nil, nil
	}

	// Tokenize the text and track word boundaries
	words, wordStartChars, wordEndChars := p.splitIntoWords(text)
	if len(words) == 0 {
		return nil, nil
	}

	// GLiNER2: structured schema prompt with all labels in a single pass.
	// The ONNX model supports dynamic label counts.
	// Output is [1, num_words, max_width, num_labels] with per-label dot-product scores.
	if p.IsGLiNER2() {
		inputs, err := p.buildGLiNER2Inputs(words, labels)
		if err != nil {
			return nil, fmt.Errorf("building GLiNER2 inputs: %w", err)
		}

		outputs, err := p.Session.Run(inputs)
		if err != nil {
			return nil, fmt.Errorf("running GLiNER2 inference: %w", err)
		}

		entities, err := p.parseOutputs(outputs, words, wordStartChars, wordEndChars, labels, text, threshold, flatNER)
		if err != nil {
			return nil, fmt.Errorf("parsing GLiNER2 outputs: %w", err)
		}

		return entities, nil
	}

	// GLiNER v1: all labels in one prompt, output has per-label scores
	// via trained span-label dot products: [batch, num_spans, num_labels].
	textTokens := p.tokenizeWords(words)
	prompt := p.buildPrompt(labels)
	promptTokens := p.Tokenizer.EncodeWithOptions(prompt, false)

	inputs, err := p.buildInputs(promptTokens, textTokens)
	if err != nil {
		return nil, fmt.Errorf("building inputs: %w", err)
	}

	outputs, err := p.Session.Run(inputs)
	if err != nil {
		return nil, fmt.Errorf("running inference: %w", err)
	}

	entities, err := p.parseOutputs(outputs, words, wordStartChars, wordEndChars, labels, text, threshold, flatNER)
	if err != nil {
		return nil, fmt.Errorf("parsing outputs: %w", err)
	}

	return entities, nil
}

// splitIntoWords splits text into words and returns word boundaries.
func (p *GLiNERPipeline) splitIntoWords(text string) ([]string, []int, []int) {
	var words []string
	var startChars, endChars []int

	wordStart := -1
	for i, r := range text {
		if isWordChar(r) {
			if wordStart == -1 {
				wordStart = i
			}
		} else {
			if wordStart != -1 {
				words = append(words, text[wordStart:i])
				startChars = append(startChars, wordStart)
				endChars = append(endChars, i)
				wordStart = -1
			}
		}
	}

	// Handle last word
	if wordStart != -1 {
		words = append(words, text[wordStart:])
		startChars = append(startChars, wordStart)
		endChars = append(endChars, len(text))
	}

	return words, startChars, endChars
}

// isWordChar returns true if the rune is part of a word.
func isWordChar(r rune) bool {
	return r != ' ' && r != '\t' && r != '\n' && r != '\r'
}

// buildPrompt constructs the label prompt for GLiNER NER.
//
// GLiNER v1 uses: <<ENT>>label<<SEP>> (special tokens 128002/128003)
// GLiNER2 uses:   [E]label[SEP_TEXT]  (special tokens 128005/128002)
func (p *GLiNERPipeline) buildPrompt(labels []string) string {
	return p.buildPromptForTask(labels, glinerTaskNER)
}

// buildPromptForTask constructs the label prompt for different GLiNER tasks.
//
// GLiNER v1 only supports NER with: <<ENT>>label<<SEP>>
//
// GLiNER2 supports multiple tasks with different prefix tokens:
//   - NER:            [E]label[SEP_TEXT]
//   - Relations:      [R]entity::relation[SEP_TEXT]
//   - Classification: [C]label[SEP_TEXT]
func (p *GLiNERPipeline) buildPromptForTask(labels []string, taskType glinerTaskType) string {
	var sb strings.Builder

	if p.IsGLiNER2() {
		var prefix string
		switch taskType {
		case glinerTaskNER:
			prefix = "[E]"
		case glinerTaskRelations:
			prefix = "[R]"
		case glinerTaskClassification:
			prefix = "[C]"
		default:
			prefix = "[E]"
		}

		for _, label := range labels {
			sb.WriteString(prefix)
			sb.WriteString(label)
			sb.WriteString("[SEP_TEXT]")
		}
	} else {
		// GLiNER v1: all tasks use <<ENT>>label<<SEP>>
		for _, label := range labels {
			sb.WriteString("<<ENT>>")
			sb.WriteString(label)
			sb.WriteString("<<SEP>>")
		}
	}

	return sb.String()
}

// buildCompositeRelationLabels creates composite labels for relation extraction.
// For each entity type and relation type, creates "entity::relation" label.
// This follows GLiNER2's approach where relations are expressed as composite labels.
func (p *GLiNERPipeline) buildCompositeRelationLabels(entityLabels []string, relationLabels []string) []string {
	compositeLabels := make([]string, 0, len(entityLabels)*len(relationLabels))
	for _, entity := range entityLabels {
		for _, relation := range relationLabels {
			compositeLabels = append(compositeLabels, entity+"::"+relation)
		}
	}
	return compositeLabels
}

// ExtractRelations extracts entities and relationships between them.
// This is a GLiNER2-specific feature that uses composite labels.
//
// The approach:
// 1. First extract all entities with the given entity labels
// 2. Then use composite labels (entity::relation) to find head entities
// 3. Match head entities with potential tail entities
func (p *GLiNERPipeline) ExtractRelations(
	ctx context.Context,
	texts []string,
	entityLabels []string,
	relationLabels []string,
) (*NEROutput, error) {
	if !p.IsGLiNER2() {
		return nil, fmt.Errorf("relation extraction requires GLiNER2 model")
	}

	if len(texts) == 0 {
		return &NEROutput{
			Entities:  [][]NEREntity{},
			Relations: [][]NERRelation{},
		}, nil
	}

	if len(entityLabels) == 0 {
		entityLabels = p.PipelineConfig.DefaultLabels
	}

	if len(relationLabels) == 0 && p.Config != nil {
		relationLabels = p.Config.RelationLabels
	}

	// Process each text
	allEntities := make([][]NEREntity, len(texts))
	allRelations := make([][]NERRelation, len(texts))

	for i, text := range texts {
		entities, relations, err := p.processTextForRelations(ctx, text, entityLabels, relationLabels)
		if err != nil {
			return nil, fmt.Errorf("processing text %d for relations: %w", i, err)
		}
		allEntities[i] = entities
		allRelations[i] = relations
	}

	return &NEROutput{
		Entities:  allEntities,
		Relations: allRelations,
	}, nil
}

// processTextForRelations extracts both entities and relations from a single text.
func (p *GLiNERPipeline) processTextForRelations(
	ctx context.Context,
	text string,
	entityLabels []string,
	relationLabels []string,
) ([]NEREntity, []NERRelation, error) {
	if text == "" {
		return nil, nil, nil
	}

	// Step 1: Extract all entities first
	entities, err := p.processText(ctx, text, entityLabels)
	if err != nil {
		return nil, nil, fmt.Errorf("extracting entities: %w", err)
	}

	if len(entities) < 2 || len(relationLabels) == 0 {
		// Need at least 2 entities for a relation
		return entities, nil, nil
	}

	// Step 2: Build composite labels for relation extraction
	compositeLabels := p.buildCompositeRelationLabels(entityLabels, relationLabels)

	// Step 3: Extract relation head entities using composite labels
	relationHeadSpans, err := p.processText(ctx, text, compositeLabels)
	if err != nil {
		return entities, nil, fmt.Errorf("extracting relation heads: %w", err)
	}

	// Step 4: Match relation heads to entities and find tail entities
	relations := p.matchRelations(entities, relationHeadSpans)

	return entities, relations, nil
}

// matchRelations matches extracted relation head spans to entities to form relations.
//
// GLiNER2 encodes relations as composite labels "entity_type::relation". The model
// produces a head span for each relation; the span's position and score carry the
// relation-specific evidence. We match each head span to its closest non-overlapping
// entity (the tail) by positional proximity, rather than forming a cartesian product.
func (p *GLiNERPipeline) matchRelations(
	entities []NEREntity,
	relationHeadSpans []NEREntity,
) []NERRelation {
	if len(entities) == 0 || len(relationHeadSpans) == 0 {
		return nil
	}

	// Build a map of entity positions for quick lookup
	entityByPos := make(map[string]*NEREntity)
	for i := range entities {
		key := fmt.Sprintf("%d-%d", entities[i].Start, entities[i].End)
		entityByPos[key] = &entities[i]
	}

	threshold := float32(0)
	if p.Config != nil {
		threshold = p.Config.RelationThreshold
	}

	var relations []NERRelation

	for _, headSpan := range relationHeadSpans {
		// Parse the composite label: "entity_type::relation"
		parts := strings.SplitN(headSpan.Label, "::", 2)
		if len(parts) != 2 {
			continue
		}
		headEntityType := parts[0]
		relationLabel := parts[1]

		// Find the matching entity for this head span by position
		headKey := fmt.Sprintf("%d-%d", headSpan.Start, headSpan.End)
		headEntity := entityByPos[headKey]
		if headEntity == nil {
			headEntity = &NEREntity{
				Text:  headSpan.Text,
				Label: headEntityType,
				Start: headSpan.Start,
				End:   headSpan.End,
				Score: headSpan.Score,
			}
		}

		// Find the closest non-overlapping entity as the tail.
		// The head span's score is the relation-specific evidence from the model.
		var bestTail *NEREntity
		bestDist := math.MaxInt
		for i := range entities {
			tail := &entities[i]
			if overlapsSpan(headSpan.Start, headSpan.End, tail.Start, tail.End) {
				continue
			}
			dist := charDistance(headSpan.Start, headSpan.End, tail.Start, tail.End)
			if dist < bestDist {
				bestDist = dist
				bestTail = tail
			}
		}

		if bestTail == nil {
			continue
		}

		score := headSpan.Score
		if threshold > 0 && score < threshold {
			continue
		}

		relations = append(relations, NERRelation{
			HeadEntity: *headEntity,
			TailEntity: *bestTail,
			Label:      relationLabel,
			Score:      score,
		})
	}

	return relations
}

// charDistance returns the character distance between two non-overlapping spans.
func charDistance(start1, end1, start2, end2 int) int {
	if end1 <= start2 {
		return start2 - end1
	}
	return start1 - end2
}

// overlapsSpan checks if two spans overlap.
func overlapsSpan(start1, end1, start2, end2 int) bool {
	return start1 < end2 && start2 < end1
}

// ClassifyText performs text classification using GLiNER2.
// Classification is implemented by treating the entire text as a single span
// and scoring it against each class label.
func (p *GLiNERPipeline) ClassifyText(
	ctx context.Context,
	texts []string,
	labels []string,
	config *NERClassificationConfig,
) ([][]NERClassification, error) {
	if !p.IsGLiNER2() {
		return nil, fmt.Errorf("classification requires GLiNER2 model")
	}

	if len(texts) == 0 {
		return [][]NERClassification{}, nil
	}

	if len(labels) == 0 {
		return nil, fmt.Errorf("classification labels are required")
	}

	if config == nil {
		config = DefaultNERClassificationConfig()
	}

	results := make([][]NERClassification, len(texts))

	for i, text := range texts {
		classifications, err := p.classifySingleText(ctx, text, labels, config)
		if err != nil {
			return nil, fmt.Errorf("classifying text %d: %w", i, err)
		}
		results[i] = classifications
	}

	return results, nil
}

// classifySingleText classifies a single text against the given labels.
//
// GLiNER2's binary classifier outputs [batch, num_spans, 1] — one score per span.
// We run inference once per label and use the max span score as that label's confidence.
func (p *GLiNERPipeline) classifySingleText(
	ctx context.Context,
	text string,
	labels []string,
	config *NERClassificationConfig,
) ([]NERClassification, error) {
	if text == "" {
		return nil, nil
	}

	words, _, _ := p.splitIntoWords(text)
	if len(words) == 0 {
		return nil, nil
	}

	// GLiNER2: use structured schema with [E] tokens (same as NER path).
	// All labels in a single pass — the ONNX model supports dynamic label counts.
	// Classification score = max span logit per label across all spans.
	if p.IsGLiNER2() {
		inputs, err := p.buildGLiNER2Inputs(words, labels)
		if err != nil {
			return nil, fmt.Errorf("building GLiNER2 classification inputs: %w", err)
		}

		outputs, err := p.Session.Run(inputs)
		if err != nil {
			return nil, fmt.Errorf("running GLiNER2 classification inference: %w", err)
		}

		// Extract per-label max scores from [1, num_words, max_width, num_labels]
		results := make([]NERClassification, 0, len(labels))
		for li, label := range labels {
			score := p.maxLabelScore(outputs, li, len(labels))
			if score < config.Threshold {
				continue
			}
			results = append(results, NERClassification{
				Label: label,
				Score: score,
			})
		}

		return sortAndTruncateClassifications(results, config), nil
	}

	// GLiNER v1: per-label inference with old prompt format.
	textTokens := p.tokenizeWords(words)

	results := make([]NERClassification, 0, len(labels))
	for _, label := range labels {
		singleLabel := []string{label}
		prompt := p.buildPromptForTask(singleLabel, glinerTaskClassification)
		promptTokens := p.Tokenizer.EncodeWithOptions(prompt, false)

		inputs, err := p.buildInputs(promptTokens, textTokens)
		if err != nil {
			return nil, fmt.Errorf("building inputs for label %s: %w", label, err)
		}

		outputs, err := p.Session.Run(inputs)
		if err != nil {
			return nil, fmt.Errorf("running inference for label %s: %w", label, err)
		}

		score := p.maxSpanScore(outputs)
		if score < config.Threshold {
			continue
		}

		results = append(results, NERClassification{
			Label: label,
			Score: score,
		})
	}

	return sortAndTruncateClassifications(results, config), nil
}

// sortAndTruncateClassifications sorts results by score descending and applies TopK/MultiLabel filtering.
func sortAndTruncateClassifications(results []NERClassification, config *NERClassificationConfig) []NERClassification {
	slices.SortFunc(results, func(a, b NERClassification) int {
		return cmp.Compare(b.Score, a.Score) // descending
	})

	if !config.MultiLabel && len(results) > 0 {
		k := 1
		if config.TopK > 0 {
			k = config.TopK
		}
		if len(results) > k {
			results = results[:k]
		}
	} else if config.TopK > 0 && len(results) > config.TopK {
		results = results[:config.TopK]
	}

	return results
}

// maxLabelScore extracts the maximum sigmoid-activated score for a specific label
// from GLiNER2 output with shape [1, num_words, max_width, num_labels].
func (p *GLiNERPipeline) maxLabelScore(outputs []backends.NamedTensor, labelIdx int, numLabels int) float32 {
	for _, out := range outputs {
		if out.Name == "logits" || len(outputs) == 1 {
			if logits, ok := out.Data.([]float32); ok {
				maxLogit := float32(-1000)
				// logits is flat: [num_words * max_width * num_labels]
				// stride through by numLabels, picking labelIdx
				for i := labelIdx; i < len(logits); i += numLabels {
					if logits[i] > maxLogit {
						maxLogit = logits[i]
					}
				}
				return sigmoid(maxLogit)
			}
		}
	}
	return 0
}

// maxSpanScore extracts the maximum sigmoid-activated span score from model outputs.
func (p *GLiNERPipeline) maxSpanScore(outputs []backends.NamedTensor) float32 {
	for _, out := range outputs {
		if out.Name == "logits" || len(outputs) == 1 {
			if logits, ok := out.Data.([]float32); ok {
				maxLogit := float32(-1000)
				for _, v := range logits {
					if v > maxLogit {
						maxLogit = v
					}
				}
				return sigmoid(maxLogit)
			}
		}
	}
	return 0
}

// sigmoid converts a logit to a probability.
func sigmoid(x float32) float32 {
	return float32(1.0 / (1.0 + math.Exp(float64(-x))))
}

// tokenizeWords tokenizes each word and returns the tokens per word.
func (p *GLiNERPipeline) tokenizeWords(words []string) [][]int {
	result := make([][]int, len(words))
	for i, word := range words {
		result[i] = p.Tokenizer.EncodeWithOptions(word, false)
	}
	return result
}

// buildGLiNER2Inputs constructs inputs for GLiNER2 models.
//
// GLiNER2 uses a structured schema prompt format:
//
//	( [P] entities ( [E] label1 [E] label2 ... ) ) [SEP_TEXT] word1 word2 ...
//
// Each part is tokenized individually (not as a single string).
// Text words are lowercased before tokenization.
// No [CLS]/[SEP] wrapping — the model was trained without them.
//
// The ONNX model takes 4 inputs:
//   - input_ids: [1, seq_len]
//   - attention_mask: [1, seq_len]
//   - words_mask: [1, seq_len] (>0 for text tokens, value = word index 1-indexed)
//   - span_idx: [1, num_spans, 2] (word-level span indices)
//
// Output: [1, num_words, max_width, num_labels]
func (p *GLiNERPipeline) buildGLiNER2Inputs(words []string, labels []string) ([]backends.NamedTensor, error) {
	// Build schema parts: ( [P] entities ( [E] label1 [E] label2 ... ) ) [SEP_TEXT]
	// The ONNX model supports dynamic label counts — no padding needed.
	schemaParts := []string{"(", "[P]", "entities", "("}
	for _, label := range labels {
		schemaParts = append(schemaParts, "[E]", label)
	}
	schemaParts = append(schemaParts, ")", ")", "[SEP_TEXT]")

	// Tokenize each schema part individually without post-processing.
	var schemaTokenIDs []int
	for _, part := range schemaParts {
		tokens := p.Tokenizer.EncodeWithOptions(part, false)
		schemaTokenIDs = append(schemaTokenIDs, tokens...)
	}

	// Tokenize text words (lowercased, each word individually)
	wordTokens := make([][]int, len(words))
	for i, word := range words {
		wordTokens[i] = p.Tokenizer.EncodeWithOptions(strings.ToLower(word), false)
	}

	// Count total text sub-tokens
	totalTextSubTokens := 0
	for _, wt := range wordTokens {
		totalTextSubTokens += len(wt)
	}

	// Build full sequence
	seqLen := len(schemaTokenIDs) + totalTextSubTokens
	maxLen := p.Config.MaxLength
	if seqLen > maxLen {
		seqLen = maxLen
	}

	inputIDs := make([]int64, seqLen)
	attentionMask := make([]int64, seqLen)
	wordsMask := make([]int64, seqLen)

	// Fill schema tokens
	idx := 0
	for _, tok := range schemaTokenIDs {
		if idx >= seqLen {
			break
		}
		inputIDs[idx] = int64(tok)
		attentionMask[idx] = 1
		idx++
	}

	// Fill text tokens with word tracking
	numWords := 0
	for i, wt := range wordTokens {
		if idx >= seqLen {
			break
		}
		for _, tok := range wt {
			if idx >= seqLen {
				break
			}
			inputIDs[idx] = int64(tok)
			attentionMask[idx] = 1
			wordsMask[idx] = int64(i + 1) // 1-indexed word ID
			idx++
		}
		numWords = i + 1
	}
	// Remaining positions are already zero from make().

	// Build word-level span indices
	maxWidth := p.PipelineConfig.MaxWidth
	numSpans := numWords * maxWidth
	if numSpans == 0 {
		numSpans = maxWidth
	}
	spanIdx := make([]int64, numSpans*2)
	spanMask := make([]bool, numSpans)

	for w := 0; w < numWords; w++ {
		for wi := range maxWidth {
			si := w*maxWidth + wi
			endWord := w + wi
			if endWord < numWords {
				spanIdx[si*2] = int64(w)
				spanIdx[si*2+1] = int64(endWord)
				spanMask[si] = true
			}
		}
	}

	return []backends.NamedTensor{
		{
			Name:  "input_ids",
			Shape: []int64{1, int64(seqLen)},
			Data:  inputIDs,
		},
		{
			Name:  "attention_mask",
			Shape: []int64{1, int64(seqLen)},
			Data:  attentionMask,
		},
		{
			Name:  "words_mask",
			Shape: []int64{1, int64(seqLen)},
			Data:  wordsMask,
		},
		{
			Name:  "span_idx",
			Shape: []int64{1, int64(numSpans), 2},
			Data:  spanIdx,
		},
		{
			Name:  "span_mask",
			Shape: []int64{1, int64(numSpans)},
			Data:  spanMask,
		},
	}, nil
}

// buildInputs constructs the model inputs for GLiNER v1 inference.
func (p *GLiNERPipeline) buildInputs(promptTokens []int, textTokens [][]int) ([]backends.NamedTensor, error) {
	// Count total text tokens (excluding special tokens added by tokenizer)
	totalTextTokens := 0
	for _, wt := range textTokens {
		// Each word's tokens may include special tokens, strip them
		for _, tok := range wt {
			if tok != 0 && tok != 1 && tok != 2 { // Skip PAD, CLS, SEP
				totalTextTokens++
			}
		}
	}

	// Build combined token sequence: [CLS] + prompt + text tokens + [SEP]
	// Note: GLiNER expects prompt and text in same segment (no separator between them)
	// promptTokens are encoded with addSpecialTokens=false, so no stripping needed.
	seqLen := len(promptTokens) + totalTextTokens + 2 // CLS at start, SEP at end

	// Limit sequence length
	maxLen := p.Config.MaxLength
	if seqLen > maxLen {
		seqLen = maxLen
	}

	// Build input_ids
	inputIDs := make([]int64, seqLen)
	attentionMask := make([]int64, seqLen)

	// DeBERTa special token IDs
	clsID := int64(1) // DeBERTa [CLS]
	sepID := int64(2) // DeBERTa [SEP]
	padID := int64(0) // DeBERTa [PAD]

	idx := 0
	inputIDs[idx] = clsID
	attentionMask[idx] = 1
	idx++

	// Add prompt tokens (label markers)
	for _, tok := range promptTokens {
		if idx >= seqLen-1 {
			break
		}
		inputIDs[idx] = int64(tok)
		attentionMask[idx] = 1
		idx++
	}

	// Build words mask to track which tokens belong to which word
	wordsMask := make([]int64, seqLen)
	textLengths := make([]int64, 1)

	// Track position of first text token
	textStartIdx := idx

	// Add text tokens with word tracking.
	wordIdx := int64(1) // Start at 1 (0 reserved for non-word tokens)
	for _, wordTokens := range textTokens {
		added := false
		for _, tok := range wordTokens {
			// Skip special tokens that tokenizer adds
			if tok == 0 || tok == 1 || tok == 2 {
				continue
			}
			if idx >= seqLen-1 {
				break
			}
			inputIDs[idx] = int64(tok)
			attentionMask[idx] = 1
			wordsMask[idx] = wordIdx
			idx++
			added = true
		}
		if added {
			wordIdx++
		}
		if idx >= seqLen-1 {
			break
		}
	}

	// Record text length (number of sub-tokens)
	numTextTokens := idx - textStartIdx
	textLengths[0] = int64(numTextTokens)

	// Add final separator
	if idx < seqLen {
		inputIDs[idx] = sepID
		attentionMask[idx] = 1
		idx++
	}

	// Pad remaining
	for ; idx < seqLen; idx++ {
		inputIDs[idx] = padID
		attentionMask[idx] = 0
	}

	maxWidth := p.PipelineConfig.MaxWidth

	// GLiNER v1: Build span indices at sub-token level.
	// The v1 ONNX model has an internal Reshape that expects
	// numSpans = numTextTokens * maxWidth.
	if numTextTokens < 1 {
		numTextTokens = 1
	}
	numSpans := numTextTokens * maxWidth
	spanIdx := make([]int64, numSpans*2)
	spanMask := make([]bool, numSpans)

	for t := 0; t < numTextTokens; t++ {
		for wi := range maxWidth {
			spanI := t*maxWidth + wi
			start := t
			end := t + wi
			spanIdx[spanI*2] = int64(start)
			spanIdx[spanI*2+1] = int64(end)
			spanMask[spanI] = end < numTextTokens
		}
	}

	// Build named tensors
	inputs := []backends.NamedTensor{
		{
			Name:  "input_ids",
			Shape: []int64{1, int64(seqLen)},
			Data:  inputIDs,
		},
		{
			Name:  "attention_mask",
			Shape: []int64{1, int64(seqLen)},
			Data:  attentionMask,
		},
		{
			Name:  "words_mask",
			Shape: []int64{1, int64(seqLen)},
			Data:  wordsMask,
		},
		{
			Name:  "text_lengths",
			Shape: []int64{1, 1},
			Data:  textLengths,
		},
		{
			Name:  "span_idx",
			Shape: []int64{1, int64(numSpans), 2},
			Data:  spanIdx,
		},
		{
			Name:  "span_mask",
			Shape: []int64{1, int64(numSpans)},
			Data:  spanMask,
		},
	}

	return inputs, nil
}

// parseOutputs extracts entities from model outputs.
func (p *GLiNERPipeline) parseOutputs(outputs []backends.NamedTensor, words []string, wordStartChars, wordEndChars []int, labels []string, originalText string, threshold float32, flatNER bool) ([]NEREntity, error) {
	// Find the logits output: prefer named "logits"/"output", fall back to first float32.
	var logits []float32
	var logitsShape []int64
	var fallbackLogits []float32
	var fallbackShape []int64

	for _, out := range outputs {
		data, ok := out.Data.([]float32)
		if !ok {
			continue
		}
		name := strings.ToLower(out.Name)
		if strings.Contains(name, "logits") || out.Name == "output" {
			logits = data
			logitsShape = out.Shape
			break
		}
		if fallbackLogits == nil {
			fallbackLogits = data
			fallbackShape = out.Shape
		}
	}

	if logits == nil {
		logits = fallbackLogits
		logitsShape = fallbackShape
	}

	if logits == nil {
		return nil, fmt.Errorf("no logits output found")
	}

	// Logits shape: [batch, num_words, max_width, num_labels] (4D)
	// Both GLiNER v1 and GLiNER2 now produce this format.
	numLabels := len(labels)
	numWords := len(words)
	maxWidth := p.PipelineConfig.MaxWidth

	// Get dimensions from logits shape
	if len(logitsShape) >= 4 {
		maxWidth = int(logitsShape[2])
		numLabels = int(logitsShape[3])
	} else if len(logitsShape) == 3 {
		numLabels = int(logitsShape[2])
	}

	// Clamp to actual label count to prevent out-of-bounds access
	if numLabels > len(labels) {
		numLabels = len(labels)
	}

	// Extract entities from spans with scores above threshold
	// The span grid is [numTokens, maxWidth] where:
	// - First index is the token position
	// - Second index is the span width index (0 = width 1, 1 = width 2, etc.)
	// We need to map token positions back to word positions for entity extraction
	var entities []NEREntity

	// For now, use word-based iteration since we need word boundaries for entity text
	// The logits are indexed by word position (after the prompt), not raw token position
	for w := range numWords {
		for wi := 0; wi < maxWidth; wi++ {
			start := w
			end := w + wi // span end position (word index)

			// Skip invalid spans (extending beyond text)
			if end >= numWords {
				continue
			}

			// Get scores for this span across all labels
			// Logits layout: [batch, token_pos, width, label]
			// Flat index: token_pos * maxWidth * numLabels + width * numLabels + label
			spanIdx := w*maxWidth*numLabels + wi*numLabels
			for labelIdx := 0; labelIdx < numLabels; labelIdx++ {
				logitIdx := spanIdx + labelIdx
				if logitIdx >= len(logits) {
					continue
				}

				score := sigmoid(logits[logitIdx])
				if score >= threshold {
					// Build entity text from words
					entityWords := words[start : end+1]
					entityText := strings.Join(entityWords, p.Config.WordsJoiner)

					// Get character positions
					charStart := wordStartChars[start]
					charEnd := wordEndChars[end]

					// Verify against original text
					if charStart < len(originalText) && charEnd <= len(originalText) {
						entityText = originalText[charStart:charEnd]
					}

					entities = append(entities, NEREntity{
						Text:  entityText,
						Label: labels[labelIdx],
						Start: charStart,
						End:   charEnd,
						Score: score,
					})
				}
			}
		}
	}

	// Apply flat NER (remove overlapping entities) if enabled
	if flatNER && len(entities) > 1 {
		entities = p.removeOverlappingEntities(entities)
	}

	// Sort by position
	slices.SortFunc(entities, func(a, b NEREntity) int {
		if c := cmp.Compare(a.Start, b.Start); c != 0 {
			return c
		}
		return cmp.Compare(a.End, b.End)
	})

	return entities, nil
}

// removeOverlappingEntities removes overlapping entities, keeping highest scoring ones.
func (p *GLiNERPipeline) removeOverlappingEntities(entities []NEREntity) []NEREntity {
	if len(entities) <= 1 {
		return entities
	}

	// Sort by score descending
	sorted := make([]NEREntity, len(entities))
	copy(sorted, entities)
	slices.SortFunc(sorted, func(a, b NEREntity) int {
		return cmp.Compare(b.Score, a.Score) // descending
	})

	var result []NEREntity
	for _, ent := range sorted {
		overlaps := false
		for _, existing := range result {
			if ent.Start < existing.End && ent.End > existing.Start {
				overlaps = true
				break
			}
		}
		if !overlaps {
			result = append(result, ent)
		}
	}

	return result
}

// Backend returns the backend type this pipeline uses.
func (p *GLiNERPipeline) Backend() backends.BackendType {
	return p.backendType
}

// Close releases resources held by the pipeline.
func (p *GLiNERPipeline) Close() error {
	var err error
	if p.Session != nil {
		err = p.Session.Close()
	}
	if p.LabelEncoderSession != nil {
		if closeErr := p.LabelEncoderSession.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	// Clear the label cache
	if p.labelCache != nil {
		p.ClearLabelEmbeddingCache()
	}
	return err
}

// ============================================================================
// BiEncoder Label Embedding Caching
// ============================================================================

// IsBiEncoder returns true if this is a BiEncoder model that supports label caching.
func (p *GLiNERPipeline) IsBiEncoder() bool {
	return p.Config != nil && p.Config.ModelType == GLiNERModelBiEncoder && p.labelCache != nil
}

// PrecomputeLabelEmbeddings precomputes and caches embeddings for the given labels.
// This is useful for BiEncoder models where label embeddings can be computed once
// and reused across many inference calls with the same labels.
//
// For BiEncoder models, this runs the labels through the label encoder to get
// embeddings that can be reused. For UniEncoder models, this is a no-op since
// labels are encoded together with the text.
func (p *GLiNERPipeline) PrecomputeLabelEmbeddings(labels []string) error {
	if !p.IsBiEncoder() {
		// Not a BiEncoder model, nothing to precompute
		return nil
	}

	if len(labels) == 0 {
		return nil
	}

	p.labelCache.mu.Lock()
	defer p.labelCache.mu.Unlock()

	// Check which labels need to be computed
	var labelsToCompute []string
	for _, label := range labels {
		if _, exists := p.labelCache.embeddings[label]; !exists {
			labelsToCompute = append(labelsToCompute, label)
		}
	}

	if len(labelsToCompute) == 0 {
		// All labels already cached
		return nil
	}

	// Compute embeddings for new labels
	embeddings, err := p.computeLabelEmbeddings(labelsToCompute)
	if err != nil {
		return fmt.Errorf("computing label embeddings: %w", err)
	}

	// Cache the embeddings
	for i, label := range labelsToCompute {
		p.labelCache.embeddings[label] = embeddings[i]
	}

	// Rebuild cached labels list from the map (single source of truth)
	p.labelCache.labels = make([]string, 0, len(p.labelCache.embeddings))
	for label := range p.labelCache.embeddings {
		p.labelCache.labels = append(p.labelCache.labels, label)
	}

	return nil
}

// computeLabelEmbeddings computes embeddings for the given labels using the label encoder.
// This is an internal method used by PrecomputeLabelEmbeddings.
func (p *GLiNERPipeline) computeLabelEmbeddings(labels []string) ([][]float32, error) {
	// Build inputs for the label encoder
	// Each label is tokenized and passed through the encoder

	embeddings := make([][]float32, len(labels))

	for i, label := range labels {
		// Tokenize the label with special formatting for GLiNER
		// GLiNER expects labels in format: <<label>>
		formattedLabel := "<<" + label + ">>"
		tokens := p.Tokenizer.EncodeWithOptions(formattedLabel, false)

		// Build input tensors for label encoding
		seqLen := len(tokens) + 2 // +2 for CLS and SEP
		inputIDs := make([]int64, seqLen)
		attentionMask := make([]int64, seqLen)

		// CLS token
		inputIDs[0] = 101
		attentionMask[0] = 1

		// Label tokens
		for j, tok := range tokens {
			inputIDs[j+1] = int64(tok)
			attentionMask[j+1] = 1
		}

		// SEP token
		inputIDs[seqLen-1] = 102
		attentionMask[seqLen-1] = 1

		inputs := []backends.NamedTensor{
			{
				Name:  "input_ids",
				Shape: []int64{1, int64(seqLen)},
				Data:  inputIDs,
			},
			{
				Name:  "attention_mask",
				Shape: []int64{1, int64(seqLen)},
				Data:  attentionMask,
			},
		}

		// Use the label encoder session if available, otherwise use main session
		session := p.LabelEncoderSession
		if session == nil {
			session = p.Session
		}

		outputs, err := session.Run(inputs)
		if err != nil {
			return nil, fmt.Errorf("running label encoder for %q: %w", label, err)
		}

		// Extract the embedding from the output
		// Typically this is the [CLS] token representation or a pooled output
		embedding, err := extractLabelEmbedding(outputs)
		if err != nil {
			return nil, fmt.Errorf("extracting embedding for %q: %w", label, err)
		}

		embeddings[i] = embedding
	}

	return embeddings, nil
}

// extractLabelEmbedding extracts the label embedding from model outputs.
// Prefers named embedding/pooler/label outputs, falls back to first float32.
func extractLabelEmbedding(outputs []backends.NamedTensor) ([]float32, error) {
	var fallback []float32
	for _, out := range outputs {
		data, ok := out.Data.([]float32)
		if !ok {
			continue
		}
		name := strings.ToLower(out.Name)
		if strings.Contains(name, "embedding") || strings.Contains(name, "pooler") || strings.Contains(name, "label") {
			return data, nil
		}
		if fallback == nil {
			fallback = data
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("no float32 embedding found in outputs")
}

// HasCachedLabelEmbeddings returns true if label embeddings are currently cached.
func (p *GLiNERPipeline) HasCachedLabelEmbeddings() bool {
	if !p.IsBiEncoder() {
		return false
	}

	p.labelCache.mu.RLock()
	defer p.labelCache.mu.RUnlock()

	return len(p.labelCache.embeddings) > 0
}

// CachedLabels returns the list of labels that are currently cached.
func (p *GLiNERPipeline) CachedLabels() []string {
	if !p.IsBiEncoder() {
		return nil
	}

	p.labelCache.mu.RLock()
	defer p.labelCache.mu.RUnlock()

	result := make([]string, len(p.labelCache.labels))
	copy(result, p.labelCache.labels)
	return result
}

// GetCachedLabelEmbedding returns the cached embedding for a label, if available.
// Returns nil if the label is not cached or this is not a BiEncoder model.
func (p *GLiNERPipeline) GetCachedLabelEmbedding(label string) []float32 {
	if !p.IsBiEncoder() {
		return nil
	}

	p.labelCache.mu.RLock()
	defer p.labelCache.mu.RUnlock()

	return p.labelCache.embeddings[label]
}

// ClearLabelEmbeddingCache clears all cached label embeddings.
func (p *GLiNERPipeline) ClearLabelEmbeddingCache() {
	if p.labelCache == nil {
		return
	}

	p.labelCache.mu.Lock()
	defer p.labelCache.mu.Unlock()

	p.labelCache.embeddings = make(map[string][]float32)
	p.labelCache.labels = nil
}

// SupportsRelationExtraction returns true if the model supports relation extraction.
func (p *GLiNERPipeline) SupportsRelationExtraction() bool {
	if p.Config == nil {
		return false
	}
	if slices.Contains(p.Config.Capabilities, "relations") {
		return true
	}
	return p.Config.ModelType == GLiNERModelMultiTask || p.Config.ModelType == GLiNERModelGLiNER2
}

// SupportsQA returns true if the model supports question answering.
func (p *GLiNERPipeline) SupportsQA() bool {
	if p.Config == nil {
		return false
	}
	if slices.Contains(p.Config.Capabilities, "answers") {
		return true
	}
	return p.Config.ModelType == GLiNERModelMultiTask || p.Config.ModelType == GLiNERModelGLiNER2
}

// SupportsClassification returns true if the model supports text classification.
func (p *GLiNERPipeline) SupportsClassification() bool {
	if p.Config == nil {
		return false
	}
	if slices.Contains(p.Config.Capabilities, "classification") {
		return true
	}
	return p.Config.ModelType == GLiNERModelGLiNER2
}

// SupportsExtraction returns true if the model supports structured schema-based extraction.
func (p *GLiNERPipeline) SupportsExtraction() bool {
	if p.Config == nil {
		return false
	}
	if slices.Contains(p.Config.Capabilities, "extraction") {
		return true
	}
	return p.Config.ModelType == GLiNERModelGLiNER2
}

// IsGLiNER2 returns true if this is a GLiNER2 model.
func (p *GLiNERPipeline) IsGLiNER2() bool {
	return p.Config != nil && p.Config.ModelType == GLiNERModelGLiNER2
}

// ============================================================================
// Loader Functions
// ============================================================================

// GLiNERLoaderOption configures GLiNER pipeline loading.
type GLiNERLoaderOption func(*glinerLoaderConfig)

type glinerLoaderConfig struct {
	threshold     float32
	maxWidth      int
	flatNER       bool
	multiLabel    bool
	defaultLabels []string
	quantized     bool
}

// WithGLiNERThreshold sets the score threshold for entity detection.
func WithGLiNERThreshold(threshold float32) GLiNERLoaderOption {
	return func(c *glinerLoaderConfig) {
		c.threshold = threshold
	}
}

// WithGLiNERMaxWidth sets the maximum entity span width.
func WithGLiNERMaxWidth(maxWidth int) GLiNERLoaderOption {
	return func(c *glinerLoaderConfig) {
		c.maxWidth = maxWidth
	}
}

// WithGLiNERFlatNER enables flat NER mode (no overlapping entities).
func WithGLiNERFlatNER(flatNER bool) GLiNERLoaderOption {
	return func(c *glinerLoaderConfig) {
		c.flatNER = flatNER
	}
}

// WithGLiNERMultiLabel enables multi-label mode.
func WithGLiNERMultiLabel(multiLabel bool) GLiNERLoaderOption {
	return func(c *glinerLoaderConfig) {
		c.multiLabel = multiLabel
	}
}

// WithGLiNERLabels sets the default labels.
func WithGLiNERLabels(labels []string) GLiNERLoaderOption {
	return func(c *glinerLoaderConfig) {
		c.defaultLabels = labels
	}
}

// WithGLiNERQuantized uses quantized model files if available.
func WithGLiNERQuantized(quantized bool) GLiNERLoaderOption {
	return func(c *glinerLoaderConfig) {
		c.quantized = quantized
	}
}

// LoadGLiNERPipeline loads a GLiNER pipeline from a model directory.
// Returns the pipeline and the backend type that was used.
func LoadGLiNERPipeline(
	modelPath string,
	sessionManager *backends.SessionManager,
	modelBackends []string,
	opts ...GLiNERLoaderOption,
) (*GLiNERPipeline, backends.BackendType, error) {
	// Apply options
	loaderCfg := &glinerLoaderConfig{
		threshold: 0.5,
		flatNER:   true,
	}
	for _, opt := range opts {
		opt(loaderCfg)
	}

	// Load model configuration
	modelConfig, err := LoadGLiNERModelConfig(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading GLiNER config: %w", err)
	}

	// Override config with loader options
	if loaderCfg.threshold > 0 {
		modelConfig.Threshold = loaderCfg.threshold
	}
	if loaderCfg.maxWidth > 0 {
		modelConfig.MaxWidth = loaderCfg.maxWidth
	}
	if loaderCfg.quantized {
		quantizedFile := FindONNXFile(modelPath, []string{"model_quantized.onnx"})
		if quantizedFile != "" {
			modelConfig.ModelFile = quantizedFile
		}
	}

	// Get a session factory for the model
	factory, backendType, err := sessionManager.GetSessionFactoryForModel(modelBackends)
	if err != nil {
		return nil, "", fmt.Errorf("getting session factory: %w", err)
	}

	// Load tokenizer
	tokenizer, err := tokenizers.LoadTokenizer(modelPath)
	if err != nil {
		return nil, "", fmt.Errorf("loading tokenizer: %w", err)
	}

	// Create session for the ONNX model
	session, err := factory.CreateSession(modelConfig.ModelFile)
	if err != nil {
		return nil, "", fmt.Errorf("creating session: %w", err)
	}

	// Build pipeline config
	pipelineConfig := &GLiNERPipelineConfig{
		Threshold:     modelConfig.Threshold,
		MaxWidth:      modelConfig.MaxWidth,
		FlatNER:       loaderCfg.flatNER || modelConfig.FlatNER,
		MultiLabel:    loaderCfg.multiLabel || modelConfig.MultiLabel,
		DefaultLabels: modelConfig.DefaultLabels,
	}

	if len(loaderCfg.defaultLabels) > 0 {
		pipelineConfig.DefaultLabels = loaderCfg.defaultLabels
	}

	pipeline := NewGLiNERPipeline(session, tokenizer, modelConfig, pipelineConfig, backendType)

	return pipeline, backendType, nil
}

// ============================================================================
// JSON Extraction Support
// ============================================================================

// NERExtractedSpan is a span extracted for JSON extraction.
// It is a type alias for NEREntity since they share the same structure.
type NERExtractedSpan = NEREntity

// ExtractSpansForLabels extracts entity spans using the given labels and threshold.
// This is a thin wrapper around processText for use by JSON extraction.
func (p *GLiNERPipeline) ExtractSpansForLabels(
	ctx context.Context,
	text string,
	labels []string,
	threshold float32,
	flatNER bool,
) ([]NERExtractedSpan, error) {
	if text == "" || len(labels) == 0 {
		return nil, nil
	}

	entities, err := p.processTextWithConfig(ctx, text, labels, threshold, flatNER)
	if err != nil {
		return nil, err
	}

	// NERExtractedSpan is a type alias for NEREntity, so no conversion needed.
	return entities, nil
}

// ClassifySpanText classifies a span of text against a set of choices.
// Uses the GLiNER2 classification prompt format.
// Returns the best matching choice and its score.
func (p *GLiNERPipeline) ClassifySpanText(
	ctx context.Context,
	spanText string,
	choices []string,
) (string, float32, error) {
	if spanText == "" || len(choices) == 0 {
		return "", 0, nil
	}

	config := &NERClassificationConfig{
		Threshold:  0.0, // Accept any score
		MultiLabel: false,
		TopK:       1,
	}

	classifications, err := p.classifySingleText(ctx, spanText, choices, config)
	if err != nil {
		return "", 0, err
	}

	if len(classifications) == 0 {
		return choices[0], 0, nil // Default to first choice
	}

	return classifications[0].Label, classifications[0].Score, nil
}
