package entity

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	docsafentity "github.com/antflydb/antfly/pkg/docsaf/entity"
	termiteclient "github.com/antflydb/termite/pkg/client"
	"github.com/antflydb/termite/pkg/client/oapi"
	"github.com/mbleigh/raymond"
)

func buildTermiteExtractor(tc *termiteclient.TermiteClient, cfg Config) (docsafentity.Extractor, error) {
	switch cfg.Provider {
	case "", "termite":
	default:
		return nil, fmt.Errorf("unsupported --extractor-provider %q", cfg.Provider)
	}

	switch cfg.Kind {
	case "", ExtractorKindRecognizer:
		if cfg.IncludeRelations {
			return &termiteRelationExtractor{client: tc, model: cfg.Model}, nil
		}
		return &termiteRecognizerExtractor{client: tc, model: cfg.Model}, nil
	case ExtractorKindGenerator:
		return &termiteGeneratorExtractor{
			client:         tc,
			model:          cfg.Model,
			promptTemplate: cfg.PromptTemplate,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported --extractor-kind %q (want recognizer or generator)", cfg.Kind)
	}
}

type termiteRecognizerExtractor struct {
	client *termiteclient.TermiteClient
	model  string
}

func (r *termiteRecognizerExtractor) Extract(ctx context.Context, texts []string, opts docsafentity.ExtractOptions) ([]docsafentity.Extraction, error) {
	resp, err := r.client.Recognize(ctx, r.model, texts, opts.EntityLabels)
	if err != nil {
		return nil, err
	}

	results := make([]docsafentity.Extraction, len(resp.Entities))
	for i, entities := range resp.Entities {
		results[i].Entities = make([]docsafentity.Entity, len(entities))
		for j, entity := range entities {
			results[i].Entities[j] = docsafentity.Entity{
				Text:  entity.Text,
				Label: entity.Label,
				Score: entity.Score,
				Start: entity.Start,
				End:   entity.End,
			}
		}
	}
	return results, nil
}

type termiteRelationExtractor struct {
	client *termiteclient.TermiteClient
	model  string
}

func (r *termiteRelationExtractor) Extract(ctx context.Context, texts []string, opts docsafentity.ExtractOptions) ([]docsafentity.Extraction, error) {
	resp, err := r.client.ExtractRelations(ctx, r.model, texts, opts.EntityLabels, opts.RelationLabels)
	if err != nil {
		return nil, err
	}

	results := make([]docsafentity.Extraction, len(resp.Entities))
	for i := range resp.Entities {
		entities := resp.Entities[i]
		results[i].Entities = make([]docsafentity.Entity, len(entities))
		for j, entity := range entities {
			results[i].Entities[j] = docsafentity.Entity{
				Text:  entity.Text,
				Label: entity.Label,
				Score: entity.Score,
				Start: entity.Start,
				End:   entity.End,
			}
		}
		if i < len(resp.Relations) {
			relations := resp.Relations[i]
			results[i].Relations = make([]docsafentity.Relation, len(relations))
			for j, relation := range relations {
				results[i].Relations[j] = docsafentity.Relation{
					Head: docsafentity.Entity{
						Text:  relation.Head.Text,
						Label: relation.Head.Label,
						Score: relation.Head.Score,
						Start: relation.Head.Start,
						End:   relation.Head.End,
					},
					Label: relation.Label,
					Score: relation.Score,
					Tail: docsafentity.Entity{
						Text:  relation.Tail.Text,
						Label: relation.Tail.Label,
						Score: relation.Tail.Score,
						Start: relation.Tail.Start,
						End:   relation.Tail.End,
					},
				}
			}
		}
	}
	return results, nil
}

type termiteGeneratorExtractor struct {
	client         *termiteclient.TermiteClient
	model          string
	promptTemplate string
}

type toolExtractionResponse struct {
	Entities []toolEntity `json:"entities"`
}

type toolEntity struct {
	Text  string  `json:"text"`
	Label string  `json:"label"`
	Score float32 `json:"score"`
	Start int     `json:"start"`
	End   int     `json:"end"`
}

func (r *termiteGeneratorExtractor) Extract(ctx context.Context, texts []string, opts docsafentity.ExtractOptions) ([]docsafentity.Extraction, error) {
	results := make([]docsafentity.Extraction, len(texts))
	for i, text := range texts {
		extraction, err := r.extractOne(ctx, text, opts)
		if err != nil {
			return nil, fmt.Errorf("tool extraction for text %d: %w", i+1, err)
		}
		results[i] = extraction
	}
	return results, nil
}

func (r *termiteGeneratorExtractor) extractOne(ctx context.Context, text string, opts docsafentity.ExtractOptions) (docsafentity.Extraction, error) {
	prompt, err := r.renderPrompt(opts)
	if err != nil {
		return docsafentity.Extraction{}, err
	}

	resp, err := r.client.Generate(ctx, r.model, []oapi.ChatMessage{
		termiteclient.NewSystemMessage(prompt),
		termiteclient.NewUserMessage(text),
	}, &termiteclient.GenerateConfig{
		MaxTokens:   512,
		Temperature: 0,
		Tools: []oapi.Tool{
			{
				Type: oapi.ToolTypeFunction,
				Function: oapi.FunctionDefinition{
					Name:        "extract_entities",
					Description: "Extract named entities from a text passage.",
					Parameters: map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"entities": map[string]interface{}{
								"type": "array",
								"items": map[string]interface{}{
									"type": "object",
									"properties": map[string]interface{}{
										"text":  map[string]interface{}{"type": "string"},
										"label": map[string]interface{}{"type": "string"},
										"score": map[string]interface{}{"type": "number"},
										"start": map[string]interface{}{"type": "integer"},
										"end":   map[string]interface{}{"type": "integer"},
									},
									"required": []string{"text", "label"},
								},
							},
						},
						"required": []string{"entities"},
					},
				},
			},
		},
		ToolChoice: termiteclient.ToolChoiceFunction("extract_entities"),
	})
	if err != nil {
		return docsafentity.Extraction{}, err
	}
	if len(resp.Choices) == 0 {
		return docsafentity.Extraction{}, fmt.Errorf("generator returned no choices")
	}

	var raw toolExtractionResponse
	found := false
	for _, call := range resp.Choices[0].Message.ToolCalls {
		if call.Function.Name != "extract_entities" {
			continue
		}
		if err := json.Unmarshal([]byte(call.Function.Arguments), &raw); err != nil {
			return docsafentity.Extraction{}, fmt.Errorf("parsing tool arguments: %w", err)
		}
		found = true
		break
	}
	if !found {
		return docsafentity.Extraction{}, fmt.Errorf("generator did not call extract_entities")
	}

	result := docsafentity.Extraction{
		Entities: make([]docsafentity.Entity, 0, len(raw.Entities)),
	}
	for _, entity := range raw.Entities {
		result.Entities = append(result.Entities, docsafentity.Entity{
			Text:  entity.Text,
			Label: entity.Label,
			Score: entity.Score,
			Start: entity.Start,
			End:   entity.End,
		})
	}
	return result, nil
}

func (r *termiteGeneratorExtractor) renderPrompt(opts docsafentity.ExtractOptions) (string, error) {
	if strings.TrimSpace(r.promptTemplate) == "" {
		prompt := "Extract named entities from the user-provided text."
		if len(opts.EntityLabels) > 0 {
			prompt += " Use only these labels when possible: " + strings.Join(opts.EntityLabels, ", ") + "."
		}
		prompt += " Call the extract_entities function exactly once."
		return prompt, nil
	}

	rendered, err := raymond.Render(r.promptTemplate, map[string]any{
		"EntityLabels":    opts.EntityLabels,
		"RelationLabels":  opts.RelationLabels,
		"HasEntityLabels": len(opts.EntityLabels) > 0,
	})
	if err != nil {
		return "", fmt.Errorf("rendering extractor prompt template: %w", err)
	}

	prompt := strings.TrimSpace(rendered)
	if prompt == "" {
		return "", fmt.Errorf("extractor prompt template rendered empty prompt")
	}
	return prompt, nil
}
