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
	"regexp"
	"strings"

	"github.com/antflydb/antfly/lib/template"
	libscraping "github.com/antflydb/antfly/pkg/libaf/scraping"
	"mvdan.cc/xurls/v2"
)

// dotpromptMediaRe matches <<<dotprompt:media:url DATA_URI_OR_URL>>>
var dotpromptMediaRe = regexp.MustCompile(`<<<dotprompt:media:url\s+(.+?)>>>`)

// textToParts converts a text string to ContentPart slices, handling both URL-based
// content and text with embedded URLs.
//
// If the text starts with a URL prefix (http:, https:, s3:, file:, data:), it returns
// a single ImageURLContent.
//
// Otherwise, it extracts any embedded URLs from the text. If there's only a single URL
// and it matches the entire text, it returns just the ImageURLContent without placeholder.
// For multiple URLs or URLs embedded in text, it replaces them with "<see appended content>"
// placeholders and returns a TextContent followed by ImageURLContent parts for each URL.
func textToParts(text string) []ContentPart {
	parts := []ContentPart{}

	// Check if the entire text is a URL
	if strings.HasPrefix(text, "http:") ||
		strings.HasPrefix(text, "https:") ||
		strings.HasPrefix(text, "s3:") ||
		strings.HasPrefix(text, "file:") ||
		strings.HasPrefix(text, "data:") {
		parts = append(parts, ImageURLContent{URL: text})
		return parts
	}

	// Extract URLs from within the text
	rx := xurls.Strict()
	urls := rx.FindAllString(text, -1)

	// If there's only a single URL and it matches the entire text, return just the URL
	if len(urls) == 1 && strings.TrimSpace(text) == urls[0] {
		parts = append(parts, ImageURLContent{URL: urls[0]})
		return parts
	}

	// Replace URLs in text with placeholder
	for _, url := range urls {
		text = strings.ReplaceAll(text, url, "<see appended content>")
	}

	// Add text content
	parts = append(parts, TextContent{Text: text})

	// Append extracted URLs as image content
	for _, url := range urls {
		parts = append(parts, ImageURLContent{URL: url})
	}

	return parts
}

// TextToParts converts a rendered template string to ContentPart slices,
// parsing dotprompt media markers (<<<dotprompt:media:url ...>>>) into proper
// BinaryContent or ImageURLContent parts. Text segments between markers become
// TextContent parts. If the string contains no dotprompt markers, it extracts
// embedded URLs as ImageURLContent parts.
func TextToParts(text string) ([]ContentPart, error) {
	// Strip any error directives that leaked through (defensive; RenderPrompt should catch these)
	text = template.StripErrorDirectives(text)
	matches := dotpromptMediaRe.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return textToParts(text), nil
	}

	var parts []ContentPart
	prev := 0
	for _, loc := range matches {
		// Text before the marker
		if loc[0] > prev {
			segment := strings.TrimSpace(text[prev:loc[0]])
			if segment != "" {
				parts = append(parts, TextContent{Text: segment})
			}
		}

		// The captured URL/data URI
		url := text[loc[2]:loc[3]]
		if strings.HasPrefix(url, "data:") {
			contentType, data, err := libscraping.ParseDataURI(url)
			if err != nil {
				return nil, fmt.Errorf("parsing data URI in dotprompt media marker: %w", err)
			}
			parts = append(parts, BinaryContent{
				MIMEType: contentType,
				Data:     data,
			})
		} else {
			parts = append(parts, ImageURLContent{URL: url})
		}

		prev = loc[1]
	}

	// Trailing text after the last marker
	if prev < len(text) {
		segment := strings.TrimSpace(text[prev:])
		if segment != "" {
			parts = append(parts, TextContent{Text: segment})
		}
	}

	return parts, nil
}

func NewGeneratorConfig(config any) (*GeneratorConfig, error) {
	var provider GeneratorProvider
	modelConfig := &GeneratorConfig{}
	switch v := config.(type) {
	case OllamaGeneratorConfig:
		provider = GeneratorProviderOllama
		if err := modelConfig.FromOllamaGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Ollama config: %w", err)
		}
	case OpenAIGeneratorConfig:
		provider = GeneratorProviderOpenai
		if err := modelConfig.FromOpenAIGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert OpenAI config: %w", err)
		}
	case GoogleGeneratorConfig:
		provider = GeneratorProviderGemini
		if err := modelConfig.FromGoogleGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Google config: %w", err)
		}
	case BedrockGeneratorConfig:
		provider = GeneratorProviderBedrock
		if err := modelConfig.FromBedrockGeneratorConfig(v); err != nil {
			return nil, fmt.Errorf("failed to convert Bedrock config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown model config type: %T", v)
	}

	modelConfig.Provider = provider
	return modelConfig, nil
}
