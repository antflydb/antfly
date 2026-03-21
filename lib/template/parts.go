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

package template

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/google/dotprompt/go/dotprompt"
)

// DocumentToParts converts a Document to dotprompt Parts using a Handlebars template.
// This is an alternate implementation to ai.TextToParts that leverages dotprompt's
// template system and supports multimodal content.
//
// The dp parameter allows callers to provide a configured Dotprompt instance with
// custom helpers. If using the remote content helpers (remoteMedia, remotePDF, remoteText),
// ensure lib/template is imported to register them.
//
// The template parameter is a Handlebars template string that can reference document
// fields via {{fieldName}} and use any registered helpers.
//
// Returns a RenderedPrompt containing messages with Parts (TextPart, MediaPart, etc.)
// that can be used with LLM APIs.
//
// Example template:
//
//	{{#if photoUrl}}
//	{{remoteMedia url=photoUrl}}
//	{{/if}}
//	{{#if pdfUrl}}
//	PDF content: {{remotePDF url=pdfUrl}}
//	{{/if}}
//	Title: {{title}}
//	Content: {{body}}
func DocumentToParts(
	ctx context.Context,
	dp *dotprompt.Dotprompt,
	doc schema.Document,
	template string,
) (prompt dotprompt.RenderedPrompt, err error) {
	// Filter out internal fields
	fields := make(map[string]any)
	for key, value := range doc.Fields {
		if key == "_embeddings" {
			continue
		}
		fields[key] = value
	}

	// Compile template
	promptFunc, err := dp.Compile(template, nil)
	if err != nil {
		return prompt, fmt.Errorf("compiling template: %w", err)
	}

	// Create data argument with document fields
	data := &dotprompt.DataArgument{
		Input: fields,
	}

	// Render the prompt (helpers will be invoked during rendering)
	renderedPrompt, err := promptFunc(data, nil)
	if err != nil {
		return renderedPrompt, fmt.Errorf("rendering prompt: %w", err)
	}
	return renderedPrompt, nil
}
