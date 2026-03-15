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

	libscraping "github.com/antflydb/antfly/pkg/libaf/scraping"
	"github.com/google/dotprompt/go/dotprompt"
)

// RenderedPromptToContentParts converts a dotprompt.RenderedPrompt to [][]ContentPart
// for use with MultimodalEmbed and other functions that work with ContentPart.
//
// Each message in the RenderedPrompt becomes a []ContentPart slice.
// The conversion rules are:
//   - TextPart -> TextContent
//   - MediaPart with data URI -> BinaryContent (parsed from data URI)
//   - MediaPart with URL -> ImageURLContent
//   - DataPart, ToolRequestPart, ToolResponsePart, etc. are ignored
//
// Returns a slice of content part slices, one per message.
func RenderedPromptToContentParts(rendered dotprompt.RenderedPrompt) ([][]ContentPart, error) {
	result := make([][]ContentPart, 0, len(rendered.Messages))

	for _, msg := range rendered.Messages {
		msgParts := make([]ContentPart, 0, len(msg.Content))

		for _, part := range msg.Content {
			switch p := part.(type) {
			case *dotprompt.TextPart:
				msgParts = append(msgParts, TextContent{
					Text: p.Text,
				})

			case *dotprompt.MediaPart:
				// Check if URL is a data URI
				if strings.HasPrefix(p.Media.URL, "data:") {
					// Parse data URI
					contentType, data, err := libscraping.ParseDataURI(p.Media.URL)
					if err != nil {
						return nil, fmt.Errorf("parsing data URI: %w", err)
					}
					msgParts = append(msgParts, BinaryContent{
						MIMEType: contentType,
						Data:     data,
					})
				} else {
					// Regular URL
					msgParts = append(msgParts, ImageURLContent{
						URL: p.Media.URL,
					})
				}

				// Ignore DataPart, ToolRequestPart, ToolResponsePart, etc.
				// They're not relevant for multimodal embedding
			}
		}

		result = append(result, msgParts)
	}

	return result, nil
}
