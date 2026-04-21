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

package reading

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMoondreamParseFields_ValidJSON(t *testing.T) {
	input := `{
		"description": "A fluffy orange cat sleeping on a blue couch",
		"mood": "peaceful",
		"possible_source": "photo",
		"tags": ["cat", "sleeping", "couch", "orange"]
	}`

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "A fluffy orange cat sleeping on a blue couch", desc)
	assert.Equal(t, "peaceful", fields["mood"])
	assert.Equal(t, "photo", fields["possible_source"])
	assert.Equal(t, "cat,sleeping,couch,orange", fields["tags"])
}

func TestMoondreamParseFields_MarkdownCodeBlock(t *testing.T) {
	input := "```json\n{\"description\": \"A sunset over mountains\", \"mood\": \"serene\"}\n```"

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "A sunset over mountains", desc)
	assert.Equal(t, "serene", fields["mood"])
}

func TestMoondreamParseFields_CodeBlockNoLanguage(t *testing.T) {
	input := "```\n{\"description\": \"A dog playing fetch\", \"mood\": \"happy\"}\n```"

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "A dog playing fetch", desc)
	assert.Equal(t, "happy", fields["mood"])
}

func TestMoondreamParseFields_JSONInText(t *testing.T) {
	input := `Here is the analysis: {"description": "A cityscape at night", "mood": "urban"} as requested.`

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "A cityscape at night", desc)
	assert.Equal(t, "urban", fields["mood"])
}

func TestMoondreamParseFields_PlainText(t *testing.T) {
	input := "This is just a plain text description of an image."

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "This is just a plain text description of an image.", desc)
	assert.Empty(t, fields)
}

func TestMoondreamParseFields_EmptyDescription(t *testing.T) {
	input := `{"description": "", "mood": "neutral"}`

	desc, fields := MoondreamParseFields(input)

	// When description is empty, fallback to raw text
	assert.JSONEq(t, `{"description": "", "mood": "neutral"}`, desc)
	assert.Equal(t, "neutral", fields["mood"])
}

func TestMoondreamParseFields_MalformedJSON(t *testing.T) {
	input := `{"description": "incomplete json`

	desc, fields := MoondreamParseFields(input)

	// Should fallback to raw text
	assert.Equal(t, `{"description": "incomplete json`, desc)
	assert.Empty(t, fields)
}

func TestMoondreamParseFields_TemporalFlow(t *testing.T) {
	input := `{
		"description": "A time-lapse of flowers blooming",
		"temporal_flow": "sequential growth over time"
	}`

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "A time-lapse of flowers blooming", desc)
	assert.Equal(t, "sequential growth over time", fields["temporal_flow"])
}

func TestMoondreamParseFields_EmptyInput(t *testing.T) {
	desc, fields := MoondreamParseFields("")

	assert.Empty(t, desc)
	assert.Empty(t, fields)
}

func TestMoondreamParseFields_WhitespaceOnly(t *testing.T) {
	desc, fields := MoondreamParseFields("   \n\t  ")

	assert.Empty(t, desc)
	assert.Empty(t, fields)
}

func TestMoondreamParseFields_NestedBraces(t *testing.T) {
	input := `{"description": "Code snippet: function() { return {}; }", "mood": "technical"}`

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "Code snippet: function() { return {}; }", desc)
	assert.Equal(t, "technical", fields["mood"])
}

func TestMoondreamParseFields_EscapedQuotes(t *testing.T) {
	input := `{"description": "He said \"hello\" to her", "mood": "conversational"}`

	desc, fields := MoondreamParseFields(input)

	assert.Equal(t, "He said \"hello\" to her", desc)
	assert.Equal(t, "conversational", fields["mood"])
}

func TestMoondreamDescriptionPrompt_Default(t *testing.T) {
	prompt := MoondreamDescriptionPrompt("")

	assert.Contains(t, prompt, "Describe this image")
	assert.Contains(t, prompt, "Respond ONLY with valid JSON")
}

func TestMoondreamDescriptionPrompt_Custom(t *testing.T) {
	prompt := MoondreamDescriptionPrompt("What animals are in this image?")

	assert.Contains(t, prompt, "What animals are in this image?")
	assert.Contains(t, prompt, "Respond ONLY with valid JSON")
}

func TestMoondreamDescriptionPrompt_ContainsExpectedFields(t *testing.T) {
	prompt := MoondreamDescriptionPrompt("")

	assert.Contains(t, prompt, "description")
	assert.Contains(t, prompt, "mood")
	assert.Contains(t, prompt, "possible_source")
	assert.Contains(t, prompt, "tags")
}
