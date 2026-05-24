package reading

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDonutCleanOutput(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "cord-v2 task tokens with JSON",
			input:    `<s_cord-v2>{"menu": "coffee"}</s_cord-v2>`,
			expected: `{"menu": "coffee"}`,
		},
		{
			name:     "docvqa task tokens preserves inner fields",
			input:    `<s_docvqa><s_question>What is the date?</s_question><s_answer>2024-01-15</s_answer></s_docvqa>`,
			expected: `<s_question>What is the date?</s_question><s_answer>2024-01-15</s_answer>`,
		},
		{
			name:     "no task tokens",
			input:    `{"plain": "json"}`,
			expected: `{"plain": "json"}`,
		},
		{
			name:     "only opening token",
			input:    `<s_cord-v2>{"menu": "coffee"}`,
			expected: `{"menu": "coffee"}`,
		},
		{
			name:     "only closing token",
			input:    `{"menu": "coffee"}</s_cord-v2>`,
			expected: `{"menu": "coffee"}`,
		},
		{
			name:     "whitespace handling",
			input:    `  <s_cord-v2>  {"menu": "coffee"}  </s_cord-v2>  `,
			expected: `{"menu": "coffee"}`,
		},
		{
			name:     "nested tokens only removes outer",
			input:    `<s_outer><s_inner>content</s_inner></s_outer>`,
			expected: `<s_inner>content</s_inner>`,
		},
		{
			name:     "empty string",
			input:    ``,
			expected: ``,
		},
		{
			name:     "just task tokens",
			input:    `<s_cord-v2></s_cord-v2>`,
			expected: ``,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DonutCleanOutput(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDonutParseFields_Simple(t *testing.T) {
	input := `<s_company>ACME Corp</s_company><s_date>2024-01-15</s_date><s_total>$123.45</s_total>`

	result := DonutParseFields(input)

	assert.Equal(t, "ACME Corp", result["company"])
	assert.Equal(t, "2024-01-15", result["date"])
	assert.Equal(t, "$123.45", result["total"])
	assert.Len(t, result, 3)
}

func TestDonutParseFields_Nested(t *testing.T) {
	input := `<s_menu><s_nm>Coffee</s_nm><s_price>$3.50</s_price></s_menu>`

	result := DonutParseFields(input)

	assert.Equal(t, "Coffee", result["menu.nm"])
	assert.Equal(t, "$3.50", result["menu.price"])
	assert.Len(t, result, 2)
}

func TestDonutParseFields_DeeplyNested(t *testing.T) {
	input := `<s_receipt><s_items><s_item><s_name>Latte</s_name><s_qty>2</s_qty></s_item></s_items></s_receipt>`

	result := DonutParseFields(input)

	assert.Equal(t, "Latte", result["receipt.items.item.name"])
	assert.Equal(t, "2", result["receipt.items.item.qty"])
}

func TestDonutParseFields_Empty(t *testing.T) {
	result := DonutParseFields("")
	assert.Empty(t, result)
}

func TestDonutParseFields_NoFields(t *testing.T) {
	result := DonutParseFields("just plain text")
	assert.Empty(t, result)
}

func TestDonutParseFields_MalformedTags(t *testing.T) {
	input := `<s_field>value</s_other>`
	result := DonutParseFields(input)
	assert.Empty(t, result, "mismatched tags should not be parsed")
}

func TestDonutParseFields_RealCORDOutput(t *testing.T) {
	input := `<s_cord-v2><s_menu><s_nm>ICED LATTE</s_nm><s_price>$4.50</s_price></s_menu><s_total><s_total_price>$11.34</s_total_price></s_total></s_cord-v2>`

	result := DonutParseFields(input)

	assert.Contains(t, result, "menu.nm")
	assert.Contains(t, result, "total.total_price")
	assert.Equal(t, "$11.34", result["total.total_price"])
}

func TestDonutDocVQAPrompt(t *testing.T) {
	prompt := DonutDocVQAPrompt("What is the total amount?")
	assert.Equal(t, "<s_docvqa><s_question>What is the total amount?</s_question><s_answer>", prompt)
}

func TestDonutCORDPrompt(t *testing.T) {
	assert.Equal(t, "<s_cord-v2>", DonutCORDPrompt())
}

func TestDonutParseDocVQAAnswer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "with closing tokens",
			input:    "The answer is 42</s_answer></s_docvqa>",
			expected: "The answer is 42",
		},
		{
			name:     "without closing tokens",
			input:    "The answer is 42",
			expected: "The answer is 42",
		},
		{
			name:     "with whitespace",
			input:    "  $123.45  </s_answer>",
			expected: "$123.45",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DonutParseDocVQAAnswer(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
