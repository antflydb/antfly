//go:generate go tool oapi-codegen --config=cfg.yaml ./openapi.yaml
package chunking

import (
	"context"
)

// Fixed chunker model names
const (
	// ModelFixedBert uses BERT's WordPiece tokenization (~30k vocab).
	// Good for general-purpose text and multilingual content.
	ModelFixedBert = "fixed-bert-tokenizer"

	// ModelFixedBPE uses OpenAI's tiktoken BPE tokenization (cl100k_base, ~100k vocab).
	// Good for GPT-style models and code.
	ModelFixedBPE = "fixed-bpe-tokenizer"
)

// MIMETypePlainText is the MIME type for text/plain content chunks.
const MIMETypePlainText = "text/plain"

// NewTextChunk creates a Chunk containing text content with the given parameters.
func NewTextChunk(id uint32, text string, startChar, endChar int) Chunk {
	var c Chunk
	c.Id = id
	c.MimeType = MIMETypePlainText
	_ = c.FromTextContent(TextContent{
		Text:      text,
		StartChar: startChar,
		EndChar:   endChar,
	})
	return c
}

// GetText returns the text content of a text chunk.
// Returns empty string if the chunk is not a text chunk or cannot be decoded.
func (c Chunk) GetText() string {
	tc, err := c.AsTextContent()
	if err != nil {
		return ""
	}
	return tc.Text
}

// Chunker splits text into semantically meaningful chunks.
// ChunkOptions is generated from openapi.yaml - see openapi.gen.go
type Chunker interface {
	// Chunk splits text using the provided per-request options.
	// Options that are nil use the chunker's default values.
	Chunk(ctx context.Context, text string, opts ChunkOptions) ([]Chunk, error)
	Close() error
}
