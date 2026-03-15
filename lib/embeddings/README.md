# Embeddings

The embeddings package provides a flexible plugin-based system for generating text embeddings using various providers. It also includes a template rendering system for preprocessing text before embedding.

## Table of Contents

- [Supported Providers](#supported-providers)
- [Configuration](#configuration)
- [Templates](#templates)
- [Environment Variables](#environment-variables)

## Supported Providers

### 1. Google AI (Gemini)

Uses Google's Gemini models for generating embeddings.

**Configuration:**
```json
{
  "model": "text-embedding-004",  // Required
  "api_key": "your-api-key"       // Optional, can use GEMINI_API_KEY env var
}
```

**Available Models:**
- `text-embedding-004` - Latest embedding model
- `embedding-001` - Legacy model

### 2. OpenAI

Supports OpenAI and OpenAI-compatible APIs for embeddings.

**Configuration:**
```json
{
  "model": "text-embedding-3-small",  // Required
  "url": "https://api.openai.com",    // Optional, defaults to OpenAI
  "api_key": "your-api-key"           // Optional, can use OPENAI_API_KEY env var
}
```

**Available Models:**
- `text-embedding-3-small` - Smaller, faster model
- `text-embedding-3-large` - Larger, more accurate model
- `text-embedding-ada-002` - Legacy model

### 3. Ollama

Local embeddings using Ollama's HTTP API.

**Configuration:**
```json
{
  "model": "all-minilm",              // Required
  "url": "http://localhost:11434"     // Optional, can use OLLAMA_HOST env var
}
```

**Popular Models:**
- `all-minilm` - 384-dimensional embeddings, good for general use
- `nomic-embed-text` - High-quality embeddings
- `mxbai-embed-large` - Larger model for better accuracy

## Configuration

Each provider is configured using a map of key-value pairs. The configuration is validated against JSON schemas to ensure correctness.

## Templates

The embeddings package includes a powerful template system for preprocessing text before embedding. This is useful for:
- Adding context or metadata to text
- Formatting documents consistently
- Creating structured prompts

### Template Features

1. **Caching**: Templates are automatically cached for performance
2. **TTL Management**: Cached templates expire after a configurable duration
3. **Go Template Syntax**: Uses standard [Go text/template syntax](https://pkg.go.dev/text/template)

### Basic Usage

```go
import "github.com/yourdomain/lib/embeddings"

// Define a template
templateStr := `Document: {{.title}}
Author: {{.author}}
Content: {{.content}}`

// Create context
context := map[string]any{
    "title": "Introduction to Embeddings",
    "author": "John Doe",
    "content": "Embeddings are vector representations of text...",
}

// Render template
result, err := embeddings.RenderTemplate(templateStr, context)
if err != nil {
    log.Fatal(err)
}

// result now contains the formatted text ready for embedding
```

### Advanced Template Examples

**Document with Metadata:**
```go
templateStr := `Title: {{.metadata.title}}
Date: {{.metadata.date}}
Tags: {{range .metadata.tags}}{{.}}, {{end}}

{{.content}}`
```

**Question-Answer Format:**
```go
templateStr := `Question: {{.question}}
Context: {{.context}}
Answer: {{.answer}}`
```

### Template Best Practices

1. **Keep templates simple** - Complex logic should be in your application code
2. **Use consistent naming** - Use clear, descriptive field names in your context
3. **Handle missing fields** - Templates use "missingkey=zero" option by default

## Environment Variables

The package supports the following environment variables for API keys and endpoints:

| Variable | Provider | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` | Google | API key for Google AI |
| `OPENAI_API_KEY` | OpenAI | API key for OpenAI |
| `OPENAI_BASE_URL` | OpenAI | Base URL for OpenAI-compatible APIs |
| `OLLAMA_HOST` | Ollama | Ollama server URL (e.g., http://localhost:11434) |
