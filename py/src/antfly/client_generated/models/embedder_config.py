from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.embedder_provider import EmbedderProvider

T = TypeVar("T", bound="EmbedderConfig")


@_attrs_define
class EmbedderConfig:
    r"""A unified configuration for an embedding provider.

    Embedders can be configured with templates to customize how documents are
    converted to text before embedding. Templates use Handlebars syntax and
    support various built-in helpers.

    **Template System:**
    - **Syntax**: Handlebars templating (https://handlebarsjs.com/guide/)
    - **Caching**: Templates are automatically cached with configurable TTL (default: 5 minutes)
    - **Context**: Templates receive the full document as context

    **Built-in Helpers:**

    1. **scrubHtml** - Remove script/style tags and extract clean text from HTML
       ```handlebars
       {{scrubHtml html_content}}
       ```
       - Removes `<script>` and `<style>` tags
       - Adds newlines after block elements (p, div, h1-h6, li, etc.)
       - Returns plain text with preserved readability

    2. **eq** - Equality comparison for conditionals
       ```handlebars
       {{#if (eq status "active")}}Active user{{/if}}
       {{#if (eq @key "special")}}Special field{{/if}}
       ```

    3. **media** - GenKit dotprompt media directive for multimodal content
       ```handlebars
       {{media url=imageDataURI}}
       {{media url=this.image_url}}
       {{media url="https://example.com/image.jpg"}}
       {{media url="s3://endpoint/bucket/image.png"}}
       {{media url="file:///path/to/image.jpg"}}
       ```

       **Supported URL Schemes:**
       - `data:` - Base64 encoded data URIs (e.g., `data:image/jpeg;base64,...`)
       - `http://` / `https://` - Web URLs with automatic content type detection
       - `file://` - Local filesystem paths
       - `s3://` - S3-compatible storage (format: `s3://endpoint/bucket/key`)

       **Automatic Content Processing:**
       - **Images**: Downloaded, resized (if needed), converted to data URIs
       - **PDFs**: Text extracted or first page rendered as image
       - **HTML**: Readable text extracted using Mozilla Readability

       **Security Controls:**
       Downloads are protected by content security settings (see Configuration Reference):
       - Allowed host whitelist
       - Private IP blocking (prevents SSRF attacks)
       - Download size limits (default: 100MB)
       - Download timeouts (default: 30s)
       - Image dimension limits (default: 2048px, auto-resized)

       See: https://antfly.io/docs/configuration#security--cors

    4. **encodeToon** - Encode data in TOON format (Token-Oriented Object Notation)
       ```handlebars
       {{encodeToon this.fields}}
       {{encodeToon this.fields lengthMarker=false indent=4}}
       {{encodeToon this.fields delimiter="\t"}}
       ```

       **What is TOON?**
       TOON is a compact, human-readable format designed for passing structured data to LLMs.
       It provides **30-60% token reduction** compared to JSON while maintaining high LLM
       comprehension accuracy.

       **Key Features:**
       - Compact syntax using `:` for key-value pairs
       - Array length markers: `tags[#3]: ai,search,ml`
       - Tabular format for uniform data structures
       - Optimized for LLM parsing and understanding
       - Maintains human readability

       **Benefits:**
       - **Lower API costs** - Reduced token usage means lower LLM API costs
       - **Faster responses** - Less tokens to process
       - **More context** - Fit more documents within token limits

       **Options:**
       - `lengthMarker` (bool): Add # prefix to array counts like `[#3]` (default: true)
       - `indent` (int): Indentation spacing for nested objects (default: 2)
       - `delimiter` (string): Field separator for tabular arrays (default: none, use `"\t"` for tabs)

       **Example output:**
       ```
       title: Introduction to Vector Search
       author: Jane Doe
       tags[#3]: ai,search,ml
       metadata:
         edition: 2
         pages: 450
       ```

       **Default in RAG:** TOON is the default format for document rendering in RAG queries.

       **References:**
       - TOON Specification: https://github.com/toon-format/toon
       - Go Implementation: https://github.com/alpkeskin/gotoon

    **Template Examples:**

    Document with metadata:
    ```handlebars
    Title: {{metadata.title}}
    Date: {{metadata.date}}
    Tags: {{#each metadata.tags}}{{this}}, {{/each}}

    {{content}}
    ```

    HTML content extraction:
    ```handlebars
    Product: {{name}}
    Description: {{scrubHtml description_html}}
    Price: ${{price}}
    ```

    Multimodal with image:
    ```handlebars
    Product: {{title}}
    {{media url=image}}
    Description: {{description}}
    ```

    Conditional formatting:
    ```handlebars
    {{title}}
    {{#if author}}By: {{author}}{{/if}}
    {{#if (eq category "premium")}}⭐ Premium Content{{/if}}
    {{body}}
    ```

    **Environment Variables:**
    - `GEMINI_API_KEY` - API key for Google AI
    - `OPENAI_API_KEY` - API key for OpenAI
    - `OPENAI_BASE_URL` - Base URL for OpenAI-compatible APIs
    - `OLLAMA_HOST` - Ollama server URL (e.g., http://localhost:11434)

    **Importing Pre-computed Embeddings:**

    You can import existing embeddings (from OpenAI, Cohere, or any provider) by including
    them directly in your documents using the `_embeddings` field. This bypasses the
    embedding generation step and writes vectors directly to the index.

    **Steps:**
    1. Create the index first with the appropriate dimension
    2. Write documents with `_embeddings: { "<indexName>": [...<embedding>...] }`

    **Example:**
    ```json
    {
      "title": "My Document",
      "content": "Document text...",
      "_embeddings": {
        "my_vector_index": [0.1, 0.2, 0.3, ...]
      }
    }
    ```

    **Use Cases:**
    - Migrating from another vector database with existing embeddings
    - Using embeddings generated by external systems
    - Importing pre-computed OpenAI, Cohere, or other provider embeddings
    - Batch processing embeddings offline before ingestion

        Example:
            {'provider': 'openai', 'model': 'text-embedding-3-small'}

        Attributes:
            provider (EmbedderProvider): The embedding provider to use.
    """

    provider: EmbedderProvider
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = EmbedderProvider(d.pop("provider"))

        embedder_config = cls(
            provider=provider,
        )

        embedder_config.additional_properties = d
        return embedder_config

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
