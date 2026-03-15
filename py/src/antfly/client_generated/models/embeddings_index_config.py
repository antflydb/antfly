from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.distance_metric import DistanceMetric
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunker_config import ChunkerConfig
    from ..models.embedder_config import EmbedderConfig
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="EmbeddingsIndexConfig")


@_attrs_define
class EmbeddingsIndexConfig:
    r"""Unified configuration for embeddings indexes. When sparse is true, creates a sparse vector index (SPLADE inverted
    index). When sparse is false (default), creates a dense vector index (HNSW). For dense indexes, dimension can be
    omitted if an embedder is configured — it will be auto-detected.

        Attributes:
            sparse (Union[Unset, bool]): When true, creates a sparse (SPLADE) inverted index. When false (default), creates
                a dense (HNSW) vector index. Default: False.
            dimension (Union[Unset, int]): Vector dimension for dense indexes. Can be omitted when an embedder is configured
                (auto-detected via probe). Ignored for sparse indexes.
            field (Union[Unset, str]): Field to extract embeddings from
            template (Union[Unset, str]): Handlebars template for generating prompts. See https://handlebarsjs.com/guide/
                for more information. Example: Hello, {{#if (eq Name "John")}}Johnathan{{else}}{{Name}}{{/if}}! You are {{Age}}
                years old..
            distance_metric (Union[Unset, DistanceMetric]): Distance metric for the vector index (dense only). Use "cosine"
                for models trained with cosine similarity (e.g. CLIP, OpenAI). Use "inner_product" for models trained with dot
                product similarity. Use "l2_squared" (default) for models trained with Euclidean distance.
            mem_only (Union[Unset, bool]): Whether to use in-memory only storage (dense only)
            embedder (Union[Unset, EmbedderConfig]): A unified configuration for an embedding provider.

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
                - Batch processing embeddings offline before ingestion Example: {'provider': 'openai', 'model': 'text-
                embedding-3-small'}.
            summarizer (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.

                Generators can be configured with custom prompts using templates. Templates use
                Handlebars syntax and support various built-in helpers for formatting and data manipulation.

                **Template System:**
                - **Syntax**: Handlebars templating (https://handlebarsjs.com/guide/)
                - **Caching**: Templates are automatically cached with configurable TTL (default: 5 minutes)
                - **Context**: Templates receive the full context data passed to the generator

                **Built-in Helpers:**

                1. **scrubHtml** - Remove script/style tags and extract clean text from HTML
                   ```handlebars
                   {{scrubHtml html_content}}
                   ```
                   - Removes `<script>` and `<style>` tags
                   - Adds newlines after block elements (p, div, h1-h6, li, etc.)
                   - Returns plain text with preserved readability
                   - Useful for cleaning web content before summarization

                2. **eq** - Equality comparison for conditionals
                   ```handlebars
                   {{#if (eq status "active")}}Active{{/if}}
                   {{#if (eq @key "special")}}Special field{{/if}}
                   ```
                   - Use in `{{#if}}` blocks for conditional logic
                   - Compares any two values for equality

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

                RAG summarization with document references:
                ```handlebars
                Based on these documents, provide a comprehensive summary:

                {{#each documents}}
                Document {{this.id}}:
                {{scrubHtml this.content}}

                {{/each}}

                Valid document IDs: {{#each documents}}{{this.id}}{{#unless @last}}, {{/unless}}{{/each}}
                ```

                Conditional formatting:
                ```handlebars
                {{#if system_prompt}}System: {{system_prompt}}{{/if}}

                User Query: {{query}}

                {{#if context}}
                Context:
                {{#each context}}
                - {{this}}
                {{/each}}
                {{/if}}
                ```

                Multimodal prompt with images:
                ```handlebars
                Analyze this image:
                {{media url=image_url}}

                Focus on: {{focus_area}}
                ```

                Structured data encoding:
                ```handlebars
                User Profile:
                {{encodeToon user_data indent=2 lengthMarker=true}}

                Please analyze this profile.
                ```

                **Common Use Cases:**
                - **RAG (Retrieval-Augmented Generation)**: Format retrieved documents with citations
                - **Summarization**: Clean HTML content and structure summaries
                - **Query Classification**: Format queries with metadata for better classification
                - **Multimodal**: Include images/audio/video in prompts
                - **Data Formatting**: Convert structured data to readable text

                **Best Practices:**
                - Keep templates simple - complex logic belongs in application code
                - Use clear, descriptive field names in context
                - Handle missing fields gracefully (templates use "missingkey=zero" by default)
                - Test templates with representative data before production use Example: {'provider': 'openai', 'model':
                'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            chunker (Union[Unset, ChunkerConfig]): A unified configuration for a chunking provider. Example: {'provider':
                'termite', 'model': 'fixed', 'text': {'target_tokens': 500, 'overlap_tokens': 50}}.
            top_k (Union[Unset, int]): Default number of results to return from search (sparse only) Default: 10.
            min_weight (Union[Unset, float]): Minimum weight threshold for sparse vector entries (sparse only) Default: 0.0.
            chunk_size (Union[Unset, int]): Number of documents per posting list chunk (sparse only) Default: 1024.
    """

    sparse: Union[Unset, bool] = False
    dimension: Union[Unset, int] = UNSET
    field: Union[Unset, str] = UNSET
    template: Union[Unset, str] = UNSET
    distance_metric: Union[Unset, DistanceMetric] = UNSET
    mem_only: Union[Unset, bool] = UNSET
    embedder: Union[Unset, "EmbedderConfig"] = UNSET
    summarizer: Union[Unset, "GeneratorConfig"] = UNSET
    chunker: Union[Unset, "ChunkerConfig"] = UNSET
    top_k: Union[Unset, int] = 10
    min_weight: Union[Unset, float] = 0.0
    chunk_size: Union[Unset, int] = 1024
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        sparse = self.sparse

        dimension = self.dimension

        field = self.field

        template = self.template

        distance_metric: Union[Unset, str] = UNSET
        if not isinstance(self.distance_metric, Unset):
            distance_metric = self.distance_metric.value

        mem_only = self.mem_only

        embedder: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.embedder, Unset):
            embedder = self.embedder.to_dict()

        summarizer: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.summarizer, Unset):
            summarizer = self.summarizer.to_dict()

        chunker: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.chunker, Unset):
            chunker = self.chunker.to_dict()

        top_k = self.top_k

        min_weight = self.min_weight

        chunk_size = self.chunk_size

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if sparse is not UNSET:
            field_dict["sparse"] = sparse
        if dimension is not UNSET:
            field_dict["dimension"] = dimension
        if field is not UNSET:
            field_dict["field"] = field
        if template is not UNSET:
            field_dict["template"] = template
        if distance_metric is not UNSET:
            field_dict["distance_metric"] = distance_metric
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only
        if embedder is not UNSET:
            field_dict["embedder"] = embedder
        if summarizer is not UNSET:
            field_dict["summarizer"] = summarizer
        if chunker is not UNSET:
            field_dict["chunker"] = chunker
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if chunk_size is not UNSET:
            field_dict["chunk_size"] = chunk_size

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunker_config import ChunkerConfig
        from ..models.embedder_config import EmbedderConfig
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        sparse = d.pop("sparse", UNSET)

        dimension = d.pop("dimension", UNSET)

        field = d.pop("field", UNSET)

        template = d.pop("template", UNSET)

        _distance_metric = d.pop("distance_metric", UNSET)
        distance_metric: Union[Unset, DistanceMetric]
        if isinstance(_distance_metric, Unset):
            distance_metric = UNSET
        else:
            distance_metric = DistanceMetric(_distance_metric)

        mem_only = d.pop("mem_only", UNSET)

        _embedder = d.pop("embedder", UNSET)
        embedder: Union[Unset, EmbedderConfig]
        if isinstance(_embedder, Unset):
            embedder = UNSET
        else:
            embedder = EmbedderConfig.from_dict(_embedder)

        _summarizer = d.pop("summarizer", UNSET)
        summarizer: Union[Unset, GeneratorConfig]
        if isinstance(_summarizer, Unset):
            summarizer = UNSET
        else:
            summarizer = GeneratorConfig.from_dict(_summarizer)

        _chunker = d.pop("chunker", UNSET)
        chunker: Union[Unset, ChunkerConfig]
        if isinstance(_chunker, Unset):
            chunker = UNSET
        else:
            chunker = ChunkerConfig.from_dict(_chunker)

        top_k = d.pop("top_k", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        chunk_size = d.pop("chunk_size", UNSET)

        embeddings_index_config = cls(
            sparse=sparse,
            dimension=dimension,
            field=field,
            template=template,
            distance_metric=distance_metric,
            mem_only=mem_only,
            embedder=embedder,
            summarizer=summarizer,
            chunker=chunker,
            top_k=top_k,
            min_weight=min_weight,
            chunk_size=chunk_size,
        )

        embeddings_index_config.additional_properties = d
        return embeddings_index_config

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
